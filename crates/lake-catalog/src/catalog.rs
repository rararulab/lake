// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Catalog state and the DataFusion `CatalogProvider`.

use std::{
    any::Any,
    collections::BTreeMap,
    sync::{
        Arc, Mutex as StdMutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use arrow_flight::IpcMessage;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    catalog::{CatalogProvider, SchemaProvider},
    datasource::TableProvider,
};
use lake_common::{Namespace, TableLocation, TableName, TableRef};
use lake_engine::TableEngineRef;
use lake_meta::{MetaStoreRef, registry, registry::TableRegistration};
use moka::future::Cache;
use snafu::Snafu;
use tokio::{sync::Mutex, time::Instant};

use crate::schema::LakeSchema;

/// Bound how long a resolved registration can hide a registry version update.
const REGISTRATION_CACHE_TTL: Duration = Duration::from_secs(5);

/// One provider per catalog table at the target deployment scale.
const PROVIDER_CACHE_CAPACITY: u64 = 100_000;

/// Bound authority scans when DDL keeps moving the directory under refresh.
const DIRECTORY_REFRESH_ATTEMPTS: usize = 3;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RegistrationCacheKey {
    table: TableRef,
    epoch: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ProviderGeneration {
    table:          TableRef,
    location:       TableLocation,
    engine:         String,
    incarnation_id: Option<String>,
    version:        u64,
}

impl ProviderGeneration {
    fn new(table: &TableRef, registration: &TableRegistration) -> Self {
        Self {
            table:          table.clone(),
            location:       registration.location.clone(),
            engine:         registration.engine.clone(),
            incarnation_id: registration.incarnation_id().map(str::to_owned),
            version:        registration.current_version.0,
        }
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum ProviderLoadError {
    #[snafu(display("no table exists at {}", location.0))]
    Missing { location: TableLocation },
    #[snafu(transparent)]
    Engine { source: lake_engine::EngineError },
}

/// Bounded process-local observability for catalog revalidation.
#[derive(Clone, Debug)]
pub struct CatalogRefreshHealth {
    warmed:               bool,
    refreshing:           bool,
    last_success_age:     Option<Duration>,
    consecutive_failures: u64,
    last_failure_age:     Option<Duration>,
}

impl CatalogRefreshHealth {
    #[must_use]
    pub const fn warmed(&self) -> bool { self.warmed }

    #[must_use]
    pub const fn refreshing(&self) -> bool { self.refreshing }

    #[must_use]
    pub const fn last_success_age(&self) -> Option<Duration> { self.last_success_age }

    #[must_use]
    pub const fn consecutive_failures(&self) -> u64 { self.consecutive_failures }

    #[must_use]
    pub const fn last_failure_age(&self) -> Option<Duration> { self.last_failure_age }
}

/// Shared state behind the catalog: the metastore (registry authority), the
/// storage engine, a cached listing snapshot, and a per-table registration
/// cache.
pub struct CatalogState {
    pub(crate) meta:      MetaStoreRef,
    pub(crate) engine:    TableEngineRef,
    /// namespace -> table names. Read by DataFusion's sync listing methods,
    /// so it must never require I/O. Refreshed by [`LakeCatalog::refresh`].
    pub(crate) snapshot:  RwLock<Arc<CatalogGeneration>>,
    /// table -> registration; shields the registry from per-query load.
    regs:                 Cache<RegistrationCacheKey, Arc<TableRegistration>>,
    /// Local invalidation generations fence stale in-flight registration
    /// fills after a proxied write acknowledgement.
    registration_epochs:  RwLock<BTreeMap<TableRef, u64>>,
    /// immutable table generation -> provider; coalesces concurrent planning
    /// misses and avoids reopening storage metadata on every query.
    providers:            Cache<ProviderGeneration, Arc<dyn TableProvider>>,
    /// Serializes authority scans without blocking snapshot readers.
    refresh_lock:         Mutex<()>,
    /// Monotonic local observation of the rollout authority marker.
    directory_authority:  AtomicBool,
    /// Generation represented by the published snapshot.
    directory_generation: RwLock<Option<Vec<u8>>>,
    /// Last complete generation publication. A missing value means startup
    /// warm has not succeeded and callers must fail closed.
    refreshed_at:         RwLock<Option<Instant>>,
    /// Admission for request-triggered detached revalidation.
    refresh_in_flight:    AtomicBool,
    refresh_failures:     AtomicU64,
    last_refresh_failure: RwLock<Option<Instant>>,
    refresh_task:         StdMutex<Option<tokio::task::JoinHandle<()>>>,
    refresh_shutdown:     AtomicBool,
}

/// One immutable, atomically published catalog listing/schema generation.
#[derive(Default)]
pub struct CatalogGeneration {
    listings: BTreeMap<Namespace, Vec<TableName>>,
    schemas:  BTreeMap<TableRef, SchemaRef>,
}

impl CatalogGeneration {
    /// Borrow namespace/table listings from this pinned generation.
    #[must_use]
    pub const fn listings(&self) -> &BTreeMap<Namespace, Vec<TableName>> { &self.listings }

    /// Borrow one table schema from the same generation as [`Self::listings`].
    #[must_use]
    pub fn table_schema(&self, table: &TableRef) -> Option<&SchemaRef> { self.schemas.get(table) }
}

impl CatalogState {
    pub(crate) async fn registration(
        &self,
        table: &TableRef,
    ) -> lake_meta::Result<Option<Arc<TableRegistration>>> {
        let epoch = self
            .registration_epochs
            .read()
            .expect("registration epoch lock poisoned")
            .get(table)
            .copied()
            .unwrap_or_default();
        let key = RegistrationCacheKey {
            table: table.clone(),
            epoch,
        };
        if let Some(hit) = self.regs.get(&key).await {
            return Ok(Some(hit));
        }
        let Some(reg) = registry::get(self.meta.as_ref(), table).await? else {
            return Ok(None);
        };
        let reg = Arc::new(reg);
        self.regs.insert(key, reg.clone()).await;
        Ok(Some(reg))
    }

    pub(crate) async fn provider(
        &self,
        table: &TableRef,
        registration: &TableRegistration,
    ) -> Result<Arc<dyn TableProvider>, Arc<ProviderLoadError>> {
        let generation = ProviderGeneration::new(table, registration);
        let engine = self.engine.clone();
        let location = registration.location.clone();
        let version = registration.current_version;
        self.providers
            .try_get_with(generation, async move {
                let handle = engine
                    .open(&location)
                    .await
                    .map_err(|source| ProviderLoadError::Engine { source })?
                    .ok_or_else(|| ProviderLoadError::Missing {
                        location: location.clone(),
                    })?;
                handle
                    .table_provider(version)
                    .await
                    .map_err(|source| ProviderLoadError::Engine { source })
            })
            .await
    }
}

/// DataFusion catalog over the lake registry + storage engine.
#[derive(Clone)]
pub struct LakeCatalog {
    state: Arc<CatalogState>,
}

impl std::fmt::Debug for LakeCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeCatalog").finish_non_exhaustive()
    }
}

impl LakeCatalog {
    pub fn new(meta: MetaStoreRef, engine: TableEngineRef) -> Self {
        Self::with_provider_cache_capacity(meta, engine, PROVIDER_CACHE_CAPACITY)
    }

    fn with_provider_cache_capacity(
        meta: MetaStoreRef,
        engine: TableEngineRef,
        provider_cache_capacity: u64,
    ) -> Self {
        Self {
            state: Arc::new(CatalogState {
                meta,
                engine,
                snapshot: RwLock::new(Arc::new(CatalogGeneration::default())),
                regs: Cache::builder()
                    .max_capacity(100_000)
                    .time_to_live(REGISTRATION_CACHE_TTL)
                    .build(),
                registration_epochs: RwLock::new(BTreeMap::new()),
                providers: Cache::builder()
                    .max_capacity(provider_cache_capacity)
                    .build(),
                refresh_lock: Mutex::new(()),
                directory_authority: AtomicBool::new(false),
                directory_generation: RwLock::new(None),
                refreshed_at: RwLock::new(None),
                refresh_in_flight: AtomicBool::new(false),
                refresh_failures: AtomicU64::new(0),
                last_refresh_failure: RwLock::new(None),
                refresh_task: StdMutex::new(None),
                refresh_shutdown: AtomicBool::new(false),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_provider_cache_capacity(
        meta: MetaStoreRef,
        engine: TableEngineRef,
        capacity: u64,
    ) -> Self {
        Self::with_provider_cache_capacity(meta, engine, capacity)
    }

    #[cfg(test)]
    pub(crate) async fn maintain_test_provider_cache(&self) {
        self.state.providers.run_pending_tasks().await;
    }

    #[cfg(test)]
    pub(crate) fn test_provider_cache_entry_count(&self) -> u64 {
        self.state.providers.entry_count()
    }

    pub fn state(&self) -> Arc<CatalogState> { self.state.clone() }

    /// Pin the current immutable listing/schema generation in O(1).
    #[must_use]
    pub fn cached_generation(&self) -> Arc<CatalogGeneration> {
        self.state
            .snapshot
            .read()
            .expect("snapshot lock poisoned")
            .clone()
    }

    /// Snapshot bounded local refresh health without authority I/O.
    #[must_use]
    pub fn refresh_health(&self) -> CatalogRefreshHealth {
        let last_success = *self
            .state
            .refreshed_at
            .read()
            .expect("refresh timestamp lock poisoned");
        let last_failure = *self
            .state
            .last_refresh_failure
            .read()
            .expect("refresh failure lock poisoned");
        CatalogRefreshHealth {
            warmed:               last_success.is_some(),
            refreshing:           self.state.refresh_in_flight.load(Ordering::Acquire),
            last_success_age:     last_success.map(|instant| instant.elapsed()),
            consecutive_failures: self.state.refresh_failures.load(Ordering::Acquire),
            last_failure_age:     last_failure.map(|instant| instant.elapsed()),
        }
    }

    /// Evict one resolved registration after this query node proxies a
    /// successful write, so the same client connection observes its commit.
    pub async fn invalidate_registration(&self, table: &TableRef) {
        let old_epoch = {
            let mut epochs = self
                .state
                .registration_epochs
                .write()
                .expect("registration epoch lock poisoned");
            let epoch = epochs.entry(table.clone()).or_default();
            let old_epoch = *epoch;
            *epoch = epoch.checked_add(1).expect("registration epoch exhausted");
            old_epoch
        };
        self.state
            .regs
            .invalidate(&RegistrationCacheKey {
                table: table.clone(),
                epoch: old_epoch,
            })
            .await;
    }

    /// Reload the listing snapshot from the registry. Call on startup and on
    /// a timer; DataFusion's sync `schema_names`/`table_names` read what this
    /// leaves behind, so they never block on the metastore.
    pub async fn refresh(&self) -> lake_meta::Result<()> { self.refresh_inner(None).await }

    /// Reload the listing snapshot only when it is older than `max_age`.
    /// The first warm is synchronous and fail-closed. Once a last-good
    /// generation exists, stale callers trigger one detached revalidation and
    /// return immediately so metadata I/O never blocks SQL planning.
    pub async fn refresh_if_stale(&self, max_age: Duration) -> lake_meta::Result<()> {
        if self.state.refresh_shutdown.load(Ordering::Acquire) {
            return Ok(());
        }
        let refreshed_at = *self
            .state
            .refreshed_at
            .read()
            .expect("refresh timestamp lock poisoned");
        if refreshed_at.is_none() {
            return self.refresh_inner(Some(max_age)).await;
        }
        if refreshed_at.is_some_and(|loaded| loaded.elapsed() < max_age) {
            return Ok(());
        }
        let mut task = self
            .state
            .refresh_task
            .lock()
            .expect("refresh task lock poisoned");
        if self.state.refresh_shutdown.load(Ordering::Acquire) {
            return Ok(());
        }
        if self
            .state
            .refresh_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let catalog = self.clone();
            *task = Some(tokio::spawn(async move {
                let _guard = RefreshFlightGuard(catalog.state.clone());
                if let Err(error) = catalog.refresh_inner(Some(max_age)).await {
                    tracing::warn!(%error, "catalog revalidation failed; serving last-good");
                }
            }));
        }
        Ok(())
    }

    /// Abort and join request-triggered revalidation during replica shutdown.
    pub async fn shutdown_revalidation(&self) {
        let task = self.take_revalidation_for_shutdown();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        self.state.refresh_in_flight.store(false, Ordering::Release);
    }

    /// Synchronously fence and abort revalidation when an owning server future
    /// is dropped and cannot await graceful task joining.
    pub fn abort_revalidation(&self) {
        if let Some(task) = self.take_revalidation_for_shutdown() {
            task.abort();
        }
        self.state.refresh_in_flight.store(false, Ordering::Release);
    }

    fn take_revalidation_for_shutdown(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.state.refresh_shutdown.store(true, Ordering::Release);
        self.state
            .refresh_task
            .lock()
            .expect("refresh task lock poisoned")
            .take()
    }

    async fn refresh_inner(&self, max_age: Option<Duration>) -> lake_meta::Result<()> {
        let _refresh = self.state.refresh_lock.lock().await;
        let refreshed_at = *self
            .state
            .refreshed_at
            .read()
            .expect("refresh timestamp lock poisoned");
        if max_age.is_some_and(|age| refreshed_at.is_some_and(|loaded| loaded.elapsed() < age)) {
            return Ok(());
        }

        let mut generation = match self.observe_directory_generation().await {
            Ok(generation) => generation,
            Err(error) => {
                self.record_refresh_failure();
                return Err(error);
            }
        };
        if generation.as_deref()
            == self
                .state
                .directory_generation
                .read()
                .expect("directory generation lock poisoned")
                .as_deref()
            && refreshed_at.is_some()
            && generation.is_some()
        {
            self.record_refresh_success();
            return Ok(());
        }

        let registrations = 'attempts: {
            for attempt in 0..DIRECTORY_REFRESH_ATTEMPTS {
                let registrations = match registry::scan_tables(self.state.meta.as_ref()).await {
                    Ok(registrations) => registrations,
                    Err(error) => {
                        self.record_refresh_failure();
                        return Err(error);
                    }
                };
                if let Some(before) = generation.as_deref() {
                    let after = match registry::directory_generation(self.state.meta.as_ref()).await
                    {
                        Ok(after) => after,
                        Err(error) => {
                            self.record_refresh_failure();
                            return Err(error);
                        }
                    };
                    if after != before {
                        generation = Some(after);
                        if attempt + 1 == DIRECTORY_REFRESH_ATTEMPTS {
                            self.record_refresh_failure();
                            return Err(lake_meta::MetaError::DirectoryGenerationChanged);
                        }
                        continue;
                    }
                }
                break 'attempts registrations;
            }
            unreachable!("directory refresh attempts are non-zero")
        };
        let mut snapshot = CatalogGeneration::default();
        for (table, registration) in registrations {
            snapshot
                .listings
                .entry(table.namespace.clone())
                .or_default()
                .push(table.name.clone());
            if let Some(schema_ipc) = registration.schema_ipc()
                && let Ok(schema) = Schema::try_from(IpcMessage(schema_ipc.to_vec().into()))
            {
                snapshot.schemas.insert(table, Arc::new(schema));
            }
        }
        *self.state.snapshot.write().expect("snapshot lock poisoned") = Arc::new(snapshot);
        *self
            .state
            .directory_generation
            .write()
            .expect("directory generation lock poisoned") = generation;
        self.record_refresh_success();
        Ok(())
    }

    async fn observe_directory_generation(&self) -> lake_meta::Result<Option<Vec<u8>>> {
        if self.state.directory_authority.load(Ordering::Acquire) {
            return registry::directory_generation(self.state.meta.as_ref())
                .await
                .map(Some);
        }
        let directory = registry::directory_state(self.state.meta.as_ref()).await?;
        if !directory.authoritative() {
            return Ok(None);
        }
        self.state
            .directory_authority
            .store(true, Ordering::Release);
        Ok(Some(
            directory
                .generation()
                .expect("authoritative directory state has a generation")
                .to_vec(),
        ))
    }

    fn record_refresh_success(&self) {
        *self
            .state
            .refreshed_at
            .write()
            .expect("refresh timestamp lock poisoned") = Some(Instant::now());
        self.state.refresh_failures.store(0, Ordering::Release);
        *self
            .state
            .last_refresh_failure
            .write()
            .expect("refresh failure lock poisoned") = None;
    }

    fn record_refresh_failure(&self) {
        self.state.refresh_failures.fetch_add(1, Ordering::AcqRel);
        *self
            .state
            .last_refresh_failure
            .write()
            .expect("refresh failure lock poisoned") = Some(Instant::now());
    }
}

struct RefreshFlightGuard(Arc<CatalogState>);

impl Drop for RefreshFlightGuard {
    fn drop(&mut self) { self.0.refresh_in_flight.store(false, Ordering::Release); }
}

impl CatalogProvider for LakeCatalog {
    fn as_any(&self) -> &dyn Any { self }

    fn schema_names(&self) -> Vec<String> {
        self.cached_generation()
            .listings()
            .keys()
            .map(|ns| ns.0.clone())
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(LakeSchema::new(
            Namespace(name.to_string()),
            self.state.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool as StdAtomicBool, AtomicU8, AtomicUsize, Ordering};

    use arrow_flight::{IpcMessage, SchemaAsIpc};
    use async_trait::async_trait;
    use datafusion::arrow::{
        datatypes::{DataType, Field, Schema},
        ipc::writer::IpcWriteOptions,
    };
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaError, MetaStore, RocksMeta};
    use tokio::sync::Notify;

    use super::*;

    struct FailingGetMeta;

    struct CountingMeta {
        inner:              Arc<RocksMeta>,
        scans:              AtomicUsize,
        inject_during_scan: StdAtomicBool,
        injected:           AtomicUsize,
    }

    impl CountingMeta {
        fn new(inner: Arc<RocksMeta>) -> Self {
            Self {
                inner,
                scans: AtomicUsize::new(0),
                inject_during_scan: StdAtomicBool::new(false),
                injected: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl MetaStore for CountingMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            self.inner.get(key).await
        }

        async fn cas(
            &self,
            key: &str,
            expected: Option<&[u8]>,
            new: &[u8],
        ) -> lake_meta::Result<bool> {
            self.inner.cas(key, expected, new).await
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.inner.list_prefix(prefix).await
        }

        async fn scan_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<(String, Vec<u8>)>> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            let entries = self.inner.scan_prefix(prefix).await?;
            if self.inject_during_scan.load(Ordering::SeqCst) {
                let index = self.injected.fetch_add(1, Ordering::SeqCst);
                let table = TableRef::new("robots", format!("concurrent-{index}"));
                registry::register(
                    self.inner.as_ref(),
                    &table,
                    &TableRegistration::new(
                        TableLocation::new("mem://concurrent"),
                        "lance",
                        lake_common::Version(1),
                        Vec::new(),
                    ),
                )
                .await?;
            }
            Ok(entries)
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    const SCAN_READY: u8 = 0;
    const SCAN_PAUSED: u8 = 1;
    const SCAN_FAIL: u8 = 2;

    struct ControlledScanMeta {
        mode:    AtomicU8,
        scans:   AtomicUsize,
        entries: RwLock<Vec<(String, Vec<u8>)>>,
        entered: Notify,
        release: Notify,
    }

    impl ControlledScanMeta {
        fn new() -> Self {
            Self {
                mode:    AtomicU8::new(SCAN_READY),
                scans:   AtomicUsize::new(0),
                entries: RwLock::new(Vec::new()),
                entered: Notify::new(),
                release: Notify::new(),
            }
        }
    }

    #[async_trait]
    impl MetaStore for ControlledScanMeta {
        async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> { Ok(None) }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            unreachable!()
        }

        async fn list_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<String>> {
            unreachable!()
        }

        async fn scan_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<(String, Vec<u8>)>> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            if self.mode.load(Ordering::SeqCst) == SCAN_PAUSED {
                self.entered.notify_one();
                self.release.notified().await;
            }
            if self.mode.load(Ordering::SeqCst) == SCAN_FAIL {
                return Err(MetaError::Dynamo {
                    message: "injected scan failure".to_owned(),
                    source:  Box::new(std::io::Error::other("catalog unavailable")),
                });
            }
            Ok(self.entries.read().unwrap().clone())
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> {
            unreachable!()
        }
    }

    #[async_trait]
    impl MetaStore for FailingGetMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            Err(MetaError::Dynamo {
                message: "injected get failure".to_string(),
                source:  Box::new(std::io::Error::other(key.to_string())),
            })
        }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            unreachable!()
        }

        async fn list_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<String>> {
            unreachable!()
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn table_lookup_propagates_metastore_failure() {
        let meta: MetaStoreRef = Arc::new(FailingGetMeta);
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let catalog = LakeCatalog::new(meta, engine);
        let schema = catalog.schema("robots").unwrap();

        let err = schema.table("episodes").await.unwrap_err();
        assert!(
            err.to_string().contains("injected get failure"),
            "registry outage must not be reported as a missing table: {err}"
        );
    }

    #[tokio::test]
    async fn generation_point_read_failure_updates_refresh_health() {
        let meta: MetaStoreRef = Arc::new(FailingGetMeta);
        let catalog = LakeCatalog::new(meta, Arc::new(LanceEngine::new()));

        catalog.refresh().await.unwrap_err();

        let health = catalog.refresh_health();
        assert_eq!(health.consecutive_failures(), 1);
        assert!(health.last_failure_age().is_some());
        assert!(!health.warmed());
    }

    #[tokio::test]
    async fn catalog_refresh_caches_registration_schemas() {
        let root = tempfile::tempdir().unwrap();
        let meta = Arc::new(RocksMeta::open(root.path()).unwrap());
        let table = TableRef::new("robots", "episodes");
        let expected = Schema::new(vec![Field::new("episode_id", DataType::Utf8, false)]);
        let IpcMessage(schema_ipc) = SchemaAsIpc::new(&expected, &IpcWriteOptions::default())
            .try_into()
            .unwrap();
        registry::register(
            meta.as_ref(),
            &table,
            &TableRegistration::new(
                lake_common::TableLocation::new("mem://episodes"),
                "lance",
                lake_common::Version(1),
                schema_ipc.to_vec(),
            ),
        )
        .await
        .unwrap();
        let meta_ref: MetaStoreRef = meta;
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let catalog = LakeCatalog::new(meta_ref, engine);

        catalog.refresh().await.unwrap();
        let generation = catalog.cached_generation();

        assert_eq!(
            generation.table_schema(&table).map(AsRef::as_ref),
            Some(&expected)
        );
        assert_eq!(
            generation.listings(),
            &BTreeMap::from([(table.namespace.clone(), vec![table.name.clone()])])
        );
    }

    #[tokio::test]
    async fn catalog_generation_skips_unchanged_registry_scan() {
        let root = tempfile::tempdir().unwrap();
        let inner = Arc::new(RocksMeta::open(root.path()).unwrap());
        let table = TableRef::new("robots", "episodes");
        registry::register(
            inner.as_ref(),
            &table,
            &TableRegistration::new(
                TableLocation::new("mem://episodes"),
                "lance",
                lake_common::Version(1),
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        registry::finalize_directory_generation(inner.as_ref())
            .await
            .unwrap();
        let meta = Arc::new(CountingMeta::new(inner));
        let catalog = LakeCatalog::new(meta.clone(), Arc::new(LanceEngine::new()));

        catalog.refresh().await.unwrap();
        catalog.refresh().await.unwrap();

        assert_eq!(meta.scans.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn append_version_churn_does_not_invalidate_directory_generation() {
        let root = tempfile::tempdir().unwrap();
        let inner = Arc::new(RocksMeta::open(root.path()).unwrap());
        let table = TableRef::new("robots", "episodes");
        let mut registration = TableRegistration::new(
            TableLocation::new("mem://episodes"),
            "lance",
            lake_common::Version(1),
            Vec::new(),
        );
        registry::register(inner.as_ref(), &table, &registration)
            .await
            .unwrap();
        registry::finalize_directory_generation(inner.as_ref())
            .await
            .unwrap();
        let meta = Arc::new(CountingMeta::new(inner.clone()));
        let catalog = LakeCatalog::new(meta.clone(), Arc::new(LanceEngine::new()));
        catalog.refresh().await.unwrap();

        for version in 2..=4 {
            registry::set_version(
                inner.as_ref(),
                &table,
                &registration,
                lake_common::Version(version),
            )
            .await
            .unwrap();
            registration.current_version = lake_common::Version(version);
            catalog.refresh().await.unwrap();
        }

        assert_eq!(meta.scans.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn legacy_writer_mode_keeps_full_catalog_revalidation() {
        let root = tempfile::tempdir().unwrap();
        let inner = Arc::new(RocksMeta::open(root.path()).unwrap());
        let meta = Arc::new(CountingMeta::new(inner.clone()));
        let catalog = LakeCatalog::new(meta.clone(), Arc::new(LanceEngine::new()));
        catalog.refresh().await.unwrap();
        let table = TableRef::new("robots", "legacy");
        let registration = TableRegistration::new(
            TableLocation::new("mem://legacy"),
            "lance",
            lake_common::Version(1),
            Vec::new(),
        );
        assert!(
            inner
                .cas(
                    "tbl/robots/legacy",
                    None,
                    &serde_json::to_vec(&registration).unwrap(),
                )
                .await
                .unwrap()
        );

        catalog.refresh().await.unwrap();

        assert_eq!(meta.scans.load(Ordering::SeqCst), 2);
        assert_eq!(
            catalog.cached_generation().listings(),
            &BTreeMap::from([(table.namespace, vec![table.name])])
        );
    }

    #[tokio::test]
    async fn catalog_generation_change_during_scan_preserves_last_good() {
        let root = tempfile::tempdir().unwrap();
        let inner = Arc::new(RocksMeta::open(root.path()).unwrap());
        registry::finalize_directory_generation(inner.as_ref())
            .await
            .unwrap();
        let meta = Arc::new(CountingMeta::new(inner));
        let catalog = LakeCatalog::new(meta.clone(), Arc::new(LanceEngine::new()));
        catalog.refresh().await.unwrap();
        let last_good = catalog.cached_generation();
        meta.inject_during_scan.store(true, Ordering::SeqCst);
        // Force the next refresh down the scan path with a real directory move.
        registry::register(
            meta.inner.as_ref(),
            &TableRef::new("robots", "before-scan"),
            &TableRegistration::new(
                TableLocation::new("mem://before-scan"),
                "lance",
                lake_common::Version(1),
                Vec::new(),
            ),
        )
        .await
        .unwrap();

        catalog.refresh().await.unwrap_err();

        assert!(Arc::ptr_eq(&last_good, &catalog.cached_generation()));
    }

    #[tokio::test]
    async fn cached_generation_clones_only_the_arc() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path()).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let catalog = LakeCatalog::new(meta, engine);
        catalog.refresh().await.unwrap();

        let first = catalog.cached_generation();
        let second = catalog.cached_generation();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn pinned_generation_keeps_names_and_schemas_together() {
        let root = tempfile::tempdir().unwrap();
        let meta = Arc::new(RocksMeta::open(root.path()).unwrap());
        let old_table = TableRef::new("robots", "episodes");
        let old_schema = Schema::new(vec![Field::new("episode_id", DataType::Utf8, false)]);
        let IpcMessage(old_ipc) = SchemaAsIpc::new(&old_schema, &IpcWriteOptions::default())
            .try_into()
            .unwrap();
        let old_registration = TableRegistration::new(
            TableLocation::new("mem://episodes"),
            "lance",
            lake_common::Version(1),
            old_ipc.to_vec(),
        );
        registry::register(meta.as_ref(), &old_table, &old_registration)
            .await
            .unwrap();
        let meta_ref: MetaStoreRef = meta.clone();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let catalog = LakeCatalog::new(meta_ref, engine);
        catalog.refresh().await.unwrap();
        let pinned = catalog.cached_generation();

        registry::delete(meta.as_ref(), &old_table, &old_registration)
            .await
            .unwrap();
        let new_table = TableRef::new("robots", "runs");
        let new_schema = Schema::new(vec![Field::new("run_id", DataType::Int64, false)]);
        let IpcMessage(new_ipc) = SchemaAsIpc::new(&new_schema, &IpcWriteOptions::default())
            .try_into()
            .unwrap();
        registry::register(
            meta.as_ref(),
            &new_table,
            &TableRegistration::new(
                TableLocation::new("mem://runs"),
                "lance",
                lake_common::Version(1),
                new_ipc.to_vec(),
            ),
        )
        .await
        .unwrap();
        catalog.refresh().await.unwrap();
        let current = catalog.cached_generation();

        assert!(!Arc::ptr_eq(&pinned, &current));
        assert_eq!(
            pinned.listings(),
            &BTreeMap::from([(old_table.namespace.clone(), vec![old_table.name.clone()])])
        );
        assert_eq!(
            pinned.table_schema(&old_table).map(AsRef::as_ref),
            Some(&old_schema)
        );
        assert!(pinned.table_schema(&new_table).is_none());
        assert_eq!(
            current.listings(),
            &BTreeMap::from([(new_table.namespace.clone(), vec![new_table.name.clone()])])
        );
        assert_eq!(
            current.table_schema(&new_table).map(AsRef::as_ref),
            Some(&new_schema)
        );
        assert!(current.table_schema(&old_table).is_none());
    }

    #[tokio::test]
    async fn failed_refresh_preserves_generation_identity() {
        let meta = Arc::new(ControlledScanMeta::new());
        let meta_ref: MetaStoreRef = meta.clone();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let catalog = LakeCatalog::new(meta_ref, engine);
        catalog.refresh().await.unwrap();
        let last_good = catalog.cached_generation();
        meta.mode.store(SCAN_FAIL, Ordering::SeqCst);

        catalog.refresh().await.unwrap_err();

        assert!(Arc::ptr_eq(&last_good, &catalog.cached_generation()));
    }

    #[tokio::test]
    async fn stale_checks_return_while_one_refresh_runs() {
        let meta = Arc::new(ControlledScanMeta::new());
        let meta_ref: MetaStoreRef = meta.clone();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let catalog = LakeCatalog::new(meta_ref, engine);
        catalog.refresh().await.unwrap();
        meta.mode.store(SCAN_PAUSED, Ordering::SeqCst);

        tokio::time::timeout(
            Duration::from_millis(50),
            catalog.refresh_if_stale(Duration::ZERO),
        )
        .await
        .expect("a warmed stale check must not wait for registry I/O")
        .unwrap();
        meta.entered.notified().await;

        for _ in 0..16 {
            tokio::time::timeout(
                Duration::from_millis(50),
                catalog.refresh_if_stale(Duration::ZERO),
            )
            .await
            .expect("concurrent stale checks must use last-good")
            .unwrap();
        }
        assert_eq!(meta.scans.load(Ordering::SeqCst), 2);
        meta.release.notify_one();
    }

    #[tokio::test]
    async fn initial_refresh_waits_and_propagates_failure() {
        let meta = Arc::new(ControlledScanMeta::new());
        meta.mode.store(SCAN_FAIL, Ordering::SeqCst);
        let meta_ref: MetaStoreRef = meta.clone();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let catalog = LakeCatalog::new(meta_ref, engine);

        let error = catalog.refresh_if_stale(Duration::ZERO).await.unwrap_err();

        assert!(error.to_string().contains("injected scan failure"));
        assert_eq!(meta.scans.load(Ordering::SeqCst), 1);
        assert!(!catalog.refresh_health().warmed());
    }

    #[tokio::test]
    async fn failed_revalidation_preserves_last_good_snapshot() {
        let meta = Arc::new(ControlledScanMeta::new());
        let table = TableRef::new("robots", "episodes");
        let registration = TableRegistration::new(
            TableLocation::new("mem://episodes"),
            "lance",
            lake_common::Version(1),
            Vec::new(),
        );
        meta.entries.write().unwrap().push((
            "robots/episodes".to_owned(),
            serde_json::to_vec(&registration).unwrap(),
        ));
        let meta_ref: MetaStoreRef = meta.clone();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let catalog = LakeCatalog::new(meta_ref, engine);
        catalog.refresh().await.unwrap();
        let last_good = catalog.cached_generation();
        meta.mode.store(SCAN_FAIL, Ordering::SeqCst);

        catalog.refresh_if_stale(Duration::ZERO).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while catalog.refresh_health().consecutive_failures() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert!(Arc::ptr_eq(&catalog.cached_generation(), &last_good));
        let health = catalog.refresh_health();
        assert!(health.warmed());
        assert_eq!(health.consecutive_failures(), 1);
        assert!(health.last_failure_age().is_some());
        assert_eq!(
            last_good.listings().get(&table.namespace),
            Some(&vec![table.name])
        );
    }

    #[tokio::test]
    async fn successful_revalidation_publishes_recovered_generation() {
        let meta = Arc::new(ControlledScanMeta::new());
        let meta_ref: MetaStoreRef = meta.clone();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let catalog = LakeCatalog::new(meta_ref, engine);
        catalog.refresh().await.unwrap();
        meta.mode.store(SCAN_FAIL, Ordering::SeqCst);
        catalog.refresh_if_stale(Duration::ZERO).await.unwrap();
        while catalog.refresh_health().consecutive_failures() == 0 {
            tokio::task::yield_now().await;
        }
        let table = TableRef::new("robots", "recovered");
        let registration = TableRegistration::new(
            TableLocation::new("mem://recovered"),
            "lance",
            lake_common::Version(1),
            Vec::new(),
        );
        meta.entries.write().unwrap().push((
            "robots/recovered".to_owned(),
            serde_json::to_vec(&registration).unwrap(),
        ));
        meta.mode.store(SCAN_READY, Ordering::SeqCst);

        catalog.refresh_if_stale(Duration::ZERO).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while catalog.refresh_health().consecutive_failures() != 0
                || !catalog
                    .cached_generation()
                    .listings()
                    .contains_key(&table.namespace)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(
            catalog.cached_generation().listings(),
            &BTreeMap::from([(table.namespace, vec![table.name])])
        );
        assert!(catalog.refresh_health().last_failure_age().is_none());
    }

    #[tokio::test]
    async fn shutdown_aborts_inflight_revalidation() {
        let meta = Arc::new(ControlledScanMeta::new());
        let meta_ref: MetaStoreRef = meta.clone();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let catalog = LakeCatalog::new(meta_ref, engine);
        catalog.refresh().await.unwrap();
        meta.mode.store(SCAN_PAUSED, Ordering::SeqCst);
        catalog.refresh_if_stale(Duration::ZERO).await.unwrap();
        meta.entered.notified().await;
        assert!(catalog.refresh_health().refreshing());

        tokio::time::timeout(Duration::from_secs(1), catalog.shutdown_revalidation())
            .await
            .expect("shutdown must not wait for stuck authority I/O");

        assert!(!catalog.refresh_health().refreshing());
        assert_eq!(meta.scans.load(Ordering::SeqCst), 2);
        catalog.refresh_if_stale(Duration::ZERO).await.unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            meta.scans.load(Ordering::SeqCst),
            2,
            "a shut down catalog must not spawn new revalidation"
        );
    }
}
