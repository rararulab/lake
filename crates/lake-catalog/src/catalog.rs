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
    sync::{Arc, RwLock},
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

/// Shared state behind the catalog: the metastore (registry authority), the
/// storage engine, a cached listing snapshot, and a per-table registration
/// cache.
pub struct CatalogState {
    pub(crate) meta:     MetaStoreRef,
    pub(crate) engine:   TableEngineRef,
    /// namespace -> table names. Read by DataFusion's sync listing methods,
    /// so it must never require I/O. Refreshed by [`LakeCatalog::refresh`].
    pub(crate) snapshot: RwLock<CatalogSnapshot>,
    /// table -> registration; shields the registry from per-query load.
    regs:                Cache<RegistrationCacheKey, Arc<TableRegistration>>,
    /// Local invalidation generations fence stale in-flight registration
    /// fills after a proxied write acknowledgement.
    registration_epochs: RwLock<BTreeMap<TableRef, u64>>,
    /// immutable table generation -> provider; coalesces concurrent planning
    /// misses and avoids reopening storage metadata on every query.
    providers:           Cache<ProviderGeneration, Arc<dyn TableProvider>>,
    /// Serializes refreshes and records when the listing snapshot was loaded.
    refreshed_at:        Mutex<Option<Instant>>,
}

#[derive(Default)]
pub(crate) struct CatalogSnapshot {
    pub(crate) listings: BTreeMap<Namespace, Vec<TableName>>,
    pub(crate) schemas:  BTreeMap<TableRef, SchemaRef>,
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
                snapshot: RwLock::new(CatalogSnapshot::default()),
                regs: Cache::builder()
                    .max_capacity(100_000)
                    .time_to_live(REGISTRATION_CACHE_TTL)
                    .build(),
                registration_epochs: RwLock::new(BTreeMap::new()),
                providers: Cache::builder()
                    .max_capacity(provider_cache_capacity)
                    .build(),
                refreshed_at: Mutex::new(None),
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

    /// Clone the warmed listing snapshot without performing metadata I/O.
    #[must_use]
    pub fn cached_snapshot(&self) -> BTreeMap<Namespace, Vec<TableName>> {
        self.state
            .snapshot
            .read()
            .expect("snapshot lock poisoned")
            .listings
            .clone()
    }

    /// Return one schema from the same immutable generation as the listing.
    #[must_use]
    pub fn cached_table_schema(&self, table: &TableRef) -> Option<SchemaRef> {
        self.state
            .snapshot
            .read()
            .expect("snapshot lock poisoned")
            .schemas
            .get(table)
            .cloned()
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
    /// Concurrent callers coalesce behind one metastore scan.
    pub async fn refresh_if_stale(&self, max_age: Duration) -> lake_meta::Result<()> {
        self.refresh_inner(Some(max_age)).await
    }

    async fn refresh_inner(&self, max_age: Option<Duration>) -> lake_meta::Result<()> {
        let mut refreshed_at = self.state.refreshed_at.lock().await;
        if max_age.is_some_and(|age| refreshed_at.is_some_and(|loaded| loaded.elapsed() < age)) {
            return Ok(());
        }

        let registrations = registry::scan_tables(self.state.meta.as_ref()).await?;
        let mut snapshot = CatalogSnapshot::default();
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
        *self.state.snapshot.write().expect("snapshot lock poisoned") = snapshot;
        *refreshed_at = Some(Instant::now());
        Ok(())
    }
}

impl CatalogProvider for LakeCatalog {
    fn as_any(&self) -> &dyn Any { self }

    fn schema_names(&self) -> Vec<String> {
        self.state
            .snapshot
            .read()
            .expect("snapshot lock poisoned")
            .listings
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
    use arrow_flight::{IpcMessage, SchemaAsIpc};
    use async_trait::async_trait;
    use datafusion::arrow::{
        datatypes::{DataType, Field, Schema},
        ipc::writer::IpcWriteOptions,
    };
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaError, MetaStore, RocksMeta};

    use super::*;

    struct FailingGetMeta;

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

        assert_eq!(
            catalog.cached_table_schema(&table).as_deref(),
            Some(&expected)
        );
        assert_eq!(
            catalog.cached_snapshot(),
            BTreeMap::from([(table.namespace.clone(), vec![table.name.clone()])])
        );
    }
}
