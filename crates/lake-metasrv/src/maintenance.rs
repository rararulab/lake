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

//! Leader-only background maintenance: periodic GC / compaction.
//!
//! The lease holder is the single node allowed to mutate stored tables, so it
//! is also the natural place to run housekeeping. [`run_maintenance_loop`]
//! wakes on a fixed interval and, while this node holds leadership, sweeps
//! every registered table through the engine's
//! [`maintain`](lake_engine::TableEngine::maintain) (compact fragments and
//! reclaim old versions). The sweep is best-effort: a single table's failure
//! is logged and the sweep moves on, so one bad table never stalls the rest.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use lake_common::TableRef;
use lake_meta::{MetaError, registry};
use tokio_util::sync::CancellationToken;

use crate::{
    MaintenanceLimits, Metasrv, MetasrvError,
    leadership::Leadership,
    operation::{AppendRecord, AppendState, OPERATION_PREFIX, active_key},
    telemetry,
};

/// Drive periodic maintenance forever, running a sweep only while `is_leader`.
///
/// Sleeps for the configured interval between rounds. A round is skipped
/// entirely unless this node currently holds leadership, so standbys stay idle
/// and only the leader does housekeeping.
pub(crate) async fn run_maintenance_loop(metasrv: Arc<Metasrv>, leadership: Arc<Leadership>) {
    run_maintenance_loop_until(
        metasrv,
        leadership,
        CancellationToken::new(),
        MaintenanceLimits::default(),
    )
    .await;
}

/// Drive maintenance until shutdown without starting another sweep afterward.
pub(crate) async fn run_maintenance_loop_until(
    metasrv: Arc<Metasrv>,
    leadership: Arc<Leadership>,
    shutdown: CancellationToken,
    limits: MaintenanceLimits,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(limits.interval()) => {}
        }
        if !leadership.is_leader() {
            continue;
        }
        sweep_until_with_limits(&metasrv, &shutdown, limits).await;
    }
}

/// Run one maintenance sweep over every registered table.
///
/// Each step degrades gracefully: a failed listing logs and moves on, and a
/// per-table `maintain` error is logged and skipped so the sweep continues.
pub(crate) async fn sweep(metasrv: &Metasrv) {
    sweep_until(metasrv, &CancellationToken::new()).await;
}

async fn sweep_until(metasrv: &Metasrv, shutdown: &CancellationToken) {
    let limits = MaintenanceLimits::default();
    sweep_until_with_limits(metasrv, shutdown, limits).await;
}

async fn sweep_until_with_limits(
    metasrv: &Metasrv,
    shutdown: &CancellationToken,
    limits: MaintenanceLimits,
) {
    if shutdown.is_cancelled() {
        return;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_secs();
    let drop_gc = sweep_drop_tombstones_until(metasrv, shutdown).await;
    tracing::debug!(
        scanned = drop_gc.scanned,
        completed = drop_gc.completed,
        "drop tombstone maintenance page complete"
    );
    telemetry::maintenance_pages("drop_tombstones", 1);
    telemetry::maintenance_items("drop_tombstones", "scanned", drop_gc.scanned);
    telemetry::maintenance_items("drop_tombstones", "completed", drop_gc.completed);
    if shutdown.is_cancelled() {
        return;
    }
    let operation_gc = sweep_operation_pages_at_until(
        metasrv,
        now,
        shutdown,
        limits.operation_gc_max_pages(),
        limits.operation_gc_max_duration(),
    )
    .await;
    tracing::debug!(
        pages = operation_gc.pages,
        scanned = operation_gc.scanned,
        deleted = operation_gc.deleted,
        budget_exhausted = operation_gc.budget_exhausted,
        time_exhausted = operation_gc.time_exhausted,
        "append operation maintenance page complete"
    );
    telemetry::maintenance_pages("append_operations", operation_gc.pages);
    telemetry::maintenance_items("append_operations", "scanned", operation_gc.scanned);
    telemetry::maintenance_items("append_operations", "deleted", operation_gc.deleted);
    telemetry::maintenance_items(
        "append_operations",
        "budget_exhausted",
        usize::from(operation_gc.budget_exhausted),
    );
    telemetry::maintenance_items(
        "append_operations",
        "time_exhausted",
        usize::from(operation_gc.time_exhausted),
    );
    if shutdown.is_cancelled() {
        return;
    }
    let tables = sweep_table_page(metasrv, shutdown, limits.table_page_size()).await;
    tracing::debug!(
        scanned = tables.scanned,
        attempted = tables.attempted,
        maintained = tables.maintained,
        skipped = tables.skipped,
        failed = tables.failed,
        "table maintenance page complete"
    );
    telemetry::maintenance_pages("tables", 1);
    telemetry::maintenance_items("tables", "scanned", tables.scanned);
    telemetry::maintenance_items("tables", "attempted", tables.attempted);
    telemetry::maintenance_items("tables", "maintained", tables.maintained);
    telemetry::maintenance_items("tables", "skipped", tables.skipped);
    telemetry::maintenance_items("tables", "failed", tables.failed);
}

#[derive(Clone, Copy, Debug, Default)]
struct TableMaintenanceStats {
    scanned:    usize,
    attempted:  usize,
    maintained: usize,
    skipped:    usize,
    failed:     usize,
}

async fn sweep_table_page(
    metasrv: &Metasrv,
    shutdown: &CancellationToken,
    page_size: usize,
) -> TableMaintenanceStats {
    let cursor = metasrv.inner.table_maintenance_cursor.lock().await.clone();
    let page =
        match registry::scan_tables_page(metasrv.meta().as_ref(), cursor.as_deref(), page_size)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                tracing::warn!(%error, "table maintenance registry page scan failed");
                return TableMaintenanceStats::default();
            }
        };
    let (tables, continuation) = page.into_parts();
    *metasrv.inner.table_maintenance_cursor.lock().await = continuation;
    let mut stats = TableMaintenanceStats {
        scanned: tables.len(),
        ..TableMaintenanceStats::default()
    };
    for (table, _scanned_registration) in tables {
        if shutdown.is_cancelled() {
            break;
        }
        let _guard = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            guard = metasrv.lock_table(&table) => guard,
        };
        if shutdown.is_cancelled() {
            break;
        }
        let registration = match metasrv.resolve(&table).await {
            Ok(Some(registration)) => registration,
            Ok(None) => {
                stats.skipped += 1;
                continue;
            }
            Err(error) => {
                stats.failed += 1;
                tracing::warn!(%table, %error, "table maintenance resolve failed");
                continue;
            }
        };
        let tombstoned =
            match crate::drop_tombstone::DropTombstone::new(table.clone(), registration.clone()) {
                Ok(tombstone) => {
                    crate::drop_tombstone::exists(metasrv.meta().as_ref(), &tombstone).await
                }
                Err(_) => Ok(false),
            };
        match tombstoned {
            Ok(true) => {
                stats.skipped += 1;
                tracing::debug!(%table, "maintenance skipped tombstoned table");
                continue;
            }
            Ok(false) => {}
            Err(error) => {
                stats.failed += 1;
                tracing::warn!(%table, %error, "maintenance could not inspect drop tombstone");
                continue;
            }
        }
        stats.attempted += 1;
        match metasrv
            .engine()
            .maintain(&registration.location, registration.current_version)
            .await
        {
            Ok(Some(version)) => {
                match registry::set_version(metasrv.meta().as_ref(), &table, &registration, version)
                    .await
                {
                    Ok(()) => {
                        stats.maintained += 1;
                        tracing::debug!(%table, %version, "maintained table");
                    }
                    Err(MetaError::Conflict { .. }) => {
                        stats.skipped += 1;
                        tracing::debug!(%table, %version, "maintenance result lost registry CAS");
                    }
                    Err(error) => {
                        stats.failed += 1;
                        tracing::warn!(%table, %error, "publishing maintenance failed");
                    }
                }
            }
            Ok(None) => {
                stats.skipped += 1;
                tracing::debug!(%table, "table needs no maintenance");
            }
            Err(error) => {
                stats.failed += 1;
                tracing::warn!(%table, %error, "maintenance failed for table");
            }
        }
    }
    stats
}

#[derive(Clone, Copy, Debug, Default)]
struct DropGcStats {
    scanned:   usize,
    completed: usize,
}

async fn sweep_drop_tombstones(metasrv: &Metasrv) -> DropGcStats {
    sweep_drop_tombstones_until(metasrv, &CancellationToken::new()).await
}

async fn sweep_drop_tombstones_until(
    metasrv: &Metasrv,
    shutdown: &CancellationToken,
) -> DropGcStats {
    let cursor = metasrv.inner.drop_gc_cursor.lock().await.clone();
    let (tombstones, continuation) = match crate::drop_tombstone::scan_page(
        metasrv.meta().as_ref(),
        cursor.as_deref(),
        metasrv.inner.operation_gc_page_size,
    )
    .await
    {
        Ok(page) => page,
        Err(error) => {
            tracing::warn!(%error, "drop tombstone maintenance scan failed");
            return DropGcStats::default();
        }
    };
    *metasrv.inner.drop_gc_cursor.lock().await = continuation;
    let mut stats = DropGcStats {
        scanned:   tombstones.len(),
        completed: 0,
    };
    for tombstone in tombstones {
        if shutdown.is_cancelled() {
            break;
        }
        let _guard = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            guard = metasrv.lock_table(&tombstone.table) => guard,
        };
        if shutdown.is_cancelled() {
            break;
        }
        match metasrv.cleanup_drop_locked(&tombstone).await {
            Ok(()) => stats.completed += 1,
            Err(error) => tracing::warn!(
                table = %tombstone.table,
                error = %error,
                "drop tombstone maintenance failed"
            ),
        }
    }
    stats
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OperationGcStats {
    pub(crate) pages:            usize,
    pub(crate) scanned:          usize,
    pub(crate) deleted:          usize,
    pub(crate) budget_exhausted: bool,
    pub(crate) time_exhausted:   bool,
}

pub(crate) async fn sweep_operations_at(metasrv: &Metasrv, now: u64) -> OperationGcStats {
    sweep_operations_at_until(metasrv, now, &CancellationToken::new()).await
}

async fn sweep_operations_at_until(
    metasrv: &Metasrv,
    now: u64,
    shutdown: &CancellationToken,
) -> OperationGcStats {
    sweep_operation_pages_at_until(
        metasrv,
        now,
        shutdown,
        1,
        MaintenanceLimits::default().operation_gc_max_duration(),
    )
    .await
}

async fn sweep_operation_pages_at_until(
    metasrv: &Metasrv,
    now: u64,
    shutdown: &CancellationToken,
    max_pages: usize,
    max_duration: std::time::Duration,
) -> OperationGcStats {
    let mut stats = OperationGcStats::default();
    let deadline = tokio::time::Instant::now() + max_duration;
    for _ in 0..max_pages {
        if shutdown.is_cancelled() {
            break;
        }
        let cursor = metasrv.inner.operation_gc_cursor.lock().await.clone();
        let page = match tokio::select! {
            biased;
            () = shutdown.cancelled() => return stats,
            () = tokio::time::sleep_until(deadline) => {
                stats.time_exhausted = true;
                return stats;
            }
            result = metasrv.meta().scan_prefix_page(
                OPERATION_PREFIX,
                cursor.as_deref(),
                metasrv.inner.operation_gc_page_size,
            ) => result,
        } {
            Ok(page) => page,
            Err(error) => {
                tracing::warn!(%error, "append operation GC page scan failed");
                return stats;
            }
        };
        let (entries, continuation) = page.into_parts();
        let has_more = continuation.is_some();
        stats.pages += 1;
        stats.scanned += entries.len();
        for (stripped, bytes) in entries {
            if shutdown.is_cancelled() {
                return stats;
            }
            let key = format!("{OPERATION_PREFIX}{stripped}");
            let record = match AppendRecord::decode(&stripped, &bytes) {
                Ok(record) => record,
                Err(error) => {
                    tracing::warn!(%error, key, "append operation GC found corrupt state");
                    continue;
                }
            };
            if now.saturating_sub(record.updated_at) <= metasrv.inner.operation_retention.as_secs()
            {
                continue;
            }
            let reconciliation = tokio::select! {
                biased;
                () = shutdown.cancelled() => return stats,
                () = tokio::time::sleep_until(deadline) => {
                    stats.time_exhausted = true;
                    return stats;
                }
                result = reconcile_and_delete_expired(metasrv, &key, &bytes, record, shutdown) => result,
            };
            match reconciliation {
                Ok(true) => stats.deleted += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%error, key, "append operation GC reconciliation failed");
                }
            }
        }
        *metasrv.inner.operation_gc_cursor.lock().await = continuation;
        if !has_more {
            return stats;
        }
    }
    stats.budget_exhausted = !shutdown.is_cancelled();
    stats
}

async fn reconcile_and_delete_expired(
    metasrv: &Metasrv,
    key: &str,
    encoded: &[u8],
    record: AppendRecord,
    shutdown: &CancellationToken,
) -> crate::Result<bool> {
    let (table, operation) = record.identity()?;
    let _guard = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Ok(false),
        guard = metasrv.lock_table(&table) => guard,
    };
    if shutdown.is_cancelled() {
        return Ok(false);
    }
    if record.state != AppendState::Committed {
        let Some(registration) = metasrv.resolve(&table).await? else {
            return delete_operation_record(
                metasrv,
                key,
                encoded,
                &table,
                &operation,
                &record.table_incarnation,
            )
            .await;
        };
        if registration.incarnation_id() != Some(record.table_incarnation.as_str()) {
            // The expired operation cannot legally target this replacement,
            // and the server rejects its UUID before record creation. Removing
            // its exact record/fence is therefore both safe and leak-free.
            return delete_operation_record(
                metasrv,
                key,
                encoded,
                &table,
                &operation,
                &record.table_incarnation,
            )
            .await;
        }
        let handle = metasrv
            .engine()
            .open(&registration.location)
            .await
            .map_err(|source| MetasrvError::Engine { source })?
            .ok_or_else(|| MetasrvError::NotFound {
                table: table.to_string(),
            })?;
        let result_version = match record.state {
            AppendState::Reserved => match handle
                .reconcile_append(&operation)
                .await
                .map_err(|source| MetasrvError::Engine { source })?
            {
                Some(version) => version,
                None if registration.current_version == record.base_version => {
                    return delete_operation_record(
                        metasrv,
                        key,
                        encoded,
                        &table,
                        &operation,
                        &record.table_incarnation,
                    )
                    .await;
                }
                None => {
                    return Err(MetasrvError::OperationRecoveryConflict {
                        operation_id: record.operation_id,
                    });
                }
            },
            AppendState::EngineCommitted => {
                record
                    .result_version
                    .ok_or_else(|| MetasrvError::CorruptOperationState {
                        operation_id: record.operation_id.clone(),
                    })?
            }
            AppendState::Committed => unreachable!(),
        };
        if record
            .result_version
            .is_some_and(|version| version != result_version)
        {
            return Err(MetasrvError::OperationRecoveryConflict {
                operation_id: record.operation_id,
            });
        }
        if registration.current_version == record.base_version {
            registry::set_version(
                metasrv.meta().as_ref(),
                &table,
                &registration,
                result_version,
            )
            .await
            .map_err(|source| MetasrvError::Registry { source })?;
        } else if registration.current_version != result_version {
            return Err(MetasrvError::OperationRecoveryConflict {
                operation_id: record.operation_id,
            });
        }
    }
    delete_operation_record(
        metasrv,
        key,
        encoded,
        &table,
        &operation,
        &record.table_incarnation,
    )
    .await
}

async fn delete_operation_record(
    metasrv: &Metasrv,
    key: &str,
    encoded: &[u8],
    table: &TableRef,
    operation: &lake_common::AppendOperation,
    table_incarnation: &str,
) -> crate::Result<bool> {
    // The operation record is the durable lifetime fence for engine-private
    // staging. Reclaim the exact stage before deleting that record so a crash
    // retries cleanup instead of orphaning invisible objects forever.
    if let Some(registration) = metasrv.resolve(table).await?
        && registration.incarnation_id() == Some(table_incarnation)
        && let Some(handle) = metasrv
            .engine()
            .open(&registration.location)
            .await
            .map_err(|source| MetasrvError::Engine { source })?
    {
        handle
            .expire_append(operation)
            .await
            .map_err(|source| MetasrvError::Engine { source })?;
    }
    let active = active_key(operation, table);
    let _ = metasrv
        .meta()
        .delete(&active, key.as_bytes())
        .await
        .map_err(|source| MetasrvError::Registry { source })?;
    metasrv
        .meta()
        .delete(key, encoded)
        .await
        .map_err(|source| MetasrvError::Registry { source })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            array::{Int64Array, RecordBatch},
            datatypes::{DataType, Field, Schema, SchemaRef},
        },
        error::DataFusionError,
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use lake_common::{
        AppendOperation, AppendOperationId, AppendPayloadDigest, TableLocation, TableRef, TenantId,
        Version,
    };
    use lake_engine::{
        ObjectReferencePage, ObjectReferenceRequest, Result as EngineResult, TableEngine,
        TableEngineRef, TableHandleRef,
    };
    use lake_engine_lance::LanceEngine;
    use lake_meta::{
        MetaScanPage, MetaStore, MetaStoreRef, RocksMeta, registry::TableRegistration,
    };

    use super::*;
    use crate::operation::operation_key;

    struct PausedMaintenanceEngine {
        calls:   AtomicUsize,
        started: Arc<tokio::sync::Notify>,
        resume:  Arc<tokio::sync::Notify>,
    }

    struct PausedRemoveEngine {
        calls:   AtomicUsize,
        started: Arc<tokio::sync::Notify>,
        resume:  Arc<tokio::sync::Notify>,
    }

    struct CountingMaintenanceEngine {
        calls:     AtomicUsize,
        locations: StdMutex<Vec<TableLocation>>,
    }

    struct RecordingScanMeta {
        inner:        MetaStoreRef,
        list_calls:   AtomicUsize,
        page_calls:   AtomicUsize,
        get_calls:    AtomicUsize,
        page_scanned: Option<Arc<tokio::sync::Notify>>,
    }

    struct CancelAfterPageMeta {
        inner:      MetaStoreRef,
        page_calls: AtomicUsize,
        shutdown:   CancellationToken,
    }

    struct BlockingGetMeta {
        inner: MetaStoreRef,
    }

    #[async_trait]
    impl MetaStore for RecordingScanMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
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
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list_prefix(prefix).await
        }

        async fn scan_prefix_page(
            &self,
            prefix: &str,
            continuation: Option<&str>,
            limit: usize,
        ) -> lake_meta::Result<MetaScanPage> {
            self.page_calls.fetch_add(1, Ordering::SeqCst);
            let page = self
                .inner
                .scan_prefix_page(prefix, continuation, limit)
                .await?;
            if let Some(scanned) = &self.page_scanned {
                scanned.notify_one();
            }
            Ok(page)
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    #[async_trait]
    impl MetaStore for CancelAfterPageMeta {
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

        async fn scan_prefix_page(
            &self,
            prefix: &str,
            continuation: Option<&str>,
            limit: usize,
        ) -> lake_meta::Result<MetaScanPage> {
            let page = self
                .inner
                .scan_prefix_page(prefix, continuation, limit)
                .await?;
            if self.page_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.shutdown.cancel();
            }
            Ok(page)
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    #[async_trait]
    impl MetaStore for BlockingGetMeta {
        async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            std::future::pending().await
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

        async fn scan_prefix_page(
            &self,
            prefix: &str,
            continuation: Option<&str>,
            limit: usize,
        ) -> lake_meta::Result<MetaScanPage> {
            self.inner
                .scan_prefix_page(prefix, continuation, limit)
                .await
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    async fn seed_committed_operations(meta: &dyn MetaStore, count: usize) {
        let tenant = TenantId::try_new("tenant-a").unwrap();
        let table = TableRef::new("robots", "episodes");
        for index in 0..count {
            let operation = AppendOperation::builder()
                .tenant(tenant.clone())
                .operation_id(
                    AppendOperationId::parse(format!("0197f0f4-7b2a-7000-8000-{index:012x}"))
                        .unwrap(),
                )
                .payload_digest(
                    AppendPayloadDigest::parse(
                        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    )
                    .unwrap(),
                )
                .build();
            let mut record =
                AppendRecord::reserved(&operation, &table, "incarnation-a", Version(1), 1);
            record.state = AppendState::Committed;
            record.result_version = Some(Version(2));
            let key = operation_key(&operation, &table);
            assert!(
                meta.cas(&key, None, &record.encode().unwrap())
                    .await
                    .unwrap()
            );
        }
    }

    #[async_trait]
    impl TableEngine for PausedMaintenanceEngine {
        fn kind(&self) -> &'static str { "test" }

        async fn create(
            &self,
            _location: &TableLocation,
            _schema: SchemaRef,
        ) -> EngineResult<TableHandleRef> {
            panic!("create is not used by maintenance boundary test")
        }

        async fn open(&self, _location: &TableLocation) -> EngineResult<Option<TableHandleRef>> {
            panic!("open is not used by maintenance boundary test")
        }

        async fn remove(&self, _location: &TableLocation) -> EngineResult<()> {
            panic!("remove is not used by maintenance boundary test")
        }

        async fn maintain(
            &self,
            _location: &TableLocation,
            _version: Version,
        ) -> EngineResult<Option<Version>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 1 {
                self.started.notify_one();
                self.resume.notified().await;
            }
            Ok(None)
        }

        async fn retained_object_references(
            &self,
            _location: &TableLocation,
            _request: ObjectReferenceRequest,
        ) -> EngineResult<ObjectReferencePage> {
            panic!("reference enumeration is not used by maintenance boundary test")
        }
    }

    #[async_trait]
    impl TableEngine for PausedRemoveEngine {
        fn kind(&self) -> &'static str { "test" }

        async fn create(
            &self,
            _location: &TableLocation,
            _schema: SchemaRef,
        ) -> EngineResult<TableHandleRef> {
            panic!("create is not used by drop GC boundary test")
        }

        async fn open(&self, _location: &TableLocation) -> EngineResult<Option<TableHandleRef>> {
            panic!("open is not used by drop GC boundary test")
        }

        async fn remove(&self, _location: &TableLocation) -> EngineResult<()> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 1 {
                self.started.notify_one();
                self.resume.notified().await;
            }
            Ok(())
        }

        async fn maintain(
            &self,
            _location: &TableLocation,
            _version: Version,
        ) -> EngineResult<Option<Version>> {
            panic!("maintain is not used by drop GC boundary test")
        }

        async fn retained_object_references(
            &self,
            _location: &TableLocation,
            _request: ObjectReferenceRequest,
        ) -> EngineResult<ObjectReferencePage> {
            panic!("reference enumeration is not used by drop GC boundary test")
        }
    }

    #[async_trait]
    impl TableEngine for CountingMaintenanceEngine {
        fn kind(&self) -> &'static str { "test" }

        async fn create(
            &self,
            _location: &TableLocation,
            _schema: SchemaRef,
        ) -> EngineResult<TableHandleRef> {
            panic!("create is not used by paged maintenance test")
        }

        async fn open(&self, _location: &TableLocation) -> EngineResult<Option<TableHandleRef>> {
            panic!("open is not used by paged maintenance test")
        }

        async fn remove(&self, _location: &TableLocation) -> EngineResult<()> {
            panic!("remove is not used by paged maintenance test")
        }

        async fn maintain(
            &self,
            location: &TableLocation,
            _version: Version,
        ) -> EngineResult<Option<Version>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.locations.lock().unwrap().push(location.clone());
            Ok(None)
        }

        async fn retained_object_references(
            &self,
            _location: &TableLocation,
            _request: ObjectReferenceRequest,
        ) -> EngineResult<ObjectReferencePage> {
            panic!("reference enumeration is not used by paged maintenance test")
        }
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    fn operation() -> AppendOperation {
        AppendOperation::builder()
            .tenant(TenantId::try_new("tenant-a").unwrap())
            .operation_id(AppendOperationId::generate())
            .payload_digest(
                AppendPayloadDigest::parse(
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                )
                .unwrap(),
            )
            .build()
    }

    #[tokio::test]
    async fn maintenance_shutdown_stops_before_next_table() {
        let meta_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        for name in ["first", "second"] {
            let table = TableRef::new("robots", name);
            let registration = TableRegistration::new(
                TableLocation::new(format!("mem://{name}")),
                "test",
                Version(1),
                vec![1],
            );
            lake_meta::registry::register(meta.as_ref(), &table, &registration)
                .await
                .unwrap();
        }
        let started = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let engine = Arc::new(PausedMaintenanceEngine {
            calls:   AtomicUsize::new(0),
            started: started.clone(),
            resume:  resume.clone(),
        });
        let metasrv = Arc::new(Metasrv::new(meta, engine.clone()));
        let shutdown = CancellationToken::new();
        let sweep = tokio::spawn({
            let metasrv = metasrv.clone();
            let shutdown = shutdown.clone();
            async move { sweep_until(&metasrv, &shutdown).await }
        });

        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("first table maintenance starts");
        shutdown.cancel();
        resume.notify_one();
        tokio::time::timeout(Duration::from_secs(1), sweep)
            .await
            .expect("cancelled sweep stops")
            .unwrap();

        assert_eq!(engine.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn table_maintenance_pages_resume_without_full_registry_sweep() {
        let meta_dir = tempfile::tempdir().unwrap();
        let inner: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let recording = Arc::new(RecordingScanMeta {
            inner,
            list_calls: AtomicUsize::new(0),
            page_calls: AtomicUsize::new(0),
            get_calls: AtomicUsize::new(0),
            page_scanned: None,
        });
        let meta: MetaStoreRef = recording.clone();
        for index in 0..3 {
            let table = TableRef::new("robots", format!("episodes-{index}"));
            let registration = TableRegistration::new(
                TableLocation::new(format!("mem://episodes-{index}")),
                "test",
                Version(1),
                vec![1],
            );
            lake_meta::registry::register(meta.as_ref(), &table, &registration)
                .await
                .unwrap();
        }
        let engine = Arc::new(CountingMaintenanceEngine {
            calls:     AtomicUsize::new(0),
            locations: StdMutex::new(Vec::new()),
        });
        let metasrv = Metasrv::new(meta, engine.clone());
        let shutdown = CancellationToken::new();

        let first = sweep_table_page(&metasrv, &shutdown, 2).await;
        assert_eq!(first.scanned, 2);
        assert_eq!(first.attempted, 2);
        assert_eq!(engine.calls.load(Ordering::SeqCst), 2);

        let second = sweep_table_page(&metasrv, &shutdown, 2).await;
        assert_eq!(second.scanned, 1);
        assert_eq!(second.attempted, 1);
        assert_eq!(engine.calls.load(Ordering::SeqCst), 3);
        assert_eq!(recording.page_calls.load(Ordering::SeqCst), 2);
        assert_eq!(recording.list_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            recording.get_calls.load(Ordering::SeqCst),
            6,
            "one current registration and one tombstone point-read per candidate"
        );
    }

    #[tokio::test]
    async fn table_maintenance_reresolves_after_scanned_generation_changes() {
        let meta_dir = tempfile::tempdir().unwrap();
        let inner: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let page_scanned = Arc::new(tokio::sync::Notify::new());
        let recording = Arc::new(RecordingScanMeta {
            inner,
            list_calls: AtomicUsize::new(0),
            page_calls: AtomicUsize::new(0),
            get_calls: AtomicUsize::new(0),
            page_scanned: Some(page_scanned.clone()),
        });
        let meta: MetaStoreRef = recording.clone();
        let table = TableRef::new("robots", "episodes");
        let old = TableRegistration::new(
            TableLocation::new("mem://old-generation"),
            "test",
            Version(1),
            vec![1],
        );
        lake_meta::registry::register(meta.as_ref(), &table, &old)
            .await
            .unwrap();
        let engine = Arc::new(CountingMaintenanceEngine {
            calls:     AtomicUsize::new(0),
            locations: StdMutex::new(Vec::new()),
        });
        let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let table_guard = metasrv.lock_table(&table).await;
        let shutdown = CancellationToken::new();
        let sweep = tokio::spawn({
            let metasrv = metasrv.clone();
            let shutdown = shutdown.clone();
            async move { sweep_table_page(&metasrv, &shutdown, 1).await }
        });

        tokio::time::timeout(Duration::from_secs(1), page_scanned.notified())
            .await
            .expect("old generation is scanned before lock acquisition");
        lake_meta::registry::delete(meta.as_ref(), &table, &old)
            .await
            .unwrap();
        let replacement = TableRegistration::new(
            TableLocation::new("mem://replacement-generation"),
            "test",
            Version(1),
            vec![1],
        );
        lake_meta::registry::register(meta.as_ref(), &table, &replacement)
            .await
            .unwrap();
        drop(table_guard);

        let stats = tokio::time::timeout(Duration::from_secs(1), sweep)
            .await
            .expect("maintenance finishes after replacement")
            .unwrap();
        assert_eq!(stats.attempted, 1);
        assert_eq!(
            *engine.locations.lock().unwrap(),
            vec![replacement.location]
        );
    }

    #[tokio::test]
    async fn drop_gc_shutdown_stops_before_next_tombstone() {
        let meta_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        for name in ["first", "second"] {
            let table = TableRef::new("robots", name);
            let registration = TableRegistration::new(
                TableLocation::new(format!("mem://{name}")),
                "test",
                Version(1),
                vec![1],
            );
            let tombstone = crate::drop_tombstone::DropTombstone::new(table, registration).unwrap();
            crate::drop_tombstone::prepare(meta.as_ref(), &tombstone)
                .await
                .unwrap();
        }
        let started = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let engine = Arc::new(PausedRemoveEngine {
            calls:   AtomicUsize::new(0),
            started: started.clone(),
            resume:  resume.clone(),
        });
        let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let shutdown = CancellationToken::new();
        let sweep = tokio::spawn({
            let metasrv = metasrv.clone();
            let shutdown = shutdown.clone();
            async move { sweep_drop_tombstones_until(&metasrv, &shutdown).await }
        });

        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("first tombstone cleanup starts");
        shutdown.cancel();
        resume.notify_one();
        let stats = tokio::time::timeout(Duration::from_secs(1), sweep)
            .await
            .expect("cancelled drop GC stops")
            .unwrap();

        assert_eq!(stats.scanned, 2);
        assert_eq!(stats.completed, 1);
        assert_eq!(engine.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            meta.list_prefix(crate::drop_tombstone::DROP_PREFIX)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn drop_tombstone_maintenance_is_bounded() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::with_operation_policy(
            meta.clone(),
            engine,
            crate::DEFAULT_APPEND_OPERATION_RETENTION,
            2,
        );
        for index in 0..3 {
            let table = TableRef::new("robots", format!("episodes-{index}"));
            let registration = TableRegistration::new(
                TableLocation::new(
                    table_dir
                        .path()
                        .join(format!("absent-{index}.lance"))
                        .to_string_lossy(),
                ),
                "lance",
                Version(1),
                vec![1, 2, 3],
            );
            let tombstone = crate::drop_tombstone::DropTombstone::new(table, registration).unwrap();
            crate::drop_tombstone::prepare(meta.as_ref(), &tombstone)
                .await
                .unwrap();
        }

        let first = sweep_drop_tombstones(&metasrv).await;
        assert_eq!(first.scanned, 2);
        assert_eq!(first.completed, 2);
        assert!(metasrv.inner.drop_gc_cursor.lock().await.is_some());
        assert_eq!(
            meta.list_prefix(crate::drop_tombstone::DROP_PREFIX)
                .await
                .unwrap()
                .len(),
            1
        );

        let second = sweep_drop_tombstones(&metasrv).await;
        assert_eq!(second.scanned, 1);
        assert_eq!(second.completed, 1);
        assert!(metasrv.inner.drop_gc_cursor.lock().await.is_none());
        assert!(
            meta.list_prefix(crate::drop_tombstone::DROP_PREFIX)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn sweep_advances_registry_to_maintenance_version() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta, engine.clone());
        let table = TableRef::new("robots", "episodes");
        let location =
            TableLocation::new(table_dir.path().join("episodes.lance").to_string_lossy());

        metasrv
            .create_table(&table, location.clone(), batch().schema())
            .await
            .unwrap();
        for _ in 0..3 {
            let b = batch();
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                b.schema(),
                futures::stream::iter(vec![Ok::<_, DataFusionError>(b)]),
            ));
            metasrv.append(&table, &operation(), stream).await.unwrap();
        }

        let before = metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .current_version;
        sweep(&metasrv).await;
        let after = metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .current_version;
        let engine_version = engine
            .open(&location)
            .await
            .unwrap()
            .unwrap()
            .current_version();

        assert!(
            engine_version > before,
            "compaction must create a new version"
        );
        assert_eq!(
            after, engine_version,
            "registry must publish maintenance commit"
        );
    }

    #[tokio::test]
    async fn operation_gc_is_bounded_and_expired_replay_fails_closed() {
        let meta_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv =
            Metasrv::with_operation_policy(meta.clone(), engine, Duration::from_secs(1), 2);
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new(meta_dir.path().join("episodes.lance").to_string_lossy());
        metasrv
            .create_table(&table, location, batch().schema())
            .await
            .unwrap();
        let incarnation = metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .incarnation_id()
            .unwrap()
            .to_owned();
        let tenant = TenantId::try_new("tenant-a").unwrap();
        let mut operations = Vec::new();
        let sweep_now = u64::MAX / 2;
        for (suffix, state, updated_at) in [
            ("000000000011", AppendState::Committed, 1),
            ("000000000012", AppendState::Committed, 1),
            ("000000000013", AppendState::Reserved, 1),
            ("000000000014", AppendState::Committed, sweep_now),
        ] {
            let operation = AppendOperation::builder()
                .tenant(tenant.clone())
                .operation_id(
                    AppendOperationId::parse(format!("0197f0f4-7b2a-7000-8000-{suffix}")).unwrap(),
                )
                .payload_digest(
                    AppendPayloadDigest::parse(
                        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    )
                    .unwrap(),
                )
                .build();
            let mut record = AppendRecord::reserved(
                &operation,
                &table,
                &incarnation,
                lake_common::Version(1),
                1,
            );
            record.state = state;
            record.updated_at = updated_at;
            if state == AppendState::Committed {
                record.result_version = Some(lake_common::Version(2));
            }
            let key = operation_key(&operation, &table);
            assert!(
                meta.cas(&key, None, &record.encode().unwrap())
                    .await
                    .unwrap()
            );
            if state == AppendState::Reserved {
                assert!(
                    meta.cas(&active_key(&operation, &table), None, key.as_bytes())
                        .await
                        .unwrap()
                );
            }
            operations.push(operation);
        }

        let first = sweep_operations_at(&metasrv, sweep_now).await;
        assert!(
            first.scanned <= 2,
            "one sweep is limited to one metadata page"
        );
        let second = sweep_operations_at(&metasrv, sweep_now).await;
        assert!(second.scanned <= 2, "the continuation remains page bounded");
        let remaining = meta.scan_prefix(OPERATION_PREFIX).await.unwrap();
        assert_eq!(remaining.len(), 1, "recent operation records remain");
        assert!(remaining[0].0.ends_with("000000000014"));

        let expired = &operations[0];
        let batch = batch();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
        ));
        assert!(matches!(
            metasrv.append(&table, expired, stream).await,
            Err(crate::MetasrvError::OperationExpired { .. })
        ));
    }

    #[tokio::test]
    async fn operation_gc_drains_multiple_pages_within_budget() {
        let meta_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        seed_committed_operations(meta.as_ref(), 3).await;
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv =
            Metasrv::with_operation_policy(meta.clone(), engine, Duration::from_secs(1), 1);

        let stats = sweep_operation_pages_at_until(
            &metasrv,
            u64::MAX / 2,
            &CancellationToken::new(),
            3,
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(stats.pages, 3);
        assert_eq!(stats.scanned, 3);
        assert_eq!(stats.deleted, 3);
        assert!(!stats.budget_exhausted);
        assert!(meta.scan_prefix(OPERATION_PREFIX).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn operation_gc_stops_at_page_budget_and_resumes() {
        let meta_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        seed_committed_operations(meta.as_ref(), 3).await;
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv =
            Metasrv::with_operation_policy(meta.clone(), engine, Duration::from_secs(1), 1);
        let shutdown = CancellationToken::new();

        let first = sweep_operation_pages_at_until(
            &metasrv,
            u64::MAX / 2,
            &shutdown,
            2,
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(first.pages, 2);
        assert_eq!(first.deleted, 2);
        assert!(first.budget_exhausted);
        assert_eq!(meta.scan_prefix(OPERATION_PREFIX).await.unwrap().len(), 1);

        let second = sweep_operation_pages_at_until(
            &metasrv,
            u64::MAX / 2,
            &shutdown,
            2,
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(second.pages, 1);
        assert_eq!(second.deleted, 1);
        assert!(!second.budget_exhausted);
        assert!(meta.scan_prefix(OPERATION_PREFIX).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn operation_gc_shutdown_stops_between_pages() {
        let meta_dir = tempfile::tempdir().unwrap();
        let inner: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        seed_committed_operations(inner.as_ref(), 3).await;
        let shutdown = CancellationToken::new();
        let recording = Arc::new(CancelAfterPageMeta {
            inner:      inner.clone(),
            page_calls: AtomicUsize::new(0),
            shutdown:   shutdown.clone(),
        });
        let meta: MetaStoreRef = recording.clone();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::with_operation_policy(meta, engine, Duration::from_secs(1), 1);

        let stats = sweep_operation_pages_at_until(
            &metasrv,
            u64::MAX / 2,
            &shutdown,
            10,
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(stats.pages, 1);
        assert_eq!(recording.page_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            stats.deleted, 0,
            "cancelled page performs no reconciliation"
        );

        let resumed = sweep_operation_pages_at_until(
            &metasrv,
            u64::MAX / 2,
            &CancellationToken::new(),
            10,
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(resumed.deleted, 3, "partial page cursor was not published");
        assert!(
            inner
                .scan_prefix(OPERATION_PREFIX)
                .await
                .expect("scan resumed operation prefix")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn operation_gc_time_budget_bounds_blocked_reconciliation() {
        let meta_dir = tempfile::tempdir().unwrap();
        let inner: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        seed_committed_operations(inner.as_ref(), 1).await;
        let blocking: MetaStoreRef = Arc::new(BlockingGetMeta {
            inner: inner.clone(),
        });
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::with_operation_policy(blocking, engine, Duration::from_secs(1), 1);

        let stats = tokio::time::timeout(
            Duration::from_secs(1),
            sweep_operation_pages_at_until(
                &metasrv,
                u64::MAX / 2,
                &CancellationToken::new(),
                16,
                Duration::from_millis(10),
            ),
        )
        .await
        .expect("operation GC obeys its wall-clock budget");

        assert!(stats.time_exhausted);
        assert_eq!(stats.pages, 1);
        assert_eq!(stats.deleted, 0);
        assert_eq!(inner.scan_prefix(OPERATION_PREFIX).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn operation_gc_reclaims_exact_lance_stage_before_record() {
        let meta_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv =
            Metasrv::with_operation_policy(meta.clone(), engine, Duration::from_secs(1), 8);
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new(meta_dir.path().join("episodes.lance").to_string_lossy());
        metasrv
            .create_table(&table, location.clone(), batch().schema())
            .await
            .unwrap();
        let operation = operation();
        let value = batch();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            value.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(value)]),
        ));
        metasrv.append(&table, &operation, stream).await.unwrap();

        let stage_header = std::path::PathBuf::from(location.as_str())
            .join("_lake")
            .join("object_refs_staging")
            .join(format!(
                "{}--{}",
                operation.tenant().as_str(),
                operation.operation_id().as_str()
            ))
            .join("0.json");
        assert!(
            stage_header.exists(),
            "committed operation retains its replay stage"
        );
        let key = operation_key(&operation, &table);
        assert!(meta.get(&key).await.unwrap().is_some());

        let stats = sweep_operations_at(&metasrv, u64::MAX).await;

        assert_eq!(stats.deleted, 1);
        assert!(
            !stage_header.exists(),
            "stage is reclaimed before operation expiry completes"
        );
        assert!(meta.get(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn operation_gc_retains_record_when_stage_cleanup_fails() {
        let meta_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv =
            Metasrv::with_operation_policy(meta.clone(), engine, Duration::from_secs(1), 8);
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new(meta_dir.path().join("episodes.lance").to_string_lossy());
        metasrv
            .create_table(&table, location.clone(), batch().schema())
            .await
            .unwrap();
        let operation = operation();
        let value = batch();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            value.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(value)]),
        ));
        metasrv.append(&table, &operation, stream).await.unwrap();
        let stage_header = std::path::PathBuf::from(location.as_str())
            .join("_lake")
            .join("object_refs_staging")
            .join(format!(
                "{}--{}",
                operation.tenant().as_str(),
                operation.operation_id().as_str()
            ))
            .join("0.json");
        std::fs::write(&stage_header, br#"{"format_version":1,"chunk_index":0}"#).unwrap();
        let key = operation_key(&operation, &table);

        let stats = sweep_operations_at(&metasrv, u64::MAX).await;

        assert_eq!(stats.deleted, 0);
        assert!(stage_header.exists(), "malformed stage remains fail-closed");
        assert!(
            meta.get(&key).await.unwrap().is_some(),
            "cleanup failure keeps durable identity for a later retry"
        );
    }
}
