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

//! The metadata layer: the stateful registry authority.
//!
//! [`Metasrv`] owns the write path for the db→table registry — create,
//! resolve, list, and commit coordination. It is a bounded,
//! leader-elected tier, NOT a fan-out one: the query layer shields it behind
//! a cache, so it sees only cache-miss and write traffic. See
//! `docs/architecture.md`.
//!
//! [`election`] adds the lease-in-KV leader election that gives this tier HA
//! (leader + standby) over the [`MetaStore`](lake_meta::MetaStore) CAS
//! primitive — no self-built consensus. `control` wraps the authority in an
//! Arrow Flight `DoAction` wire surface, and [`serve`] runs it alongside a
//! background `leadership` campaign so writes gate on the lease.

pub mod election;

mod control;
mod drop_tombstone;
mod fenced_meta;
mod leadership;
mod maintenance;
mod operation;
mod placement;

use std::{
    collections::HashMap,
    net::AddrParseError,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::{arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream};
use lake_catalog::create_table;
use lake_common::{AppendOperation, Namespace, TableLocation, TableName, TableRef, Version};
use lake_engine::TableEngineRef;
use lake_flight::{ClientSecurity, ServerSecurity};
use lake_meta::{MetaStoreRef, registry, registry::TableRegistration};
pub use placement::{PlacementError, TablePlacement};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::{
    control::{AppendAdmission, MetasrvFlightService},
    election::LeaseElection,
    fenced_meta::FencedMetaStore,
    leadership::{Leadership, run_campaign_loop_until},
    maintenance::run_maintenance_loop_until,
    operation::{AppendRecord, AppendState, active_key, operation_key},
};

/// Production default for replay protection and durable operation records.
pub const DEFAULT_APPEND_OPERATION_RETENTION: Duration = Duration::from_hours(7 * 24);
const DEFAULT_OPERATION_GC_PAGE_SIZE: usize = 128;
const MAX_APPEND_OPERATION_CLOCK_SKEW: Duration = Duration::from_mins(5);

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetasrvError {
    #[snafu(display("registry error"))]
    Registry { source: lake_meta::MetaError },

    #[snafu(display("create-table failed"))]
    Create { source: lake_catalog::CatalogError },

    #[snafu(display("engine error"))]
    Engine { source: lake_engine::EngineError },

    #[snafu(display("durable drop protocol failed: {message}"))]
    DropProtocol { message: String },

    #[snafu(display("append operation '{operation_id}' conflicts with its durable payload"))]
    OperationConflict { operation_id: String },

    #[snafu(display("append operation '{operation_id}' is older than the retention window"))]
    OperationExpired { operation_id: String },

    #[snafu(display("append operation '{operation_id}' timestamp is too far in the future"))]
    OperationFromFuture { operation_id: String },

    #[snafu(display("append operation '{operation_id}' belongs to a dropped table incarnation"))]
    OperationTableRecreated { operation_id: String },

    #[snafu(display("append operation '{operation_id}' has corrupt durable state"))]
    CorruptOperationState { operation_id: String },

    #[snafu(display("table '{table}' is coordinated by another durable append operation"))]
    OperationInProgress { table: String },

    #[snafu(display("append operation '{operation_id}' conflicts with registry state"))]
    OperationRecoveryConflict { operation_id: String },

    #[snafu(display("table '{table}' not found"))]
    NotFound { table: String },

    #[snafu(display("invalid listen address {addr:?}"))]
    Address {
        addr:   String,
        source: AddrParseError,
    },

    #[snafu(display("metasrv control plane server failed"))]
    Serve { source: tonic::transport::Error },

    #[snafu(display("invalid Flight security configuration"))]
    Security {
        source: lake_flight::FlightSecurityError,
    },

    #[snafu(display("invalid append admission limits: {message}"))]
    InvalidAppendLimits { message: String },

    #[snafu(display("invalid maintenance limits: {message}"))]
    InvalidMaintenanceLimits { message: String },

    #[snafu(display("Metasrv Flight connections did not drain within {grace:?}"))]
    DrainTimeout { grace: Duration },

    #[snafu(display("Metasrv background tasks did not stop within {grace:?}"))]
    BackgroundDrainTimeout { grace: Duration },

    #[snafu(display("Metasrv background task '{task}' failed"))]
    BackgroundTask {
        task:   &'static str,
        source: tokio::task::JoinError,
    },
}

pub type Result<T> = std::result::Result<T, MetasrvError>;

/// Per-process admission and control-payload bounds for FILE appends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendLimits {
    max_concurrent:     usize,
    queue_wait:         Duration,
    max_stream_bytes:   usize,
    max_buffered_bytes: usize,
}

impl AppendLimits {
    /// Validate finite append concurrency and memory bounds.
    pub fn try_new(
        max_concurrent: usize,
        queue_wait: Duration,
        max_stream_bytes: usize,
        max_buffered_bytes: usize,
    ) -> Result<Self> {
        for (valid, message) in [
            (
                (1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&max_concurrent),
                "max_concurrent must fit the Tokio semaphore and be greater than zero",
            ),
            (
                !queue_wait.is_zero(),
                "queue_wait must be greater than zero",
            ),
            (
                max_stream_bytes > 0,
                "max_stream_bytes must be greater than zero",
            ),
            (
                max_buffered_bytes >= max_stream_bytes,
                "max_buffered_bytes must be at least max_stream_bytes",
            ),
            (
                u32::try_from(max_stream_bytes).is_ok(),
                "max_stream_bytes must fit a weighted semaphore permit",
            ),
            (
                u32::try_from(max_buffered_bytes).is_ok(),
                "max_buffered_bytes must fit a weighted semaphore permit",
            ),
        ] {
            if !valid {
                return Err(MetasrvError::InvalidAppendLimits {
                    message: message.to_owned(),
                });
            }
        }
        Ok(Self {
            max_concurrent,
            queue_wait,
            max_stream_bytes,
            max_buffered_bytes,
        })
    }

    /// Maximum append requests occupying one Metasrv process.
    #[must_use]
    pub const fn max_concurrent(&self) -> usize { self.max_concurrent }

    /// Maximum time an append waits for process-local admission.
    #[must_use]
    pub const fn queue_wait(&self) -> Duration { self.queue_wait }

    /// Maximum encoded Flight control bytes accepted from one append.
    #[must_use]
    pub const fn max_stream_bytes(&self) -> usize { self.max_stream_bytes }

    /// Maximum worst-case Flight control bytes reserved across appends.
    #[must_use]
    pub const fn max_buffered_bytes(&self) -> usize { self.max_buffered_bytes }
}

impl Default for AppendLimits {
    fn default() -> Self {
        Self {
            max_concurrent:     8,
            queue_wait:         Duration::from_millis(100),
            max_stream_bytes:   64 * 1024 * 1024,
            max_buffered_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Immutable cadence and per-tick table-work bound for leader maintenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintenanceLimits {
    interval:        Duration,
    table_page_size: usize,
}

impl MaintenanceLimits {
    /// Validate a positive interval and bounded table page.
    pub fn try_new(interval: Duration, table_page_size: usize) -> Result<Self> {
        if interval.is_zero() {
            return Err(MetasrvError::InvalidMaintenanceLimits {
                message: "interval must be greater than zero".to_owned(),
            });
        }
        if !(1..=10_000).contains(&table_page_size) {
            return Err(MetasrvError::InvalidMaintenanceLimits {
                message: "table_page_size must be within 1..=10000".to_owned(),
            });
        }
        Ok(Self {
            interval,
            table_page_size,
        })
    }

    /// Delay between leader maintenance ticks.
    #[must_use]
    pub const fn interval(&self) -> Duration { self.interval }

    /// Maximum registry candidates handled by one tick.
    #[must_use]
    pub const fn table_page_size(&self) -> usize { self.table_page_size }
}

impl Default for MaintenanceLimits {
    fn default() -> Self {
        Self {
            interval:        Duration::from_mins(1),
            table_page_size: 128,
        }
    }
}

/// Deterministic post-commit response gate for cross-crate crash tests.
#[cfg(feature = "test")]
#[derive(Debug, Default)]
pub struct AppendResultGate {
    armed:   std::sync::atomic::AtomicBool,
    fail:    std::sync::atomic::AtomicBool,
    blocked: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(feature = "test")]
impl AppendResultGate {
    /// Create a gate that blocks the first committed append response.
    #[must_use]
    pub fn armed() -> Self {
        Self {
            armed:   std::sync::atomic::AtomicBool::new(true),
            fail:    std::sync::atomic::AtomicBool::new(false),
            blocked: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }

    /// Wait until a response is blocked after its append committed.
    pub async fn wait_until_blocked(&self) { self.blocked.notified().await; }

    /// Disable the fault and release a currently blocked response.
    pub fn disable(&self) {
        self.armed.store(false, std::sync::atomic::Ordering::SeqCst);
        self.release.notify_one();
    }

    /// Release the blocked post-commit request as a lost-response failure.
    pub fn fail_blocked(&self) {
        self.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        self.release.notify_one();
    }

    pub(crate) async fn block_first(&self) -> bool {
        if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.blocked.notify_one();
            self.release.notified().await;
            return self.fail.load(std::sync::atomic::Ordering::SeqCst);
        }
        false
    }
}

/// Network security for one Metasrv node and its follower-to-leader hop.
#[derive(Clone, Debug)]
pub struct MetasrvServerConfig {
    server_security:    ServerSecurity,
    peer_security:      ClientSecurity,
    table_placement:    Option<TablePlacement>,
    allow_insecure:     bool,
    append_limits:      AppendLimits,
    maintenance_limits: MaintenanceLimits,
    shutdown_grace:     Duration,
    #[cfg(feature = "test")]
    append_result_gate: Option<Arc<AppendResultGate>>,
}

impl MetasrvServerConfig {
    /// Explicit loopback development configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            server_security: ServerSecurity::insecure(),
            peer_security: ClientSecurity::new(),
            table_placement: None,
            allow_insecure: false,
            append_limits: AppendLimits::default(),
            maintenance_limits: MaintenanceLimits::default(),
            shutdown_grace: Duration::from_secs(30),
            #[cfg(feature = "test")]
            append_result_gate: None,
        }
    }

    /// Authenticate inbound RPCs and optionally enable server TLS.
    #[must_use]
    pub fn with_server_security(mut self, security: ServerSecurity) -> Self {
        self.server_security = security;
        self
    }

    /// Configure TLS and service identity for follower forwarding.
    #[must_use]
    pub fn with_peer_security(mut self, security: ClientSecurity) -> Self {
        self.peer_security = security;
        self
    }

    /// Configure the trusted policy for remotely-created table datasets.
    #[must_use]
    pub fn with_table_placement(mut self, placement: TablePlacement) -> Self {
        self.table_placement = Some(placement);
        self
    }

    /// Explicit deployment escape hatch when a trusted proxy terminates both
    /// TLS and authentication before Lake.
    #[must_use]
    pub const fn allow_insecure(mut self, allow: bool) -> Self {
        self.allow_insecure = allow;
        self
    }

    /// Apply process-local append admission and control-payload bounds.
    #[must_use]
    pub const fn with_append_limits(mut self, limits: AppendLimits) -> Self {
        self.append_limits = limits;
        self
    }

    /// Apply the leader maintenance cadence and per-tick table bound.
    #[must_use]
    pub const fn with_maintenance_limits(mut self, limits: MaintenanceLimits) -> Self {
        self.maintenance_limits = limits;
        self
    }

    /// Bound how long existing Flight connections may drain during shutdown.
    #[must_use]
    pub const fn with_shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    /// Block the first post-commit result for deterministic crash testing.
    #[cfg(feature = "test")]
    #[must_use]
    pub fn with_append_result_gate(mut self, gate: Arc<AppendResultGate>) -> Self {
        self.append_result_gate = Some(gate);
        self
    }
}

impl Default for MetasrvServerConfig {
    fn default() -> Self { Self::new() }
}

/// The registry authority. Holds the durable metastore and the storage
/// engine used to materialize new tables.
struct MetasrvInner {
    meta:                     MetaStoreRef,
    engine:                   TableEngineRef,
    /// One coordinator per table. Metadata writes are rare and the catalog's
    /// design ceiling is ~10^4 tables, so retaining these locks is bounded.
    table_locks:              Mutex<HashMap<TableRef, Arc<Mutex<()>>>>,
    operation_retention:      Duration,
    operation_gc_page_size:   usize,
    operation_gc_cursor:      Mutex<Option<String>>,
    drop_gc_cursor:           Mutex<Option<String>>,
    table_maintenance_cursor: Mutex<Option<String>>,
}

#[derive(Clone)]
/// Cloneable handle to the registry authority and its per-table write
/// coordinators.
pub struct Metasrv {
    inner: Arc<MetasrvInner>,
}

impl Metasrv {
    pub fn new(meta: MetaStoreRef, engine: TableEngineRef) -> Self {
        Self::with_operation_policy(
            meta,
            engine,
            DEFAULT_APPEND_OPERATION_RETENTION,
            DEFAULT_OPERATION_GC_PAGE_SIZE,
        )
    }

    /// Construct an authority with an explicit deployment-visible replay
    /// retention horizon.
    pub fn with_operation_retention(
        meta: MetaStoreRef,
        engine: TableEngineRef,
        operation_retention: Duration,
    ) -> Self {
        Self::with_operation_policy(
            meta,
            engine,
            operation_retention,
            DEFAULT_OPERATION_GC_PAGE_SIZE,
        )
    }

    /// Construct an authority with explicit retention and bounded GC page size.
    pub fn with_operation_policy(
        meta: MetaStoreRef,
        engine: TableEngineRef,
        operation_retention: Duration,
        operation_gc_page_size: usize,
    ) -> Self {
        Self {
            inner: Arc::new(MetasrvInner {
                meta,
                engine,
                table_locks: Mutex::new(HashMap::new()),
                operation_retention,
                operation_gc_page_size: operation_gc_page_size.max(1),
                operation_gc_cursor: Mutex::new(None),
                drop_gc_cursor: Mutex::new(None),
                table_maintenance_cursor: Mutex::new(None),
            }),
        }
    }

    /// Build the production server view: reads share the raw authority, while
    /// every metadata CAS/delete is translated into a lease-guarded mutation.
    fn fenced_for_server(&self, leadership: Arc<Leadership>) -> Arc<Self> {
        let meta: MetaStoreRef = Arc::new(FencedMetaStore::new(self.meta().clone(), leadership));
        Arc::new(Self::with_operation_policy(
            meta,
            self.engine().clone(),
            self.inner.operation_retention,
            self.inner.operation_gc_page_size,
        ))
    }

    pub(crate) async fn lock_table(&self, table: &TableRef) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.inner.table_locks.lock().await;
            locks
                .entry(table.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    /// Create a table: materialize the dataset via the engine, then register
    /// it (dataset-first, so a registry entry never points at nothing).
    pub async fn create_table(
        &self,
        table: &TableRef,
        location: TableLocation,
        schema: SchemaRef,
    ) -> Result<()> {
        let _guard = self.lock_table(table).await;
        self.resume_drop_locked(table).await?;
        create_table(self.meta(), self.engine(), table, location, schema)
            .await
            .context(CreateSnafu)
    }

    /// Append rows to a table under the commit protocol: the engine writes a
    /// new immutable version, then the registry pointer is CAS-advanced to
    /// it. A lost CAS race surfaces as a registry conflict for the caller to
    /// retry.
    pub async fn append(
        &self,
        table: &TableRef,
        operation: &AppendOperation,
        batches: SendableRecordBatchStream,
    ) -> Result<Version> {
        let _guard = self.lock_table(table).await;
        self.resume_drop_locked(table).await?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_secs();
        let operation_time = operation.operation_id().unix_seconds();
        if operation_time > now.saturating_add(MAX_APPEND_OPERATION_CLOCK_SKEW.as_secs()) {
            return Err(MetasrvError::OperationFromFuture {
                operation_id: operation.operation_id().to_string(),
            });
        }
        if now.saturating_sub(operation_time) > self.inner.operation_retention.as_secs() {
            return Err(MetasrvError::OperationExpired {
                operation_id: operation.operation_id().to_string(),
            });
        }
        let reg = self.resolve(table).await?.context(NotFoundSnafu {
            table: table.to_string(),
        })?;
        let reg = registry::ensure_incarnation(self.meta().as_ref(), table, &reg)
            .await
            .context(RegistrySnafu)?;
        let table_incarnation = reg
            .incarnation_id()
            .expect("ensured table registration has an incarnation");
        let key = operation_key(operation, table);
        let active = active_key(operation, table);
        let active_value = key.as_bytes();
        let mut record = AppendRecord::reserved(
            operation,
            table,
            table_incarnation,
            reg.current_version,
            now,
        );
        let mut encoded = record.encode()?;
        let mut created_record = false;
        loop {
            match self.meta().get(&key).await.context(RegistrySnafu)? {
                Some(existing) => {
                    record = AppendRecord::decode(operation.operation_id().as_str(), &existing)?;
                    record.validate(operation, table, table_incarnation)?;
                    encoded = existing;
                    if record.state == AppendState::Committed {
                        let version = record.result_version.ok_or_else(|| {
                            MetasrvError::CorruptOperationState {
                                operation_id: operation.operation_id().to_string(),
                            }
                        })?;
                        // A crash after terminal publication but before fence
                        // cleanup must not permanently block the table.
                        let _ = self
                            .meta()
                            .delete(&active, active_value)
                            .await
                            .context(RegistrySnafu)?;
                        return Ok(version);
                    }
                    break;
                }
                None => {
                    if self
                        .meta()
                        .cas(&key, None, &encoded)
                        .await
                        .context(RegistrySnafu)?
                    {
                        created_record = true;
                        break;
                    }
                }
            }
        }

        // Reserve the operation record first. A crash can then leave an inert,
        // recoverable record, never an ownerless fence that blocks the table.
        let owns_fence = self
            .meta()
            .cas(&active, None, active_value)
            .await
            .context(RegistrySnafu)?
            || self
                .meta()
                .get(&active)
                .await
                .context(RegistrySnafu)?
                .as_deref()
                == Some(active_value);
        if !owns_fence {
            if created_record {
                let _ = self
                    .meta()
                    .delete(&key, &encoded)
                    .await
                    .context(RegistrySnafu)?;
            }
            return Err(MetasrvError::OperationInProgress {
                table: table.to_string(),
            });
        }

        // Another request for this same identity may have advanced the record
        // while this request acquired the shared exact-value fence.
        if let Some(current_record) = self.meta().get(&key).await.context(RegistrySnafu)? {
            record = AppendRecord::decode(operation.operation_id().as_str(), &current_record)?;
            record.validate(operation, table, table_incarnation)?;
            encoded = current_record;
        }

        let current = self.resolve(table).await?.context(NotFoundSnafu {
            table: table.to_string(),
        })?;
        let handle = self
            .inner
            .engine
            .open(&current.location)
            .await
            .context(EngineSnafu)?
            .context(NotFoundSnafu {
                table: table.to_string(),
            })?;
        let reconciled = if record.result_version.is_none() && !created_record {
            handle
                .reconcile_append(operation)
                .await
                .context(EngineSnafu)?
        } else {
            None
        };
        if record.state == AppendState::Reserved
            && reconciled.is_none()
            && record.base_version != current.current_version
        {
            let mut refreshed = record.clone();
            refreshed.base_version = current.current_version;
            refreshed.updated_at = now;
            let updated = refreshed.encode()?;
            if self
                .meta()
                .cas(&key, Some(&encoded), &updated)
                .await
                .context(RegistrySnafu)?
            {
                record = refreshed;
                encoded = updated;
            } else if let Some(current_record) =
                self.meta().get(&key).await.context(RegistrySnafu)?
            {
                record = AppendRecord::decode(operation.operation_id().as_str(), &current_record)?;
                record.validate(operation, table, table_incarnation)?;
                encoded = current_record;
            }
        }
        let new_version = match record.result_version {
            Some(version) => version,
            None => match reconciled {
                Some(version) => version,
                None => handle
                    .append_reserved(operation, batches)
                    .await
                    .context(EngineSnafu)?,
            },
        };
        if record.result_version != Some(new_version) || record.state == AppendState::Reserved {
            let mut engine_committed = record.clone();
            engine_committed.result_version = Some(new_version);
            engine_committed.state = AppendState::EngineCommitted;
            engine_committed.updated_at = now;
            let updated = engine_committed.encode()?;
            if self
                .meta()
                .cas(&key, Some(&encoded), &updated)
                .await
                .context(RegistrySnafu)?
            {
                record = engine_committed;
                encoded = updated;
            } else {
                let current = self
                    .meta()
                    .get(&key)
                    .await
                    .context(RegistrySnafu)?
                    .ok_or_else(|| MetasrvError::CorruptOperationState {
                        operation_id: operation.operation_id().to_string(),
                    })?;
                let converged = AppendRecord::decode(operation.operation_id().as_str(), &current)?;
                converged.validate(operation, table, table_incarnation)?;
                if converged.result_version != Some(new_version)
                    || converged.state == AppendState::Reserved
                {
                    return Err(MetasrvError::OperationRecoveryConflict {
                        operation_id: operation.operation_id().to_string(),
                    });
                }
                record = converged;
                encoded = current;
            }
        }

        let current = self.resolve(table).await?.context(NotFoundSnafu {
            table: table.to_string(),
        })?;
        if current.current_version == record.base_version {
            registry::set_version(self.meta().as_ref(), table, &current, new_version)
                .await
                .context(RegistrySnafu)?;
        } else if current.current_version != new_version {
            return Err(MetasrvError::OperationRecoveryConflict {
                operation_id: operation.operation_id().to_string(),
            });
        }

        let mut committed = record;
        committed.state = AppendState::Committed;
        committed.updated_at = now;
        let terminal = committed.encode()?;
        if !self
            .meta()
            .cas(&key, Some(&encoded), &terminal)
            .await
            .context(RegistrySnafu)?
        {
            let current = self
                .meta()
                .get(&key)
                .await
                .context(RegistrySnafu)?
                .ok_or_else(|| MetasrvError::CorruptOperationState {
                    operation_id: operation.operation_id().to_string(),
                })?;
            let converged = AppendRecord::decode(operation.operation_id().as_str(), &current)?;
            converged.validate(operation, table, table_incarnation)?;
            if converged.state != AppendState::Committed
                || converged.result_version != Some(new_version)
            {
                return Err(MetasrvError::OperationRecoveryConflict {
                    operation_id: operation.operation_id().to_string(),
                });
            }
        }
        let _ = self
            .meta()
            .delete(&active, active_value)
            .await
            .context(RegistrySnafu)?;
        Ok(new_version)
    }

    /// Durably detach and remove one table incarnation. The immutable
    /// tombstone is published before either the registry or objects change, so
    /// any successor can restart the exact same cleanup sequence.
    pub async fn drop_table(&self, table: &TableRef) -> Result<()> {
        let _guard = self.lock_table(table).await;
        self.resume_drop_locked(table).await?;
        let Some(registration) = self.resolve(table).await? else {
            return Ok(());
        };
        let registration = registry::ensure_incarnation(self.meta().as_ref(), table, &registration)
            .await
            .context(RegistrySnafu)?;
        let tombstone = drop_tombstone::DropTombstone::new(table.clone(), registration)
            .map_err(Self::drop_protocol_error)?;
        drop_tombstone::prepare(self.meta().as_ref(), &tombstone)
            .await
            .map_err(Self::drop_protocol_error)?;
        self.cleanup_drop_locked(&tombstone).await
    }

    async fn resume_drop_locked(&self, table: &TableRef) -> Result<()> {
        let tombstones = drop_tombstone::list_for_table(self.meta().as_ref(), table)
            .await
            .map_err(Self::drop_protocol_error)?;
        for tombstone in tombstones {
            self.cleanup_drop_locked(&tombstone).await?;
        }
        Ok(())
    }

    pub(crate) async fn cleanup_drop_locked(
        &self,
        tombstone: &drop_tombstone::DropTombstone,
    ) -> Result<()> {
        if let Some(current) = self.resolve(&tombstone.table).await? {
            if current == tombstone.registration {
                registry::delete(self.meta().as_ref(), &tombstone.table, &current)
                    .await
                    .context(RegistrySnafu)?;
            } else if current.incarnation_id() == tombstone.registration.incarnation_id() {
                return Err(MetasrvError::DropProtocol {
                    message: format!(
                        "registration changed within incarnation for {}",
                        tombstone.table
                    ),
                });
            }
        }
        self.engine()
            .remove(&tombstone.registration.location)
            .await
            .context(EngineSnafu)?;
        drop_tombstone::finish(self.meta().as_ref(), tombstone)
            .await
            .map_err(Self::drop_protocol_error)
    }

    fn drop_protocol_error(error: drop_tombstone::DropTombstoneError) -> MetasrvError {
        MetasrvError::DropProtocol {
            message: error.to_string(),
        }
    }

    /// Resolve a table to its current registration.
    pub async fn resolve(&self, table: &TableRef) -> Result<Option<TableRegistration>> {
        registry::get(self.meta().as_ref(), table)
            .await
            .context(RegistrySnafu)
    }

    /// List the tables in a namespace.
    pub async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<TableName>> {
        registry::list(self.meta().as_ref(), namespace)
            .await
            .context(RegistrySnafu)
    }

    /// List all namespaces.
    pub async fn list_namespaces(&self) -> Result<Vec<Namespace>> {
        registry::list_namespaces(self.meta().as_ref())
            .await
            .context(RegistrySnafu)
    }

    pub fn meta(&self) -> &MetaStoreRef { &self.inner.meta }

    pub fn engine(&self) -> &TableEngineRef { &self.inner.engine }
}

/// Run the metadata server: the Arrow Flight control plane plus a background
/// leader-election campaign.
///
/// Spawns a campaign loop that renews the lease and publishes leadership into
/// shared state, a leader-only maintenance sweep, then binds a tonic server
/// exposing the control-plane
/// [`FlightService`](arrow_flight::flight_service_server::FlightService) over
/// `DoAction`. Writes that land on a follower are forwarded to the current
/// leader; reads are always served locally. The node id is `addr`, unique
/// enough per instance in dev. Runs until the server stops or the process is
/// killed.
pub async fn serve(metasrv: Arc<Metasrv>, addr: &str) -> Result<()> {
    serve_with_config(metasrv, addr, MetasrvServerConfig::new()).await
}

/// Run Metasrv with explicit inbound and peer Flight security.
pub async fn serve_with_config(
    metasrv: Arc<Metasrv>,
    addr: &str,
    config: MetasrvServerConfig,
) -> Result<()> {
    serve_with_config_and_shutdown(metasrv, addr, config, std::future::pending()).await
}

/// Run Metasrv until `shutdown` fires, drain RPCs, then resign and join all
/// owned background work before returning.
pub async fn serve_with_config_and_shutdown<F>(
    metasrv: Arc<Metasrv>,
    addr: &str,
    config: MetasrvServerConfig,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    serve_with_config_and_termination(metasrv, addr, config, shutdown, false).await
}

async fn join_background_tasks_until(
    mut maintenance: tokio::task::JoinHandle<()>,
    mut campaign: tokio::task::JoinHandle<()>,
    deadline: tokio::time::Instant,
    grace: Duration,
) -> Result<()> {
    let joined = tokio::time::timeout_at(deadline, async {
        let (maintenance, campaign) = tokio::join!(&mut maintenance, &mut campaign);
        maintenance.map_err(|source| MetasrvError::BackgroundTask {
            task: "maintenance",
            source,
        })?;
        campaign.map_err(|source| MetasrvError::BackgroundTask {
            task: "leadership-campaign",
            source,
        })?;
        Ok(())
    })
    .await;
    match joined {
        Ok(result) => result,
        Err(_) => {
            maintenance.abort();
            campaign.abort();
            let _ = tokio::join!(maintenance, campaign);
            Err(MetasrvError::BackgroundDrainTimeout { grace })
        }
    }
}

/// Run until `crash` fires, then drop RPCs and campaigns without resigning.
///
/// This test-only entry point models process death: the durable lease remains
/// until TTL expiry and accepted connections receive no graceful response.
#[cfg(feature = "test")]
pub async fn serve_with_config_and_crash<F>(
    metasrv: Arc<Metasrv>,
    addr: &str,
    config: MetasrvServerConfig,
    crash: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    serve_with_config_and_termination(metasrv, addr, config, crash, true).await
}

async fn serve_with_config_and_termination<F>(
    metasrv: Arc<Metasrv>,
    addr: &str,
    config: MetasrvServerConfig,
    shutdown: F,
    crash: bool,
) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let socket = addr.parse().context(AddressSnafu { addr })?;
    config
        .server_security
        .validate_exposure(socket, config.allow_insecure)
        .context(SecuritySnafu)?;
    let mut server = Server::builder();
    if let Some(tls) = config.server_security.tls_config() {
        server = server.tls_config(tls).context(ServeSnafu)?;
    }

    let election = LeaseElection::new(metasrv.meta().clone(), addr, Duration::from_secs(10));
    let leadership = Arc::new(Leadership::new());
    let metasrv = metasrv.fenced_for_server(leadership.clone());
    let maintenance_shutdown = CancellationToken::new();
    let campaign_shutdown = CancellationToken::new();
    let maintenance = tokio::spawn(run_maintenance_loop_until(
        metasrv.clone(),
        leadership.clone(),
        maintenance_shutdown.clone(),
        config.maintenance_limits,
    ));
    let campaign = tokio::spawn(run_campaign_loop_until(
        election,
        leadership.clone(),
        campaign_shutdown.clone(),
    ));

    let svc = MetasrvFlightService {
        metasrv,
        leadership,
        own_addr: addr.to_string(),
        peer_security: config.peer_security,
        table_placement: config.table_placement,
        append_admission: AppendAdmission::new(config.append_limits),
        #[cfg(feature = "test")]
        append_result_gate: config.append_result_gate,
    };

    tracing::info!(
        %addr,
        "metasrv control plane ready (Flight do_action; writes gated on leadership)"
    );
    let server_shutdown = CancellationToken::new();
    let server_shutdown_waiter = server_shutdown.clone();
    let mut server = Box::pin(
        server
            .layer(tonic::service::InterceptorLayer::new(
                config.server_security.interceptor(),
            ))
            .add_service(FlightServiceServer::new(svc))
            .serve_with_shutdown(socket, async move {
                server_shutdown_waiter.cancelled().await;
            }),
    );
    let mut shutdown = Box::pin(shutdown);

    let mut shutdown_deadline = None;
    let server_result = tokio::select! {
        result = server.as_mut() => result.context(ServeSnafu),
        () = shutdown.as_mut() => {
            let deadline = tokio::time::Instant::now() + config.shutdown_grace;
            shutdown_deadline = Some(deadline);
            maintenance_shutdown.cancel();
            server_shutdown.cancel();
            if crash {
                Ok(())
            } else {
                match tokio::time::timeout_at(deadline, server.as_mut()).await {
                    Ok(result) => result.context(ServeSnafu),
                    Err(_) => Err(MetasrvError::DrainTimeout { grace: config.shutdown_grace }),
                }
            }
        }
    };

    // Dropping the server first guarantees no accepted write can outlive the
    // leadership lease. Only then may the campaign resign.
    drop(server);
    if crash {
        maintenance.abort();
        campaign.abort();
        let _ = maintenance.await;
        let _ = campaign.await;
        return server_result;
    }
    maintenance_shutdown.cancel();
    campaign_shutdown.cancel();
    let cleanup_deadline =
        shutdown_deadline.unwrap_or_else(|| tokio::time::Instant::now() + config.shutdown_grace);
    let background_result = join_background_tasks_until(
        maintenance,
        campaign,
        cleanup_deadline,
        config.shutdown_grace,
    )
    .await;
    server_result?;
    background_result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            array::Int64Array,
            datatypes::{DataType, Field, Schema, SchemaRef},
            record_batch::RecordBatch,
        },
        error::DataFusionError,
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use lake_common::{AppendOperationId, AppendPayloadDigest, TenantId};
    use lake_engine::{
        ObjectReferencePage, ObjectReferenceRequest, Result as EngineResult, TableEngine,
        TableEngineRef, TableHandleRef,
    };
    use lake_engine_lance::LanceEngine;
    use lake_meta::{DynamoMeta, GuardedMutation, MetaScanPage, MetaStore, RocksMeta};
    use tokio::sync::{Notify, oneshot};

    use super::*;
    use crate::election::LeaseStatus;

    struct RecordingMeta {
        inner:           MetaStoreRef,
        ordinary_cas:    AtomicUsize,
        ordinary_delete: AtomicUsize,
        guarded:         AtomicUsize,
    }

    struct RegistryDeleteProbeMeta {
        inner:          MetaStoreRef,
        table:          TableRef,
        expected:       TableRegistration,
        race_version:   bool,
        tombstone_seen: AtomicUsize,
    }

    #[async_trait]
    impl MetaStore for RegistryDeleteProbeMeta {
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
            self.inner.scan_prefix(prefix).await
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
            if key.starts_with("tbl/") {
                let tombstone_key =
                    format!("drop/{}/{}", self.table.namespace.0, self.table.name.0);
                assert!(
                    self.inner.get(&tombstone_key).await?.is_some(),
                    "tombstone must be durable before registry deletion"
                );
                self.tombstone_seen.fetch_add(1, Ordering::SeqCst);
                if self.race_version {
                    registry::set_version(
                        self.inner.as_ref(),
                        &self.table,
                        &self.expected,
                        Version(2),
                    )
                    .await?;
                }
            }
            self.inner.delete(key, expected).await
        }
    }

    #[async_trait]
    impl MetaStore for RecordingMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            self.inner.get(key).await
        }

        async fn cas(
            &self,
            key: &str,
            expected: Option<&[u8]>,
            new: &[u8],
        ) -> lake_meta::Result<bool> {
            self.ordinary_cas.fetch_add(1, Ordering::SeqCst);
            self.inner.cas(key, expected, new).await
        }

        async fn guarded_mutate(&self, mutation: GuardedMutation<'_>) -> lake_meta::Result<bool> {
            self.guarded.fetch_add(1, Ordering::SeqCst);
            self.inner.guarded_mutate(mutation).await
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.inner.list_prefix(prefix).await
        }

        async fn scan_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<(String, Vec<u8>)>> {
            self.inner.scan_prefix(prefix).await
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.ordinary_delete.fetch_add(1, Ordering::SeqCst);
            self.inner.delete(key, expected).await
        }
    }

    struct PausedRemoveEngine {
        inner:          LanceEngine,
        remove_started: Arc<Notify>,
        resume_remove:  Arc<Notify>,
    }

    struct PartialRemoveEngine {
        inner:     LanceEngine,
        fail_once: std::sync::atomic::AtomicBool,
    }

    struct PauseFirstRemoveEngine {
        inner:          LanceEngine,
        calls:          AtomicUsize,
        remove_started: Arc<Notify>,
        resume_first:   Arc<Notify>,
    }

    struct PauseAfterRemoveEngine {
        inner:           LanceEngine,
        calls:           AtomicUsize,
        remove_finished: Arc<Notify>,
        resume_first:    Arc<Notify>,
    }

    #[async_trait]
    impl TableEngine for PauseAfterRemoveEngine {
        fn kind(&self) -> &'static str { self.inner.kind() }

        async fn create(
            &self,
            location: &TableLocation,
            schema: SchemaRef,
        ) -> EngineResult<TableHandleRef> {
            self.inner.create(location, schema).await
        }

        async fn open(&self, location: &TableLocation) -> EngineResult<Option<TableHandleRef>> {
            self.inner.open(location).await
        }

        async fn remove(&self, location: &TableLocation) -> EngineResult<()> {
            self.inner.remove(location).await?;
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.remove_finished.notify_one();
                self.resume_first.notified().await;
            }
            Ok(())
        }

        async fn maintain(
            &self,
            location: &TableLocation,
            version: Version,
        ) -> EngineResult<Option<Version>> {
            self.inner.maintain(location, version).await
        }

        async fn retained_object_references(
            &self,
            location: &TableLocation,
            request: ObjectReferenceRequest,
        ) -> EngineResult<ObjectReferencePage> {
            self.inner
                .retained_object_references(location, request)
                .await
        }
    }

    #[async_trait]
    impl TableEngine for PauseFirstRemoveEngine {
        fn kind(&self) -> &'static str { self.inner.kind() }

        async fn create(
            &self,
            location: &TableLocation,
            schema: SchemaRef,
        ) -> EngineResult<TableHandleRef> {
            self.inner.create(location, schema).await
        }

        async fn open(&self, location: &TableLocation) -> EngineResult<Option<TableHandleRef>> {
            self.inner.open(location).await
        }

        async fn remove(&self, location: &TableLocation) -> EngineResult<()> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.remove_started.notify_one();
                self.resume_first.notified().await;
            }
            self.inner.remove(location).await
        }

        async fn maintain(
            &self,
            location: &TableLocation,
            version: Version,
        ) -> EngineResult<Option<Version>> {
            self.inner.maintain(location, version).await
        }

        async fn retained_object_references(
            &self,
            location: &TableLocation,
            request: ObjectReferenceRequest,
        ) -> EngineResult<ObjectReferencePage> {
            self.inner
                .retained_object_references(location, request)
                .await
        }
    }

    fn remove_one_file(path: &std::path::Path) -> std::io::Result<bool> {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                if remove_one_file(&entry.path())? {
                    return Ok(true);
                }
            } else if file_type.is_file() {
                std::fs::remove_file(entry.path())?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[async_trait]
    impl TableEngine for PartialRemoveEngine {
        fn kind(&self) -> &'static str { self.inner.kind() }

        async fn create(
            &self,
            location: &TableLocation,
            schema: SchemaRef,
        ) -> EngineResult<TableHandleRef> {
            self.inner.create(location, schema).await
        }

        async fn open(&self, location: &TableLocation) -> EngineResult<Option<TableHandleRef>> {
            self.inner.open(location).await
        }

        async fn remove(&self, location: &TableLocation) -> EngineResult<()> {
            if self.fail_once.swap(false, Ordering::SeqCst) {
                let removed = remove_one_file(std::path::Path::new(location.as_str()))
                    .map_err(lake_engine::EngineError::backend)?;
                assert!(
                    removed,
                    "partial cleanup must remove one real dataset object"
                );
                return Err(lake_engine::EngineError::backend(std::io::Error::other(
                    "injected failure after partial object deletion",
                )));
            }
            self.inner.remove(location).await
        }

        async fn maintain(
            &self,
            location: &TableLocation,
            version: Version,
        ) -> EngineResult<Option<Version>> {
            self.inner.maintain(location, version).await
        }

        async fn retained_object_references(
            &self,
            location: &TableLocation,
            request: ObjectReferenceRequest,
        ) -> EngineResult<ObjectReferencePage> {
            self.inner
                .retained_object_references(location, request)
                .await
        }
    }

    #[async_trait]
    impl TableEngine for PausedRemoveEngine {
        fn kind(&self) -> &'static str { self.inner.kind() }

        async fn create(
            &self,
            location: &TableLocation,
            schema: SchemaRef,
        ) -> EngineResult<TableHandleRef> {
            self.inner.create(location, schema).await
        }

        async fn open(&self, location: &TableLocation) -> EngineResult<Option<TableHandleRef>> {
            self.inner.open(location).await
        }

        async fn remove(&self, location: &TableLocation) -> EngineResult<()> {
            self.remove_started.notify_one();
            self.resume_remove.notified().await;
            self.inner.remove(location).await
        }

        async fn maintain(
            &self,
            location: &TableLocation,
            version: Version,
        ) -> EngineResult<Option<Version>> {
            self.inner.maintain(location, version).await
        }

        async fn retained_object_references(
            &self,
            location: &TableLocation,
            request: ObjectReferenceRequest,
        ) -> EngineResult<ObjectReferencePage> {
            self.inner
                .retained_object_references(location, request)
                .await
        }
    }

    #[tokio::test]
    async fn create_waits_for_inflight_drop_of_same_table() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine = Arc::new(PausedRemoveEngine {
            inner:          LanceEngine::new(),
            remove_started: Arc::new(Notify::new()),
            resume_remove:  Arc::new(Notify::new()),
        });
        let engine_ref: TableEngineRef = engine.clone();
        let metasrv = Arc::new(Metasrv::new(meta, engine_ref));
        let table = TableRef::new("robots", "arm");
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        let original = TableLocation::new(table_dir.path().join("old.lance").to_string_lossy());
        let replacement = TableLocation::new(table_dir.path().join("new.lance").to_string_lossy());

        metasrv
            .create_table(&table, original, schema.clone())
            .await
            .unwrap();

        let drop_task = tokio::spawn({
            let metasrv = metasrv.clone();
            let table = table.clone();
            async move { metasrv.drop_table(&table).await }
        });
        engine.remove_started.notified().await;

        let mut create_task = tokio::spawn({
            let metasrv = metasrv.clone();
            let table = table.clone();
            async move { metasrv.create_table(&table, replacement, schema).await }
        });
        tokio::select! {
            result = &mut create_task => {
                panic!("same-table create completed before drop released: {result:?}");
            }
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }

        engine.resume_remove.notify_one();
        drop_task.await.unwrap().unwrap();
        create_task.await.unwrap().unwrap();
        assert!(metasrv.resolve(&table).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn drop_resumes_after_tombstone_publication_crash() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let first = Metasrv::new(meta.clone(), engine.clone());
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new(table_dir.path().join("old.lance").to_string_lossy());
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        first
            .create_table(&table, location.clone(), schema)
            .await
            .unwrap();
        let registration = first.resolve(&table).await.unwrap().unwrap();
        let tombstone = drop_tombstone::DropTombstone::new(table.clone(), registration).unwrap();
        drop_tombstone::prepare(meta.as_ref(), &tombstone)
            .await
            .unwrap();

        let successor = Metasrv::new(meta.clone(), engine.clone());
        successor.drop_table(&table).await.unwrap();

        assert!(successor.resolve(&table).await.unwrap().is_none());
        assert!(engine.open(&location).await.unwrap().is_none());
        assert!(
            drop_tombstone::list_for_table(meta.as_ref(), &table)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn drop_persists_tombstone_before_registry_or_object_delete() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let raw: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new(table_dir.path().join("old.lance").to_string_lossy());
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        let initial = Metasrv::new(raw.clone(), engine.clone());
        initial
            .create_table(&table, location, schema)
            .await
            .unwrap();
        let expected = initial.resolve(&table).await.unwrap().unwrap();
        let probe = Arc::new(RegistryDeleteProbeMeta {
            inner: raw,
            table: table.clone(),
            expected,
            race_version: false,
            tombstone_seen: AtomicUsize::new(0),
        });
        let meta: MetaStoreRef = probe.clone();

        Metasrv::new(meta, engine).drop_table(&table).await.unwrap();

        assert_eq!(probe.tombstone_seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn same_incarnation_version_race_preserves_table_and_tombstone() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let raw: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new(table_dir.path().join("old.lance").to_string_lossy());
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        let initial = Metasrv::new(raw.clone(), engine.clone());
        initial
            .create_table(&table, location.clone(), schema)
            .await
            .unwrap();
        let expected = initial.resolve(&table).await.unwrap().unwrap();
        let incarnation = expected.incarnation_id().unwrap().to_owned();
        let probe = Arc::new(RegistryDeleteProbeMeta {
            inner: raw.clone(),
            table: table.clone(),
            expected,
            race_version: true,
            tombstone_seen: AtomicUsize::new(0),
        });
        let meta: MetaStoreRef = probe.clone();

        Metasrv::new(meta, engine.clone())
            .drop_table(&table)
            .await
            .expect_err("exact registry delete must lose the injected version race");

        let current = registry::get(raw.as_ref(), &table)
            .await
            .unwrap()
            .expect("racing registration remains");
        assert_eq!(current.incarnation_id(), Some(incarnation.as_str()));
        assert_eq!(current.current_version, Version(2));
        assert!(engine.open(&location).await.unwrap().is_some());
        assert_eq!(
            drop_tombstone::list_for_table(raw.as_ref(), &table)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(probe.tombstone_seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[ignore = "requires localstack DynamoDB; set LAKE_DYNAMODB_ENDPOINT and run with --ignored"]
    async fn dynamo_drop_resume_uses_point_tombstone_lookup() {
        let Ok(endpoint) = std::env::var("LAKE_DYNAMODB_ENDPOINT") else {
            return;
        };
        let dynamo_table = format!(
            "lake_drop_test_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dynamo = Arc::new(
            DynamoMeta::connect(Some(&endpoint), &dynamo_table)
                .await
                .unwrap(),
        );
        dynamo.ensure_table().await.unwrap();
        for index in 0..12 {
            assert!(
                dynamo
                    .cas(&format!("unrelated/{index:02}"), None, b"value")
                    .await
                    .unwrap()
            );
        }
        let meta: MetaStoreRef = dynamo;
        let table_dir = tempfile::tempdir().unwrap();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new(table_dir.path().join("old.lance").to_string_lossy());
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        let first = Metasrv::new(meta.clone(), engine.clone());
        first
            .create_table(&table, location.clone(), schema)
            .await
            .unwrap();
        let registration = first.resolve(&table).await.unwrap().unwrap();
        let tombstone = drop_tombstone::DropTombstone::new(table.clone(), registration).unwrap();
        drop_tombstone::prepare(meta.as_ref(), &tombstone)
            .await
            .unwrap();

        Metasrv::new(meta.clone(), engine.clone())
            .drop_table(&table)
            .await
            .unwrap();

        assert!(
            registry::get(meta.as_ref(), &table)
                .await
                .unwrap()
                .is_none()
        );
        assert!(engine.open(&location).await.unwrap().is_none());
        assert!(
            drop_tombstone::list_for_table(meta.as_ref(), &table)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn drop_resumes_after_partial_object_deletion() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine = Arc::new(PartialRemoveEngine {
            inner:     LanceEngine::new(),
            fail_once: std::sync::atomic::AtomicBool::new(true),
        });
        let engine_ref: TableEngineRef = engine.clone();
        let metasrv = Metasrv::new(meta.clone(), engine_ref);
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new(table_dir.path().join("old.lance").to_string_lossy());
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        metasrv
            .create_table(&table, location.clone(), schema)
            .await
            .unwrap();

        metasrv
            .drop_table(&table)
            .await
            .expect_err("first cleanup fails after deleting one object");
        assert!(metasrv.resolve(&table).await.unwrap().is_none());
        assert_eq!(
            drop_tombstone::list_for_table(meta.as_ref(), &table)
                .await
                .unwrap()
                .len(),
            1
        );

        metasrv.drop_table(&table).await.unwrap();
        assert!(engine.open(&location).await.unwrap().is_none());
        assert!(
            drop_tombstone::list_for_table(meta.as_ref(), &table)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn stale_drop_cannot_delete_recreated_table() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine = Arc::new(PauseFirstRemoveEngine {
            inner:          LanceEngine::new(),
            calls:          AtomicUsize::new(0),
            remove_started: Arc::new(Notify::new()),
            resume_first:   Arc::new(Notify::new()),
        });
        let engine_ref: TableEngineRef = engine.clone();
        let old_authority = Arc::new(Metasrv::new(meta.clone(), engine_ref.clone()));
        let successor = Metasrv::new(meta.clone(), engine_ref);
        let placement = TablePlacement::local(table_dir.path().to_path_buf());
        let table = TableRef::new("robots", "episodes");
        let old_location = placement.place(&table).unwrap();
        let new_location = placement.place(&table).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        old_authority
            .create_table(&table, old_location.clone(), schema.clone())
            .await
            .unwrap();

        let stale_drop = tokio::spawn({
            let authority = old_authority.clone();
            let table = table.clone();
            async move { authority.drop_table(&table).await }
        });
        engine.remove_started.notified().await;

        successor
            .create_table(&table, new_location.clone(), schema)
            .await
            .unwrap();
        engine.resume_first.notify_one();
        stale_drop.await.unwrap().unwrap();

        let replacement = successor.resolve(&table).await.unwrap().unwrap();
        assert_eq!(replacement.location, new_location);
        assert!(engine.open(&replacement.location).await.unwrap().is_some());
        assert!(engine.open(&old_location).await.unwrap().is_none());
        assert!(
            drop_tombstone::list_for_table(meta.as_ref(), &table)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn stale_leader_cannot_finalize_drop_after_takeover() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let raw: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let election_a = LeaseElection::new(raw.clone(), "a", Duration::from_millis(10));
        let LeaseStatus::Leader { guard: guard_a, .. } = election_a.campaign_at(0).await.unwrap()
        else {
            panic!("a acquires lease");
        };
        let leadership_a = Arc::new(Leadership::new());
        leadership_a.assume_guarded_leader("a", guard_a, Duration::from_mins(1));
        let engine = Arc::new(PauseAfterRemoveEngine {
            inner:           LanceEngine::new(),
            calls:           AtomicUsize::new(0),
            remove_finished: Arc::new(Notify::new()),
            resume_first:    Arc::new(Notify::new()),
        });
        let engine_ref: TableEngineRef = engine.clone();
        let authority_a =
            Metasrv::new(raw.clone(), engine_ref.clone()).fenced_for_server(leadership_a);
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new(table_dir.path().join("old.lance").to_string_lossy());
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        authority_a
            .create_table(&table, location, schema)
            .await
            .unwrap();

        let stale_drop = tokio::spawn({
            let authority = authority_a.clone();
            let table = table.clone();
            async move { authority.drop_table(&table).await }
        });
        engine.remove_finished.notified().await;

        let election_b = LeaseElection::new(raw.clone(), "b", Duration::from_millis(10));
        let LeaseStatus::Leader { guard: guard_b, .. } = election_b.campaign_at(20).await.unwrap()
        else {
            panic!("b takes over");
        };
        engine.resume_first.notify_one();
        stale_drop
            .await
            .unwrap()
            .expect_err("stale tombstone finalization must be fenced");
        assert_eq!(
            drop_tombstone::list_for_table(raw.as_ref(), &table)
                .await
                .unwrap()
                .len(),
            1
        );

        let leadership_b = Arc::new(Leadership::new());
        leadership_b.assume_guarded_leader("b", guard_b, Duration::from_mins(1));
        let authority_b = Metasrv::new(raw.clone(), engine_ref).fenced_for_server(leadership_b);
        authority_b.drop_table(&table).await.unwrap();
        assert!(
            drop_tombstone::list_for_table(raw.as_ref(), &table)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn production_metadata_mutations_use_guarded_store() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let rocks: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let recording = Arc::new(RecordingMeta {
            inner:           rocks,
            ordinary_cas:    AtomicUsize::new(0),
            ordinary_delete: AtomicUsize::new(0),
            guarded:         AtomicUsize::new(0),
        });
        let raw: MetaStoreRef = recording.clone();
        let election = LeaseElection::new(raw.clone(), "node-a", Duration::from_secs(10));
        let status = election.campaign_at(0).await.unwrap();
        let LeaseStatus::Leader { guard, .. } = status else {
            panic!("node-a must acquire the lease");
        };
        let leadership = Arc::new(Leadership::new());
        leadership.assume_guarded_leader("node-a", guard, Duration::from_mins(1));
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let authority = Metasrv::with_operation_policy(
            raw.clone(),
            engine,
            Duration::ZERO,
            DEFAULT_OPERATION_GC_PAGE_SIZE,
        )
        .fenced_for_server(leadership);

        recording.ordinary_cas.store(0, Ordering::SeqCst);
        recording.ordinary_delete.store(0, Ordering::SeqCst);
        recording.guarded.store(0, Ordering::SeqCst);
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        let location =
            TableLocation::new(table_dir.path().join("episodes.lance").to_string_lossy());
        authority
            .create_table(&table, location, schema.clone())
            .await
            .unwrap();
        let after_create = recording.guarded.load(Ordering::SeqCst);
        assert!(after_create > 0, "registry creation must be guarded");

        for _ in 0..3 {
            let operation = AppendOperation::builder()
                .tenant(TenantId::try_new("tenant-a").unwrap())
                .operation_id(AppendOperationId::generate())
                .payload_digest(
                    AppendPayloadDigest::parse(
                        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    )
                    .unwrap(),
                )
                .build();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
            ));
            authority.append(&table, &operation, stream).await.unwrap();
        }
        let after_append = recording.guarded.load(Ordering::SeqCst);
        assert!(
            after_append > after_create,
            "append records, fences, registry publication, and cleanup must be guarded"
        );

        maintenance::sweep(&authority).await;
        let after_maintenance = recording.guarded.load(Ordering::SeqCst);
        assert!(
            after_maintenance > after_append,
            "maintenance version publication must be guarded"
        );
        let gc = maintenance::sweep_operations_at(&authority, u64::MAX).await;
        assert!(gc.deleted > 0, "operation GC must exercise guarded deletes");
        let after_gc = recording.guarded.load(Ordering::SeqCst);
        assert!(after_gc > after_maintenance, "operation GC must be guarded");
        authority.drop_table(&table).await.unwrap();
        let after_drop = recording.guarded.load(Ordering::SeqCst);
        assert!(after_drop > after_gc, "registry deletion must be guarded");

        assert_eq!(recording.ordinary_cas.load(Ordering::SeqCst), 0);
        assert_eq!(recording.ordinary_delete.load(Ordering::SeqCst), 0);
        assert!(authority.resolve(&table).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn background_shutdown_aborts_owned_tasks_at_total_deadline() {
        let maintenance_resource = Arc::new(());
        let campaign_resource = Arc::new(());
        let maintenance = tokio::spawn({
            let resource = maintenance_resource.clone();
            async move {
                pending::<()>().await;
                drop(resource);
            }
        });
        let campaign = tokio::spawn({
            let resource = campaign_resource.clone();
            async move {
                pending::<()>().await;
                drop(resource);
            }
        });
        let grace = Duration::from_millis(20);

        let error = join_background_tasks_until(
            maintenance,
            campaign,
            tokio::time::Instant::now() + grace,
            grace,
        )
        .await
        .expect_err("stuck owned tasks must time out");

        assert!(matches!(
            error,
            MetasrvError::BackgroundDrainTimeout { grace: actual } if actual == grace
        ));
        assert_eq!(Arc::strong_count(&maintenance_resource), 1);
        assert_eq!(Arc::strong_count(&campaign_resource), 1);
    }

    #[tokio::test]
    async fn metasrv_shutdown_releases_listener_and_background_tasks() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let meta_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let observer = LeaseElection::new(meta.clone(), "observer", Duration::from_secs(10));
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Arc::new(Metasrv::new(meta, engine));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn({
            let metasrv = metasrv.clone();
            async move {
                serve_with_config_and_shutdown(
                    metasrv,
                    &addr.to_string(),
                    MetasrvServerConfig::new().with_shutdown_grace(Duration::from_millis(500)),
                    async move {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
            }
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if observer.current_leader().await.unwrap().as_deref() == Some(&addr.to_string()) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("metasrv binds and acquires leadership");

        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("metasrv joins within its grace period")
            .unwrap()
            .unwrap();

        assert_eq!(observer.current_leader().await.unwrap(), None);
        assert_eq!(Arc::strong_count(&metasrv), 1);
        std::net::TcpListener::bind(addr).expect("shutdown releases the listener");
    }
}
