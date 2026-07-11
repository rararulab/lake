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
mod leadership;
mod maintenance;

use std::{collections::HashMap, net::AddrParseError, sync::Arc, time::Duration};

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::{arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream};
use lake_catalog::create_table;
use lake_common::{Namespace, TableLocation, TableName, TableRef, Version};
use lake_engine::TableEngineRef;
use lake_flight::{ClientSecurity, ServerSecurity};
use lake_meta::{MetaStoreRef, registry, registry::TableRegistration};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::{
    control::MetasrvFlightService,
    election::LeaseElection,
    leadership::{Leadership, run_campaign_loop_until},
    maintenance::run_maintenance_loop_until,
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetasrvError {
    #[snafu(display("registry error"))]
    Registry { source: lake_meta::MetaError },

    #[snafu(display("create-table failed"))]
    Create { source: lake_catalog::CatalogError },

    #[snafu(display("engine error"))]
    Engine { source: lake_engine::EngineError },

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

    #[snafu(display("Metasrv Flight connections did not drain within {grace:?}"))]
    DrainTimeout { grace: Duration },

    #[snafu(display("Metasrv background task '{task}' failed"))]
    BackgroundTask {
        task:   &'static str,
        source: tokio::task::JoinError,
    },
}

pub type Result<T> = std::result::Result<T, MetasrvError>;

/// Network security for one Metasrv node and its follower-to-leader hop.
#[derive(Clone, Debug)]
pub struct MetasrvServerConfig {
    server_security: ServerSecurity,
    peer_security:   ClientSecurity,
    allow_insecure:  bool,
    shutdown_grace:  Duration,
}

impl MetasrvServerConfig {
    /// Explicit loopback development configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            server_security: ServerSecurity::insecure(),
            peer_security:   ClientSecurity::new(),
            allow_insecure:  false,
            shutdown_grace:  Duration::from_secs(30),
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

    /// Explicit deployment escape hatch when a trusted proxy terminates both
    /// TLS and authentication before Lake.
    #[must_use]
    pub const fn allow_insecure(mut self, allow: bool) -> Self {
        self.allow_insecure = allow;
        self
    }

    /// Bound how long existing Flight connections may drain during shutdown.
    #[must_use]
    pub const fn with_shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }
}

impl Default for MetasrvServerConfig {
    fn default() -> Self { Self::new() }
}

/// The registry authority. Holds the durable metastore and the storage
/// engine used to materialize new tables.
struct MetasrvInner {
    meta:        MetaStoreRef,
    engine:      TableEngineRef,
    /// One coordinator per table. Metadata writes are rare and the catalog's
    /// design ceiling is ~10^4 tables, so retaining these locks is bounded.
    table_locks: Mutex<HashMap<TableRef, Arc<Mutex<()>>>>,
}

#[derive(Clone)]
/// Cloneable handle to the registry authority and its per-table write
/// coordinators.
pub struct Metasrv {
    inner: Arc<MetasrvInner>,
}

impl Metasrv {
    pub fn new(meta: MetaStoreRef, engine: TableEngineRef) -> Self {
        Self {
            inner: Arc::new(MetasrvInner {
                meta,
                engine,
                table_locks: Mutex::new(HashMap::new()),
            }),
        }
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
        batches: SendableRecordBatchStream,
    ) -> Result<Version> {
        let _guard = self.lock_table(table).await;
        let reg = self.resolve(table).await?.context(NotFoundSnafu {
            table: table.to_string(),
        })?;
        let handle = self
            .inner
            .engine
            .open(&reg.location)
            .await
            .context(EngineSnafu)?
            .context(NotFoundSnafu {
                table: table.to_string(),
            })?;
        let new_version = handle.append(batches).await.context(EngineSnafu)?;
        registry::set_version(self.meta().as_ref(), table, &reg, new_version)
            .await
            .context(RegistrySnafu)?;
        Ok(new_version)
    }

    /// Drop a table: delete its data via the engine, then remove the registry
    /// entry. Idempotent — dropping an absent table is a no-op. Data-first so a
    /// crash leaves at worst orphaned data (reclaimable by GC), never a
    /// registry entry pointing at deleted data.
    ///
    /// ponytail: query-layer caches self-heal (a dropped table's dataset is
    /// gone, so `open` returns `None`) and refresh drops it from listings; a
    /// push-based cache invalidation across instances is a v2 concern.
    pub async fn drop_table(&self, table: &TableRef) -> Result<()> {
        let _guard = self.lock_table(table).await;
        let Some(reg) = self.resolve(table).await? else {
            return Ok(());
        };
        self.inner
            .engine
            .remove(&reg.location)
            .await
            .context(EngineSnafu)?;
        registry::delete(self.meta().as_ref(), table, &reg)
            .await
            .context(RegistrySnafu)?;
        Ok(())
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
    let maintenance_shutdown = CancellationToken::new();
    let campaign_shutdown = CancellationToken::new();
    let maintenance = tokio::spawn(run_maintenance_loop_until(
        metasrv.clone(),
        leadership.clone(),
        maintenance_shutdown.clone(),
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

    let server_result = tokio::select! {
        result = server.as_mut() => result.context(ServeSnafu),
        () = shutdown.as_mut() => {
            maintenance_shutdown.cancel();
            server_shutdown.cancel();
            match tokio::time::timeout(config.shutdown_grace, server.as_mut()).await {
                Ok(result) => result.context(ServeSnafu),
                Err(_) => Err(MetasrvError::DrainTimeout { grace: config.shutdown_grace }),
            }
        }
    };

    // Dropping the server first guarantees no accepted write can outlive the
    // leadership lease. Only then may the campaign resign.
    drop(server);
    maintenance_shutdown.cancel();
    campaign_shutdown.cancel();
    let maintenance_result = maintenance
        .await
        .map_err(|source| MetasrvError::BackgroundTask {
            task: "maintenance",
            source,
        });
    let campaign_result = campaign
        .await
        .map_err(|source| MetasrvError::BackgroundTask {
            task: "leadership-campaign",
            source,
        });
    server_result?;
    maintenance_result?;
    campaign_result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use lake_engine::{Result as EngineResult, TableEngine, TableEngineRef, TableHandleRef};
    use lake_engine_lance::LanceEngine;
    use lake_meta::RocksMeta;
    use tokio::sync::{Notify, oneshot};

    use super::*;

    struct PausedRemoveEngine {
        inner:          LanceEngine,
        remove_started: Arc<Notify>,
        resume_remove:  Arc<Notify>,
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
