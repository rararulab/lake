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
//! resolve, list, and (future) commit coordination. It is a bounded,
//! leader-elected tier, NOT a fan-out one: the query layer shields it behind
//! a cache, so it sees only cache-miss and write traffic. See
//! `docs/architecture.md`.
//!
//! ponytail: v0 is a single instance with no leader election and an
//! in-process API. The gRPC wire surface and lease-in-KV election are v1/v2
//! — the authority logic here is what they will wrap.

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
use lake_meta::{MetaStoreRef, registry, registry::TableRegistration};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tonic::transport::Server;

use crate::{
    control::MetasrvFlightService,
    election::LeaseElection,
    leadership::{Leadership, run_campaign_loop},
    maintenance::run_maintenance_loop,
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
}

pub type Result<T> = std::result::Result<T, MetasrvError>;

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
    let election = LeaseElection::new(metasrv.meta().clone(), addr, Duration::from_secs(10));
    let leadership = Arc::new(Leadership::new());
    // The maintenance sweep gates on the same deadline-aware leadership state
    // as writes, so an expired local lease cannot authorize housekeeping.
    tokio::spawn(run_maintenance_loop(metasrv.clone(), leadership.clone()));
    tokio::spawn(run_campaign_loop(election, leadership.clone()));

    let svc = MetasrvFlightService {
        metasrv,
        leadership,
        own_addr: addr.to_string(),
    };

    let socket = addr.parse().context(AddressSnafu { addr })?;
    tracing::info!(
        %addr,
        "metasrv control plane ready (Flight do_action; writes gated on leadership)"
    );
    Server::builder()
        .add_service(FlightServiceServer::new(svc))
        .serve(socket)
        .await
        .context(ServeSnafu)?;
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
    use tokio::sync::Notify;

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
}
