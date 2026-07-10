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
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use lake_common::TableRef;
use lake_meta::{MetaError, registry};

use crate::Metasrv;

/// How often the maintenance loop wakes to consider a sweep.
const MAINTENANCE_INTERVAL: Duration = Duration::from_mins(1);

/// Drive periodic maintenance forever, running a sweep only while `is_leader`.
///
/// Sleeps [`MAINTENANCE_INTERVAL`] between rounds. A round is skipped entirely
/// unless this node currently holds leadership, so standbys stay idle and only
/// the leader does housekeeping.
pub(crate) async fn run_maintenance_loop(metasrv: Arc<Metasrv>, is_leader: Arc<AtomicBool>) {
    loop {
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
        if !is_leader.load(Ordering::Relaxed) {
            continue;
        }
        sweep(&metasrv).await;
    }
}

/// Run one maintenance sweep over every registered table.
///
/// Each step degrades gracefully: a failed listing logs and moves on, and a
/// per-table `maintain` error is logged and skipped so the sweep continues.
async fn sweep(metasrv: &Metasrv) {
    let namespaces = match metasrv.list_namespaces().await {
        Ok(namespaces) => namespaces,
        Err(err) => {
            tracing::warn!(error = %err, "maintenance sweep: listing namespaces failed");
            return;
        }
    };

    for namespace in namespaces {
        let tables = match metasrv.list_tables(&namespace).await {
            Ok(tables) => tables,
            Err(err) => {
                tracing::warn!(
                    namespace = %namespace.0,
                    error = %err,
                    "maintenance sweep: listing tables failed; skipping namespace"
                );
                continue;
            }
        };

        for name in tables {
            let table = TableRef::new(namespace.0.clone(), name.0);
            match metasrv.resolve(&table).await {
                Ok(Some(reg)) => match metasrv
                    .engine()
                    .maintain(&reg.location, reg.current_version)
                    .await
                {
                    Ok(Some(version)) => match registry::set_version(
                        metasrv.meta().as_ref(),
                        &table,
                        &reg,
                        version,
                    )
                    .await
                    {
                        Ok(()) => tracing::debug!(%table, %version, "maintained table"),
                        Err(MetaError::Conflict { .. }) => {
                            tracing::debug!(%table, %version, "maintenance result lost registry CAS")
                        }
                        Err(err) => {
                            tracing::warn!(%table, error = %err, "publishing maintenance failed")
                        }
                    },
                    Ok(None) => tracing::debug!(%table, "table needs no maintenance"),
                    Err(err) => {
                        tracing::warn!(%table, error = %err, "maintenance failed for table");
                    }
                },
                // Dropped between listing and resolve — nothing to maintain.
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(%table, error = %err, "maintenance sweep: resolve failed");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::{
            array::{Int64Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        },
        error::DataFusionError,
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use lake_common::{TableLocation, TableRef};
    use lake_engine::TableEngineRef;
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaStoreRef, RocksMeta};

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[tokio::test]
    async fn sweep_advances_registry_to_maintenance_version() {
        let meta_dir = tempfile::tempdir().unwrap();
        let table_dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta, engine.clone());
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new(table_dir.path().join("episodes.lance").to_string_lossy());

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
            metasrv.append(&table, stream).await.unwrap();
        }

        let before = metasrv.resolve(&table).await.unwrap().unwrap().current_version;
        sweep(&metasrv).await;
        let after = metasrv.resolve(&table).await.unwrap().unwrap().current_version;
        let engine_version = engine
            .open(&location)
            .await
            .unwrap()
            .unwrap()
            .current_version();

        assert!(engine_version > before, "compaction must create a new version");
        assert_eq!(after, engine_version, "registry must publish maintenance commit");
    }
}
