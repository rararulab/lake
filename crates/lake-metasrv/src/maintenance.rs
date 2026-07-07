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
                Ok(Some(reg)) => match metasrv.engine().maintain(&reg.location).await {
                    Ok(()) => tracing::debug!(%table, "maintained table"),
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
