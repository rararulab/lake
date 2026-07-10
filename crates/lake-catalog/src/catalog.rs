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

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use lake_common::{Namespace, TableName, TableRef};
use lake_engine::TableEngineRef;
use lake_meta::{MetaStoreRef, registry, registry::TableRegistration};
use moka::future::Cache;
use tokio::{sync::Mutex, time::Instant};

use crate::schema::LakeSchema;

/// Bound how long a resolved registration can hide a registry version update.
const REGISTRATION_CACHE_TTL: Duration = Duration::from_secs(5);

/// Shared state behind the catalog: the metastore (registry authority), the
/// storage engine, a cached listing snapshot, and a per-table registration
/// cache.
pub struct CatalogState {
    pub(crate) meta:     MetaStoreRef,
    pub(crate) engine:   TableEngineRef,
    /// namespace -> table names. Read by DataFusion's sync listing methods,
    /// so it must never require I/O. Refreshed by [`LakeCatalog::refresh`].
    pub(crate) snapshot: RwLock<BTreeMap<Namespace, Vec<TableName>>>,
    /// table -> registration; shields the registry from per-query load.
    pub(crate) regs:     Cache<TableRef, Arc<TableRegistration>>,
    /// Serializes refreshes and records when the listing snapshot was loaded.
    refreshed_at:        Mutex<Option<Instant>>,
}

impl CatalogState {
    pub(crate) async fn registration(
        &self,
        table: &TableRef,
    ) -> lake_meta::Result<Option<Arc<TableRegistration>>> {
        if let Some(hit) = self.regs.get(table).await {
            return Ok(Some(hit));
        }
        let Some(reg) = registry::get(self.meta.as_ref(), table).await? else {
            return Ok(None);
        };
        let reg = Arc::new(reg);
        self.regs.insert(table.clone(), reg.clone()).await;
        Ok(Some(reg))
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
        Self {
            state: Arc::new(CatalogState {
                meta,
                engine,
                snapshot: RwLock::new(BTreeMap::new()),
                regs: Cache::builder()
                    .max_capacity(100_000)
                    .time_to_live(REGISTRATION_CACHE_TTL)
                    .build(),
                refreshed_at: Mutex::new(None),
            }),
        }
    }

    pub fn state(&self) -> Arc<CatalogState> { self.state.clone() }

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

        let namespaces = registry::list_namespaces(self.state.meta.as_ref()).await?;
        let mut snap = BTreeMap::new();
        for ns in namespaces {
            let tables = registry::list(self.state.meta.as_ref(), &ns).await?;
            snap.insert(ns, tables);
        }
        *self.state.snapshot.write().expect("snapshot lock poisoned") = snap;
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
    use async_trait::async_trait;
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaError, MetaStore};

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
}
