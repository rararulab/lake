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
};

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use lake_common::{Namespace, TableName, TableRef};
use lake_engine::TableEngineRef;
use lake_meta::{MetaStoreRef, registry, registry::TableRegistration};
use moka::future::Cache;

use crate::schema::LakeSchema;

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
}

impl CatalogState {
    pub(crate) async fn registration(&self, table: &TableRef) -> Option<Arc<TableRegistration>> {
        if let Some(hit) = self.regs.get(table).await {
            return Some(hit);
        }
        let reg = registry::get(self.meta.as_ref(), table).await.ok()??;
        let reg = Arc::new(reg);
        self.regs.insert(table.clone(), reg.clone()).await;
        Some(reg)
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
                regs: Cache::new(100_000),
            }),
        }
    }

    pub fn state(&self) -> Arc<CatalogState> { self.state.clone() }

    /// Reload the listing snapshot from the registry. Call on startup and on
    /// a timer; DataFusion's sync `schema_names`/`table_names` read what this
    /// leaves behind, so they never block on the metastore.
    pub async fn refresh(&self) -> lake_meta::Result<()> {
        let namespaces = registry::list_namespaces(self.state.meta.as_ref()).await?;
        let mut snap = BTreeMap::new();
        for ns in namespaces {
            let tables = registry::list(self.state.meta.as_ref(), &ns).await?;
            snap.insert(ns, tables);
        }
        *self.state.snapshot.write().expect("snapshot lock poisoned") = snap;
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
