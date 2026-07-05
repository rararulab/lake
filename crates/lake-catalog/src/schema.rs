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

//! Per-namespace `SchemaProvider`.

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result as DfResult},
};
use lake_common::{Namespace, TableName, TableRef};

use crate::catalog::CatalogState;

/// A DataFusion schema = one lake namespace.
pub struct LakeSchema {
    namespace: Namespace,
    state:     Arc<CatalogState>,
}

impl LakeSchema {
    pub(crate) fn new(namespace: Namespace, state: Arc<CatalogState>) -> Self {
        Self { namespace, state }
    }

    fn table_ref(&self, name: &str) -> TableRef {
        TableRef {
            namespace: self.namespace.clone(),
            name:      TableName(name.to_string()),
        }
    }
}

impl std::fmt::Debug for LakeSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSchema")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for LakeSchema {
    fn as_any(&self) -> &dyn Any { self }

    fn table_names(&self) -> Vec<String> {
        // Reads the cached snapshot — never blocks on the metastore.
        self.state
            .snapshot
            .read()
            .expect("snapshot lock poisoned")
            .get(&self.namespace)
            .map(|tables| tables.iter().map(|t| t.0.clone()).collect())
            .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let table = self.table_ref(name);
        let Some(reg) = self.state.registration(&table).await else {
            return Ok(None);
        };
        let handle = self
            .state
            .engine
            .open(&reg.location)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(handle.map(|h| h.table_provider(reg.current_version)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.state
            .snapshot
            .read()
            .expect("snapshot lock poisoned")
            .get(&self.namespace)
            .is_some_and(|tables| tables.iter().any(|t| t.0 == name))
    }
}
