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

//! DataFusion catalog backed by the KV metastore. Table resolution: KV
//! version pointer -> immutable manifest -> parquet file list.

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider},
    datasource::{
        TableProvider,
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::{DataFusionError, Result as DfResult},
    execution::session_state::{SessionState, SessionStateBuilder},
};

use crate::{manifest, meta::MetaStoreRef};

#[derive(Debug)]
pub struct LakeCatalog {
    schema: Arc<LakeSchema>,
}

impl LakeCatalog {
    pub fn new(meta: MetaStoreRef, table_root: PathBuf) -> Self {
        Self {
            schema: Arc::new(LakeSchema {
                meta,
                table_root,
                state: SessionStateBuilder::new().with_default_features().build(),
            }),
        }
    }
}

impl CatalogProvider for LakeCatalog {
    fn schema_names(&self) -> Vec<String> { vec!["public".to_string()] }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        (name == "public").then(|| self.schema.clone() as _)
    }
}

pub struct LakeSchema {
    meta:       MetaStoreRef,
    table_root: PathBuf,
    /// Only used for parquet schema inference.
    state:      SessionState,
}

impl std::fmt::Debug for LakeSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSchema")
            .field("table_root", &self.table_root)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for LakeSchema {
    fn table_names(&self) -> Vec<String> { self.meta.list_prefix("ptr/").unwrap_or_default() }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let Some(manifest) = manifest::load_current(self.meta.as_ref(), &self.table_root, name)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        else {
            return Ok(None);
        };
        // ponytail: no caching — every query re-reads pointer + manifest.
        // Manifests are immutable, so add a (table, version) -> provider
        // cache when read QPS matters.
        let urls = manifest
            .files
            .iter()
            .map(|f| ListingTableUrl::parse(f))
            .collect::<DfResult<Vec<_>>>()?;
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .infer_schema(&self.state)
            .await?;
        Ok(Some(Arc::new(ListingTable::try_new(config)?)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.meta
            .get(&format!("ptr/{name}"))
            .ok()
            .flatten()
            .is_some()
    }
}
