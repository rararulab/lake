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

//! Schema provider: resolves table names through the metastore.

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::SchemaProvider,
    datasource::{
        TableProvider,
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::{DataFusionError, Result as DfResult},
    execution::session_state::{SessionState, SessionStateBuilder},
};
use lake_meta::MetaStoreRef;

pub struct LakeSchema {
    meta:       MetaStoreRef,
    table_root: PathBuf,
    /// Only used for parquet schema inference.
    state:      SessionState,
}

impl LakeSchema {
    pub(crate) fn new(meta: MetaStoreRef, table_root: PathBuf) -> Self {
        Self {
            meta,
            table_root,
            state: SessionStateBuilder::new().with_default_features().build(),
        }
    }
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
    fn table_names(&self) -> Vec<String> {
        // ponytail: DataFusion's trait method is sync; block_on is safe here
        // because RocksMeta futures are ready immediately. Revisit with a
        // cached table list when the DynamoDB (network-bound) backend lands.
        futures::executor::block_on(self.meta.list_prefix("ptr/")).unwrap_or_default()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let Some(manifest) =
            lake_manifest::load_current(self.meta.as_ref(), &self.table_root, name)
                .await
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
        // ponytail: see table_names — same sync-trait bridge.
        futures::executor::block_on(self.meta.get(&format!("ptr/{name}")))
            .ok()
            .flatten()
            .is_some()
    }
}
