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

//! Top-level catalog: one `public` schema over the metastore.

use std::{path::PathBuf, sync::Arc};

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use lake_meta::MetaStoreRef;

use crate::schema::LakeSchema;

#[derive(Debug)]
pub struct LakeCatalog {
    schema: Arc<LakeSchema>,
}

impl LakeCatalog {
    pub fn new(meta: MetaStoreRef, table_root: PathBuf) -> Self {
        Self {
            schema: Arc::new(LakeSchema::new(meta, table_root)),
        }
    }
}

impl CatalogProvider for LakeCatalog {
    fn schema_names(&self) -> Vec<String> { vec!["public".to_string()] }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        (name == "public").then(|| self.schema.clone() as _)
    }
}
