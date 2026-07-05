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

//! CLI command handlers and the shared context that wires the tiers.

pub mod selftest;
pub mod serve;
pub mod sql;
pub mod table;

use std::{path::PathBuf, sync::Arc};

use lake_common::{TableLocation, TableRef};
use lake_engine::TableEngineRef;
use lake_engine_lance::LanceEngine;
use lake_meta::{MetaStoreRef, RocksMeta};
use lake_metasrv::Metasrv;

/// Shared, process-wide handles built from `--data-dir`. In dev this is a
/// local RocksDB metastore + Lance datasets on the local filesystem.
pub struct Context {
    pub meta:       MetaStoreRef,
    pub engine:     TableEngineRef,
    pub metasrv:    Arc<Metasrv>,
    pub table_root: PathBuf,
}

impl Context {
    pub fn open(data_dir: &str) -> anyhow::Result<Self> {
        let root = PathBuf::from(data_dir);
        std::fs::create_dir_all(&root)?;
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.join("meta"))?);
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        Ok(Self {
            meta,
            engine,
            metasrv,
            table_root: root.join("tables"),
        })
    }

    /// The on-disk Lance dataset location for a table.
    pub fn location(&self, table: &TableRef) -> TableLocation {
        let path = self
            .table_root
            .join(&table.namespace.0)
            .join(format!("{}.lance", table.name.0));
        TableLocation::new(path.to_string_lossy().to_string())
    }
}
