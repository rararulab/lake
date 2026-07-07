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
//!
//! Two storage modes, selected by environment:
//!
//! - **local** (default) — RocksDB metastore and local-filesystem Lance
//!   datasets under `--data-dir`. Zero config; the dev/laptop path.
//! - **cloud** — set `LAKE_S3_BUCKET` to use `DynamoMeta` (the prod HA
//!   registry) with Lance datasets on S3. The DynamoDB endpoint/table come from
//!   `LAKE_DYNAMODB_*`, and the S3 endpoint plus credentials from
//!   `LAKE_S3_ENDPOINT` and the standard `AWS_*` variables.

pub mod ingest;
pub mod selftest;
pub mod serve;
pub mod sql;
pub mod table;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use lake_common::{TableLocation, TableRef};
use lake_engine::TableEngineRef;
use lake_engine_lance::LanceEngine;
use lake_meta::{DynamoMeta, MetaStoreRef, RocksMeta};
use lake_metasrv::Metasrv;

/// Where table data lives — decides how `location()` names a table.
enum Storage {
    Local { table_root: PathBuf },
    S3 { bucket: String },
}

/// Shared, process-wide handles. Built from `--data-dir` (local) or the
/// `LAKE_S3_BUCKET`/`LAKE_DYNAMODB_*`/`AWS_*` environment (cloud).
pub struct Context {
    pub meta:    MetaStoreRef,
    pub engine:  TableEngineRef,
    pub metasrv: Arc<Metasrv>,
    storage:     Storage,
}

impl Context {
    pub async fn open(data_dir: &str) -> anyhow::Result<Self> {
        match std::env::var("LAKE_S3_BUCKET") {
            Ok(bucket) => Self::open_cloud(bucket).await,
            Err(_) => Self::open_local(data_dir),
        }
    }

    /// Dev path: RocksDB + local-filesystem Lance datasets.
    fn open_local(data_dir: &str) -> anyhow::Result<Self> {
        let root = PathBuf::from(data_dir);
        std::fs::create_dir_all(&root)?;
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.join("meta"))?);
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        Ok(Self::wire(
            meta,
            engine,
            Storage::Local {
                table_root: root.join("tables"),
            },
        ))
    }

    /// Prod path: DynamoDB registry + Lance datasets on S3.
    async fn open_cloud(bucket: String) -> anyhow::Result<Self> {
        let endpoint = std::env::var("LAKE_DYNAMODB_ENDPOINT").ok();
        let table = std::env::var("LAKE_DYNAMODB_TABLE").unwrap_or_else(|_| "lake_registry".into());
        let dynamo = DynamoMeta::connect(endpoint.as_deref(), &table).await?;
        dynamo.ensure_table().await?;
        let meta: MetaStoreRef = Arc::new(dynamo);
        let engine: TableEngineRef = Arc::new(LanceEngine::for_object_store(
            meta.clone(),
            s3_storage_options(),
        ));
        Ok(Self::wire(meta, engine, Storage::S3 { bucket }))
    }

    fn wire(meta: MetaStoreRef, engine: TableEngineRef, storage: Storage) -> Self {
        let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        Self {
            meta,
            engine,
            metasrv,
            storage,
        }
    }

    /// The Lance dataset location for a table (local path or `s3://` URI).
    pub fn location(&self, table: &TableRef) -> TableLocation {
        match &self.storage {
            Storage::Local { table_root } => {
                let path = table_root
                    .join(&table.namespace.0)
                    .join(format!("{}.lance", table.name.0));
                TableLocation::new(path.to_string_lossy().to_string())
            }
            Storage::S3 { bucket } => TableLocation::new(format!(
                "s3://{bucket}/{}/{}.lance",
                table.namespace.0, table.name.0
            )),
        }
    }
}

/// object_store S3 config keys from the environment. `LAKE_S3_ENDPOINT` (e.g.
/// localstack) switches on path-style + http; credentials/region come from the
/// standard `AWS_*` variables.
fn s3_storage_options() -> HashMap<String, String> {
    let mut opts = HashMap::new();
    // Direct object_store key <- env var mappings.
    //
    // `proxy_excludes` is an escape hatch for ambient HTTP-proxy env vars:
    // lance-io folds every env var (lowercased) into object_store's config, so
    // a `PROXY_URL` in the environment silently routes S3 through that proxy;
    // object_store's bypass key is `proxy_excludes`, which no standard env var
    // maps to. Set `LAKE_S3_PROXY_EXCLUDES` (e.g. for a private/loopback
    // endpoint behind a corporate proxy) to restore direct connections.
    //
    // The drop path (`engine.remove`) builds object_store directly rather than
    // through lance-io, so its client falls back to the system/`reqwest` proxy;
    // behind a proxy, also set the standard `NO_PROXY` for the endpoint host.
    for (key, var) in [
        ("aws_access_key_id", "AWS_ACCESS_KEY_ID"),
        ("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY"),
        ("aws_region", "AWS_REGION"),
        ("proxy_excludes", "LAKE_S3_PROXY_EXCLUDES"),
    ] {
        if let Ok(v) = std::env::var(var) {
            opts.insert(key.to_owned(), v);
        }
    }
    if let Ok(endpoint) = std::env::var("LAKE_S3_ENDPOINT") {
        opts.insert("aws_endpoint".to_owned(), endpoint);
        opts.insert(
            "aws_virtual_hosted_style_request".to_owned(),
            "false".to_owned(),
        );
        opts.insert("aws_allow_http".to_owned(), "true".to_owned());
    }
    opts
}
