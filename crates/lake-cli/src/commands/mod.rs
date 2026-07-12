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

pub mod client;
pub mod gc;
pub mod ingest;
mod limits;
mod security;
pub mod selftest;
pub mod serve;
pub mod sql;
pub mod table;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use lake_common::{ManagedStageDescriptor, TableLocation, TableRef};
use lake_engine::TableEngineRef;
use lake_engine_lance::{LanceEngine, LanceMaintenancePolicy};
use lake_meta::{DynamoMeta, MetaStoreRef, RocksMeta};
use lake_metasrv::{Metasrv, TablePlacement};

use self::limits::{lance_maintenance_policy_from_env, operation_policy_from_env};

/// Shared, process-wide handles. Built from `--data-dir` (local) or the
/// `LAKE_S3_BUCKET`/`LAKE_DYNAMODB_*`/`AWS_*` environment (cloud).
pub struct Context {
    pub meta:        MetaStoreRef,
    pub engine:      TableEngineRef,
    pub metasrv:     Arc<Metasrv>,
    table_placement: TablePlacement,
    managed_stage:   ManagedStageDescriptor,
}

impl Context {
    pub async fn open(data_dir: &str) -> anyhow::Result<Self> {
        let maintenance_policy = lance_maintenance_policy_from_env()?;
        match std::env::var("LAKE_S3_BUCKET") {
            Ok(bucket) => Self::open_cloud(bucket, maintenance_policy).await,
            Err(_) => Self::open_local(data_dir, maintenance_policy),
        }
    }

    /// Dev path: RocksDB + local-filesystem Lance datasets.
    fn open_local(
        data_dir: &str,
        maintenance_policy: LanceMaintenancePolicy,
    ) -> anyhow::Result<Self> {
        let root = PathBuf::from(data_dir);
        std::fs::create_dir_all(&root)?;
        let root = std::fs::canonicalize(root)?;
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.join("meta"))?);
        let engine: TableEngineRef =
            Arc::new(LanceEngine::new().with_maintenance_policy(maintenance_policy));
        let managed_stage = local_managed_stage_descriptor(&root);
        Self::wire(
            meta,
            engine,
            TablePlacement::local(root.join("tables")),
            managed_stage,
        )
    }

    /// Prod path: DynamoDB registry + Lance datasets on S3.
    async fn open_cloud(
        bucket: String,
        maintenance_policy: LanceMaintenancePolicy,
    ) -> anyhow::Result<Self> {
        let endpoint = std::env::var("LAKE_DYNAMODB_ENDPOINT").ok();
        let table = std::env::var("LAKE_DYNAMODB_TABLE").unwrap_or_else(|_| "lake_registry".into());
        let dynamo = DynamoMeta::connect(endpoint.as_deref(), &table).await?;
        dynamo.ensure_table().await?;
        let meta: MetaStoreRef = Arc::new(dynamo);
        let engine: TableEngineRef = Arc::new(
            LanceEngine::for_object_store(meta.clone(), s3_storage_options())
                .with_maintenance_policy(maintenance_policy),
        );
        let table_prefix = std::env::var("LAKE_TABLE_PREFIX").unwrap_or_default();
        let managed_prefix = std::env::var("LAKE_MANAGED_OBJECT_PREFIX")
            .unwrap_or_else(|_| "managed-objects".to_owned());
        let managed_stage = s3_managed_stage_descriptor(
            &bucket,
            &managed_prefix,
            std::env::var("AWS_REGION").ok(),
            std::env::var("LAKE_S3_ENDPOINT").ok(),
        );
        let table_placement = TablePlacement::s3(&bucket, table_prefix)?;
        Self::wire(meta, engine, table_placement, managed_stage)
    }

    fn wire(
        meta: MetaStoreRef,
        engine: TableEngineRef,
        table_placement: TablePlacement,
        managed_stage: ManagedStageDescriptor,
    ) -> anyhow::Result<Self> {
        let (operation_retention, operation_gc_page_size) = operation_policy_from_env()?;
        let metasrv = Arc::new(Metasrv::with_operation_policy(
            meta.clone(),
            engine.clone(),
            operation_retention,
            operation_gc_page_size,
        ));
        Ok(Self {
            meta,
            engine,
            metasrv,
            table_placement,
            managed_stage,
        })
    }

    /// Credential-free managed `FILE` stage advertised by query.
    pub fn managed_stage(&self) -> &ManagedStageDescriptor { &self.managed_stage }

    /// Trusted placement policy shared with the metadata control plane.
    pub fn table_placement(&self) -> &TablePlacement { &self.table_placement }

    /// The Lance dataset location for a table (local path or `s3://` URI).
    pub fn location(&self, table: &TableRef) -> anyhow::Result<TableLocation> {
        Ok(self.table_placement.place(table)?)
    }
}

fn local_managed_stage_descriptor(root: &std::path::Path) -> ManagedStageDescriptor {
    ManagedStageDescriptor::local(root.join("managed-objects").to_string_lossy().into_owned())
}

fn s3_managed_stage_descriptor(
    bucket: &str,
    prefix: &str,
    region: Option<String>,
    endpoint: Option<String>,
) -> ManagedStageDescriptor {
    let force_path_style = endpoint.is_some();
    ManagedStageDescriptor::s3(bucket, prefix, region, endpoint, force_path_style)
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

#[cfg(test)]
mod managed_stage_tests {
    use std::path::Path;

    use lake_common::ManagedStageBackend;

    use super::{local_managed_stage_descriptor, s3_managed_stage_descriptor};

    #[test]
    fn local_query_advertises_a_sibling_managed_object_directory() {
        let descriptor = local_managed_stage_descriptor(Path::new("/var/lib/lake"));

        assert!(matches!(
            descriptor.backend(),
            ManagedStageBackend::Local { root }
                if root == "/var/lib/lake/managed-objects"
        ));
    }

    #[test]
    fn cloud_query_advertises_s3_without_credentials() {
        let descriptor = s3_managed_stage_descriptor(
            "embodied-data",
            "episodes/files",
            Some("us-east-1".to_owned()),
            Some("http://s3.internal:4566".to_owned()),
        );

        assert!(matches!(
            descriptor.backend(),
            ManagedStageBackend::S3 {
                bucket,
                prefix,
                region,
                endpoint,
                force_path_style: true,
            } if bucket == "embodied-data"
                && prefix == "episodes/files"
                && region.as_deref() == Some("us-east-1")
                && endpoint.as_deref() == Some("http://s3.internal:4566")
        ));
        let wire = descriptor.to_wire().expect("encode descriptor");
        let json = std::str::from_utf8(&wire).expect("JSON wire");
        assert!(!json.contains("access_key"));
        assert!(!json.contains("secret"));
    }
}
