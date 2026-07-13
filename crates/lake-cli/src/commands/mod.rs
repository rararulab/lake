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
//! - **cloud** — set `LAKE_S3_BUCKET` to use separate `DynamoMeta` authorities
//!   for the prod HA registry (`LAKE_DYNAMODB_TABLE`) and Lance physical
//!   manifests (`LAKE_MANIFEST_DYNAMODB_TABLE`), with datasets on S3. Query
//!   opens only the manifest authority without provisioning; Metasrv opens
//!   both.

pub mod catalog_finalize;
pub mod client;
pub mod dynamo_migrate;
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

const DEFAULT_REGISTRY_TABLE: &str = "lake_registry";
const DEFAULT_MANIFEST_TABLE: &str = "lake_manifests";
const DEFAULT_ASYNC_TABLE: &str = "lake_async_queries";
const DYNAMO_V2_TABLE_SUFFIX_BYTES: usize = "_prefix_v2".len();

#[derive(Clone, Debug, Eq, PartialEq)]
struct CloudStoragePlan {
    registry:    String,
    manifest:    String,
    async_state: Option<String>,
}

impl CloudStoragePlan {
    fn try_new(
        registry_table: impl Into<String>,
        manifest_table: impl Into<String>,
        async_table: Option<impl Into<String>>,
    ) -> anyhow::Result<Self> {
        let registry_table = registry_table.into();
        let manifest_table = manifest_table.into();
        let async_table = async_table.map(Into::into);
        validate_dynamo_table_name("LAKE_DYNAMODB_TABLE", &registry_table)?;
        validate_dynamo_table_name("LAKE_MANIFEST_DYNAMODB_TABLE", &manifest_table)?;
        if let Some(table) = &async_table {
            validate_dynamo_table_name("LAKE_ASYNC_DYNAMODB_TABLE", table)?;
        }
        ensure_disjoint_authorities([
            ("catalog", Some(registry_table.as_str())),
            ("manifest", Some(manifest_table.as_str())),
            ("async", async_table.as_deref()),
        ])?;
        Ok(Self {
            registry:    registry_table,
            manifest:    manifest_table,
            async_state: async_table,
        })
    }

    fn from_env(async_enabled: bool) -> anyhow::Result<Self> {
        Self::try_new(
            std::env::var("LAKE_DYNAMODB_TABLE")
                .unwrap_or_else(|_| DEFAULT_REGISTRY_TABLE.to_owned()),
            std::env::var("LAKE_MANIFEST_DYNAMODB_TABLE")
                .unwrap_or_else(|_| DEFAULT_MANIFEST_TABLE.to_owned()),
            async_enabled.then(|| {
                std::env::var("LAKE_ASYNC_DYNAMODB_TABLE")
                    .unwrap_or_else(|_| DEFAULT_ASYNC_TABLE.to_owned())
            }),
        )
    }

    fn metadata_authorities(&self) -> [&str; 2] { [&self.registry, &self.manifest] }

    fn query_authorities(&self) -> [&str; 1] { [&self.manifest] }

    fn async_authority(&self) -> Option<&str> { self.async_state.as_deref() }
}

fn ensure_disjoint_authorities<const N: usize>(
    authorities: [(&str, Option<&str>); N],
) -> anyhow::Result<()> {
    let mut physical = std::collections::BTreeMap::new();
    for (authority, base) in authorities {
        let Some(base) = base else { continue };
        for table in [base.to_owned(), format!("{base}_prefix_v2")] {
            if let Some(previous) = physical.insert(table.clone(), authority) {
                anyhow::bail!(
                    "{authority} and {previous} DynamoDB authorities overlap at physical table \
                     '{table}'"
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn async_queries_enabled_from_env() -> anyhow::Result<bool> {
    match std::env::var("LAKE_ASYNC_QUERIES") {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(error.into()),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => anyhow::bail!("LAKE_ASYNC_QUERIES must be a boolean"),
        },
    }
}

fn validate_dynamo_table_name(variable: &str, table: &str) -> anyhow::Result<()> {
    let maximum = 255 - DYNAMO_V2_TABLE_SUFFIX_BYTES;
    anyhow::ensure!(
        (3..=maximum).contains(&table.len())
            && table
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')),
        "{variable} must be a valid DynamoDB table name of 3..={maximum} ASCII characters"
    );
    Ok(())
}

/// Shared, process-wide handles. Built from `--data-dir` (local) or the
/// `LAKE_S3_BUCKET`/`LAKE_DYNAMODB_*`/`AWS_*` environment (cloud).
pub struct Context {
    pub meta:        MetaStoreRef,
    pub engine:      TableEngineRef,
    pub metasrv:     Arc<Metasrv>,
    table_placement: TablePlacement,
    managed_stage:   ManagedStageDescriptor,
}

/// Minimal handles held by a served Query replica.
///
/// Catalog authority is reachable only through the authenticated Metasrv
/// client constructed by `serve`; this context intentionally cannot expose a
/// registry `MetaStore`, local `Metasrv`, or table-placement policy.
pub struct QueryContext {
    pub engine:    TableEngineRef,
    managed_stage: ManagedStageDescriptor,
    async_table:   Option<String>,
    async_enabled: bool,
}

impl QueryContext {
    pub async fn open(data_dir: &str) -> anyhow::Result<Self> {
        let maintenance_policy = lance_maintenance_policy_from_env()?;
        let async_enabled = async_queries_enabled_from_env()?;
        match std::env::var("LAKE_S3_BUCKET") {
            Ok(bucket) => Self::open_cloud(bucket, maintenance_policy, async_enabled).await,
            Err(_) => Self::open_local(data_dir, maintenance_policy, async_enabled),
        }
    }

    fn open_local(
        data_dir: &str,
        maintenance_policy: LanceMaintenancePolicy,
        async_enabled: bool,
    ) -> anyhow::Result<Self> {
        let root = PathBuf::from(data_dir);
        std::fs::create_dir_all(&root)?;
        let root = std::fs::canonicalize(root)?;
        Ok(Self {
            engine: Arc::new(LanceEngine::new().with_maintenance_policy(maintenance_policy)),
            managed_stage: local_managed_stage_descriptor(&root),
            async_table: None,
            async_enabled,
        })
    }

    async fn open_cloud(
        bucket: String,
        maintenance_policy: LanceMaintenancePolicy,
        async_enabled: bool,
    ) -> anyhow::Result<Self> {
        let endpoint = std::env::var("LAKE_DYNAMODB_ENDPOINT").ok();
        let plan = CloudStoragePlan::from_env(async_enabled)?;
        let [manifest_table] = plan.query_authorities();
        let manifests = DynamoMeta::connect(endpoint.as_deref(), manifest_table).await?;
        // This waits for pre-provisioned tables and reads the monotonic v2
        // marker with Describe/Get only. Query never calls ensure/create.
        manifests.open_tables().await?;
        let manifests: MetaStoreRef = Arc::new(manifests);
        let engine: TableEngineRef = Arc::new(
            LanceEngine::for_read_only_object_store(manifests, s3_storage_options())
                .with_maintenance_policy(maintenance_policy),
        );
        let managed_prefix = std::env::var("LAKE_MANAGED_OBJECT_PREFIX")
            .unwrap_or_else(|_| "managed-objects".to_owned());
        Ok(Self {
            engine,
            managed_stage: s3_managed_stage_descriptor(
                &bucket,
                &managed_prefix,
                std::env::var("AWS_REGION").ok(),
                std::env::var("LAKE_S3_ENDPOINT").ok(),
            ),
            async_table: plan.async_state,
            async_enabled,
        })
    }

    pub fn managed_stage(&self) -> &ManagedStageDescriptor { &self.managed_stage }

    pub fn async_table(&self) -> Option<&str> { self.async_table.as_deref() }

    pub const fn async_enabled(&self) -> bool { self.async_enabled }
}

impl Context {
    pub async fn open(data_dir: &str) -> anyhow::Result<Self> {
        let maintenance_policy = lance_maintenance_policy_from_env()?;
        let async_enabled = async_queries_enabled_from_env()?;
        match std::env::var("LAKE_S3_BUCKET") {
            Ok(bucket) => Self::open_cloud(bucket, maintenance_policy, async_enabled).await,
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
        async_enabled: bool,
    ) -> anyhow::Result<Self> {
        let endpoint = std::env::var("LAKE_DYNAMODB_ENDPOINT").ok();
        let plan = CloudStoragePlan::from_env(async_enabled)?;
        let [registry_table, manifest_table] = plan.metadata_authorities();
        let registry = DynamoMeta::connect(endpoint.as_deref(), registry_table).await?;
        registry.open_tables().await?;
        let manifests = DynamoMeta::connect(endpoint.as_deref(), manifest_table).await?;
        manifests.open_tables().await?;
        let meta: MetaStoreRef = Arc::new(registry);
        let manifests: MetaStoreRef = Arc::new(manifests);
        let engine: TableEngineRef = Arc::new(
            LanceEngine::for_object_store(manifests, s3_storage_options())
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

#[cfg(test)]
mod authority_tests {
    use lake_common::ManagedStageBackend;
    use lake_engine_lance::LanceMaintenancePolicy;

    use super::{CloudStoragePlan, QueryContext};

    #[test]
    fn cloud_manifest_table_alias_fails_before_connect() {
        assert!(CloudStoragePlan::try_new("lake-registry", "lake-registry", None::<&str>).is_err());
        assert!(CloudStoragePlan::try_new("foo", "foo_prefix_v2", None::<&str>).is_err());
        assert!(CloudStoragePlan::try_new("foo_prefix_v2", "foo", None::<&str>).is_err());
        assert!(CloudStoragePlan::try_new("", "lake-manifests", None::<&str>).is_err());
        assert!(CloudStoragePlan::try_new("lake-registry", "", None::<&str>).is_err());
        assert!(
            CloudStoragePlan::try_new("lake-registry", "lake-manifests", Some("lake-registry"))
                .is_err()
        );
        assert!(
            CloudStoragePlan::try_new(
                "lake-registry",
                "lake-manifests",
                Some("lake-registry_prefix_v2")
            )
            .is_err()
        );
        assert!(
            CloudStoragePlan::try_new("lake-registry", "lake-manifests", Some("lake-manifests"))
                .is_err()
        );
        assert!(
            CloudStoragePlan::try_new(
                "lake-registry",
                "lake-manifests",
                Some("lake-manifests_prefix_v2")
            )
            .is_err()
        );
    }

    #[test]
    fn cloud_storage_wiring_separates_registry_and_manifest_authority() {
        let plan = CloudStoragePlan::try_new(
            "lake-registry",
            "lake-manifests",
            Some("lake-async-queries"),
        )
        .unwrap();

        assert_eq!(
            plan.metadata_authorities(),
            ["lake-registry", "lake-manifests"]
        );
        assert_eq!(plan.query_authorities(), ["lake-manifests"]);
        assert_eq!(plan.async_authority(), Some("lake-async-queries"));
        let wiring = include_str!("mod.rs");
        assert!(wiring.contains("LanceEngine::for_read_only_object_store"));
    }

    #[test]
    fn query_context_has_no_catalog_authority() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("query");

        let context = QueryContext::open_local(
            data.to_str().unwrap(),
            LanceMaintenancePolicy::default(),
            false,
        )
        .unwrap();

        assert!(!data.join("meta").exists());
        assert!(matches!(
            context.managed_stage().backend(),
            ManagedStageBackend::Local { .. }
        ));
        let main = include_str!("../main.rs");
        assert!(main.contains("commands::QueryContext::open(data_dir)"));
        assert!(main.contains("Query is dispatched before Context::open"));
    }
}
