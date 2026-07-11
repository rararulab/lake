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

//! End-to-end proof of lake's production storage path against localstack.
//!
//! This drives the crown-jewel combination through lake's own engine API:
//!
//! * `LanceEngine::for_object_store` writing a **Lance dataset on S3**
//!   (localstack, path-style, http endpoint), and
//! * lake's `MetaManifestStore` acting as Lance's `ExternalManifestStore`, with
//!   the commit pointer living in **DynamoDB** via [`DynamoMeta`].
//!
//! Everything goes through the real `TableEngine`/`TableHandle` surface
//! (`create`/`append`/`open`/`table_provider`), so a green run proves the
//! engine — not a hand-rolled Lance call — reaches S3 and commits through
//! DynamoDB.
//!
//! # Running
//!
//! `#[ignore]` by default. To run:
//!
//! ```text
//! LAKE_DYNAMODB_ENDPOINT=http://localhost:4566 AWS_ACCESS_KEY_ID=test \
//!   AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
//!   cargo nextest run -p lake-engine-lance --run-ignored all
//! ```
//!
//! When `LAKE_DYNAMODB_ENDPOINT` is unset the test returns early (no-op), so it
//! is safe to invoke via `--run-ignored all` without localstack present.

use std::{collections::HashMap, sync::Arc};

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use datafusion::{
    arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    },
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::SessionContext,
};
use lake_common::{
    AppendOperation, AppendOperationId, AppendPayloadDigest, ObjectReferenceDelta, TableLocation,
    TenantId, Version,
};
use lake_engine::TableEngine;
use lake_engine_lance::{LanceEngine, MetaManifestStore};
use lake_meta::{DynamoMeta, MetaStoreRef};
use lance::dataset::builder::DatasetBuilder;
use lance_table::io::commit::external_manifest::{
    ExternalManifestCommitHandler, ExternalManifestStore,
};

/// A unique suffix so parallel/repeat runs never collide on bucket, table, or
/// dataset paths.
fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos()
}

fn append_operation() -> AppendOperation {
    AppendOperation::builder()
        .tenant(TenantId::try_new("integration").expect("valid tenant"))
        .operation_id(AppendOperationId::generate())
        .payload_digest(
            AppendPayloadDigest::parse(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect("valid digest"),
        )
        .build()
}

/// Read a credential from the environment, defaulting to localstack's `test`.
fn cred(var: &str) -> String { std::env::var(var).unwrap_or_else(|_| "test".to_owned()) }

/// The object_store S3 keys Lance needs to talk to localstack: custom endpoint,
/// path-style, plain http, static creds. These are `AmazonS3ConfigKey` string
/// forms (object_store 0.13), parsed by lance-io's aws provider.
fn s3_storage_options(endpoint: &str) -> HashMap<String, String> {
    HashMap::from([
        ("aws_access_key_id".to_owned(), cred("AWS_ACCESS_KEY_ID")),
        (
            "aws_secret_access_key".to_owned(),
            cred("AWS_SECRET_ACCESS_KEY"),
        ),
        ("aws_region".to_owned(), "us-east-1".to_owned()),
        ("aws_endpoint".to_owned(), endpoint.to_owned()),
        // Path-style (localstack serves `endpoint/bucket/key`, not vhost).
        (
            "aws_virtual_hosted_style_request".to_owned(),
            "false".to_owned(),
        ),
        // Allow the plain-http localstack endpoint.
        ("aws_allow_http".to_owned(), "true".to_owned()),
        // Bypass any ambient HTTP proxy for the loopback localstack endpoint.
        //
        // lance-io's S3 provider folds *every* environment variable (lowercased)
        // into the object_store client config: a `PROXY_URL` env var thus becomes
        // object_store's `proxy_url`, routing S3 traffic through that proxy. But
        // the companion bypass list uses object_store's `proxy_excludes` key,
        // which no standard env var name maps to — so a proxy that cannot reach
        // this host's loopback returns 502 on every S3 request. Setting
        // `proxy_excludes` here restores direct connections for localstack.
        // Harmless when no proxy is configured. object_store's `with_env_s3` only
        // fills keys the caller did not set, so this explicit value always wins.
        (
            "proxy_excludes".to_owned(),
            "localhost,127.0.0.1,::1".to_owned(),
        ),
    ])
}

/// Wrap one batch as the `Result` stream item type `TableHandle::append` wants.
fn one_batch_stream(schema: Arc<Schema>, batch: RecordBatch) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
    ))
}

#[tokio::test]
#[ignore = "requires localstack S3 + DynamoDB; set LAKE_DYNAMODB_ENDPOINT and run with --ignored"]
async fn lance_engine_on_s3_with_dynamo_external_manifest() {
    let Ok(endpoint) = std::env::var("LAKE_DYNAMODB_ENDPOINT") else {
        // Skip when the localstack endpoint is not provisioned.
        return;
    };
    let suffix = unique_suffix();

    // 1. Provision an S3 bucket in localstack (path-style, http, static creds).
    let bucket = format!("lake-lance-test-{suffix}");
    let s3_conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            cred("AWS_ACCESS_KEY_ID"),
            cred("AWS_SECRET_ACCESS_KEY"),
            None,
            None,
            "localstack",
        ))
        .force_path_style(true)
        .build();
    let s3 = aws_sdk_s3::Client::from_conf(s3_conf);
    match s3.create_bucket().bucket(&bucket).send().await {
        Ok(_) => {}
        Err(e)
            if e.as_service_error()
                .is_some_and(|se| se.is_bucket_already_owned_by_you()) => {}
        Err(e) => panic!("create_bucket {bucket}: {e}"),
    }

    // 2. Build DynamoMeta over localstack as the metastore behind the external
    //    manifest store.
    let table = format!("lake_lance_manifest_{suffix}");
    let dynamo = DynamoMeta::connect(Some(&endpoint), &table)
        .await
        .expect("connect to localstack dynamodb");
    dynamo.ensure_table().await.expect("create manifest table");
    let meta: MetaStoreRef = Arc::new(dynamo);

    // 3. Build the engine for object storage: commits route through DynamoDB's
    //    external manifest store, storage options point Lance at localstack S3.
    let engine = LanceEngine::for_object_store(meta.clone(), s3_storage_options(&endpoint));
    let uri = format!("s3://{bucket}/ns/tbl");
    let location = TableLocation::new(uri);
    let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));

    // 4. Create the table on S3 (v1) through the engine.
    let handle = engine
        .create(&location, schema.clone())
        .await
        .expect("engine create table on s3");
    assert_eq!(handle.current_version().0, 1, "create is version 1");

    // 5. Append 3 rows through the engine (v2).
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .expect("build append batch");
    let appended = handle
        .append(&append_operation(), one_batch_stream(schema.clone(), batch))
        .await
        .expect("engine append on s3");
    assert!(
        appended.0 > 1,
        "append advances the version (got {appended})"
    );
    let journal = s3
        .get_object()
        .bucket(&bucket)
        .key(format!("ns/tbl/_lake/object_refs/{}/0.json", appended.0))
        .send()
        .await
        .expect("append reference journal exists on S3")
        .body
        .collect()
        .await
        .expect("read append reference journal")
        .into_bytes();
    let journal = ObjectReferenceDelta::decode(&journal).expect("valid S3 reference journal");
    assert_eq!(journal.table_version(), appended);
    assert!(journal.added().is_empty(), "Int64 rows contain no FILEs");

    // 6. Reopen through the engine and read back via DataFusion.
    let reopened = engine
        .open(&location)
        .await
        .expect("engine open on s3")
        .expect("table exists after create");
    assert_eq!(
        reopened.current_version(),
        appended,
        "reopen resolves the latest committed version"
    );

    let ctx = SessionContext::new();
    ctx.register_table(
        "tbl",
        reopened
            .table_provider(appended)
            .await
            .expect("open appended snapshot"),
    )
    .expect("register lance provider");
    let rows = ctx
        .sql("SELECT count(*) AS n FROM tbl")
        .await
        .expect("plan count query")
        .collect()
        .await
        .expect("execute count query");
    let count = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column is Int64")
        .value(0);
    assert_eq!(count, 3, "reads back exactly the 3 rows written");

    // 7. Prove the current pointer really lives in DynamoDB (not just S3). Latest
    //    is one fixed point key; advancing to v2 archived v1 under the immutable
    //    history prefix.
    let history = meta
        .list_prefix("lance-manifest/")
        .await
        .expect("list manifest history in dynamodb");
    assert!(
        history.iter().any(|key| key.ends_with("/1")),
        "advancing latest archived v1; got {history:?}"
    );
    assert!(
        !history.iter().any(|key| key.ends_with("/2")),
        "the current version belongs only in the fixed pointer; got {history:?}"
    );
    let latest = meta
        .list_prefix("lance-manifest-latest/")
        .await
        .expect("list latest manifest pointers in dynamodb");
    let latest_key = latest
        .iter()
        .find(|key| key.contains("tbl"))
        .unwrap_or_else(|| panic!("no fixed manifest pointer for {appended}; got {latest:?}"));
    let pointer = meta
        .get(&format!("lance-manifest-latest/{latest_key}"))
        .await
        .expect("get manifest pointer from dynamodb")
        .expect("manifest pointer present in dynamodb");
    let pointer: serde_json::Value =
        serde_json::from_slice(&pointer).expect("latest pointer is JSON");
    assert_eq!(
        pointer["version"].as_u64(),
        Some(appended.0),
        "fixed pointer names the appended version"
    );

    // 8. Drop the table: `remove` must delete every S3 object under the dataset
    //    path (the object_store S3 delete path, distinct from local FS) and `open`
    //    must then report the table gone.
    engine.remove(&location).await.expect("engine remove on s3");
    assert!(
        engine
            .open(&location)
            .await
            .expect("engine open after remove")
            .is_none(),
        "table is gone after remove"
    );
    let remaining = s3
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("ns/tbl.lance/")
        .send()
        .await
        .expect("list objects after remove");
    assert_eq!(
        remaining.key_count().unwrap_or(0),
        0,
        "remove deleted all S3 objects under the dataset"
    );

    let stale_manifests = meta
        .list_prefix("lance-manifest/")
        .await
        .expect("list manifest pointers after remove");
    assert!(
        stale_manifests.is_empty(),
        "remove must clear external manifest history; got {stale_manifests:?}"
    );
    let stale_latest = meta
        .list_prefix("lance-manifest-latest/")
        .await
        .expect("list latest manifest pointers after remove");
    assert_eq!(
        stale_latest.len(),
        1,
        "remove retains only its durable anti-ABA deletion marker; got {stale_latest:?}"
    );
    let deleted = meta
        .get(&format!("lance-manifest-latest/{}", stale_latest[0]))
        .await
        .expect("read durable deletion marker")
        .expect("durable deletion marker exists");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&deleted).expect("marker JSON")["state"],
        "deleted",
        "remove must replace the live pointer with a durable deleted marker"
    );

    let recreated = engine
        .create(&location, schema)
        .await
        .expect("recreate the same S3 location after remove");
    assert_eq!(
        recreated.current_version().0,
        1,
        "a recreated dataset starts a fresh version history"
    );
}

#[tokio::test]
#[ignore = "requires localstack S3 + DynamoDB; set LAKE_DYNAMODB_ENDPOINT and run with --ignored"]
async fn external_manifest_cleanup_localstack() {
    let Ok(endpoint) = std::env::var("LAKE_DYNAMODB_ENDPOINT") else {
        return;
    };
    let suffix = unique_suffix();
    let bucket = format!("lake-lance-cleanup-{suffix}");
    let s3_conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            cred("AWS_ACCESS_KEY_ID"),
            cred("AWS_SECRET_ACCESS_KEY"),
            None,
            None,
            "localstack",
        ))
        .force_path_style(true)
        .build();
    let s3 = aws_sdk_s3::Client::from_conf(s3_conf);
    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create cleanup bucket");

    let table = format!("lake_lance_cleanup_{suffix}");
    let dynamo = DynamoMeta::connect(Some(&endpoint), &table)
        .await
        .expect("connect cleanup metastore");
    dynamo.ensure_table().await.expect("create cleanup table");
    let meta: MetaStoreRef = Arc::new(dynamo);
    let engine = LanceEngine::for_object_store(meta.clone(), s3_storage_options(&endpoint));
    let location = TableLocation::new(format!("s3://{bucket}/ns/cleanup"));
    let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
    let handle = engine
        .create(&location, schema.clone())
        .await
        .expect("create cleanup dataset");
    let mut version = handle.current_version();
    for value in 0_i64..12 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .expect("cleanup append batch");
        version = handle
            .append(&append_operation(), one_batch_stream(schema.clone(), batch))
            .await
            .expect("append cleanup version");
    }
    let before_keys = meta
        .list_prefix("lance-manifest/")
        .await
        .expect("history before cleanup");
    let mut before_paths = HashMap::new();
    for key in &before_keys {
        let bytes = meta
            .get(&format!("lance-manifest/{key}"))
            .await
            .expect("read history record")
            .expect("history record exists");
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("history JSON");
        before_paths.insert(
            key.clone(),
            value["path"].as_str().expect("manifest path").to_owned(),
        );
    }
    let v1_key = before_keys
        .iter()
        .find(|key| key.ends_with("/1"))
        .expect("v1 history key")
        .clone();
    let external_store: Arc<dyn ExternalManifestStore> =
        Arc::new(MetaManifestStore::new(meta.clone()));
    DatasetBuilder::from_uri(location.as_str())
        .with_storage_options(s3_storage_options(&endpoint))
        .with_commit_handler(Arc::new(ExternalManifestCommitHandler {
            external_manifest_store: external_store,
        }))
        .load()
        .await
        .expect("open dataset for tag")
        .tags()
        .create("retain-v1", 1_u64)
        .await
        .expect("tag v1");

    engine
        .maintain(&location, version)
        .await
        .expect("cleanup maintenance");

    let after_keys = meta
        .list_prefix("lance-manifest/")
        .await
        .expect("history after cleanup");
    assert!(
        after_keys.len() < before_keys.len(),
        "Dynamo history shrank"
    );
    assert!(after_keys.contains(&v1_key), "tagged v1 history remains");
    s3.head_object()
        .bucket(&bucket)
        .key(&before_paths[&v1_key])
        .send()
        .await
        .expect("tagged v1 manifest remains on S3");
    engine
        .open(&location)
        .await
        .expect("open after cleanup")
        .expect("dataset remains")
        .table_provider(Version(1))
        .await
        .expect("tagged v1 snapshot remains readable");
    for removed in before_keys.iter().filter(|key| !after_keys.contains(key)) {
        let path = &before_paths[removed];
        assert!(
            s3.head_object()
                .bucket(&bucket)
                .key(path)
                .send()
                .await
                .is_err(),
            "external history is deleted only after S3 manifest absence: {path}"
        );
    }
    assert!(
        !meta
            .list_prefix("lance-manifest-latest/")
            .await
            .expect("latest after cleanup")
            .is_empty(),
        "fixed latest remains"
    );
}

#[test]
fn external_manifest_cleanup_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("lake-engine-lance"));
    assert!(integration.contains("--run-ignored"));
}
