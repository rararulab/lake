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
use lake_common::TableLocation;
use lake_engine::TableEngine;
use lake_engine_lance::LanceEngine;
use lake_meta::{DynamoMeta, MetaStoreRef};

/// A unique suffix so parallel/repeat runs never collide on bucket, table, or
/// dataset paths.
fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos()
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
        .append(one_batch_stream(schema.clone(), batch))
        .await
        .expect("engine append on s3");
    assert!(
        appended.0 > 1,
        "append advances the version (got {appended})"
    );

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

    // 7. Prove the commit pointer really lives in DynamoDB (not just S3): the
    //    external store recorded a `lance-manifest/<base_uri>/<version>` key per
    //    commit.
    let listed = meta
        .list_prefix("lance-manifest/")
        .await
        .expect("list manifest pointers in dynamodb");
    assert!(
        !listed.is_empty(),
        "commit went through the external store: expected lance-manifest/* keys in dynamodb"
    );
    let latest_key = listed
        .iter()
        .find(|k| k.ends_with(&format!("/{}", appended.0)) && k.contains("tbl"))
        .unwrap_or_else(|| panic!("no manifest pointer for {appended}; got {listed:?}"));
    let pointer = meta
        .get(&format!("lance-manifest/{latest_key}"))
        .await
        .expect("get manifest pointer from dynamodb")
        .expect("manifest pointer present in dynamodb");
    assert!(
        pointer.starts_with(b"{"),
        "manifest pointer is the JSON value written by MetaManifestStore"
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
}
