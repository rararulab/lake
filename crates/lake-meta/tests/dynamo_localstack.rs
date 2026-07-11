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

//! Integration test for [`DynamoMeta`] against a localstack DynamoDB.
//!
//! This test is `#[ignore]` — the default `cargo nextest` / CI gate must not
//! run it. To run it locally:
//!
//! 1. Start the checkout-scoped test env: `mise run test-env-up` (writes
//!    `.lake/test-env.env` with `LAKE_DYNAMODB_ENDPOINT=...`).
//! 2. Run with the endpoint exported, e.g.: `LAKE_DYNAMODB_ENDPOINT=http://127.0.0.1:PORT
//!    \ cargo nextest run -p lake-meta --run-ignored all` (or `cargo test -p
//!    lake-meta -- --ignored`).
//!
//! When `LAKE_DYNAMODB_ENDPOINT` is unset the test is a no-op (returns early),
//! so it is safe to invoke via `--run-ignored all` without the env present.

use lake_meta::{DynamoMeta, MetaStore};

#[tokio::test]
#[ignore = "requires localstack DynamoDB; set LAKE_DYNAMODB_ENDPOINT and run with --ignored"]
async fn dynamo_meta_roundtrip() {
    let Ok(endpoint) = std::env::var("LAKE_DYNAMODB_ENDPOINT") else {
        // Skip when the localstack endpoint is not provisioned.
        return;
    };

    // Unique per-run table name so parallel/repeat runs never collide.
    let table = format!(
        "lake_meta_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    );

    let meta = DynamoMeta::connect(Some(&endpoint), &table)
        .await
        .expect("connect to localstack dynamodb");
    meta.ensure_table().await.expect("create test table");

    // Same assertions as the RocksMeta unit tests.

    // cas: create succeeds, then None-when-exists and wrong-expected both fail,
    // then the correct expected swaps.
    assert!(meta.cas("k", None, b"1").await.unwrap());
    assert!(
        !meta.cas("k", None, b"2").await.unwrap(),
        "key exists, None must fail"
    );
    assert!(
        !meta.cas("k", Some(b"9"), b"2").await.unwrap(),
        "wrong expected must fail"
    );
    assert!(meta.cas("k", Some(b"1"), b"2").await.unwrap());

    // get roundtrip reflects the swapped value.
    assert_eq!(meta.get("k").await.unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(meta.get("missing").await.unwrap(), None);

    // list_prefix strips the prefix and filters out non-matching keys.
    for k in ["ptr/a", "ptr/b", "other/c"] {
        assert!(meta.cas(k, None, b"v").await.unwrap());
    }
    let mut listed = meta.list_prefix("ptr/").await.unwrap();
    listed.sort();
    assert_eq!(listed, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(
        meta.scan_prefix("ptr/").await.unwrap(),
        vec![
            ("a".to_owned(), b"v".to_vec()),
            ("b".to_owned(), b"v".to_vec()),
        ]
    );
}
