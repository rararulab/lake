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

use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue};
use lake_common::{TableLocation, TableRef, Version};
use lake_meta::{
    DynamoMeta, GuardedMutation, MetaStore,
    registry::{self, TableRegistration},
};
use metrics_exporter_prometheus::PrometheusBuilder;

async fn consume_and_delete_pages(meta: &DynamoMeta, prefix: &str) {
    let expected = ["a", "b", "c"];
    for suffix in expected {
        assert!(
            meta.cas(&format!("{prefix}{suffix}"), None, suffix.as_bytes())
                .await
                .expect("seed delete-while-paging key")
        );
    }
    let mut continuation = None;
    let mut consumed = Vec::new();
    loop {
        let page = meta
            .scan_prefix_page(prefix, continuation.as_deref(), 1)
            .await
            .expect("scan delete-while-paging page");
        let (entries, next) = page.into_parts();
        for (suffix, value) in entries {
            assert!(
                meta.delete(&format!("{prefix}{suffix}"), &value)
                    .await
                    .expect("delete consumed page entry")
            );
            consumed.push(suffix);
        }
        continuation = next;
        if continuation.is_none() {
            break;
        }
    }
    consumed.sort_unstable();
    assert_eq!(consumed, expected);
    assert!(
        meta.scan_prefix(prefix)
            .await
            .expect("verify consumed prefix")
            .is_empty()
    );
}

async fn exercise_atomic_directory_signal(meta: &DynamoMeta, suffix: &str) {
    let before = registry::directory_state(meta)
        .await
        .expect("read initial directory state")
        .generation()
        .expect("finalized directory has generation")
        .to_vec();
    let table = TableRef::new("directory", suffix);
    let registration = TableRegistration::new(
        TableLocation::new(format!("mem://{suffix}")),
        "lance",
        Version(1),
        vec![1, 2, 3],
    );
    registry::register(meta, &table, &registration)
        .await
        .expect("atomically register and signal");
    let after_register = registry::directory_state(meta)
        .await
        .expect("read post-register directory state");
    assert_ne!(after_register.generation().expect("generation"), before);

    registry::set_version(meta, &table, &registration, Version(2))
        .await
        .expect("advance version without directory invalidation");
    let after_version = registry::directory_state(meta)
        .await
        .expect("read post-version directory state");
    assert_eq!(after_version.generation(), after_register.generation());

    let current = registry::get(meta, &table)
        .await
        .expect("read current registration")
        .expect("registration exists");
    registry::delete(meta, &table, &current)
        .await
        .expect("atomically delete and signal");
    let after_delete = registry::directory_state(meta)
        .await
        .expect("read post-delete directory state");
    assert_ne!(after_delete.generation(), after_register.generation());
}

#[tokio::test]
#[ignore = "requires localstack DynamoDB; set LAKE_DYNAMODB_ENDPOINT and run with --ignored"]
async fn dynamo_v1_dual_v2_migration_roundtrip() {
    let Ok(endpoint) = std::env::var("LAKE_DYNAMODB_ENDPOINT") else {
        // Skip when the localstack endpoint is not provisioned.
        return;
    };
    let recorder = PrometheusBuilder::new().build_recorder();
    let metrics = recorder.handle();
    let _recorder = metrics::set_default_local_recorder(&recorder);

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
    let shared = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let raw = aws_sdk_dynamodb::Client::from_conf(
        aws_sdk_dynamodb::config::Builder::from(&shared)
            .endpoint_url(&endpoint)
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                "test",
                "test",
                None,
                None,
                "localstack",
            ))
            .build(),
    );
    raw.put_item()
        .table_name(&table)
        .item("pk", AttributeValue::S("ptr/legacy".to_owned()))
        .item("val", AttributeValue::B(Blob::new(b"old")))
        .send()
        .await
        .expect("seed a pre-upgrade v1-only key");
    let stale_dual = DynamoMeta::connect(Some(&endpoint), &table)
        .await
        .expect("connect second dual node");
    stale_dual
        .open_tables()
        .await
        .expect("open pre-provisioned layouts without creating them");

    assert!(
        registry::finalize_directory_generation(&meta)
            .await
            .expect("finalize directory authority")
    );
    exercise_atomic_directory_signal(&meta, "v1").await;

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
    assert_eq!(
        listed,
        vec!["a".to_string(), "b".to_string(), "legacy".to_string()]
    );
    assert_eq!(
        meta.scan_prefix("ptr/").await.unwrap(),
        vec![
            ("a".to_owned(), b"v".to_vec()),
            ("b".to_owned(), b"v".to_vec()),
            ("legacy".to_owned(), b"old".to_vec()),
        ]
    );
    let mut continuation = None;
    let mut paged = Vec::new();
    loop {
        let page = meta
            .scan_prefix_page("ptr/", continuation.as_deref(), 1)
            .await
            .unwrap();
        assert!(page.entries().len() <= 1, "Dynamo scan page is bounded");
        let (entries, next) = page.into_parts();
        paged.extend(entries);
        continuation = next;
        if continuation.is_none() {
            break;
        }
    }
    paged.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        paged,
        vec![
            ("a".to_owned(), b"v".to_vec()),
            ("b".to_owned(), b"v".to_vec()),
            ("legacy".to_owned(), b"old".to_vec()),
        ]
    );
    consume_and_delete_pages(&meta, "delete-while-paging-v1/").await;

    assert!(meta.cas("lease", None, b"epoch-1").await.unwrap());
    assert!(
        meta.guarded_mutate(GuardedMutation::create(
            "lease", b"epoch-1", "guarded", b"one",
        ))
        .await
        .unwrap()
    );
    assert!(
        meta.guarded_mutate(GuardedMutation::update(
            "lease", b"epoch-1", "guarded", b"one", b"two",
        ))
        .await
        .unwrap()
    );
    assert!(
        meta.cas("lease", Some(b"epoch-1"), b"epoch-2")
            .await
            .unwrap()
    );
    assert!(
        !meta
            .guarded_mutate(GuardedMutation::delete(
                "lease", b"epoch-1", "guarded", b"two",
            ))
            .await
            .unwrap()
    );
    assert_eq!(
        meta.get("guarded").await.unwrap().as_deref(),
        Some(&b"two"[..])
    );
    assert!(
        meta.guarded_mutate(GuardedMutation::delete(
            "lease", b"epoch-2", "guarded", b"two",
        ))
        .await
        .unwrap()
    );

    loop {
        let page = meta.migrate_v2_page(2).await.unwrap();
        assert!(page.scanned <= 2, "legacy migration page is bounded");
        if page.complete {
            break;
        }
    }
    let verification = meta.verify_and_finalize_v2(2).await.unwrap();
    assert!(verification.finalized);
    assert_eq!(verification.legacy_items, verification.v2_items);
    assert!(meta.is_v2_authoritative());
    assert_eq!(meta.get("k").await.unwrap().as_deref(), Some(&b"2"[..]));
    assert!(
        !stale_dual
            .cas("blocked-by-finalize", None, b"v")
            .await
            .unwrap(),
        "a stale pre-finalization node must fail closed on the durable barrier"
    );
    stale_dual.refresh_authority().await.unwrap();
    assert!(stale_dual.is_v2_authoritative());
    assert!(
        stale_dual
            .cas("accepted-after-refresh", None, b"v")
            .await
            .unwrap()
    );
    exercise_atomic_directory_signal(&meta, "v2").await;

    let mut continuation = None;
    let mut v2_paged = Vec::new();
    loop {
        let page = meta
            .scan_prefix_page("ptr/", continuation.as_deref(), 1)
            .await
            .unwrap();
        assert!(page.entries().len() <= 1, "v2 query page is bounded");
        let (entries, next) = page.into_parts();
        v2_paged.extend(entries);
        continuation = next;
        if continuation.is_none() {
            break;
        }
    }
    v2_paged.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        v2_paged,
        vec![
            ("a".to_owned(), b"v".to_vec()),
            ("b".to_owned(), b"v".to_vec()),
            ("legacy".to_owned(), b"old".to_vec()),
        ]
    );
    consume_and_delete_pages(&meta, "delete-while-paging-v2/").await;

    let rendered = metrics.render();
    for expected in [
        "lake_dynamo_v2_authoritative 1",
        "lake_dynamo_finalize_barrier_held 1",
        "lake_dynamo_prefix_requests_total{layout=\"v1\",api=\"scan\",outcome=\"success\"}",
        "lake_dynamo_prefix_requests_total{layout=\"v2\",api=\"query\",outcome=\"success\"}",
    ] {
        assert!(
            rendered.contains(expected),
            "missing {expected}:\n{rendered}"
        );
    }
}

#[test]
fn dynamo_delete_while_paging_localstack_is_wired() {
    let source = include_str!("dynamo_localstack.rs");
    assert!(source.contains("consume_and_delete_pages(&meta, \"delete-while-paging-v1/\")"));
    assert!(source.contains("consume_and_delete_pages(&meta, \"delete-while-paging-v2/\")"));
}

#[test]
fn dynamo_catalog_generation_atomicity_localstack_is_wired() {
    let source = include_str!("dynamo_localstack.rs");
    assert!(source.contains("exercise_atomic_directory_signal(&meta, \"v1\")"));
    assert!(source.contains("exercise_atomic_directory_signal(&meta, \"v2\")"));
}
