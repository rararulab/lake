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

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::{Credentials, Region},
    primitives::ByteStream,
    types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart},
};
use lake_objects::{
    GcPlanApplier, GcPlanWriter, GcPlanner, InventoryRequest, ManagedObjectInventory,
    ManagedObjectScope, ManagedObjectStore, ObjectCandidate, ObjectError, S3ObjectStore,
};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::oneshot,
};

const PART_BYTES: usize = 5 * 1024 * 1024;

fn localstack_client(endpoint: &str) -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "localstack"))
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

async fn stage() -> Option<(aws_sdk_s3::Client, S3ObjectStore, String)> {
    let endpoint = std::env::var("LAKE_S3_ENDPOINT").ok()?;
    let client = localstack_client(&endpoint);
    let bucket = format!("lake-objects-{}", uuid::Uuid::now_v7());
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create LocalStack bucket");
    let store = S3ObjectStore::new(client.clone(), &bucket, "managed/objects")
        .expect("valid managed stage");
    Some((client, store, bucket))
}

#[test]
fn s3_multipart_roundtrip_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("lake-objects"));
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn s3_range_read_returns_requested_bytes_localstack() {
    let Some((_client, store, _bucket)) = stage().await else {
        return;
    };
    let bytes = (0..(PART_BYTES + 1024))
        .map(|index| u8::try_from(index % 251).expect("bounded byte"))
        .collect::<Vec<_>>();
    let location = store
        .put_reader(
            Box::pin(std::io::Cursor::new(bytes.clone())),
            "video/mp4".to_owned(),
        )
        .await
        .expect("multipart upload");
    let start = u64::try_from(PART_BYTES - 7).expect("part size fits u64");
    let end = u64::try_from(PART_BYTES + 13).expect("part size fits u64");

    let mut reader = store
        .open_range(&location, start..end)
        .await
        .expect("S3 range GET");
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).await.expect("read range");

    assert_eq!(actual, bytes[PART_BYTES - 7..PART_BYTES + 13]);
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn s3_presigned_range_get_localstack() {
    let Some((_client, store, _bucket)) = stage().await else {
        return;
    };
    let bytes = (0..4096)
        .map(|index| u8::try_from(index % 251).expect("bounded byte"))
        .collect::<Vec<_>>();
    let location = store
        .put_reader(
            Box::pin(std::io::Cursor::new(bytes.clone())),
            "video/mp4".to_owned(),
        )
        .await
        .expect("upload managed object");
    let capability = store
        .presign_read(&location, Duration::from_mins(1))
        .await
        .expect("presign managed GET");

    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build direct HTTP client");
    let mut request = client.get(capability.url());
    for (name, value) in capability.headers() {
        request = request.header(name.as_str(), value.as_str());
    }
    let response = request
        .header(reqwest::header::RANGE, "bytes=100-199")
        .send()
        .await
        .expect("execute presigned Range GET");

    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.bytes().await.expect("read partial body").as_ref(),
        &bytes[100..200]
    );
}

#[test]
fn s3_range_read_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("lake-objects"));
    assert!(integration.contains("--run-ignored"));
}

#[test]
fn s3_presigned_range_get_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    let workflow = include_str!("../../../.github/workflows/ci.yml");

    assert!(integration.contains("lake-objects"));
    assert!(integration.contains("--run-ignored"));
    assert!(workflow.contains("mise run test-integration-external"));
}

#[test]
fn managed_s3_integration_runner_is_shared_with_ci() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    let workflow = include_str!("../../../.github/workflows/ci.yml");

    for package in ["lake-objects", "lake-sdk", "lake-meta", "lake-engine-lance"] {
        assert!(integration.contains(package));
    }
    assert!(integration.contains("ignored-only"));
    assert!(integration.contains("profileArgs"));
    assert!(integration.contains("\"ci\""));
    let mise = include_str!("../../../mise.toml");
    assert!(mise.contains("[tasks.test-integration-external]"));
    assert!(mise.contains("bun scripts/test-integration.ts --external"));
    assert!(workflow.contains("mise run test-integration-external"));
    assert!(!workflow.contains("cargo nextest run -p lake-meta -p lake-engine-lance"));

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("bun")
        .args(["test", "scripts/test-integration-env.test.ts"])
        .current_dir(root)
        .output()
        .expect("run integration environment isolation tests");
    assert!(
        output.status.success(),
        "integration environment isolation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn s3_multipart_roundtrip_localstack() {
    let Some((_client, store, bucket)) = stage().await else {
        return;
    };
    let bytes = (0..(PART_BYTES * 5 + 12_345))
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect::<Vec<_>>();

    let location = store
        .put_reader(
            Box::pin(std::io::Cursor::new(bytes.clone())),
            "video/mp4".to_owned(),
        )
        .await
        .expect("multipart upload");

    assert!(
        location
            .uri
            .starts_with(&format!("s3://{bucket}/managed/objects/"))
    );
    assert_eq!(location.content_type, "video/mp4");
    assert_eq!(location.size_bytes, bytes.len() as u64);
    assert_eq!(location.sha256, format!("{:x}", Sha256::digest(&bytes)));
    let mut reader = store
        .open_reader(&location)
        .await
        .expect("open direct reader");
    let mut downloaded = Vec::new();
    reader.read_to_end(&mut downloaded).await.unwrap();
    assert_eq!(downloaded, bytes);
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn async_result_s3_upload_is_tenant_and_query_scoped_localstack() {
    let Some((_client, store, bucket)) = stage().await else {
        return;
    };
    let scope =
        ManagedObjectScope::try_new("tenant-a", "0198f73b-12b0-7d20-b8ab-8195ce8bfe73").unwrap();
    let bytes = b"bounded arrow result part".to_vec();
    let location = store
        .put_scoped_reader(
            &scope,
            "part",
            Box::pin(std::io::Cursor::new(bytes.clone())),
            "application/vnd.apache.arrow.stream".to_owned(),
        )
        .await
        .expect("scoped S3 result upload");

    assert!(location.uri.starts_with(&format!(
        "s3://{bucket}/managed/objects/tenant-a/0198f73b-12b0-7d20-b8ab-8195ce8bfe73/part/"
    )));
    let mut reader = store.open_reader(&location).await.unwrap();
    let mut downloaded = Vec::new();
    reader.read_to_end(&mut downloaded).await.unwrap();
    assert_eq!(downloaded, bytes);
}

#[test]
fn async_result_s3_scope_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("lake-objects"));
    assert!(integration.contains("--run-ignored"));
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn s3_inventory_is_bounded_sorted_and_stage_scoped_localstack() {
    let Some((client, store, bucket)) = stage().await else {
        return;
    };
    for key in [
        "managed/objects/c",
        "managed/objects/a",
        "managed/objects/b",
        "managed/objects/internal/metadata",
        "somebody-else/object",
    ] {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"inventory"))
            .send()
            .await
            .unwrap();
    }

    let first = store
        .inventory_page(InventoryRequest::try_new(None, 2).unwrap())
        .await
        .unwrap();
    let second = store
        .inventory_page(
            InventoryRequest::try_new(first.next_cursor().map(ToOwned::to_owned), 2).unwrap(),
        )
        .await
        .unwrap();
    let candidates = first
        .candidates()
        .iter()
        .chain(second.candidates())
        .collect::<Vec<_>>();
    assert_eq!(candidates.len(), 3);
    assert!(candidates.windows(2).all(|pair| pair[0].uri < pair[1].uri));
    assert!(candidates.iter().all(|candidate| {
        candidate
            .uri
            .starts_with(&format!("s3://{bucket}/managed/objects/"))
            && !candidate.uri.contains("/internal/")
    }));
    assert!(second.next_cursor().is_none());
}

#[test]
fn s3_inventory_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("lake-objects"));
    assert!(integration.contains("--run-ignored"));
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn s3_gc_apply_resumes_from_checkpoint_localstack() {
    let Some((client, store, bucket)) = stage().await else {
        return;
    };
    for key in [
        "managed/objects/live",
        "managed/objects/young",
        "managed/objects/orphan-a",
        "managed/objects/orphan-b",
    ] {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"gc"))
            .send()
            .await
            .unwrap();
    }
    let prefix = format!("s3://{bucket}/managed/objects/");
    let orphan = |name: &str| ObjectCandidate {
        uri:              format!("{prefix}{name}"),
        size_bytes:       2,
        last_modified_ms: 10,
    };
    let temp = tempfile::tempdir().unwrap();
    let plan_path = temp.path().join("plan");
    let checkpoint = temp.path().join("apply.json");
    let pages = GcPlanner::try_new(&prefix, 100, 1, true)
        .unwrap()
        .plan(vec![orphan("orphan-a"), orphan("orphan-b")], Vec::new());
    GcPlanWriter::try_new(&plan_path, &prefix, 100, 1)
        .unwrap()
        .write(pages)
        .unwrap();

    let mut first = GcPlanApplier::open(&plan_path, &checkpoint).await.unwrap();
    let progress = first.apply_next(&store).await.unwrap();
    assert_eq!(progress.completed_pages(), 1);
    assert!(!progress.is_complete());
    drop(first);

    // S3 DeleteObject is idempotent. Make the next planned object absent to
    // prove a restarted apply still advances its durable checkpoint.
    client
        .delete_object()
        .bucket(&bucket)
        .key("managed/objects/orphan-b")
        .send()
        .await
        .unwrap();
    let mut resumed = GcPlanApplier::open(&plan_path, &checkpoint).await.unwrap();
    let progress = resumed.apply_next(&store).await.unwrap();
    assert!(progress.is_complete());
    assert_eq!(progress.completed_pages(), 2);
    assert_eq!(progress.processed_objects(), 2);
    drop(resumed);

    let checkpoint_json: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&checkpoint).await.unwrap()).unwrap();
    assert_eq!(checkpoint_json["complete"], true);
    assert_eq!(checkpoint_json["next_page_index"], 2);
    for key in ["managed/objects/live", "managed/objects/young"] {
        client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .expect("live and young objects remain");
    }
}

#[test]
fn s3_gc_apply_resumes_from_checkpoint_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("lake-objects"));
    assert!(integration.contains("--run-ignored"));
}

struct FailingReader {
    bytes:      Vec<u8>,
    position:   usize,
    fail_after: usize,
}

struct BlockingReader {
    bytes_remaining: usize,
    blocked:         Option<oneshot::Sender<()>>,
}

impl AsyncRead for BlockingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.bytes_remaining == 0 {
            if let Some(blocked) = self.blocked.take() {
                let _ = blocked.send(());
            }
            return Poll::Pending;
        }
        let count = self.bytes_remaining.min(output.remaining());
        output.initialize_unfilled_to(count).fill(7);
        output.advance(count);
        self.bytes_remaining -= count;
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for FailingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.position >= self.fail_after {
            return Poll::Ready(Err(io::Error::other("injected source failure")));
        }
        let end = self
            .bytes
            .len()
            .min(self.fail_after)
            .min(self.position + output.remaining());
        output.put_slice(&self.bytes[self.position..end]);
        self.position = end;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn interrupted_s3_upload_is_aborted() {
    let Some((client, store, bucket)) = stage().await else {
        return;
    };
    let reader = FailingReader {
        bytes:      vec![7; PART_BYTES + 1024],
        position:   0,
        fail_after: PART_BYTES + 1024,
    };

    let result = store
        .put_reader(Box::pin(reader), "application/octet-stream".to_owned())
        .await;

    assert!(matches!(result, Err(ObjectError::Read { .. })));
    let uploads = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert!(
        uploads.uploads().is_empty(),
        "multipart upload was not aborted"
    );
    let objects = client
        .list_objects_v2()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert!(
        objects.contents().is_empty(),
        "failed upload published an object"
    );
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn cancelled_s3_upload_is_aborted() {
    let Some((client, store, bucket)) = stage().await else {
        return;
    };
    let (blocked_tx, blocked_rx) = oneshot::channel();
    let upload = tokio::spawn(async move {
        store
            .put_reader(
                Box::pin(BlockingReader {
                    bytes_remaining: PART_BYTES,
                    blocked:         Some(blocked_tx),
                }),
                "application/octet-stream".to_owned(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), blocked_rx)
        .await
        .expect("upload reached the blocked second source part")
        .expect("blocking reader remained alive");
    let uploads = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(uploads.uploads().len(), 1);

    upload.abort();
    assert!(upload.await.unwrap_err().is_cancelled());

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let uploads = client
                .list_multipart_uploads()
                .bucket(&bucket)
                .send()
                .await
                .unwrap();
            if uploads.uploads().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cancelled upload cleanup must converge");
}

#[test]
fn cancelled_s3_upload_is_aborted_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("--run-ignored"));
    assert!(integration.contains("ignored-only"));
}

#[test]
fn interrupted_s3_upload_is_aborted_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("--run-ignored"));
    assert!(integration.contains("ignored-only"));
}

async fn seed_resumable_checkpoint(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    source: &std::path::Path,
    checkpoint: &std::path::Path,
) {
    let key = format!("managed/objects/{}", uuid::Uuid::now_v7());
    let created = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(&key)
        .content_type("video/mp4")
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .send()
        .await
        .expect("create resumable multipart upload");
    let upload_id = created.upload_id().expect("S3 returns upload id");
    let bytes = tokio::fs::read(source).await.expect("read source");
    let first = &bytes[..PART_BYTES];
    let uploaded = client
        .upload_part()
        .bucket(bucket)
        .key(&key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(first.to_vec()))
        .send()
        .await
        .expect("seed first multipart part");
    let metadata = tokio::fs::metadata(source).await.expect("source metadata");
    let modified = metadata
        .modified()
        .expect("source modification time")
        .duration_since(std::time::UNIX_EPOCH)
        .expect("source modification time after epoch");
    let document = serde_json::json!({
        "version": 1,
        "bucket": bucket,
        "prefix": "managed/objects",
        "content_type": "video/mp4",
        "part_size_bytes": PART_BYTES,
        "source": {
            "size_bytes": metadata.len(),
            "modified_unix_nanos": u64::try_from(modified.as_nanos()).expect("mtime fits u64")
        },
        "object_key": key,
        "upload_id": upload_id,
        "parts": [{
            "number": 1,
            "size_bytes": PART_BYTES,
            "e_tag": uploaded.e_tag().expect("S3 returns part ETag"),
            "checksum_crc32": uploaded.checksum_crc32(),
            "sha256": format!("{:x}", Sha256::digest(first))
        }]
    });
    tokio::fs::write(
        checkpoint,
        serde_json::to_vec_pretty(&document).expect("encode checkpoint"),
    )
    .await
    .expect("write checkpoint");
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn resumable_s3_upload_reuses_completed_parts_localstack() {
    let Some((client, store, bucket)) = stage().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("episode.mp4");
    let checkpoint = dir.path().join("episode.upload.json");
    let bytes = (0..(PART_BYTES + 12_345))
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect::<Vec<_>>();
    tokio::fs::write(&source, &bytes).await.unwrap();
    seed_resumable_checkpoint(&client, &bucket, &source, &checkpoint).await;

    let location = store
        .put_path(source, "video/mp4".to_owned(), Some(checkpoint.clone()))
        .await
        .unwrap();

    assert!(!checkpoint.exists());
    assert_eq!(location.size_bytes, bytes.len() as u64);
    assert_eq!(location.sha256, format!("{:x}", Sha256::digest(&bytes)));
    let uploads = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert!(uploads.uploads().is_empty());
    let mut reader = store.open_reader(&location).await.unwrap();
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).await.unwrap();
    assert_eq!(actual, bytes);
}

#[test]
fn resumable_s3_upload_reuses_completed_parts_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("--run-ignored"));
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn resumable_s3_pipeline_overwrites_ambiguous_suffix_localstack() {
    let Some((client, store, bucket)) = stage().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("episode.mp4");
    let checkpoint = dir.path().join("episode.upload.json");
    let bytes = (0..(PART_BYTES * 3 + 12_345))
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect::<Vec<_>>();
    tokio::fs::write(&source, &bytes).await.unwrap();
    seed_resumable_checkpoint(&client, &bucket, &source, &checkpoint).await;
    let mut document: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&checkpoint).await.unwrap()).unwrap();
    document["upload_concurrency"] = serde_json::json!(4);
    tokio::fs::write(&checkpoint, serde_json::to_vec_pretty(&document).unwrap())
        .await
        .unwrap();
    let key = document["object_key"].as_str().unwrap();
    let upload_id = document["upload_id"].as_str().unwrap();
    for number in [2, 4] {
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(number)
            .body(ByteStream::from(vec![
                0_u8;
                if number == 4 {
                    12_345
                } else {
                    PART_BYTES
                }
            ]))
            .send()
            .await
            .expect("seed an uncheckpointed remote suffix part");
    }

    let location = store
        .put_path(source, "video/mp4".to_owned(), Some(checkpoint.clone()))
        .await
        .expect("resume overwrites every untrusted suffix part");

    assert!(!checkpoint.exists());
    assert_eq!(location.size_bytes, bytes.len() as u64);
    assert_eq!(location.sha256, format!("{:x}", Sha256::digest(&bytes)));
    let mut reader = store.open_reader(&location).await.unwrap();
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).await.unwrap();
    assert_eq!(actual, bytes);
}

#[test]
fn resumable_s3_pipeline_overwrites_ambiguous_suffix_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("lake-objects"));
    assert!(integration.contains("--run-ignored"));
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn resumable_s3_pipeline_rejects_suffix_outside_creator_window_localstack() {
    let Some((client, store, bucket)) = stage().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("episode.mp4");
    let checkpoint = dir.path().join("episode.upload.json");
    let bytes = vec![7_u8; PART_BYTES * 3];
    tokio::fs::write(&source, &bytes).await.unwrap();
    seed_resumable_checkpoint(&client, &bucket, &source, &checkpoint).await;
    let document: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&checkpoint).await.unwrap()).unwrap();
    client
        .upload_part()
        .bucket(&bucket)
        .key(document["object_key"].as_str().unwrap())
        .upload_id(document["upload_id"].as_str().unwrap())
        .part_number(3)
        .body(ByteStream::from(vec![0_u8; PART_BYTES]))
        .send()
        .await
        .unwrap();

    let result = store
        .put_path(source, "video/mp4".to_owned(), Some(checkpoint))
        .await;

    assert!(matches!(
        result,
        Err(ObjectError::CheckpointMismatch {
            field: "remote completed parts",
        })
    ));
}

#[test]
fn bounded_s3_upload_pipeline_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    let tests = include_str!("s3_localstack.rs");
    assert!(integration.contains("lake-objects"));
    assert!(integration.contains("--run-ignored"));
    for selector in [
        "s3_multipart_roundtrip_localstack",
        "interrupted_s3_upload_is_aborted",
        "resumable_s3_pipeline_overwrites_ambiguous_suffix_localstack",
    ] {
        assert!(tests.contains(selector));
    }
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn resumable_s3_upload_rejects_changed_source_localstack() {
    let Some((client, store, bucket)) = stage().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("episode.mp4");
    let checkpoint = dir.path().join("episode.upload.json");
    tokio::fs::write(&source, vec![7; PART_BYTES + 32])
        .await
        .unwrap();
    seed_resumable_checkpoint(&client, &bucket, &source, &checkpoint).await;
    tokio::fs::write(&source, vec![8; PART_BYTES + 32])
        .await
        .unwrap();

    let result = store
        .put_path(source, "video/mp4".to_owned(), Some(checkpoint))
        .await;
    assert!(matches!(
        result,
        Err(ObjectError::CheckpointMismatch { .. })
    ));
}

#[test]
fn resumable_s3_upload_rejects_changed_source_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("--run-ignored"));
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn cancel_resumable_s3_upload_aborts_and_removes_checkpoint_localstack() {
    let Some((client, store, bucket)) = stage().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("episode.mp4");
    let checkpoint = dir.path().join("episode.upload.json");
    tokio::fs::write(&source, vec![7; PART_BYTES + 32])
        .await
        .unwrap();
    seed_resumable_checkpoint(&client, &bucket, &source, &checkpoint).await;

    store.cancel_upload(checkpoint.clone()).await.unwrap();

    assert!(!checkpoint.exists());
    let uploads = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert!(uploads.uploads().is_empty());
}

#[test]
fn cancel_resumable_s3_upload_aborts_and_removes_checkpoint_localstack_is_wired() {
    let integration = include_str!("../../../scripts/test-integration.ts");
    assert!(integration.contains("--run-ignored"));
}

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn resumable_s3_upload_recovers_ambiguous_completion_localstack() {
    let Some((client, store, bucket)) = stage().await else {
        return;
    };
    let dir = tempfile::tempdir().expect("temporary source directory");
    let source = dir.path().join("episode.mp4");
    let checkpoint = dir.path().join("episode.upload.json");
    let bytes = vec![9; PART_BYTES];
    tokio::fs::write(&source, &bytes)
        .await
        .expect("write source");
    seed_resumable_checkpoint(&client, &bucket, &source, &checkpoint).await;
    let document: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&checkpoint).await.expect("read checkpoint"))
            .expect("decode checkpoint");
    let key = document["object_key"].as_str().expect("object key");
    let upload_id = document["upload_id"].as_str().expect("upload id");
    let part = &document["parts"][0];
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part["e_tag"].as_str().expect("part ETag"))
                .set_checksum_crc32(part["checksum_crc32"].as_str().map(ToOwned::to_owned))
                .build(),
        )
        .build();
    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("complete while retaining local checkpoint");

    let location = store
        .put_path(source, "video/mp4".to_owned(), Some(checkpoint.clone()))
        .await
        .expect("recover completed destination");

    assert!(!checkpoint.exists());
    assert_eq!(location.size_bytes, bytes.len() as u64);
    assert_eq!(location.sha256, format!("{:x}", Sha256::digest(bytes)));
}
