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
};

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use lake_objects::{ManagedObjectStore, ObjectError, S3ObjectStore};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

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

#[tokio::test]
#[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
async fn s3_multipart_roundtrip_localstack() {
    let Some((_client, store, bucket)) = stage().await else {
        return;
    };
    let bytes = (0..(PART_BYTES + 12_345))
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

struct FailingReader {
    bytes:      Vec<u8>,
    position:   usize,
    fail_after: usize,
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
