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

//! S3-backed managed object stage.

use std::{
    collections::BTreeMap,
    future::Future,
    marker::PhantomData,
    ops::Range,
    path::{Path, PathBuf},
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use aws_sdk_s3::{
    Client,
    presigning::PresigningConfig,
    primitives::ByteStream,
    types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart},
};
use futures::{StreamExt, stream::FuturesUnordered};
use lake_common::{DataLocation, ManagedStageBackend, ManagedStageDescriptor};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};
use url::Url;

use crate::{
    DeleteOutcome, InventoryPage, InventoryRequest, ManagedObjectDeleter, ManagedObjectInventory,
    ManagedObjectStore, ManagedReadCapabilityIssuer, ObjectCandidate, ObjectError, ObjectReader,
    PresignedRead, Result,
    checkpoint::{
        CheckpointBinding, CheckpointLock, CheckpointPart, SourceIdentity, UploadCheckpointV1,
    },
    integrity::exact_range_reader,
    validate_presign_expiration, validate_range,
};

pub(crate) const LEGACY_MULTIPART_PART_BYTES: usize = 5 * 1024 * 1024;
pub(crate) const MULTIPART_PART_BYTES: usize = 64 * 1024 * 1024;
const MAX_MULTIPART_PART_NUMBER: i32 = 10_000;
const DEFAULT_UPLOAD_CONCURRENCY: usize = 4;
pub(crate) const MAX_UPLOAD_CONCURRENCY: usize = 16;
const MULTIPART_ABORT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_SCOPED_DELETE_OBJECTS: usize = 4_100;

type PartFuture<'a> = Pin<Box<dyn Future<Output = Result<UploadedPipelinePart>> + Send + 'a>>;

fn checked_multipart_part_number(part_number: i32) -> Result<i32> {
    if (1..=MAX_MULTIPART_PART_NUMBER).contains(&part_number) {
        return Ok(part_number);
    }
    Err(ObjectError::S3MultipartPartLimit {
        part_number,
        maximum: MAX_MULTIPART_PART_NUMBER,
    })
}

pub(crate) const fn is_supported_resumable_part_size(part_size_bytes: usize) -> bool {
    matches!(
        part_size_bytes,
        LEGACY_MULTIPART_PART_BYTES | MULTIPART_PART_BYTES
    )
}

/// Preserve an accepted checkpoint's source partitioning during resume.
fn resumable_part_size(checkpoint: &UploadCheckpointV1) -> usize { checkpoint.part_size_bytes() }

fn validate_range_response(
    range: &Range<u64>,
    size_bytes: u64,
    length: u64,
    content_range: Option<&str>,
    content_length: Option<i64>,
) -> Result<()> {
    let inclusive_end = range
        .end
        .checked_sub(1)
        .ok_or(ObjectError::InvalidS3RangeResponse)?;
    let expected_content_range = format!("bytes {}-{inclusive_end}/{size_bytes}", range.start);
    let expected_content_length =
        i64::try_from(length).map_err(|_| ObjectError::InvalidS3RangeResponse)?;
    if content_range != Some(expected_content_range.as_str())
        || content_length != Some(expected_content_length)
    {
        return Err(ObjectError::InvalidS3RangeResponse);
    }
    Ok(())
}

fn is_dns_safe_bucket(bucket: &str) -> bool {
    bucket.split('.').all(|label| {
        let bytes = label.as_bytes();
        let Some(&first) = bytes.first() else {
            return false;
        };
        let Some(&last) = bytes.last() else {
            return false;
        };
        (first.is_ascii_lowercase() || first.is_ascii_digit())
            && (last.is_ascii_lowercase() || last.is_ascii_digit())
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
    })
}

fn is_raw_uri_path(prefix: &str) -> bool {
    let bytes = prefix.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'/' || is_uri_pchar(byte) {
            index += 1;
        } else if byte == b'%'
            && bytes
                .get(index + 1..index + 3)
                .is_some_and(|escape| escape.iter().all(u8::is_ascii_hexdigit))
        {
            index += 3;
        } else {
            return false;
        }
    }
    true
}

fn is_uri_pchar(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'@'
        )
}

struct UploadedPipelinePart {
    number:     i32,
    size_bytes: usize,
    sha256:     String,
    completed:  CompletedPart,
}

struct UploadPipelineSummary {
    size_bytes: u64,
    sha256:     String,
}

/// Owns cancellation cleanup for one non-resumable multipart upload. Dropping
/// the caller-facing owner closes the decision channel, which makes the
/// bounded background task abort the upload without retaining object bytes.
struct MultipartCleanupOwner {
    decision: Option<tokio::sync::oneshot::Sender<CleanupDecision>>,
    task:     Option<tokio::task::JoinHandle<()>>,
}

#[derive(Clone, Copy)]
enum CleanupDecision {
    Abort,
    Disarm,
}

impl MultipartCleanupOwner {
    fn new(client: Client, bucket: String, key: String, upload_id: String) -> Self {
        Self::spawn(async move {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await;
        })
    }

    fn spawn<F>(abort: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (decision, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            if matches!(
                receiver.await.unwrap_or(CleanupDecision::Abort),
                CleanupDecision::Abort
            ) {
                let _ = tokio::time::timeout(MULTIPART_ABORT_TIMEOUT, abort).await;
            }
        });
        Self {
            decision: Some(decision),
            task:     Some(task),
        }
    }

    async fn abort(mut self) { self.finish(CleanupDecision::Abort).await }

    async fn disarm(mut self) { self.finish(CleanupDecision::Disarm).await }

    async fn finish(&mut self, decision_value: CleanupDecision) {
        if let Some(decision) = self.decision.take() {
            let _ = decision.send(decision_value);
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

/// Reads and hashes parts in source order while polling a finite set of upload
/// futures concurrently and yielding their metadata in part-number order. No
/// detached tasks outlive this value.
struct PartUploadPipeline<'a, R, U, Fut>
where
    R: AsyncRead + Unpin,
    U: Fn(i32, Vec<u8>) -> Fut,
    Fut: Future<Output = Result<CompletedPart>> + Send + 'a,
{
    input:           &'a mut R,
    first:           Option<Vec<u8>>,
    next_number:     i32,
    part_size_bytes: usize,
    concurrency:     usize,
    exhausted:       bool,
    pending:         FuturesUnordered<PartFuture<'a>>,
    ready:           BTreeMap<i32, UploadedPipelinePart>,
    next_yield:      i32,
    uploader:        U,
    hasher:          Sha256,
    size_bytes:      u64,
    future_marker:   PhantomData<fn() -> Fut>,
}

impl<'a, R, U, Fut> PartUploadPipeline<'a, R, U, Fut>
where
    R: AsyncRead + Unpin,
    U: Fn(i32, Vec<u8>) -> Fut,
    Fut: Future<Output = Result<CompletedPart>> + Send + 'a,
{
    fn new(
        input: &'a mut R,
        first: Vec<u8>,
        first_number: i32,
        part_size_bytes: usize,
        concurrency: usize,
        uploader: U,
        hasher: Sha256,
        size_bytes: u64,
    ) -> Self {
        Self {
            input,
            first: Some(first),
            next_number: first_number,
            part_size_bytes,
            concurrency,
            exhausted: false,
            pending: FuturesUnordered::new(),
            ready: BTreeMap::new(),
            next_yield: first_number,
            uploader,
            hasher,
            size_bytes,
            future_marker: PhantomData,
        }
    }

    fn new_resumable(
        input: &'a mut R,
        first: Vec<u8>,
        first_number: i32,
        checkpoint: &UploadCheckpointV1,
        concurrency: usize,
        uploader: U,
        hasher: Sha256,
        size_bytes: u64,
    ) -> Self {
        Self::new(
            input,
            first,
            first_number,
            resumable_part_size(checkpoint),
            concurrency,
            uploader,
            hasher,
            size_bytes,
        )
    }

    async fn fill(&mut self) -> Result<()> {
        while self.pending.len() + self.ready.len() < self.concurrency && !self.exhausted {
            let bytes = match self.first.take() {
                Some(first) => first,
                None => read_part_with_size(self.input, self.part_size_bytes).await?,
            };
            if bytes.is_empty() {
                self.exhausted = true;
                break;
            }
            let number = checked_multipart_part_number(self.next_number)?;
            self.next_number = number + 1;
            let size_bytes = bytes.len();
            let sha256 = format!("{:x}", Sha256::digest(&bytes));
            self.hasher.update(&bytes);
            self.size_bytes = self.size_bytes.saturating_add(size_bytes as u64);
            let upload = (self.uploader)(number, bytes);
            self.pending.push(Box::pin(async move {
                let completed = upload.await?;
                Ok(UploadedPipelinePart {
                    number,
                    size_bytes,
                    sha256,
                    completed,
                })
            }));
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<UploadedPipelinePart>> {
        self.fill().await?;
        if let Some(part) = self.ready.remove(&self.next_yield) {
            self.next_yield += 1;
            return Ok(Some(part));
        }
        while let Some(result) = self.pending.next().await {
            let part = result?;
            self.ready.insert(part.number, part);
            if let Some(part) = self.ready.remove(&self.next_yield) {
                self.next_yield += 1;
                return Ok(Some(part));
            }
        }
        Ok(None)
    }

    fn finish(self) -> UploadPipelineSummary {
        debug_assert!(self.exhausted);
        debug_assert!(self.pending.is_empty());
        debug_assert!(self.ready.is_empty());
        UploadPipelineSummary {
            size_bytes: self.size_bytes,
            sha256:     format!("{:x}", self.hasher.finalize()),
        }
    }
}

/// Lake-owned S3 bucket prefix used for managed `FILE` values.
#[derive(Clone, Debug)]
pub struct S3ObjectStore {
    client:             Client,
    bucket:             String,
    prefix:             String,
    upload_concurrency: usize,
}

/// Query-owned S3 signer that derives one store from each tenant-scoped stage.
#[derive(Clone)]
pub struct S3ReadCapabilityIssuer {
    client: Client,
}

impl S3ReadCapabilityIssuer {
    /// Build an issuer from the Query process's existing S3 client.
    #[must_use]
    pub fn new(client: Client) -> Self { Self { client } }

    fn store_for(&self, stage: &ManagedStageDescriptor) -> Result<S3ObjectStore> {
        let ManagedStageBackend::S3 { bucket, prefix, .. } = stage.backend() else {
            return Err(ObjectError::PresignUnsupported);
        };
        S3ObjectStore::new(self.client.clone(), bucket, prefix)
    }
}

impl std::fmt::Debug for S3ReadCapabilityIssuer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3ReadCapabilityIssuer")
            .field("client", &"<redacted>")
            .finish()
    }
}

impl S3ObjectStore {
    /// Bind a client to one non-empty, URI-safe Lake-owned bucket prefix.
    pub fn new(
        client: Client,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self> {
        let bucket = bucket.into();
        let prefix = prefix.into().trim_matches('/').to_owned();
        if !is_dns_safe_bucket(&bucket) || !is_raw_uri_path(&prefix) {
            return Err(ObjectError::InvalidS3Stage);
        }
        Ok(Self {
            client,
            bucket,
            prefix,
            upload_concurrency: DEFAULT_UPLOAD_CONCURRENCY,
        })
    }

    /// Set the finite number of S3 UploadPart requests one object may keep in
    /// flight. Each request owns at most one 64 MiB part buffer.
    pub fn with_upload_concurrency(mut self, value: usize) -> Result<Self> {
        if !(1..=MAX_UPLOAD_CONCURRENCY).contains(&value) {
            return Err(ObjectError::InvalidS3UploadConcurrency {
                value,
                maximum: MAX_UPLOAD_CONCURRENCY,
            });
        }
        self.upload_concurrency = value;
        Ok(self)
    }

    /// Open an S3 response body only after enforcing this stage's ownership
    /// boundary locally.
    pub async fn open_reader(&self, location: &DataLocation) -> Result<ObjectReader> {
        let key = self.managed_key(&location.uri)?;
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| ObjectError::S3 {
                action:  "get_object",
                message: error.to_string(),
            })?;
        Ok(Box::pin(output.body.into_async_read()))
    }

    /// Open exactly one non-empty half-open byte range with an S3 Range GET.
    pub async fn open_range(
        &self,
        location: &DataLocation,
        range: Range<u64>,
    ) -> Result<ObjectReader> {
        let length = validate_range(location, &range)?;
        let key = self.managed_key(&location.uri)?;
        let inclusive_end = range.end - 1;
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(format!("bytes={}-{inclusive_end}", range.start))
            .send()
            .await
            .map_err(|error| ObjectError::S3 {
                action:  "get_object_range",
                message: error.to_string(),
            })?;
        validate_range_response(
            &range,
            location.size_bytes,
            length,
            output.content_range(),
            output.content_length(),
        )?;
        Ok(exact_range_reader(
            Box::pin(output.body.into_async_read()),
            length,
        ))
    }

    /// Mint a bounded GET capability without issuing an object request.
    pub async fn presign_read(
        &self,
        location: &DataLocation,
        expires_in: Duration,
    ) -> Result<PresignedRead> {
        validate_presign_expiration(expires_in)?;
        let key = self.managed_key(&location.uri)?;
        let start_time = SystemTime::now();
        let config = PresigningConfig::builder()
            .start_time(start_time)
            .expires_in(expires_in)
            .build()
            .map_err(|error| ObjectError::S3 {
                action:  "configure_presigned_get",
                message: error.to_string(),
            })?;
        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(config)
            .await
            .map_err(|_error| ObjectError::S3 {
                action:  "presign_get_object",
                message: "could not construct signed GET capability".to_owned(),
            })?;
        debug_assert_eq!(request.method(), "GET");
        let headers = request
            .headers()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        Ok(PresignedRead::new(
            request.uri(),
            headers,
            start_time + expires_in,
        ))
    }

    /// Validate an immutable identity without issuing a network request.
    pub fn validate_location(&self, location: &DataLocation) -> Result<()> {
        self.managed_key(&location.uri).map(drop)
    }

    fn managed_key(&self, uri: &str) -> Result<String> {
        let parsed = Url::parse(uri).map_err(|_| ObjectError::InvalidS3Uri {
            uri: redacted_s3_identity(uri),
        })?;
        if parsed.scheme() != "s3"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(ObjectError::InvalidS3Uri {
                uri: redacted_s3_identity(uri),
            });
        }
        let bucket = parsed.host_str().ok_or_else(|| ObjectError::InvalidS3Uri {
            uri: redacted_s3_identity(uri),
        })?;
        let key = parsed.path().trim_start_matches('/');
        let managed_prefix = format!("{}/", self.prefix);
        if bucket != self.bucket || !key.starts_with(&managed_prefix) || key == managed_prefix {
            return Err(ObjectError::OutsideManagedS3Prefix {
                uri:    uri.to_owned(),
                bucket: self.bucket.clone(),
                prefix: self.prefix.clone(),
            });
        }
        Ok(key.to_owned())
    }

    async fn upload_nonempty(
        &self,
        key: &str,
        first_part: Vec<u8>,
        input: &mut ObjectReader,
        content_type: &str,
    ) -> Result<(u64, String)> {
        let created = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .map_err(|error| ObjectError::S3 {
                action:  "create_multipart_upload",
                message: error.to_string(),
            })?;
        let upload_id = created.upload_id().ok_or_else(|| ObjectError::S3 {
            action:  "create_multipart_upload",
            message: "S3 response omitted upload_id".to_owned(),
        })?;
        let cleanup = MultipartCleanupOwner::new(
            self.client.clone(),
            self.bucket.clone(),
            key.to_owned(),
            upload_id.to_owned(),
        );

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key_owned = key.to_owned();
        let upload_id_owned = upload_id.to_owned();
        let uploader = move |part_number, bytes| {
            let client = client.clone();
            let bucket = bucket.clone();
            let key = key_owned.clone();
            let upload_id = upload_id_owned.clone();
            async move {
                let uploaded = client
                    .upload_part()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .body(ByteStream::from(bytes))
                    .send()
                    .await
                    .map_err(|error| ObjectError::S3 {
                        action:  "upload_part",
                        message: format!("{error:?}"),
                    })?;
                let e_tag = uploaded.e_tag().ok_or_else(|| ObjectError::S3 {
                    action:  "upload_part",
                    message: "S3 response omitted ETag".to_owned(),
                })?;
                Ok(CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(e_tag)
                    .set_checksum_crc32(uploaded.checksum_crc32().map(ToOwned::to_owned))
                    .build())
            }
        };
        let mut pipeline = PartUploadPipeline::new(
            input,
            first_part,
            1,
            MULTIPART_PART_BYTES,
            self.upload_concurrency,
            uploader,
            Sha256::new(),
            0,
        );
        let mut parts = Vec::new();
        loop {
            match pipeline.next().await {
                Ok(Some(part)) => parts.push(part.completed),
                Ok(None) => break,
                Err(error) => {
                    // Cancel every request still owned by the pipeline before
                    // the upload itself is aborted. Otherwise a late
                    // UploadPart may race the AbortMultipartUpload request.
                    drop(pipeline);
                    cleanup.abort().await;
                    return Err(error);
                }
            }
        }
        let summary = pipeline.finish();
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        if let Err(error) = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
        {
            cleanup.abort().await;
            return Err(ObjectError::S3 {
                action:  "complete_multipart_upload",
                message: error.to_string(),
            });
        }
        cleanup.disarm().await;
        Ok((summary.size_bytes, summary.sha256))
    }

    async fn abort(&self, key: &str, upload_id: &str) {
        let _ = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
    }

    async fn put_path_resumable(
        &self,
        source: &Path,
        content_type: String,
        checkpoint_path: &Path,
    ) -> Result<DataLocation> {
        let _lock = CheckpointLock::acquire(checkpoint_path).await?;
        let mut input = tokio::fs::File::open(source)
            .await
            .map_err(|source_error| ObjectError::Io {
                action: "opening".to_owned(),
                path:   source.to_path_buf(),
                source: source_error,
            })?;
        let metadata = input
            .metadata()
            .await
            .map_err(|source_error| ObjectError::Io {
                action: "reading metadata for".to_owned(),
                path:   source.to_path_buf(),
                source: source_error,
            })?;
        let modified = metadata
            .modified()
            .and_then(|value| {
                value
                    .duration_since(UNIX_EPOCH)
                    .map_err(std::io::Error::other)
            })
            .map_err(|source_error| ObjectError::Io {
                action: "reading modification time for".to_owned(),
                path:   source.to_path_buf(),
                source: source_error,
            })?;
        let binding = CheckpointBinding {
            bucket:             self.bucket.clone(),
            prefix:             self.prefix.clone(),
            content_type:       content_type.clone(),
            part_size_bytes:    MULTIPART_PART_BYTES,
            upload_concurrency: self.upload_concurrency,
            source:             SourceIdentity {
                size_bytes:          metadata.len(),
                modified_unix_nanos: u64::try_from(modified.as_nanos()).map_err(|_| {
                    ObjectError::CheckpointMismatch {
                        field: "source modification time",
                    }
                })?,
            },
        };

        if metadata.len() == 0 {
            if checkpoint_path.exists() {
                return Err(ObjectError::CheckpointMismatch {
                    field: "empty source checkpoint",
                });
            }
            let key = format!("{}/{}", self.prefix, uuid::Uuid::now_v7());
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .content_type(&content_type)
                .body(ByteStream::from_static(&[]))
                .send()
                .await
                .map_err(|error| ObjectError::S3 {
                    action:  "put_object",
                    message: error.to_string(),
                })?;
            return Ok(DataLocation::builder()
                .uri(format!("s3://{}/{key}", self.bucket))
                .content_type(content_type)
                .size_bytes(0)
                .sha256(format!("{:x}", Sha256::digest([])))
                .build());
        }

        let mut checkpoint = if checkpoint_path.exists() {
            let checkpoint = UploadCheckpointV1::load(checkpoint_path).await?;
            checkpoint.validate(&binding)?;
            checkpoint
        } else {
            let key = format!("{}/{}", self.prefix, uuid::Uuid::now_v7());
            let created = self
                .client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(&key)
                .content_type(&content_type)
                .checksum_algorithm(ChecksumAlgorithm::Crc32)
                .send()
                .await
                .map_err(|error| ObjectError::S3 {
                    action:  "create_multipart_upload",
                    message: error.to_string(),
                })?;
            let upload_id = created.upload_id().ok_or_else(|| ObjectError::S3 {
                action:  "create_multipart_upload",
                message: "S3 response omitted upload_id".to_owned(),
            })?;
            let checkpoint = UploadCheckpointV1::new(binding, key, upload_id.to_owned());
            if let Err(error) = checkpoint.save_atomic(checkpoint_path).await {
                self.abort(checkpoint.object_key(), checkpoint.upload_id())
                    .await;
                return Err(error);
            }
            checkpoint
        };
        let mut hasher = Sha256::new();
        let mut completed_size = 0_u64;
        for completed in checkpoint.parts() {
            let part = read_resumable_part(&mut input, &checkpoint).await?;
            if part.len() != completed.size_bytes
                || format!("{:x}", Sha256::digest(&part)) != completed.sha256
            {
                return Err(ObjectError::CheckpointMismatch {
                    field: "completed source part",
                });
            }
            hasher.update(&part);
            completed_size = completed_size.saturating_add(part.len() as u64);
        }
        if let Err(reconcile_error) = self.reconcile_parts(&checkpoint).await {
            if completed_size == metadata.len() {
                let expected_sha256 = format!("{:x}", hasher.clone().finalize());
                if let Some(location) = self
                    .recover_completed_object(
                        &checkpoint,
                        &content_type,
                        metadata.len(),
                        &expected_sha256,
                    )
                    .await?
                {
                    tokio::fs::remove_file(checkpoint_path)
                        .await
                        .map_err(|source| ObjectError::CheckpointIo {
                            action: "removing recovered",
                            path: checkpoint_path.to_path_buf(),
                            source,
                        })?;
                    return Ok(location);
                }
            }
            return Err(reconcile_error);
        }
        if checkpoint.raise_upload_concurrency(self.upload_concurrency) {
            checkpoint.save_atomic(checkpoint_path).await?;
        }

        let first = read_resumable_part(&mut input, &checkpoint).await?;
        let first_number =
            i32::try_from(checkpoint.parts().len() + 1).map_err(|_| ObjectError::S3 {
                action:  "upload_part",
                message: "multipart upload exceeded the S3 part limit".to_owned(),
            })?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = checkpoint.object_key().to_owned();
        let upload_id = checkpoint.upload_id().to_owned();
        let uploader = move |number, bytes| {
            let client = client.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let upload_id = upload_id.clone();
            async move {
                let uploaded = client
                    .upload_part()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .part_number(number)
                    .body(ByteStream::from(bytes))
                    .send()
                    .await
                    .map_err(|error| ObjectError::S3 {
                        action:  "upload_part",
                        message: format!("{error:?}"),
                    })?;
                let e_tag = uploaded.e_tag().ok_or_else(|| ObjectError::S3 {
                    action:  "upload_part",
                    message: "S3 response omitted ETag".to_owned(),
                })?;
                Ok(CompletedPart::builder()
                    .part_number(number)
                    .e_tag(e_tag)
                    .set_checksum_crc32(uploaded.checksum_crc32().map(ToOwned::to_owned))
                    .build())
            }
        };
        let mut pipeline = PartUploadPipeline::new_resumable(
            &mut input,
            first,
            first_number,
            &checkpoint,
            self.upload_concurrency,
            uploader,
            hasher,
            completed_size,
        );
        loop {
            match pipeline.next().await {
                Ok(Some(part)) => {
                    checkpoint.push_part(CheckpointPart {
                        number:         part.number,
                        size_bytes:     part.size_bytes,
                        e_tag:          part
                            .completed
                            .e_tag()
                            .ok_or_else(|| ObjectError::S3 {
                                action:  "upload_part",
                                message: "S3 response omitted ETag".to_owned(),
                            })?
                            .to_owned(),
                        checksum_crc32: part.completed.checksum_crc32().map(ToOwned::to_owned),
                        sha256:         part.sha256,
                    });
                    checkpoint.save_atomic(checkpoint_path).await?;
                }
                Ok(None) => break,
                Err(error @ ObjectError::S3MultipartPartLimit { .. }) => {
                    drop(pipeline);
                    self.abort(checkpoint.object_key(), checkpoint.upload_id())
                        .await;
                    let _ = tokio::fs::remove_file(checkpoint_path).await;
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        let summary = pipeline.finish();
        if summary.size_bytes != metadata.len() {
            return Err(ObjectError::CheckpointMismatch {
                field: "source size during upload",
            });
        }

        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(
                checkpoint
                    .parts()
                    .iter()
                    .map(|part| {
                        CompletedPart::builder()
                            .part_number(part.number)
                            .e_tag(&part.e_tag)
                            .set_checksum_crc32(part.checksum_crc32.clone())
                            .build()
                    })
                    .collect(),
            ))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(checkpoint.object_key())
            .upload_id(checkpoint.upload_id())
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|error| ObjectError::S3 {
                action:  "complete_multipart_upload",
                message: error.to_string(),
            })?;
        tokio::fs::remove_file(checkpoint_path)
            .await
            .map_err(|source| ObjectError::CheckpointIo {
                action: "removing completed",
                path: checkpoint_path.to_path_buf(),
                source,
            })?;
        Ok(DataLocation::builder()
            .uri(format!("s3://{}/{}", self.bucket, checkpoint.object_key()))
            .content_type(content_type)
            .size_bytes(metadata.len())
            .sha256(summary.sha256)
            .build())
    }

    async fn reconcile_parts(&self, checkpoint: &UploadCheckpointV1) -> Result<()> {
        let mut remote_parts = Vec::new();
        let mut marker = None;
        loop {
            let output = self
                .client
                .list_parts()
                .bucket(&self.bucket)
                .key(checkpoint.object_key())
                .upload_id(checkpoint.upload_id())
                .set_part_number_marker(marker)
                .send()
                .await
                .map_err(|error| ObjectError::S3 {
                    action:  "list_parts",
                    message: error.to_string(),
                })?;
            remote_parts.extend(output.parts().iter().cloned());
            if output.is_truncated() != Some(true) {
                break;
            }
            marker = Some(
                output
                    .next_part_number_marker()
                    .ok_or(ObjectError::CheckpointMismatch {
                        field: "remote part pagination",
                    })?
                    .to_owned(),
            );
        }

        let prefix_len = checkpoint.parts().len();
        let suffix_max_number = prefix_len.saturating_add(checkpoint.upload_concurrency());
        if remote_parts.len() < prefix_len
            || remote_parts.len() > prefix_len.saturating_add(checkpoint.upload_concurrency())
            || !remote_parts
                .iter()
                .take(prefix_len)
                .zip(checkpoint.parts())
                .all(|(remote, local)| {
                    remote.part_number() == Some(local.number)
                        && remote.e_tag() == Some(local.e_tag.as_str())
                        && remote.size().and_then(|size| usize::try_from(size).ok())
                            == Some(local.size_bytes)
                        && remote.checksum_crc32() == local.checksum_crc32.as_deref()
                })
            || !remote_parts.iter().skip(prefix_len).all(|remote| {
                remote
                    .part_number()
                    .and_then(|number| usize::try_from(number).ok())
                    .is_some_and(|number| number > prefix_len && number <= suffix_max_number)
            })
        {
            return Err(ObjectError::CheckpointMismatch {
                field: "remote completed parts",
            });
        }
        Ok(())
    }

    async fn recover_completed_object(
        &self,
        checkpoint: &UploadCheckpointV1,
        content_type: &str,
        expected_size: u64,
        expected_sha256: &str,
    ) -> Result<Option<DataLocation>> {
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(checkpoint.object_key())
            .send()
            .await
        {
            Ok(output) => output,
            Err(_) => return Ok(None),
        };
        let mut reader = output.body.into_async_read();
        let mut hasher = Sha256::new();
        let mut size = 0_u64;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let read = reader
                .read(&mut buffer)
                .await
                .map_err(|source| ObjectError::Read { source })?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            size = size.saturating_add(read as u64);
        }
        let actual_sha256 = format!("{:x}", hasher.finalize());
        if size != expected_size || actual_sha256 != expected_sha256 {
            return Err(ObjectError::CheckpointMismatch {
                field: "completed destination object",
            });
        }
        Ok(Some(
            DataLocation::builder()
                .uri(format!("s3://{}/{}", self.bucket, checkpoint.object_key()))
                .content_type(content_type)
                .size_bytes(size)
                .sha256(actual_sha256)
                .build(),
        ))
    }
}

#[async_trait]
impl ManagedReadCapabilityIssuer for S3ReadCapabilityIssuer {
    fn validate(&self, stage: &ManagedStageDescriptor, location: &DataLocation) -> Result<()> {
        self.store_for(stage)?.validate_location(location)
    }

    async fn issue(
        &self,
        stage: &ManagedStageDescriptor,
        location: &DataLocation,
        expires_in: Duration,
    ) -> Result<PresignedRead> {
        self.store_for(stage)?
            .presign_read(location, expires_in)
            .await
    }
}

fn redacted_s3_identity(uri: &str) -> String {
    let Ok(parsed) = Url::parse(uri) else {
        return "<redacted-invalid-s3-uri>".to_owned();
    };
    let Some(host) = parsed.host_str() else {
        return "<redacted-invalid-s3-uri>".to_owned();
    };
    format!("{}://{}{}", parsed.scheme(), host, parsed.path())
}

#[async_trait]
impl ManagedObjectStore for S3ObjectStore {
    fn stage_identity(&self) -> String { format!("s3://{}/{}", self.bucket, self.prefix) }

    async fn put_reader(
        &self,
        mut input: ObjectReader,
        content_type: String,
    ) -> Result<DataLocation> {
        let key = format!("{}/{}", self.prefix, uuid::Uuid::now_v7());
        let first_part = read_part(&mut input).await?;
        let hasher = Sha256::new();
        let (size_bytes, sha256) = if first_part.is_empty() {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .content_type(&content_type)
                .body(ByteStream::from_static(&[]))
                .send()
                .await
                .map_err(|error| ObjectError::S3 {
                    action:  "put_object",
                    message: error.to_string(),
                })?;
            (0, format!("{:x}", hasher.finalize()))
        } else {
            self.upload_nonempty(&key, first_part, &mut input, &content_type)
                .await?
        };
        Ok(DataLocation::builder()
            .uri(format!("s3://{}/{key}", self.bucket))
            .content_type(content_type)
            .size_bytes(size_bytes)
            .sha256(sha256)
            .build())
    }

    async fn put_scoped_reader(
        &self,
        scope: &crate::ManagedObjectScope,
        class: &str,
        input: ObjectReader,
        content_type: String,
    ) -> Result<DataLocation> {
        let scoped = Self {
            client:             self.client.clone(),
            bucket:             self.bucket.clone(),
            prefix:             format!("{}/{}", self.prefix, scope.relative_prefix(class)?),
            upload_concurrency: self.upload_concurrency,
        };
        ManagedObjectStore::put_reader(&scoped, input, content_type).await
    }

    async fn delete_scope(&self, scope: &crate::ManagedObjectScope) -> Result<()> {
        let prefix = format!("{}/{}/", self.prefix, scope.relative_scope_prefix());
        let mut continuation = None;
        let mut deleted = 0_usize;
        loop {
            let output = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix)
                .set_continuation_token(continuation.take())
                .send()
                .await
                .map_err(|error| ObjectError::S3 {
                    action:  "list_objects_v2",
                    message: error.to_string(),
                })?;
            if deleted.saturating_add(output.contents().len()) > MAX_SCOPED_DELETE_OBJECTS {
                return Err(ObjectError::ScopedDeleteTooLarge);
            }
            for object in output.contents() {
                let key = object.key().ok_or_else(|| ObjectError::S3 {
                    action:  "list_objects_v2",
                    message: "scoped S3 object omitted key".to_owned(),
                })?;
                self.client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(|error| ObjectError::S3 {
                        action:  "delete_object",
                        message: error.to_string(),
                    })?;
                deleted += 1;
            }
            if output.is_truncated() != Some(true) {
                return Ok(());
            }
            continuation = Some(
                output
                    .next_continuation_token()
                    .ok_or_else(|| ObjectError::S3 {
                        action:  "list_objects_v2",
                        message: "truncated scoped S3 listing omitted continuation token"
                            .to_owned(),
                    })?
                    .to_owned(),
            );
        }
    }

    async fn open_reader(&self, location: &DataLocation) -> Result<ObjectReader> {
        S3ObjectStore::open_reader(self, location).await
    }

    async fn put_path(
        &self,
        path: PathBuf,
        content_type: String,
        checkpoint: Option<PathBuf>,
    ) -> Result<DataLocation> {
        match checkpoint {
            Some(checkpoint) => {
                self.put_path_resumable(&path, content_type, &checkpoint)
                    .await
            }
            None => {
                let input =
                    tokio::fs::File::open(&path)
                        .await
                        .map_err(|source| ObjectError::Io {
                            action: "opening".to_owned(),
                            path,
                            source,
                        })?;
                self.put_reader(Box::pin(input), content_type).await
            }
        }
    }

    async fn cancel_upload(&self, checkpoint: PathBuf) -> Result<()> {
        let _lock = CheckpointLock::acquire(&checkpoint).await?;
        let state = UploadCheckpointV1::load(&checkpoint).await?;
        if !state.stage_matches(&self.bucket, &self.prefix) {
            return Err(ObjectError::CheckpointMismatch { field: "stage" });
        }
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(state.object_key())
            .upload_id(state.upload_id())
            .send()
            .await
            .map_err(|error| ObjectError::S3 {
                action:  "abort_multipart_upload",
                message: error.to_string(),
            })?;
        tokio::fs::remove_file(&checkpoint)
            .await
            .map_err(|source| ObjectError::CheckpointIo {
                action: "removing cancelled",
                path: checkpoint,
                source,
            })
    }

    async fn open_range(&self, location: &DataLocation, range: Range<u64>) -> Result<ObjectReader> {
        S3ObjectStore::open_range(self, location, range).await
    }

    async fn presign_read(
        &self,
        location: &DataLocation,
        expires_in: Duration,
    ) -> Result<PresignedRead> {
        S3ObjectStore::presign_read(self, location, expires_in).await
    }
}

#[async_trait]
impl ManagedObjectInventory for S3ObjectStore {
    fn managed_uri_prefix(&self) -> String { format!("s3://{}/{}/", self.bucket, self.prefix) }

    async fn inventory_page(&self, request: InventoryRequest) -> Result<InventoryPage> {
        let (cursor, max_items) = request.into_parts();
        let listing_prefix = format!("{}/", self.prefix);
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&listing_prefix)
            .delimiter("/")
            .max_keys(i32::try_from(max_items).expect("inventory page limit fits i32"))
            .set_continuation_token(cursor)
            .send()
            .await
            .map_err(|error| ObjectError::S3 {
                action:  "list_objects_v2",
                message: error.to_string(),
            })?;
        let mut candidates = Vec::with_capacity(output.contents().len());
        for object in output.contents() {
            let key = object.key().ok_or_else(|| ObjectError::S3 {
                action:  "list_objects_v2",
                message: "S3 inventory entry omitted key".to_owned(),
            })?;
            if key == listing_prefix {
                continue;
            }
            let size_bytes = u64::try_from(object.size().ok_or_else(|| ObjectError::S3 {
                action:  "list_objects_v2",
                message: format!("S3 inventory entry '{key}' omitted size"),
            })?)
            .map_err(|_| ObjectError::S3 {
                action:  "list_objects_v2",
                message: format!("S3 inventory entry '{key}' has negative size"),
            })?;
            let modified = object.last_modified().ok_or_else(|| ObjectError::S3 {
                action:  "list_objects_v2",
                message: format!("S3 inventory entry '{key}' omitted last_modified"),
            })?;
            let last_modified_ms =
                u64::try_from(modified.as_nanos() / 1_000_000).map_err(|_| ObjectError::S3 {
                    action:  "list_objects_v2",
                    message: format!("S3 inventory entry '{key}' has invalid last_modified"),
                })?;
            candidates.push(ObjectCandidate {
                uri: format!("s3://{}/{key}", self.bucket),
                size_bytes,
                last_modified_ms,
            });
        }
        if !candidates.windows(2).all(|pair| pair[0].uri < pair[1].uri) {
            return Err(ObjectError::GcInputUnsorted {
                input: "S3 inventory page",
            });
        }
        let next_cursor = if output.is_truncated() == Some(true) {
            Some(
                output
                    .next_continuation_token()
                    .ok_or_else(|| ObjectError::S3 {
                        action:  "list_objects_v2",
                        message: "truncated S3 inventory omitted continuation token".to_owned(),
                    })?
                    .to_owned(),
            )
        } else {
            None
        };
        Ok(InventoryPage::new(candidates, next_cursor))
    }
}

#[async_trait]
impl ManagedObjectDeleter for S3ObjectStore {
    fn managed_uri_prefix(&self) -> String {
        <Self as ManagedObjectInventory>::managed_uri_prefix(self)
    }

    async fn delete_candidate(&self, candidate: &ObjectCandidate) -> Result<DeleteOutcome> {
        let key = self.managed_key(&candidate.uri)?;
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| ObjectError::S3 {
                action:  "delete_object",
                message: error.to_string(),
            })?;
        Ok(DeleteOutcome::DeletedOrAbsent)
    }
}

async fn read_part<R>(input: &mut R) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin + ?Sized,
{
    read_part_with_size(input, MULTIPART_PART_BYTES).await
}

async fn read_resumable_part<R>(input: &mut R, checkpoint: &UploadCheckpointV1) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin + ?Sized,
{
    read_part_with_size(input, resumable_part_size(checkpoint)).await
}

async fn read_part_with_size<R>(input: &mut R, part_size_bytes: usize) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut part = Vec::with_capacity(part_size_bytes);
    let mut buffer = vec![0_u8; 64 * 1024];
    while part.len() < part_size_bytes {
        let remaining = part_size_bytes - part.len();
        let chunk = remaining.min(buffer.len());
        let read = input
            .read(&mut buffer[..chunk])
            .await
            .map_err(|source| ObjectError::Read { source })?;
        if read == 0 {
            break;
        }
        part.extend_from_slice(&buffer[..read]);
    }
    Ok(part)
}

#[cfg(test)]
mod pipeline_tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use aws_config::BehaviorVersion;
    use aws_sdk_s3::{
        config::{Credentials, Region},
        types::CompletedPart,
    };
    use lake_common::{DataLocation, ManagedStageDescriptor};
    use sha2::{Digest, Sha256};
    use tokio::sync::{Notify, Semaphore};

    use super::{
        LEGACY_MULTIPART_PART_BYTES, MAX_UPLOAD_CONCURRENCY, MULTIPART_PART_BYTES,
        MultipartCleanupOwner, PartUploadPipeline, S3ObjectStore, S3ReadCapabilityIssuer,
        checked_multipart_part_number, read_part_with_size, read_resumable_part,
        validate_range_response,
    };
    use crate::{
        ManagedObjectStore, ManagedReadCapabilityIssuer, ObjectError, ObjectReader,
        checkpoint::{CheckpointBinding, CheckpointPart, SourceIdentity, UploadCheckpointV1},
    };

    const TEST_MULTIPART_PART_BYTES: usize = 1024;

    fn update_peak(peak: &AtomicUsize, value: usize) {
        let mut observed = peak.load(Ordering::SeqCst);
        while value > observed {
            match peak.compare_exchange(observed, value, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(current) => observed = current,
            }
        }
    }

    fn test_client() -> aws_sdk_s3::Client {
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url("http://127.0.0.1:1")
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .force_path_style(true)
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }

    fn test_store() -> S3ObjectStore {
        S3ObjectStore::new(test_client(), "lake-managed", "managed/objects").unwrap()
    }

    #[test]
    fn s3_range_response_requires_exact_interval() {
        let requested = 3..7;
        validate_range_response(&requested, 10, 4, Some("bytes 3-6/10"), Some(4))
            .expect("exact S3 range response");

        for (content_range, content_length) in [
            (None, Some(4)),
            (Some("bytes 0-3/10"), Some(4)),
            (Some("bytes 3-5/10"), Some(4)),
            (Some("bytes 3-6/11"), Some(4)),
            (Some("bytes 3-6/10"), None),
            (Some("bytes 3-6/10"), Some(3)),
        ] {
            assert!(matches!(
                validate_range_response(&requested, 10, 4, content_range, content_length),
                Err(ObjectError::InvalidS3RangeResponse)
            ));
        }
    }

    #[test]
    fn s3_stage_rejects_unsafe_uri_components_before_io() {
        let store =
            S3ObjectStore::new(test_client(), "lake-managed", "tenants/tenant-a/objects").unwrap();
        assert!(
            store
                .stage_identity()
                .bytes()
                .all(|byte| (0x21..=0x7e).contains(&byte) && byte != b'"' && byte != b'\\')
        );
        assert_eq!(
            S3ObjectStore::new(test_client(), "lake-managed", "tenants/%7Etenant-a/objects",)
                .unwrap()
                .stage_identity(),
            "s3://lake-managed/tenants/%7Etenant-a/objects"
        );

        for (bucket, prefix) in [
            ("lake managed", "tenants/tenant-a/objects"),
            ("lakeémanaged", "tenants/tenant-a/objects"),
            ("lake\"managed", "tenants/tenant-a/objects"),
            ("lake\\managed", "tenants/tenant-a/objects"),
            ("lake-managed", "tenants/tenant a/objects"),
            ("lake-managed", "tenants/tenant-é/objects"),
            ("lake-managed", "tenants/tenant-\"/objects"),
            ("lake-managed", "tenants/tenant-\\/objects"),
            ("lake-managed", "tenants/tenant?/objects"),
            ("lake-managed", "tenants/tenant#/objects"),
            ("lake-managed", "tenants/tenant%2/objects"),
            ("lake-managed", "tenants/tenant%GG/objects"),
        ] {
            assert!(matches!(
                S3ObjectStore::new(test_client(), bucket, prefix),
                Err(ObjectError::InvalidS3Stage)
            ));
        }
    }

    #[test]
    fn s3_upload_concurrency_rejects_unbounded_values() {
        let store = test_store();
        assert!(store.clone().with_upload_concurrency(0).is_err());
        assert!(store.clone().with_upload_concurrency(1).is_ok());
        assert!(
            store
                .clone()
                .with_upload_concurrency(MAX_UPLOAD_CONCURRENCY)
                .is_ok()
        );
        assert!(
            store
                .with_upload_concurrency(MAX_UPLOAD_CONCURRENCY + 1)
                .is_err()
        );
    }

    #[test]
    fn s3_read_capability_issuer_rejects_unsafe_identity_before_signing() {
        let issuer = S3ReadCapabilityIssuer::new(test_store().client.clone());
        let stage = ManagedStageDescriptor::s3(
            "lake-managed",
            "managed/objects",
            Some("us-east-1".to_owned()),
            None,
            true,
        );
        let location = DataLocation::builder()
            .uri("s3://lake-managed/managed/objects/tenants/tenant-a/episode.mp4?leak=1")
            .content_type("video/mp4")
            .size_bytes(42)
            .sha256("f00d")
            .build();

        assert!(matches!(
            issuer.validate(&stage, &location),
            Err(ObjectError::InvalidS3Uri { .. })
        ));
    }

    #[test]
    fn multipart_part_number_limit_rejects_10001st_part() {
        assert_eq!(checked_multipart_part_number(10_000).unwrap(), 10_000);
        assert!(matches!(
            checked_multipart_part_number(10_001),
            Err(ObjectError::S3MultipartPartLimit {
                part_number: 10_001,
                maximum:     10_000,
            })
        ));
    }

    #[tokio::test]
    async fn multipart_pipeline_accepts_10000th_part_and_rejects_10001st_before_upload() {
        let accepted_uploads = Arc::new(AtomicUsize::new(0));
        let mut input: ObjectReader = Box::pin(std::io::Cursor::new(vec![8]));
        let uploader = {
            let accepted_uploads = accepted_uploads.clone();
            move |number, _bytes: Vec<u8>| {
                let accepted_uploads = accepted_uploads.clone();
                Box::pin(async move {
                    accepted_uploads.fetch_add(1, Ordering::SeqCst);
                    Ok(CompletedPart::builder()
                        .part_number(number)
                        .e_tag(format!("part-{number}"))
                        .build())
                })
            }
        };
        let mut pipeline = PartUploadPipeline::new(
            &mut input,
            vec![7],
            10_000,
            1,
            1,
            uploader,
            Sha256::new(),
            0,
        );
        assert_eq!(pipeline.next().await.unwrap().unwrap().number, 10_000);
        assert_eq!(accepted_uploads.load(Ordering::SeqCst), 1);
        assert!(matches!(
            pipeline.next().await,
            Err(ObjectError::S3MultipartPartLimit {
                part_number: 10_001,
                maximum:     10_000,
            })
        ));
        assert_eq!(accepted_uploads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn multipart_cleanup_owner_state_transitions() {
        let aborts = Arc::new(AtomicUsize::new(0));
        let owner = {
            let aborts = aborts.clone();
            MultipartCleanupOwner::spawn(async move {
                aborts.fetch_add(1, Ordering::SeqCst);
            })
        };
        drop(owner);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while aborts.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drop starts cancellation cleanup");

        let owner = {
            let aborts = aborts.clone();
            MultipartCleanupOwner::spawn(async move {
                aborts.fetch_add(1, Ordering::SeqCst);
            })
        };
        owner.disarm().await;
        assert_eq!(aborts.load(Ordering::SeqCst), 1);

        let owner = {
            let aborts = aborts.clone();
            MultipartCleanupOwner::spawn(async move {
                aborts.fetch_add(1, Ordering::SeqCst);
            })
        };
        owner.abort().await;
        assert_eq!(aborts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn bounded_multipart_pipeline_overlaps_with_exact_resource_cap() {
        let concurrency = 3;
        let source = (0..(TEST_MULTIPART_PART_BYTES * 5 + 17))
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>();
        let live_requests = Arc::new(AtomicUsize::new(0));
        let peak_requests = Arc::new(AtomicUsize::new(0));
        let live_bytes = Arc::new(AtomicUsize::new(0));
        let peak_bytes = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));

        let task = tokio::spawn({
            let live_requests = live_requests.clone();
            let peak_requests = peak_requests.clone();
            let live_bytes = live_bytes.clone();
            let peak_bytes = peak_bytes.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                let mut input: ObjectReader = Box::pin(std::io::Cursor::new(source));
                let first = read_part_with_size(&mut input, TEST_MULTIPART_PART_BYTES)
                    .await
                    .unwrap();
                let uploader = move |number, bytes: Vec<u8>| {
                    let live_requests = live_requests.clone();
                    let peak_requests = peak_requests.clone();
                    let live_bytes = live_bytes.clone();
                    let peak_bytes = peak_bytes.clone();
                    let entered = entered.clone();
                    let release = release.clone();
                    Box::pin(async move {
                        let requests = live_requests.fetch_add(1, Ordering::SeqCst) + 1;
                        let bytes_live =
                            live_bytes.fetch_add(bytes.len(), Ordering::SeqCst) + bytes.len();
                        update_peak(&peak_requests, requests);
                        update_peak(&peak_bytes, bytes_live);
                        entered.notify_waiters();
                        release.acquire().await.unwrap().forget();
                        live_requests.fetch_sub(1, Ordering::SeqCst);
                        live_bytes.fetch_sub(bytes.len(), Ordering::SeqCst);
                        Ok(CompletedPart::builder()
                            .part_number(number)
                            .e_tag(format!("part-{number}"))
                            .build())
                    })
                };
                let mut pipeline = PartUploadPipeline::new(
                    &mut input,
                    first,
                    1,
                    TEST_MULTIPART_PART_BYTES,
                    concurrency,
                    uploader,
                    Sha256::new(),
                    0,
                );
                let mut parts = Vec::new();
                while let Some(part) = pipeline.next().await.unwrap() {
                    parts.push(part.completed);
                }
                (parts, pipeline.finish())
            }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while peak_requests.load(Ordering::SeqCst) < concurrency {
                entered.notified().await;
            }
        })
        .await
        .expect("the configured request window fills");
        assert_eq!(peak_requests.load(Ordering::SeqCst), concurrency);
        assert_eq!(
            peak_bytes.load(Ordering::SeqCst),
            concurrency * TEST_MULTIPART_PART_BYTES
        );
        release.add_permits(6);
        let (parts, summary) = task.await.unwrap();
        assert_eq!(parts.len(), 6);
        assert_eq!(
            summary.size_bytes,
            (TEST_MULTIPART_PART_BYTES * 5 + 17) as u64
        );
    }

    #[tokio::test]
    async fn multipart_pipeline_orders_parts_and_source_hash() {
        let source = (0..(TEST_MULTIPART_PART_BYTES * 3 + 11))
            .map(|index| u8::try_from(index % 239).unwrap())
            .collect::<Vec<_>>();
        let expected_hash = format!("{:x}", Sha256::digest(&source));
        let mut input: ObjectReader = Box::pin(std::io::Cursor::new(source.clone()));
        let first = read_part_with_size(&mut input, TEST_MULTIPART_PART_BYTES)
            .await
            .unwrap();
        let uploader = |number, _bytes: Vec<u8>| {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(
                    u64::try_from(5 - number).unwrap(),
                ))
                .await;
                Ok(CompletedPart::builder()
                    .part_number(number)
                    .e_tag(format!("part-{number}"))
                    .build())
            })
        };
        let mut pipeline = PartUploadPipeline::new(
            &mut input,
            first,
            1,
            TEST_MULTIPART_PART_BYTES,
            4,
            uploader,
            Sha256::new(),
            0,
        );
        let mut numbers = Vec::new();
        while let Some(part) = pipeline.next().await.unwrap() {
            numbers.push(part.number);
        }
        let summary = pipeline.finish();

        assert_eq!(numbers, vec![1, 2, 3, 4]);
        assert_eq!(summary.size_bytes, source.len() as u64);
        assert_eq!(summary.sha256, expected_hash);
    }

    #[tokio::test]
    async fn multipart_pipeline_failure_stops_admission() {
        let concurrency = 3;
        let source = vec![7_u8; TEST_MULTIPART_PART_BYTES * 6];
        let admitted = Arc::new(AtomicUsize::new(0));
        let hold_first = Arc::new(Semaphore::new(0));
        let mut input: ObjectReader = Box::pin(std::io::Cursor::new(source));
        let first = read_part_with_size(&mut input, TEST_MULTIPART_PART_BYTES)
            .await
            .unwrap();
        let uploader = {
            let admitted = admitted.clone();
            let hold_first = hold_first.clone();
            move |number, _bytes: Vec<u8>| {
                let admitted = admitted.clone();
                let hold_first = hold_first.clone();
                Box::pin(async move {
                    admitted.fetch_add(1, Ordering::SeqCst);
                    if number == 1 {
                        hold_first.acquire().await.unwrap().forget();
                    }
                    if number == 2 {
                        return Err(crate::ObjectError::S3 {
                            action:  "upload_part",
                            message: "injected failure".to_owned(),
                        });
                    }
                    Ok(CompletedPart::builder()
                        .part_number(number)
                        .e_tag(format!("part-{number}"))
                        .build())
                })
            }
        };
        let mut pipeline = PartUploadPipeline::new(
            &mut input,
            first,
            1,
            TEST_MULTIPART_PART_BYTES,
            concurrency,
            uploader,
            Sha256::new(),
            0,
        );

        let result = tokio::time::timeout(std::time::Duration::from_millis(100), pipeline.next())
            .await
            .expect("a later failed request must not wait for an earlier response");
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("the injected upload failure must be returned"),
        };

        assert!(error.to_string().contains("injected failure"));
        assert!((2..=concurrency).contains(&admitted.load(Ordering::SeqCst)));
    }

    #[tokio::test]
    async fn resumable_pipeline_checkpoint_stays_contiguous() {
        let source = vec![3_u8; TEST_MULTIPART_PART_BYTES * 4];
        let mut input: ObjectReader = Box::pin(std::io::Cursor::new(source.clone()));
        let first = read_part_with_size(&mut input, TEST_MULTIPART_PART_BYTES)
            .await
            .unwrap();
        let uploader = |number, _bytes: Vec<u8>| {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(
                    u64::try_from(5 - number).unwrap(),
                ))
                .await;
                Ok(CompletedPart::builder()
                    .part_number(number)
                    .e_tag(format!("part-{number}"))
                    .build())
            })
        };
        let mut pipeline = PartUploadPipeline::new(
            &mut input,
            first,
            1,
            TEST_MULTIPART_PART_BYTES,
            4,
            uploader,
            Sha256::new(),
            0,
        );
        let mut checkpoint = UploadCheckpointV1::new(
            CheckpointBinding {
                bucket:             "lake-managed".to_owned(),
                prefix:             "objects".to_owned(),
                content_type:       "video/mp4".to_owned(),
                part_size_bytes:    TEST_MULTIPART_PART_BYTES,
                upload_concurrency: 4,
                source:             SourceIdentity {
                    size_bytes:          source.len() as u64,
                    modified_unix_nanos: 42,
                },
            },
            "objects/random".to_owned(),
            "upload-id".to_owned(),
        );

        while let Some(part) = pipeline.next().await.unwrap() {
            assert_eq!(part.number as usize, checkpoint.parts().len() + 1);
            checkpoint.push_part(CheckpointPart {
                number:         part.number,
                size_bytes:     part.size_bytes,
                e_tag:          part.completed.e_tag().unwrap().to_owned(),
                checksum_crc32: None,
                sha256:         part.sha256,
            });
        }

        assert_eq!(
            checkpoint
                .parts()
                .iter()
                .map(|part| part.number)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    }

    #[tokio::test]
    async fn resumable_pipeline_keeps_legacy_checkpoint_part_size_for_remaining_input() {
        let legacy_binding = CheckpointBinding {
            bucket:             "lake-managed".to_owned(),
            prefix:             "objects".to_owned(),
            content_type:       "video/mp4".to_owned(),
            part_size_bytes:    LEGACY_MULTIPART_PART_BYTES,
            upload_concurrency: 1,
            source:             SourceIdentity {
                size_bytes:          (LEGACY_MULTIPART_PART_BYTES * 3 + 1) as u64,
                modified_unix_nanos: 42,
            },
        };
        let checkpoint = UploadCheckpointV1::new(
            legacy_binding.clone(),
            "objects/random".to_owned(),
            "upload-id".to_owned(),
        );
        let mut default_binding = legacy_binding;
        default_binding.part_size_bytes = MULTIPART_PART_BYTES;
        checkpoint.validate(&default_binding).unwrap();

        // The completed prefix has already been rehashed. This is the source
        // remaining for the first resumed upload and its pipeline successor.
        let mut input: ObjectReader = Box::pin(std::io::Cursor::new(vec![
            7_u8;
            LEGACY_MULTIPART_PART_BYTES
                + 1
        ]));
        let first = read_resumable_part(&mut input, &checkpoint).await.unwrap();
        let uploaded_sizes = Arc::new(Mutex::new(Vec::new()));
        let uploader = {
            let uploaded_sizes = uploaded_sizes.clone();
            move |number, bytes: Vec<u8>| {
                let uploaded_sizes = uploaded_sizes.clone();
                Box::pin(async move {
                    uploaded_sizes.lock().unwrap().push((number, bytes.len()));
                    Ok(CompletedPart::builder()
                        .part_number(number)
                        .e_tag(format!("part-{number}"))
                        .build())
                })
            }
        };
        let mut pipeline = PartUploadPipeline::new_resumable(
            &mut input,
            first,
            2,
            &checkpoint,
            1,
            uploader,
            Sha256::new(),
            LEGACY_MULTIPART_PART_BYTES as u64,
        );

        while pipeline.next().await.unwrap().is_some() {}

        assert_eq!(
            *uploaded_sizes.lock().unwrap(),
            vec![(2, LEGACY_MULTIPART_PART_BYTES), (3, 1)]
        );
    }
}
