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
    ops::Range,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use async_trait::async_trait;
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart},
};
use lake_common::DataLocation;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};
use url::Url;

use crate::{
    DeleteOutcome, InventoryPage, InventoryRequest, ManagedObjectDeleter, ManagedObjectInventory,
    ManagedObjectStore, ObjectCandidate, ObjectError, ObjectReader, Result,
    checkpoint::{
        CheckpointBinding, CheckpointLock, CheckpointPart, SourceIdentity, UploadCheckpointV1,
    },
    validate_range,
};

const MULTIPART_PART_BYTES: usize = 5 * 1024 * 1024;

/// Lake-owned S3 bucket prefix used for managed `FILE` values.
#[derive(Clone, Debug)]
pub struct S3ObjectStore {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3ObjectStore {
    /// Bind a client to one non-empty Lake-owned bucket prefix.
    pub fn new(
        client: Client,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self> {
        let bucket = bucket.into();
        let prefix = prefix.into().trim_matches('/').to_owned();
        if bucket.is_empty() || prefix.is_empty() {
            return Err(ObjectError::InvalidS3Stage);
        }
        Ok(Self {
            client,
            bucket,
            prefix,
        })
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
        validate_range(location, &range)?;
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
        Ok(Box::pin(output.body.into_async_read()))
    }

    fn managed_key(&self, uri: &str) -> Result<String> {
        let parsed = Url::parse(uri).map_err(|_| ObjectError::InvalidS3Uri {
            uri: uri.to_owned(),
        })?;
        if parsed.scheme() != "s3"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(ObjectError::InvalidS3Uri {
                uri: uri.to_owned(),
            });
        }
        let bucket = parsed.host_str().ok_or_else(|| ObjectError::InvalidS3Uri {
            uri: uri.to_owned(),
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
        hasher: &mut Sha256,
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

        let result = self
            .upload_parts(key, upload_id, first_part, input, hasher)
            .await;
        let (parts, size_bytes) = match result {
            Ok(value) => value,
            Err(error) => {
                self.abort(key, upload_id).await;
                return Err(error);
            }
        };
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
            self.abort(key, upload_id).await;
            return Err(ObjectError::S3 {
                action:  "complete_multipart_upload",
                message: error.to_string(),
            });
        }
        Ok((size_bytes, format!("{:x}", hasher.finalize_reset())))
    }

    async fn upload_parts(
        &self,
        key: &str,
        upload_id: &str,
        mut part: Vec<u8>,
        input: &mut ObjectReader,
        hasher: &mut Sha256,
    ) -> Result<(Vec<CompletedPart>, u64)> {
        let mut completed = Vec::new();
        let mut size_bytes = 0_u64;
        let mut part_number = 1_i32;
        loop {
            hasher.update(&part);
            size_bytes = size_bytes.saturating_add(part.len() as u64);
            let uploaded = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(part))
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
            let part_builder = CompletedPart::builder()
                .part_number(part_number)
                .e_tag(e_tag)
                .set_checksum_crc32(uploaded.checksum_crc32().map(ToOwned::to_owned));
            completed.push(part_builder.build());
            part = read_part(input).await?;
            if part.is_empty() {
                return Ok((completed, size_bytes));
            }
            part_number = part_number.checked_add(1).ok_or_else(|| ObjectError::S3 {
                action:  "upload_part",
                message: "multipart upload exceeded the S3 part limit".to_owned(),
            })?;
        }
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
            bucket:          self.bucket.clone(),
            prefix:          self.prefix.clone(),
            content_type:    content_type.clone(),
            part_size_bytes: MULTIPART_PART_BYTES,
            source:          SourceIdentity {
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
            let part = read_part(&mut input).await?;
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

        loop {
            let part = read_part(&mut input).await?;
            if part.is_empty() {
                break;
            }
            let number =
                i32::try_from(checkpoint.parts().len() + 1).map_err(|_| ObjectError::S3 {
                    action:  "upload_part",
                    message: "multipart upload exceeded the S3 part limit".to_owned(),
                })?;
            let part_sha256 = format!("{:x}", Sha256::digest(&part));
            hasher.update(&part);
            let size_bytes = part.len();
            let uploaded = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(checkpoint.object_key())
                .upload_id(checkpoint.upload_id())
                .part_number(number)
                .body(ByteStream::from(part))
                .send()
                .await
                .map_err(|error| ObjectError::S3 {
                    action:  "upload_part",
                    message: format!("{error:?}"),
                })?;
            checkpoint.push_part(CheckpointPart {
                number,
                size_bytes,
                e_tag: uploaded
                    .e_tag()
                    .ok_or_else(|| ObjectError::S3 {
                        action:  "upload_part",
                        message: "S3 response omitted ETag".to_owned(),
                    })?
                    .to_owned(),
                checksum_crc32: uploaded.checksum_crc32().map(ToOwned::to_owned),
                sha256: part_sha256,
            });
            checkpoint.save_atomic(checkpoint_path).await?;
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
            .sha256(format!("{:x}", hasher.finalize()))
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

        if !(remote_parts.len() == checkpoint.parts().len()
            || remote_parts.len() == checkpoint.parts().len() + 1)
            || !remote_parts
                .iter()
                .take(checkpoint.parts().len())
                .zip(checkpoint.parts())
                .all(|(remote, local)| {
                    remote.part_number() == Some(local.number)
                        && remote.e_tag() == Some(local.e_tag.as_str())
                        && remote.size().and_then(|size| usize::try_from(size).ok())
                            == Some(local.size_bytes)
                        && remote.checksum_crc32() == local.checksum_crc32.as_deref()
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
impl ManagedObjectStore for S3ObjectStore {
    fn stage_identity(&self) -> String { format!("s3://{}/{}", self.bucket, self.prefix) }

    async fn put_reader(
        &self,
        mut input: ObjectReader,
        content_type: String,
    ) -> Result<DataLocation> {
        let key = format!("{}/{}", self.prefix, uuid::Uuid::now_v7());
        let first_part = read_part(&mut input).await?;
        let mut hasher = Sha256::new();
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
            self.upload_nonempty(&key, first_part, &mut input, &content_type, &mut hasher)
                .await?
        };
        Ok(DataLocation::builder()
            .uri(format!("s3://{}/{key}", self.bucket))
            .content_type(content_type)
            .size_bytes(size_bytes)
            .sha256(sha256)
            .build())
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
    let mut part = Vec::with_capacity(MULTIPART_PART_BYTES);
    let mut buffer = vec![0_u8; 64 * 1024];
    while part.len() < MULTIPART_PART_BYTES {
        let remaining = MULTIPART_PART_BYTES - part.len();
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
