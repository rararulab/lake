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

use async_trait::async_trait;
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart},
};
use lake_common::DataLocation;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use url::Url;

use crate::{ManagedObjectStore, ObjectError, ObjectReader, Result};

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
}

#[async_trait]
impl ManagedObjectStore for S3ObjectStore {
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
}

async fn read_part(input: &mut ObjectReader) -> Result<Vec<u8>> {
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
