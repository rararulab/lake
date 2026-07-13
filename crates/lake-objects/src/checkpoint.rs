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

//! Durable local state for one resumable managed S3 multipart upload.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{ObjectError, Result};

const CHECKPOINT_VERSION: u8 = 1;

pub(crate) struct CheckpointLock {
    _file: std::fs::File,
}

impl CheckpointLock {
    pub(crate) async fn acquire(checkpoint: &Path) -> Result<Self> {
        let lock_path = lock_path(checkpoint);
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)
                .map_err(|source| ObjectError::CheckpointIo {
                    action: "opening lock for",
                    path: lock_path.clone(),
                    source,
                })?;
            match file.try_lock() {
                Ok(()) => Ok(Self { _file: file }),
                Err(std::fs::TryLockError::WouldBlock) => {
                    Err(ObjectError::CheckpointInUse { path: lock_path })
                }
                Err(std::fs::TryLockError::Error(source)) => Err(ObjectError::CheckpointIo {
                    action: "locking",
                    path: lock_path,
                    source,
                }),
            }
        })
        .await
        .map_err(|source| ObjectError::CheckpointIo {
            action: "joining lock task for",
            path:   checkpoint.to_path_buf(),
            source: std::io::Error::other(source),
        })?
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SourceIdentity {
    pub(crate) size_bytes:          u64,
    pub(crate) modified_unix_nanos: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointBinding {
    pub(crate) bucket:             String,
    pub(crate) prefix:             String,
    pub(crate) content_type:       String,
    pub(crate) part_size_bytes:    usize,
    pub(crate) upload_concurrency: usize,
    pub(crate) source:             SourceIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct CheckpointPart {
    pub(crate) number:         i32,
    pub(crate) size_bytes:     usize,
    pub(crate) e_tag:          String,
    pub(crate) checksum_crc32: Option<String>,
    pub(crate) sha256:         String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct UploadCheckpointV1 {
    version:            u8,
    bucket:             String,
    prefix:             String,
    content_type:       String,
    part_size_bytes:    usize,
    #[serde(default = "legacy_upload_concurrency")]
    upload_concurrency: usize,
    source:             SourceIdentity,
    object_key:         String,
    upload_id:          String,
    parts:              Vec<CheckpointPart>,
}

const fn legacy_upload_concurrency() -> usize { 1 }

impl UploadCheckpointV1 {
    pub(crate) fn new(binding: CheckpointBinding, object_key: String, upload_id: String) -> Self {
        Self {
            version: CHECKPOINT_VERSION,
            bucket: binding.bucket,
            prefix: binding.prefix,
            content_type: binding.content_type,
            part_size_bytes: binding.part_size_bytes,
            upload_concurrency: binding.upload_concurrency,
            source: binding.source,
            object_key,
            upload_id,
            parts: Vec::new(),
        }
    }

    pub(crate) fn push_part(&mut self, part: CheckpointPart) { self.parts.push(part); }

    pub(crate) fn object_key(&self) -> &str { &self.object_key }

    pub(crate) fn upload_id(&self) -> &str { &self.upload_id }

    pub(crate) fn parts(&self) -> &[CheckpointPart] { &self.parts }

    pub(crate) const fn part_size_bytes(&self) -> usize { self.part_size_bytes }

    pub(crate) const fn upload_concurrency(&self) -> usize { self.upload_concurrency }

    /// Persist a wider possible crash suffix before starting more concurrent
    /// requests. Never shrink the bound recorded by an earlier attempt.
    pub(crate) fn raise_upload_concurrency(&mut self, value: usize) -> bool {
        if value <= self.upload_concurrency {
            return false;
        }
        self.upload_concurrency = value;
        true
    }

    pub(crate) fn stage_matches(&self, bucket: &str, prefix: &str) -> bool {
        self.bucket == bucket && self.prefix == prefix
    }

    pub(crate) fn validate(&self, binding: &CheckpointBinding) -> Result<()> {
        if self.version != CHECKPOINT_VERSION {
            return Err(ObjectError::CheckpointMismatch { field: "version" });
        }
        if self.bucket != binding.bucket || self.prefix != binding.prefix {
            return Err(ObjectError::CheckpointMismatch { field: "stage" });
        }
        if self.content_type != binding.content_type {
            return Err(ObjectError::CheckpointMismatch {
                field: "content type",
            });
        }
        if !crate::s3::is_supported_resumable_part_size(self.part_size_bytes)
            || !crate::s3::is_supported_resumable_part_size(binding.part_size_bytes)
        {
            return Err(ObjectError::CheckpointMismatch { field: "part size" });
        }
        if !(1..=crate::s3::MAX_UPLOAD_CONCURRENCY).contains(&self.upload_concurrency) {
            return Err(ObjectError::CheckpointMismatch {
                field: "upload concurrency",
            });
        }
        if self.source != binding.source {
            return Err(ObjectError::CheckpointMismatch { field: "source" });
        }
        Ok(())
    }

    pub(crate) async fn load(path: &Path) -> Result<Self> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|source| ObjectError::CheckpointIo {
                action: "reading",
                path: path.to_path_buf(),
                source,
            })?;
        serde_json::from_slice(&bytes).map_err(|source| ObjectError::InvalidCheckpoint {
            path: path.to_path_buf(),
            source,
        })
    }

    pub(crate) async fn save_atomic(&self, path: &Path) -> Result<()> {
        let parent = path.parent().ok_or_else(|| ObjectError::CheckpointIo {
            action: "resolving parent of",
            path:   path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "checkpoint has no parent directory",
            ),
        })?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| ObjectError::CheckpointIo {
                action: "creating directory for",
                path: path.to_path_buf(),
                source,
            })?;
        let temporary = temporary_path(path);
        let bytes =
            serde_json::to_vec_pretty(self).map_err(|source| ObjectError::InvalidCheckpoint {
                path: path.to_path_buf(),
                source,
            })?;
        let result = async {
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                options.mode(0o600);
            }
            let mut file =
                options
                    .open(&temporary)
                    .await
                    .map_err(|source| ObjectError::CheckpointIo {
                        action: "creating",
                        path: temporary.clone(),
                        source,
                    })?;
            file.write_all(&bytes)
                .await
                .map_err(|source| ObjectError::CheckpointIo {
                    action: "writing",
                    path: temporary.clone(),
                    source,
                })?;
            file.sync_all()
                .await
                .map_err(|source| ObjectError::CheckpointIo {
                    action: "syncing",
                    path: temporary.clone(),
                    source,
                })?;
            tokio::fs::rename(&temporary, path)
                .await
                .map_err(|source| ObjectError::CheckpointIo {
                    action: "publishing",
                    path: path.to_path_buf(),
                    source,
                })?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map_or_else(|| "checkpoint".into(), std::ffi::OsString::from);
    name.push(format!(".{}.tmp", uuid::Uuid::now_v7()));
    path.with_file_name(name)
}

fn lock_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map_or_else(|| "checkpoint".into(), std::ffi::OsString::from);
    name.push(".lock");
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn resumable_checkpoint_validates_source_and_stage() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("episode.upload.json");
        let binding = CheckpointBinding {
            bucket:             "lake-managed".to_owned(),
            prefix:             "objects".to_owned(),
            content_type:       "video/mp4".to_owned(),
            part_size_bytes:    5 * 1024 * 1024,
            upload_concurrency: 4,
            source:             SourceIdentity {
                size_bytes:          8 * 1024 * 1024,
                modified_unix_nanos: 42,
            },
        };
        let mut checkpoint = UploadCheckpointV1::new(
            binding.clone(),
            "objects/random-key".to_owned(),
            "s3-upload-id".to_owned(),
        );
        checkpoint.push_part(CheckpointPart {
            number:         1,
            size_bytes:     5 * 1024 * 1024,
            e_tag:          "etag-1".to_owned(),
            checksum_crc32: Some("crc32-1".to_owned()),
            sha256:         "part-sha256".to_owned(),
        });

        checkpoint.save_atomic(&path).await.unwrap();
        let first_lock = CheckpointLock::acquire(&path).await.unwrap();
        assert!(matches!(
            CheckpointLock::acquire(&path).await,
            Err(ObjectError::CheckpointInUse { .. })
        ));
        drop(first_lock);
        let _recovered_after_drop = CheckpointLock::acquire(&path).await.unwrap();
        let loaded = UploadCheckpointV1::load(&path).await.unwrap();
        loaded.validate(&binding).unwrap();
        assert_eq!(loaded, checkpoint);
        assert_eq!(loaded.upload_concurrency(), 4);

        let json = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(!json.contains("credential"));
        assert!(!json.contains("secret"));
        assert!(!json.contains("bearer"));

        let mut changed_source = binding.clone();
        changed_source.source.size_bytes += 1;
        assert!(matches!(
            loaded.validate(&changed_source),
            Err(ObjectError::CheckpointMismatch { field: "source" })
        ));

        let mut changed_stage = binding;
        changed_stage.bucket = "somebody-else".to_owned();
        assert!(matches!(
            loaded.validate(&changed_stage),
            Err(ObjectError::CheckpointMismatch { field: "stage" })
        ));
    }

    #[test]
    fn resumable_checkpoint_accepts_legacy_part_size_when_default_grows() {
        let legacy_binding = CheckpointBinding {
            bucket:             "lake-managed".to_owned(),
            prefix:             "objects".to_owned(),
            content_type:       "video/mp4".to_owned(),
            part_size_bytes:    5 * 1024 * 1024,
            upload_concurrency: 1,
            source:             SourceIdentity {
                size_bytes:          8 * 1024 * 1024,
                modified_unix_nanos: 42,
            },
        };
        let checkpoint = UploadCheckpointV1::new(
            legacy_binding.clone(),
            "objects/random-key".to_owned(),
            "s3-upload-id".to_owned(),
        );
        let mut default_binding = legacy_binding;
        default_binding.part_size_bytes = 64 * 1024 * 1024;

        checkpoint.validate(&default_binding).unwrap();
    }

    #[test]
    fn resumable_checkpoint_rejects_unrecognized_part_size() {
        let binding = CheckpointBinding {
            bucket:             "lake-managed".to_owned(),
            prefix:             "objects".to_owned(),
            content_type:       "video/mp4".to_owned(),
            part_size_bytes:    1,
            upload_concurrency: 1,
            source:             SourceIdentity {
                size_bytes:          8 * 1024 * 1024,
                modified_unix_nanos: 42,
            },
        };
        let checkpoint = UploadCheckpointV1::new(
            binding.clone(),
            "objects/random-key".to_owned(),
            "s3-upload-id".to_owned(),
        );

        assert!(matches!(
            checkpoint.validate(&binding),
            Err(ObjectError::CheckpointMismatch { field: "part size" })
        ));
    }

    #[test]
    fn legacy_checkpoint_defaults_to_serial_creator_window() {
        let legacy = r#"{
          "version": 1,
          "bucket": "lake-managed",
          "prefix": "objects",
          "content_type": "video/mp4",
          "part_size_bytes": 5242880,
          "source": {"size_bytes": 8, "modified_unix_nanos": 42},
          "object_key": "objects/random-key",
          "upload_id": "upload-id",
          "parts": []
        }"#;

        let checkpoint: UploadCheckpointV1 = serde_json::from_str(legacy).unwrap();

        assert_eq!(checkpoint.upload_concurrency(), 1);
    }

    #[test]
    fn checkpoint_creator_window_only_grows() {
        let binding = CheckpointBinding {
            bucket:             "lake-managed".to_owned(),
            prefix:             "objects".to_owned(),
            content_type:       "video/mp4".to_owned(),
            part_size_bytes:    5 * 1024 * 1024,
            upload_concurrency: 1,
            source:             SourceIdentity {
                size_bytes:          10 * 1024 * 1024,
                modified_unix_nanos: 42,
            },
        };
        let mut checkpoint = UploadCheckpointV1::new(
            binding,
            "objects/random-key".to_owned(),
            "upload-id".to_owned(),
        );

        assert!(checkpoint.raise_upload_concurrency(4));
        assert_eq!(checkpoint.upload_concurrency(), 4);
        assert!(!checkpoint.raise_upload_concurrency(2));
        assert_eq!(checkpoint.upload_concurrency(), 4);
    }
}
