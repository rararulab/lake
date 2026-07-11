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

//! Checkpointed application of immutable GC plans.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{GcPlan, ObjectCandidate, ObjectError, Result, checkpoint::CheckpointLock};

const APPLY_CHECKPOINT_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteOutcome {
    Deleted,
    AlreadyAbsent,
    /// Backend deletion is idempotent but does not distinguish prior absence.
    DeletedOrAbsent,
}

#[async_trait]
pub trait ManagedObjectDeleter: Send + Sync {
    fn managed_uri_prefix(&self) -> String;

    async fn delete_candidate(&self, candidate: &ObjectCandidate) -> Result<DeleteOutcome>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcApplyProgress {
    completed_pages:   usize,
    processed_objects: u64,
    deleted_objects:   u64,
    absent_objects:    u64,
    complete:          bool,
}

impl GcApplyProgress {
    #[must_use]
    pub fn completed_pages(&self) -> usize { self.completed_pages }

    #[must_use]
    pub fn processed_objects(&self) -> u64 { self.processed_objects }

    #[must_use]
    pub fn deleted_objects(&self) -> u64 { self.deleted_objects }

    #[must_use]
    pub fn absent_objects(&self) -> u64 { self.absent_objects }

    #[must_use]
    pub fn is_complete(&self) -> bool { self.complete }
}

pub struct GcPlanApplier {
    plan:            GcPlan,
    checkpoint_path: PathBuf,
    checkpoint:      GcApplyCheckpoint,
    _lock:           CheckpointLock,
}

impl GcPlanApplier {
    pub async fn open(
        plan_path: impl Into<PathBuf>,
        checkpoint_path: impl Into<PathBuf>,
    ) -> Result<Self> {
        let plan = GcPlan::open(plan_path)?;
        let checkpoint_path = checkpoint_path.into();
        let lock = CheckpointLock::acquire(&checkpoint_path).await?;
        let checkpoint = if checkpoint_path.exists() {
            let checkpoint = GcApplyCheckpoint::load(&checkpoint_path).await?;
            checkpoint.validate(&plan)?;
            checkpoint
        } else {
            let checkpoint = GcApplyCheckpoint::new(&plan);
            checkpoint.save_atomic(&checkpoint_path).await?;
            checkpoint
        };
        Ok(Self {
            plan,
            checkpoint_path,
            checkpoint,
            _lock: lock,
        })
    }

    pub async fn apply_next<D>(&mut self, deleter: &D) -> Result<GcApplyProgress>
    where
        D: ManagedObjectDeleter + ?Sized,
    {
        if deleter.managed_uri_prefix() != self.plan.managed_prefix() {
            return Err(ObjectError::GcApplyCheckpointMismatch { field: "stage" });
        }
        if self.checkpoint.complete {
            return Ok(self.progress());
        }
        let expected_hash = self.checkpoint.next_page_hash.as_deref().ok_or(
            ObjectError::GcApplyCheckpointMismatch {
                field: "next page hash",
            },
        )?;
        let page = self
            .plan
            .page(self.checkpoint.next_page_index, expected_hash)?;
        for candidate in &page.candidates {
            match deleter.delete_candidate(candidate).await? {
                DeleteOutcome::Deleted => {
                    self.checkpoint.deleted_objects =
                        checked_increment(self.checkpoint.deleted_objects, "deleted object count")?;
                }
                DeleteOutcome::AlreadyAbsent => {
                    self.checkpoint.absent_objects =
                        checked_increment(self.checkpoint.absent_objects, "absent object count")?;
                }
                DeleteOutcome::DeletedOrAbsent => {}
            }
            self.checkpoint.processed_objects =
                checked_increment(self.checkpoint.processed_objects, "processed object count")?;
        }
        self.checkpoint.next_page_index = self.checkpoint.next_page_index.checked_add(1).ok_or(
            ObjectError::GcApplyCheckpointMismatch {
                field: "completed page count",
            },
        )?;
        self.checkpoint.next_page_hash = page.next_page_hash;
        self.checkpoint.complete = self.checkpoint.next_page_index == self.plan.page_count();
        if self.checkpoint.complete && self.checkpoint.next_page_hash.is_some() {
            return Err(ObjectError::GcApplyCheckpointMismatch {
                field: "terminal page hash",
            });
        }
        self.checkpoint.save_atomic(&self.checkpoint_path).await?;
        Ok(self.progress())
    }

    fn progress(&self) -> GcApplyProgress {
        GcApplyProgress {
            completed_pages:   self.checkpoint.next_page_index,
            processed_objects: self.checkpoint.processed_objects,
            deleted_objects:   self.checkpoint.deleted_objects,
            absent_objects:    self.checkpoint.absent_objects,
            complete:          self.checkpoint.complete,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GcApplyCheckpoint {
    version:           u8,
    plan_digest:       String,
    page_count:        usize,
    next_page_index:   usize,
    next_page_hash:    Option<String>,
    processed_objects: u64,
    deleted_objects:   u64,
    absent_objects:    u64,
    complete:          bool,
}

impl GcApplyCheckpoint {
    fn new(plan: &GcPlan) -> Self {
        Self {
            version:           APPLY_CHECKPOINT_VERSION,
            plan_digest:       plan.digest().to_owned(),
            page_count:        plan.page_count(),
            next_page_index:   0,
            next_page_hash:    plan.first_page_hash().map(ToOwned::to_owned),
            processed_objects: 0,
            deleted_objects:   0,
            absent_objects:    0,
            complete:          plan.page_count() == 0,
        }
    }

    fn validate(&self, plan: &GcPlan) -> Result<()> {
        if self.version != APPLY_CHECKPOINT_VERSION {
            return Err(ObjectError::GcApplyCheckpointMismatch { field: "version" });
        }
        if self.plan_digest != plan.digest() || self.page_count != plan.page_count() {
            return Err(ObjectError::GcApplyCheckpointMismatch { field: "plan" });
        }
        if self.next_page_index > self.page_count
            || self.complete != (self.next_page_index == self.page_count)
            || (self.complete && self.next_page_hash.is_some())
            || (!self.complete && self.next_page_hash.is_none())
        {
            return Err(ObjectError::GcApplyCheckpointMismatch { field: "progress" });
        }
        if self.next_page_hash != plan.expected_page_hash(self.next_page_index)? {
            return Err(ObjectError::GcApplyCheckpointMismatch {
                field: "page chain",
            });
        }
        Ok(())
    }

    async fn load(path: &Path) -> Result<Self> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|source| checkpoint_io("reading", path, source))?;
        serde_json::from_slice(&bytes).map_err(|source| ObjectError::GcApplyCheckpointCorrupt {
            path: path.to_path_buf(),
            source,
        })
    }

    async fn save_atomic(&self, path: &Path) -> Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| ObjectError::GcApplyCheckpointIo {
                action: "resolving parent of",
                path:   path.to_path_buf(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "checkpoint has no parent directory",
                ),
            })?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| checkpoint_io("creating directory for", path, source))?;
        let temporary = temporary_path(path);
        let bytes = serde_json::to_vec_pretty(self).map_err(|source| {
            ObjectError::GcApplyCheckpointCorrupt {
                path: path.to_path_buf(),
                source,
            }
        })?;
        let result = async {
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut file = options
                .open(&temporary)
                .await
                .map_err(|source| checkpoint_io("creating", &temporary, source))?;
            file.write_all(&bytes)
                .await
                .map_err(|source| checkpoint_io("writing", &temporary, source))?;
            file.sync_all()
                .await
                .map_err(|source| checkpoint_io("syncing", &temporary, source))?;
            tokio::fs::rename(&temporary, path)
                .await
                .map_err(|source| checkpoint_io("publishing", path, source))?;
            let directory = tokio::fs::File::open(parent)
                .await
                .map_err(|source| checkpoint_io("opening directory for", path, source))?;
            directory
                .sync_all()
                .await
                .map_err(|source| checkpoint_io("syncing directory for", path, source))
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
        .map_or_else(|| "gc-apply".into(), std::ffi::OsString::from);
    name.push(format!(".{}.tmp", uuid::Uuid::now_v7()));
    path.with_file_name(name)
}

fn checkpoint_io(action: &'static str, path: &Path, source: std::io::Error) -> ObjectError {
    ObjectError::GcApplyCheckpointIo {
        action,
        path: path.to_path_buf(),
        source,
    }
}

fn checked_increment(value: u64, field: &'static str) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or(ObjectError::GcApplyCheckpointMismatch { field })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::{GcPlanWriter, GcPlanner, ObjectCandidate};

    #[derive(Default)]
    struct RecordingDeleter {
        seen: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ManagedObjectDeleter for RecordingDeleter {
        fn managed_uri_prefix(&self) -> String { "s3://lake/objects/".to_owned() }

        async fn delete_candidate(
            &self,
            candidate: &ObjectCandidate,
        ) -> crate::Result<DeleteOutcome> {
            self.seen.lock().unwrap().push(candidate.uri.clone());
            Ok(DeleteOutcome::DeletedOrAbsent)
        }
    }

    fn candidate(uri: &str) -> ObjectCandidate {
        ObjectCandidate {
            uri:              uri.to_owned(),
            size_bytes:       42,
            last_modified_ms: 10,
        }
    }

    #[tokio::test]
    async fn gc_apply_checkpoints_completed_pages() {
        let temp = tempfile::tempdir().unwrap();
        let plan_path = temp.path().join("plan");
        let checkpoint = temp.path().join("apply.json");
        let pages = GcPlanner::try_new("s3://lake/objects/", 100, 1, true)
            .unwrap()
            .plan(
                vec![
                    candidate("s3://lake/objects/a"),
                    candidate("s3://lake/objects/b"),
                ],
                Vec::new(),
            );
        GcPlanWriter::try_new(&plan_path, "s3://lake/objects/", 100, 1)
            .unwrap()
            .write(pages)
            .unwrap();
        let deleter = RecordingDeleter::default();

        let mut first = GcPlanApplier::open(&plan_path, &checkpoint).await.unwrap();
        let progress = first.apply_next(&deleter).await.unwrap();
        assert_eq!(progress.completed_pages(), 1);
        assert!(!progress.is_complete());
        drop(first);

        let mut resumed = GcPlanApplier::open(&plan_path, &checkpoint).await.unwrap();
        let progress = resumed.apply_next(&deleter).await.unwrap();
        assert!(progress.is_complete());
        assert_eq!(progress.completed_pages(), 2);
        assert_eq!(
            deleter.seen.lock().unwrap().as_slice(),
            &["s3://lake/objects/a", "s3://lake/objects/b"]
        );
        drop(resumed);

        let mut complete = GcPlanApplier::open(&plan_path, &checkpoint).await.unwrap();
        assert!(complete.apply_next(&deleter).await.unwrap().is_complete());
        assert_eq!(deleter.seen.lock().unwrap().len(), 2);
    }
}
