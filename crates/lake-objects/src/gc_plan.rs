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

//! Immutable, content-addressed GC plans.

use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{GcPlanPage, ObjectCandidate, ObjectError, Result};

const PLAN_VERSION: u8 = 1;
const MANIFEST_FILE: &str = "manifest.json";

#[derive(Clone, Debug)]
pub struct GcPlanWriter {
    plan_path:          PathBuf,
    managed_prefix:     String,
    cutoff_ms:          u64,
    page_size:          usize,
    source_fingerprint: Option<String>,
}

impl GcPlanWriter {
    pub fn try_new(
        plan_path: impl Into<PathBuf>,
        managed_prefix: impl Into<String>,
        cutoff_ms: u64,
        page_size: usize,
    ) -> Result<Self> {
        let managed_prefix = managed_prefix.into();
        if !managed_prefix.ends_with('/') {
            return Err(ObjectError::InvalidGcConfig {
                message: "plan managed URI prefix must end with '/'".to_owned(),
            });
        }
        if page_size == 0 || page_size > 1_024 {
            return Err(ObjectError::InvalidGcConfig {
                message: "plan page size must be within 1..=1024".to_owned(),
            });
        }
        Ok(Self {
            plan_path: plan_path.into(),
            managed_prefix,
            cutoff_ms,
            page_size,
            source_fingerprint: None,
        })
    }

    #[must_use]
    pub fn with_source_fingerprint(mut self, fingerprint: impl Into<String>) -> Self {
        self.source_fingerprint = Some(fingerprint.into());
        self
    }

    pub fn write<I>(self, pages: I) -> Result<GcPlan>
    where
        I: IntoIterator<Item = Result<GcPlanPage>>,
    {
        if self.plan_path.exists() {
            return Err(ObjectError::GcPlanIo {
                action: "creating",
                path:   self.plan_path,
                source: std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "plan path already exists",
                ),
            });
        }
        let parent = self
            .plan_path
            .parent()
            .ok_or_else(|| ObjectError::GcPlanIo {
                action: "resolving parent of",
                path:   self.plan_path.clone(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "plan path has no parent",
                ),
            })?;
        create_dir_all(parent)?;
        let temporary = parent.join(format!(".gc-plan-{}.tmp", Uuid::now_v7()));
        create_dir(&temporary)?;
        match self.write_in(&temporary, pages) {
            Ok(manifest) => {
                sync_dir(&temporary)?;
                rename(&temporary, &self.plan_path)?;
                sync_dir(parent)?;
                Ok(GcPlan {
                    path: self.plan_path,
                    manifest,
                })
            }
            Err(error) => {
                let _ = fs::remove_dir_all(&temporary);
                Err(error)
            }
        }
    }

    fn write_in<I>(&self, temporary: &Path, pages: I) -> Result<GcPlanManifest>
    where
        I: IntoIterator<Item = Result<GcPlanPage>>,
    {
        let mut page_count = 0_usize;
        let mut candidate_count = 0_u64;
        let mut total_size_bytes = 0_u64;
        let mut previous_uri: Option<String> = None;
        for page in pages {
            let page = page?;
            if page.candidates().is_empty() || page.candidates().len() > self.page_size {
                return Err(ObjectError::GcPlanMismatch { field: "page size" });
            }
            for candidate in page.candidates() {
                validate_candidate(
                    candidate,
                    &self.managed_prefix,
                    self.cutoff_ms,
                    previous_uri.as_deref(),
                )?;
                previous_uri = Some(candidate.uri.clone());
                candidate_count =
                    candidate_count
                        .checked_add(1)
                        .ok_or(ObjectError::GcPlanMismatch {
                            field: "candidate count overflow",
                        })?;
                total_size_bytes = total_size_bytes.checked_add(candidate.size_bytes).ok_or(
                    ObjectError::GcPlanMismatch {
                        field: "candidate size overflow",
                    },
                )?;
            }
            let draft = draft_path(temporary, page_count);
            write_json(&draft, page.candidates())?;
            page_count = page_count
                .checked_add(1)
                .ok_or(ObjectError::GcPlanMismatch {
                    field: "page count overflow",
                })?;
        }

        let mut next_page_hash = None;
        for index in (0..page_count).rev() {
            let draft = draft_path(temporary, index);
            let candidates: Vec<ObjectCandidate> = read_json(&draft)?;
            let envelope = GcPlanPageEnvelope {
                version: PLAN_VERSION,
                index,
                candidates,
                next_page_hash: next_page_hash.clone(),
            };
            let bytes = encode_json(&draft, &envelope)?;
            let page_hash = sha256_hex(&bytes);
            write_bytes(&page_path(temporary, index, &page_hash), &bytes)?;
            remove_file(&draft)?;
            next_page_hash = Some(page_hash);
        }

        let binding = GcPlanBinding {
            version: PLAN_VERSION,
            managed_prefix: self.managed_prefix.clone(),
            cutoff_ms: self.cutoff_ms,
            page_size: self.page_size,
            page_count,
            candidate_count,
            total_size_bytes,
            first_page_hash: next_page_hash,
            source_fingerprint: self.source_fingerprint.clone(),
        };
        let digest = sha256_hex(&encode_json(&temporary.join(MANIFEST_FILE), &binding)?);
        let manifest = GcPlanManifest { binding, digest };
        write_json(&temporary.join(MANIFEST_FILE), &manifest)?;
        Ok(manifest)
    }
}

#[derive(Clone, Debug)]
pub struct GcPlan {
    path:     PathBuf,
    manifest: GcPlanManifest,
}

impl GcPlan {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let manifest: GcPlanManifest = read_json(&path.join(MANIFEST_FILE))?;
        let expected = sha256_hex(&encode_json(&path.join(MANIFEST_FILE), &manifest.binding)?);
        if manifest.digest != expected {
            return Err(ObjectError::GcPlanMismatch {
                field: "manifest digest",
            });
        }
        let plan = Self { path, manifest };
        plan.verify_pages()?;
        Ok(plan)
    }

    #[must_use]
    pub fn digest(&self) -> &str { &self.manifest.digest }

    #[must_use]
    pub fn page_count(&self) -> usize { self.manifest.binding.page_count }

    #[must_use]
    pub fn candidate_count(&self) -> u64 { self.manifest.binding.candidate_count }

    #[must_use]
    pub fn total_size_bytes(&self) -> u64 { self.manifest.binding.total_size_bytes }

    #[must_use]
    pub fn managed_prefix(&self) -> &str { &self.manifest.binding.managed_prefix }

    #[must_use]
    pub fn source_fingerprint(&self) -> Option<&str> {
        self.manifest.binding.source_fingerprint.as_deref()
    }

    pub(crate) fn page(&self, index: usize, expected_hash: &str) -> Result<GcPlanPageEnvelope> {
        validate_hash(expected_hash)?;
        let path = page_path(&self.path, index, expected_hash);
        let bytes = read_bytes(&path)?;
        if sha256_hex(&bytes) != expected_hash {
            return Err(ObjectError::GcPlanMismatch {
                field: "page digest",
            });
        }
        let page: GcPlanPageEnvelope = decode_json(&path, &bytes)?;
        if page.version != PLAN_VERSION || page.index != index {
            return Err(ObjectError::GcPlanMismatch {
                field: "page identity",
            });
        }
        Ok(page)
    }

    pub(crate) fn first_page_hash(&self) -> Option<&str> {
        self.manifest.binding.first_page_hash.as_deref()
    }

    pub(crate) fn expected_page_hash(&self, index: usize) -> Result<Option<String>> {
        if index > self.page_count() {
            return Err(ObjectError::GcPlanMismatch {
                field: "page index",
            });
        }
        let mut expected = self.manifest.binding.first_page_hash.clone();
        for prior_index in 0..index {
            let hash = expected.as_deref().ok_or(ObjectError::GcPlanMismatch {
                field: "page chain length",
            })?;
            expected = self.page(prior_index, hash)?.next_page_hash;
        }
        Ok(expected)
    }

    fn verify_pages(&self) -> Result<()> {
        if self.manifest.binding.version != PLAN_VERSION {
            return Err(ObjectError::GcPlanMismatch { field: "version" });
        }
        let mut expected_hash = self.manifest.binding.first_page_hash.clone();
        let mut previous_uri: Option<String> = None;
        let mut candidate_count = 0_u64;
        let mut total_size_bytes = 0_u64;
        for index in 0..self.page_count() {
            let hash = expected_hash
                .as_deref()
                .ok_or(ObjectError::GcPlanMismatch {
                    field: "page chain length",
                })?;
            let page = self.page(index, hash)?;
            if page.candidates.is_empty() || page.candidates.len() > self.manifest.binding.page_size
            {
                return Err(ObjectError::GcPlanMismatch { field: "page size" });
            }
            for candidate in &page.candidates {
                validate_candidate(
                    candidate,
                    self.managed_prefix(),
                    self.manifest.binding.cutoff_ms,
                    previous_uri.as_deref(),
                )?;
                previous_uri = Some(candidate.uri.clone());
                candidate_count =
                    candidate_count
                        .checked_add(1)
                        .ok_or(ObjectError::GcPlanMismatch {
                            field: "candidate count overflow",
                        })?;
                total_size_bytes = total_size_bytes.checked_add(candidate.size_bytes).ok_or(
                    ObjectError::GcPlanMismatch {
                        field: "candidate size overflow",
                    },
                )?;
            }
            expected_hash = page.next_page_hash;
            if let Some(hash) = &expected_hash {
                validate_hash(hash)?;
            }
        }
        if expected_hash.is_some()
            || candidate_count != self.candidate_count()
            || total_size_bytes != self.total_size_bytes()
        {
            return Err(ObjectError::GcPlanMismatch {
                field: "plan totals",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GcPlanManifest {
    binding: GcPlanBinding,
    digest:  String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GcPlanBinding {
    version:            u8,
    managed_prefix:     String,
    cutoff_ms:          u64,
    page_size:          usize,
    page_count:         usize,
    candidate_count:    u64,
    total_size_bytes:   u64,
    first_page_hash:    Option<String>,
    source_fingerprint: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct GcPlanPageEnvelope {
    version:                   u8,
    index:                     usize,
    pub(crate) candidates:     Vec<ObjectCandidate>,
    pub(crate) next_page_hash: Option<String>,
}

fn validate_candidate(
    candidate: &ObjectCandidate,
    managed_prefix: &str,
    cutoff_ms: u64,
    previous_uri: Option<&str>,
) -> Result<()> {
    if !candidate.uri.starts_with(managed_prefix) {
        return Err(ObjectError::GcCandidateOutsideStage {
            uri:   candidate.uri.clone(),
            stage: managed_prefix.to_owned(),
        });
    }
    if candidate.last_modified_ms > cutoff_ms {
        return Err(ObjectError::GcPlanMismatch {
            field: "age cutoff",
        });
    }
    if previous_uri.is_some_and(|previous| previous >= candidate.uri.as_str()) {
        return Err(ObjectError::GcInputUnsorted { input: "plan" });
    }
    Ok(())
}

fn draft_path(root: &Path, index: usize) -> PathBuf { root.join(format!("draft-{index:020}.json")) }

fn page_path(root: &Path, index: usize, hash: &str) -> PathBuf {
    root.join(format!("page-{index:020}-{hash}.json"))
}

fn sha256_hex(bytes: &[u8]) -> String { format!("{:x}", Sha256::digest(bytes)) }

fn validate_hash(hash: &str) -> Result<()> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ObjectError::GcPlanMismatch { field: "page hash" });
    }
    Ok(())
}

fn encode_json<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|source| ObjectError::GcPlanCorrupt {
        path: path.to_path_buf(),
        source,
    })
}

fn decode_json<T: for<'de> Deserialize<'de>>(path: &Path, bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|source| ObjectError::GcPlanCorrupt {
        path: path.to_path_buf(),
        source,
    })
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    decode_json(path, &read_bytes(path)?)
}

fn write_json<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<()> {
    write_bytes(path, &encode_json(path, value)?)
}

fn read_bytes(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).map_err(|source| plan_io("reading", path, source))
}

fn write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path).map_err(|source| plan_io("creating", path, source))?;
    file.write_all(bytes)
        .map_err(|source| plan_io("writing", path, source))?;
    file.sync_all()
        .map_err(|source| plan_io("syncing", path, source))
}

fn create_dir_all(path: &Path) -> Result<()> {
    fs::create_dir_all(path).map_err(|source| plan_io("creating directory", path, source))
}

fn create_dir(path: &Path) -> Result<()> {
    fs::create_dir(path).map_err(|source| plan_io("creating directory", path, source))
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| plan_io("syncing directory", path, source))
}

fn remove_file(path: &Path) -> Result<()> {
    fs::remove_file(path).map_err(|source| plan_io("removing", path, source))
}

fn rename(from: &Path, to: &Path) -> Result<()> {
    fs::rename(from, to).map_err(|source| plan_io("publishing", to, source))
}

fn plan_io(action: &'static str, path: &Path, source: std::io::Error) -> ObjectError {
    ObjectError::GcPlanIo {
        action,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GcPlanner, ObjectCandidate, ObjectError};

    fn candidate(uri: &str) -> ObjectCandidate {
        ObjectCandidate {
            uri:              uri.to_owned(),
            size_bytes:       42,
            last_modified_ms: 10,
        }
    }

    #[test]
    fn gc_plan_is_published_only_after_full_verification() {
        let temp = tempfile::tempdir().unwrap();
        let plan_path = temp.path().join("verified-plan");
        let planner = GcPlanner::try_new("s3://lake/objects/", 100, 1, true).unwrap();
        let pages = planner.plan(
            vec![
                candidate("s3://lake/objects/a"),
                candidate("s3://lake/objects/b"),
            ],
            Vec::new(),
        );
        let plan = GcPlanWriter::try_new(&plan_path, "s3://lake/objects/", 100, 1)
            .unwrap()
            .write(pages)
            .unwrap();

        assert_eq!(plan.page_count(), 2);
        assert_eq!(plan.candidate_count(), 2);
        assert_eq!(GcPlan::open(&plan_path).unwrap().digest(), plan.digest());

        let first_hash = plan.first_page_hash().unwrap();
        std::fs::write(page_path(&plan_path, 0, first_hash), b"corrupt").unwrap();
        assert!(matches!(
            GcPlan::open(&plan_path),
            Err(ObjectError::GcPlanMismatch {
                field: "page digest",
            })
        ));

        let rejected = temp.path().join("rejected-plan");
        let first_page = GcPlanner::try_new("s3://lake/objects/", 100, 1, true)
            .unwrap()
            .plan(vec![candidate("s3://lake/objects/a")], Vec::new())
            .next()
            .unwrap()
            .unwrap();
        let pages = vec![Ok(first_page), Err(ObjectError::GcLineageIncomplete)];
        let result = GcPlanWriter::try_new(&rejected, "s3://lake/objects/", 100, 1)
            .unwrap()
            .write(pages);
        assert!(matches!(result, Err(ObjectError::GcLineageIncomplete)));
        assert!(!rejected.exists());
    }
}
