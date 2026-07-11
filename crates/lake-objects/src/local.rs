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

//! Local-filesystem storage for managed objects in development.

use std::{
    collections::BTreeMap,
    io::SeekFrom,
    ops::Range,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use async_trait::async_trait;
use lake_common::DataLocation;
use sha2::{Digest, Sha256};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, Take},
};
use url::Url;

use crate::{
    InventoryPage, InventoryRequest, ManagedObjectInventory, ManagedObjectStore, ObjectCandidate,
    ObjectError, ObjectReader, Result, validate_range,
};

/// Bounded copy chunk, chosen to keep multi-gigabyte uploads off the heap.
const COPY_BUFFER_BYTES: usize = 64 * 1024;

/// Development managed stage rooted at a Lake-owned local directory.
#[derive(Clone, Debug)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// Open or create the Lake-owned managed stage at `root`.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = std::path::absolute(root.as_ref()).map_err(|source| ObjectError::Io {
            action: "absolutizing".to_owned(),
            path: root.as_ref().to_path_buf(),
            source,
        })?;
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|source| ObjectError::Io {
                action: "creating".to_owned(),
                path: root.clone(),
                source,
            })?;
        let root = tokio::fs::canonicalize(&root)
            .await
            .map_err(|source| ObjectError::Io {
                action: "resolving".to_owned(),
                path: root.clone(),
                source,
            })?;
        Ok(Self { root })
    }

    /// Stream a local file into the managed stage and return its immutable
    /// identity.
    pub async fn put_file(
        &self,
        source: impl AsRef<Path>,
        content_type: impl Into<String>,
    ) -> Result<DataLocation> {
        let source = source.as_ref();
        let input = File::open(source)
            .await
            .map_err(|source_error| ObjectError::Io {
                action: "opening".to_owned(),
                path:   source.to_path_buf(),
                source: source_error,
            })?;
        self.put_reader(input, content_type).await
    }

    /// Stream an arbitrary SDK reader into the managed stage.
    pub async fn put_reader<R>(
        &self,
        mut input: R,
        content_type: impl Into<String>,
    ) -> Result<DataLocation>
    where
        R: AsyncRead + Send + Unpin,
    {
        let object_id = uuid::Uuid::now_v7().to_string();
        let staging = self.root.join(format!(".{object_id}.uploading"));
        let destination = self.root.join(object_id);
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)
            .await
            .map_err(|source_error| ObjectError::Io {
                action: "creating".to_owned(),
                path:   staging.clone(),
                source: source_error,
            })?;

        let copied: Result<(u64, String)> = async {
            let mut hasher = Sha256::new();
            let mut size_bytes = 0_u64;
            let mut buffer = vec![0; COPY_BUFFER_BYTES];
            loop {
                let read = input
                    .read(&mut buffer)
                    .await
                    .map_err(|source| ObjectError::Read { source })?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                output
                    .write_all(&buffer[..read])
                    .await
                    .map_err(|source_error| ObjectError::Io {
                        action: "writing".to_owned(),
                        path:   staging.clone(),
                        source: source_error,
                    })?;
                size_bytes = size_bytes.saturating_add(read as u64);
            }
            output
                .flush()
                .await
                .map_err(|source_error| ObjectError::Io {
                    action: "flushing".to_owned(),
                    path:   staging.clone(),
                    source: source_error,
                })?;
            output.sync_all().await.map_err(|source| ObjectError::Io {
                action: "durably syncing".to_owned(),
                path: staging.clone(),
                source,
            })?;
            Ok((size_bytes, format!("{:x}", hasher.finalize())))
        }
        .await;
        drop(output);
        let (size_bytes, sha256) = match copied {
            Ok(copied) => copied,
            Err(error) => {
                let _ = tokio::fs::remove_file(&staging).await;
                return Err(error);
            }
        };
        if let Err(source) = tokio::fs::rename(&staging, &destination).await {
            let _ = tokio::fs::remove_file(&staging).await;
            return Err(ObjectError::Io {
                action: "publishing".to_owned(),
                path: destination.clone(),
                source,
            });
        }
        let directory_sync = File::open(&self.root)
            .await
            .map_err(|source| ObjectError::Io {
                action: "opening".to_owned(),
                path: self.root.clone(),
                source,
            })?
            .sync_all()
            .await
            .map_err(|source| ObjectError::Io {
                action: "durably syncing".to_owned(),
                path: self.root.clone(),
                source,
            });
        if let Err(error) = directory_sync {
            let _ = tokio::fs::remove_file(&destination).await;
            return Err(error);
        }

        let uri = Url::from_file_path(&destination)
            .map_err(|()| ObjectError::FileUri {
                path: destination.clone(),
            })?
            .to_string();
        Ok(DataLocation::builder()
            .uri(uri)
            .content_type(content_type)
            .size_bytes(size_bytes)
            .sha256(sha256)
            .build())
    }

    /// Open a direct local reader for a `FILE` in the managed `file://` stage.
    pub async fn open_reader(&self, location: &DataLocation) -> Result<File> {
        let path = Url::parse(&location.uri)
            .ok()
            .and_then(|url| url.to_file_path().ok())
            .ok_or_else(|| ObjectError::InvalidLocalUri {
                uri: location.uri.clone(),
            })?;
        let path = tokio::fs::canonicalize(&path)
            .await
            .map_err(|source| ObjectError::Io {
                action: "resolving".to_owned(),
                path,
                source,
            })?;
        if !path.starts_with(&self.root) {
            return Err(ObjectError::OutsideManagedPrefix {
                path,
                root: self.root.clone(),
            });
        }
        File::open(&path).await.map_err(|source| ObjectError::Io {
            action: "opening".to_owned(),
            path,
            source,
        })
    }

    /// Open exactly one non-empty half-open byte range from a managed object.
    pub async fn open_range(
        &self,
        location: &DataLocation,
        range: Range<u64>,
    ) -> Result<Take<File>> {
        let length = validate_range(location, &range)?;
        let mut file = self.open_reader(location).await?;
        file.seek(SeekFrom::Start(range.start))
            .await
            .map_err(|source| ObjectError::Io {
                action: "seeking".to_owned(),
                path: PathBuf::from(&location.uri),
                source,
            })?;
        Ok(file.take(length))
    }
}

#[async_trait]
impl ManagedObjectStore for LocalObjectStore {
    fn stage_identity(&self) -> String { format!("file://{}", self.root.display()) }

    async fn put_reader(&self, input: ObjectReader, content_type: String) -> Result<DataLocation> {
        LocalObjectStore::put_reader(self, input, content_type).await
    }

    async fn open_reader(&self, location: &DataLocation) -> Result<ObjectReader> {
        Ok(Box::pin(
            LocalObjectStore::open_reader(self, location).await?,
        ))
    }

    async fn open_range(&self, location: &DataLocation, range: Range<u64>) -> Result<ObjectReader> {
        Ok(Box::pin(
            LocalObjectStore::open_range(self, location, range).await?,
        ))
    }
}

#[async_trait]
impl ManagedObjectInventory for LocalObjectStore {
    fn managed_uri_prefix(&self) -> String {
        Url::from_directory_path(&self.root)
            .expect("canonical absolute directory forms a file URI")
            .to_string()
    }

    async fn inventory_page(&self, request: InventoryRequest) -> Result<InventoryPage> {
        let prefix = self.managed_uri_prefix();
        let (cursor, max_items) = request.into_parts();
        if cursor
            .as_deref()
            .is_some_and(|cursor| !cursor.starts_with(&prefix))
        {
            return Err(ObjectError::GcCandidateOutsideStage {
                uri:   cursor.expect("cursor was checked"),
                stage: prefix,
            });
        }

        // Local storage is a development backend. Re-scan its flat directory
        // for each page while retaining only the next bounded set, so memory
        // does not grow with the number of objects.
        let mut entries =
            tokio::fs::read_dir(&self.root)
                .await
                .map_err(|source| ObjectError::Io {
                    action: "listing".to_owned(),
                    path: self.root.clone(),
                    source,
                })?;
        let mut selected = BTreeMap::new();
        let mut has_more = false;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|source| ObjectError::Io {
                action: "reading directory entry in".to_owned(),
                path: self.root.clone(),
                source,
            })?
        {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with('.') {
                continue;
            }
            let file_type = entry.file_type().await.map_err(|source| ObjectError::Io {
                action: "reading file type for".to_owned(),
                path: entry.path(),
                source,
            })?;
            if !file_type.is_file() {
                continue;
            }
            let uri = Url::from_file_path(entry.path())
                .map_err(|()| ObjectError::FileUri { path: entry.path() })?
                .to_string();
            if cursor
                .as_deref()
                .is_some_and(|cursor| uri.as_str() <= cursor)
            {
                continue;
            }
            let metadata = entry.metadata().await.map_err(|source| ObjectError::Io {
                action: "reading metadata for".to_owned(),
                path: entry.path(),
                source,
            })?;
            let modified = metadata
                .modified()
                .and_then(|modified| {
                    modified
                        .duration_since(UNIX_EPOCH)
                        .map_err(std::io::Error::other)
                })
                .map_err(|source| ObjectError::Io {
                    action: "reading modification time for".to_owned(),
                    path: entry.path(),
                    source,
                })?;
            selected.insert(
                uri.clone(),
                ObjectCandidate {
                    uri,
                    size_bytes: metadata.len(),
                    last_modified_ms: u64::try_from(modified.as_millis()).map_err(|_| {
                        ObjectError::InvalidGcConfig {
                            message: "object modification time exceeds u64 milliseconds".to_owned(),
                        }
                    })?,
                },
            );
            if selected.len() > max_items {
                selected.pop_last();
                has_more = true;
            }
        }
        let candidates = selected.into_values().collect::<Vec<_>>();
        let next_cursor = has_more.then(|| {
            candidates
                .last()
                .expect("non-empty bounded page")
                .uri
                .clone()
        });
        Ok(InventoryPage::new(candidates, next_cursor))
    }
}
