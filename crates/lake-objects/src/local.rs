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

use std::path::{Path, PathBuf};

use lake_common::DataLocation;
use sha2::{Digest, Sha256};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use url::Url;

use crate::{ObjectError, Result};

/// Bounded copy chunk, chosen to keep multi-gigabyte uploads off the heap.
const COPY_BUFFER_BYTES: usize = 64 * 1024;

/// Development object storage rooted at a Lake-owned local directory.
#[derive(Clone, Debug)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// Open or create the Lake-owned object prefix at `root`.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = std::path::absolute(root.as_ref()).map_err(|source| ObjectError::Io {
            action: "absolutizing".to_owned(),
            path:   root.as_ref().to_path_buf(),
            source,
        })?;
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|source| ObjectError::Io {
                action: "creating".to_owned(),
                path:   root.clone(),
                source,
            })?;
        Ok(Self { root })
    }

    /// Stream a local file into the managed prefix and return its immutable identity.
    pub async fn put_file(
        &self,
        source: impl AsRef<Path>,
        content_type: impl Into<String>,
    ) -> Result<DataLocation> {
        let source = source.as_ref();
        let mut input = File::open(source).await.map_err(|source_error| ObjectError::Io {
            action: "opening".to_owned(),
            path:   source.to_path_buf(),
            source: source_error,
        })?;
        let destination = self.root.join(uuid::Uuid::now_v7().to_string());
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
            .await
            .map_err(|source_error| ObjectError::Io {
                action: "creating".to_owned(),
                path:   destination.clone(),
                source: source_error,
            })?;

        let mut hasher = Sha256::new();
        let mut size_bytes = 0_u64;
        let mut buffer = [0; COPY_BUFFER_BYTES];
        loop {
            let read = input.read(&mut buffer).await.map_err(|source_error| ObjectError::Io {
                action: "reading".to_owned(),
                path:   source.to_path_buf(),
                source: source_error,
            })?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            output
                .write_all(&buffer[..read])
                .await
                .map_err(|source_error| ObjectError::Io {
                    action: "writing".to_owned(),
                    path:   destination.clone(),
                    source: source_error,
                })?;
            size_bytes = size_bytes.saturating_add(read as u64);
        }
        output.flush().await.map_err(|source_error| ObjectError::Io {
            action: "flushing".to_owned(),
            path:   destination.clone(),
            source: source_error,
        })?;

        let uri = Url::from_file_path(&destination)
            .map_err(|()| ObjectError::FileUri {
                path: destination.clone(),
            })?
            .to_string();
        Ok(DataLocation::builder()
            .uri(uri)
            .content_type(content_type)
            .size_bytes(size_bytes)
            .sha256(format!("{:x}", hasher.finalize()))
            .build())
    }

    /// Open a direct local reader for a managed `file://` location.
    pub async fn open_reader(&self, location: &DataLocation) -> Result<File> {
        let path = Url::parse(&location.uri)
            .ok()
            .and_then(|url| url.to_file_path().ok())
            .ok_or_else(|| ObjectError::InvalidLocalUri {
                uri: location.uri.clone(),
            })?;
        File::open(&path).await.map_err(|source| ObjectError::Io {
            action: "opening".to_owned(),
            path,
            source,
        })
    }
}
