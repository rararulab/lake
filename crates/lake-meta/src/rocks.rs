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

//! Dev backend: RocksDB. CAS is emulated with a process-local mutex.

use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use async_trait::async_trait;
use rocksdb::{Direction, IteratorMode};
use snafu::ResultExt;

use crate::{
    error::{BackendSnafu, Result},
    store::{MetaScanPage, MetaStore},
};

pub struct RocksMeta {
    db:         rocksdb::DB,
    // ponytail: process-local mutex makes get+put atomic; in prod CAS is a
    // DynamoDB conditional put, no lock needed.
    write_lock: Mutex<()>,
}

impl RocksMeta {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = rocksdb::DB::open_default(&path).context(BackendSnafu {
            key: path.as_ref().display().to_string(),
        })?;
        Ok(Self {
            db,
            write_lock: Mutex::new(()),
        })
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_lock.lock().expect("metastore lock poisoned")
    }
}

// ponytail: RocksDB calls are sync and fast (local disk); they run inline
// on the async executor. Move to spawn_blocking if profiling ever shows
// them stalling worker threads.
#[async_trait]
impl MetaStore for RocksMeta {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.db.get(key).context(BackendSnafu { key })
    }

    async fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool> {
        let _guard = self.lock();
        let current = self.db.get(key).context(BackendSnafu { key })?;
        if current.as_deref() != expected {
            return Ok(false);
        }
        self.db.put(key, new).context(BackendSnafu { key })?;
        Ok(true)
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for item in self.db.prefix_iterator(prefix) {
            let (key, _) = item.context(BackendSnafu { key: prefix })?;
            let key = String::from_utf8_lossy(&key);
            let Some(stripped) = key.strip_prefix(prefix) else {
                break;
            };
            out.push(stripped.to_string());
        }
        Ok(out)
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let mut out = Vec::new();
        for item in self.db.prefix_iterator(prefix) {
            let (key, value) = item.context(BackendSnafu { key: prefix })?;
            let key = String::from_utf8_lossy(&key);
            let Some(stripped) = key.strip_prefix(prefix) else {
                break;
            };
            out.push((stripped.to_owned(), value.to_vec()));
        }
        Ok(out)
    }

    async fn scan_prefix_page(
        &self,
        prefix: &str,
        continuation: Option<&str>,
        limit: usize,
    ) -> Result<MetaScanPage> {
        if limit == 0 {
            return Ok(MetaScanPage::new(Vec::new(), None));
        }
        let start = continuation.unwrap_or(prefix);
        let mut entries = Vec::with_capacity(limit);
        let mut last_full_key = None;
        let mut has_more = false;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_bytes(), Direction::Forward))
        {
            let (key, value) = item.context(BackendSnafu { key: prefix })?;
            let key = String::from_utf8_lossy(&key);
            if continuation.is_some_and(|cursor| key.as_ref() <= cursor) {
                continue;
            }
            let Some(stripped) = key.strip_prefix(prefix) else {
                break;
            };
            if entries.len() == limit {
                has_more = true;
                break;
            }
            last_full_key = Some(key.to_string());
            entries.push((stripped.to_owned(), value.to_vec()));
        }
        Ok(MetaScanPage::new(
            entries,
            has_more.then(|| last_full_key).flatten(),
        ))
    }

    async fn delete(&self, key: &str, expected: &[u8]) -> Result<bool> {
        let _guard = self.lock();
        let current = self.db.get(key).context(BackendSnafu { key })?;
        if current.as_deref() != Some(expected) {
            return Ok(false);
        }
        self.db.delete(key).context(BackendSnafu { key })?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (tempfile::TempDir, RocksMeta) {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksMeta::open(dir.path()).unwrap();
        (dir, meta)
    }

    #[tokio::test]
    async fn cas_swaps_only_on_expected_match() {
        let (_dir, meta) = open_temp();
        assert!(meta.cas("k", None, b"1").await.unwrap());
        assert!(
            !meta.cas("k", None, b"2").await.unwrap(),
            "key exists, None must fail"
        );
        assert!(
            !meta.cas("k", Some(b"9"), b"2").await.unwrap(),
            "wrong expected must fail"
        );
        assert!(meta.cas("k", Some(b"1"), b"2").await.unwrap());
        assert_eq!(meta.get("k").await.unwrap().as_deref(), Some(&b"2"[..]));
    }

    #[tokio::test]
    async fn list_prefix_strips_and_filters() {
        let (_dir, meta) = open_temp();
        for k in ["ptr/a", "ptr/b", "other/c"] {
            assert!(meta.cas(k, None, b"v").await.unwrap());
        }
        assert_eq!(meta.list_prefix("ptr/").await.unwrap(), vec!["a", "b"]);
    }

    #[tokio::test]
    async fn prefix_entry_scan_returns_stripped_keys_and_values() {
        let (_dir, meta) = open_temp();
        assert!(meta.cas("tbl/a/x", None, b"schema-x").await.unwrap());
        assert!(meta.cas("tbl/a/y", None, b"schema-y").await.unwrap());
        assert!(meta.cas("other/z", None, b"hidden").await.unwrap());

        assert_eq!(
            meta.scan_prefix("tbl/a/").await.unwrap(),
            vec![
                ("x".to_owned(), b"schema-x".to_vec()),
                ("y".to_owned(), b"schema-y".to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn prefix_entry_scan_page_is_bounded() {
        let (_dir, meta) = open_temp();
        for key in ["op/a", "op/b", "op/c", "other/z"] {
            assert!(meta.cas(key, None, key.as_bytes()).await.unwrap());
        }

        let first = meta.scan_prefix_page("op/", None, 2).await.unwrap();
        assert_eq!(first.entries().len(), 2);
        assert_eq!(first.entries()[0].0, "a");
        assert_eq!(first.entries()[1].0, "b");
        let second = meta
            .scan_prefix_page("op/", first.continuation(), 2)
            .await
            .unwrap();
        assert_eq!(second.entries(), &[("c".to_owned(), b"op/c".to_vec())]);
        assert!(second.continuation().is_none());
    }
}
