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

//! KV metadata store. Only holds tiny mutable pointers (table -> current
//! version); everything else lives as immutable files. Prod impl: DynamoDB
//! with conditional puts.

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use snafu::ResultExt;

use crate::error::{MetaSnafu, Result};

pub type MetaStoreRef = Arc<dyn MetaStore>;

pub trait MetaStore: Send + Sync {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Atomic compare-and-set. `expected = None` means "key must not exist".
    /// Returns false if the current value didn't match.
    fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool>;

    /// List keys under a prefix, prefix stripped.
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}

pub struct RocksMeta {
    db:         rocksdb::DB,
    // ponytail: process-local mutex makes get+put atomic; in prod CAS is a
    // DynamoDB conditional put, no lock needed.
    write_lock: Mutex<()>,
}

impl RocksMeta {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = rocksdb::DB::open_default(&path).context(MetaSnafu {
            key: path.as_ref().display().to_string(),
        })?;
        Ok(Self {
            db,
            write_lock: Mutex::new(()),
        })
    }
}

impl MetaStore for RocksMeta {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.db.get(key).context(MetaSnafu { key })
    }

    fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool> {
        let _guard = self.write_lock.lock().expect("metastore lock poisoned");
        let current = self.db.get(key).context(MetaSnafu { key })?;
        if current.as_deref() != expected {
            return Ok(false);
        }
        self.db.put(key, new).context(MetaSnafu { key })?;
        Ok(true)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for item in self.db.prefix_iterator(prefix) {
            let (key, _) = item.context(MetaSnafu { key: prefix })?;
            let key = String::from_utf8_lossy(&key);
            let Some(stripped) = key.strip_prefix(prefix) else {
                break;
            };
            out.push(stripped.to_string());
        }
        Ok(out)
    }
}
