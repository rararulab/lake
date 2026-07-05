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

//! The `MetaStore` trait — the only interface the rest of lake programs
//! against. Async-first: the prod backend (DynamoDB) is network-bound.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;

pub type MetaStoreRef = Arc<dyn MetaStore>;

#[async_trait]
pub trait MetaStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Atomic compare-and-set. `expected = None` means "key must not exist".
    /// Returns false if the current value didn't match.
    async fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool>;

    /// List keys under a prefix, prefix stripped.
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}
