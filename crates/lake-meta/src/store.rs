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

use crate::error::{MetaError, Result};

pub type MetaStoreRef = Arc<dyn MetaStore>;

/// One exact target transition protected by an exact guard value.
///
/// Constructors enforce that create, update, and delete always carry the
/// target condition required for an atomic compare-and-set. Backends must
/// check the guard and target condition in the same transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuardedMutation<'a> {
    pub(crate) guard_key:      &'a str,
    pub(crate) guard_expected: &'a [u8],
    pub(crate) target_key:     &'a str,
    pub(crate) target:         GuardedTarget<'a>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GuardedTarget<'a> {
    Put {
        expected: Option<&'a [u8]>,
        value:    &'a [u8],
    },
    Delete {
        expected: &'a [u8],
    },
}

impl<'a> GuardedMutation<'a> {
    /// Create an absent target while the guard has `guard_expected`.
    #[must_use]
    pub const fn create(
        guard_key: &'a str,
        guard_expected: &'a [u8],
        target_key: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            guard_key,
            guard_expected,
            target_key,
            target: GuardedTarget::Put {
                expected: None,
                value,
            },
        }
    }

    /// Replace an exact target value while the guard has `guard_expected`.
    #[must_use]
    pub const fn update(
        guard_key: &'a str,
        guard_expected: &'a [u8],
        target_key: &'a str,
        target_expected: &'a [u8],
        value: &'a [u8],
    ) -> Self {
        Self {
            guard_key,
            guard_expected,
            target_key,
            target: GuardedTarget::Put {
                expected: Some(target_expected),
                value,
            },
        }
    }

    /// Delete an exact target value while the guard has `guard_expected`.
    #[must_use]
    pub const fn delete(
        guard_key: &'a str,
        guard_expected: &'a [u8],
        target_key: &'a str,
        target_expected: &'a [u8],
    ) -> Self {
        Self {
            guard_key,
            guard_expected,
            target_key,
            target: GuardedTarget::Delete {
                expected: target_expected,
            },
        }
    }

    pub(crate) fn validate(self) -> Result<Self> {
        if self.guard_key == self.target_key {
            return Err(MetaError::InvalidGuardedMutation);
        }
        Ok(self)
    }
}

/// One bounded page from a prefix scan. The continuation token is opaque to
/// callers and may be passed back only with the same prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaScanPage {
    entries:      Vec<(String, Vec<u8>)>,
    continuation: Option<String>,
}

impl MetaScanPage {
    /// Construct a page with backend-opaque continuation state.
    #[must_use]
    pub fn new(entries: Vec<(String, Vec<u8>)>, continuation: Option<String>) -> Self {
        Self {
            entries,
            continuation,
        }
    }

    /// Borrow the stripped keys and values returned in this page.
    #[must_use]
    pub fn entries(&self) -> &[(String, Vec<u8>)] { &self.entries }

    /// Borrow the token to use for the next scan of the same prefix.
    #[must_use]
    pub fn continuation(&self) -> Option<&str> { self.continuation.as_deref() }

    /// Consume the page into entries and continuation state.
    #[must_use]
    pub fn into_parts(self) -> (Vec<(String, Vec<u8>)>, Option<String>) {
        (self.entries, self.continuation)
    }
}

#[async_trait]
pub trait MetaStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Atomic compare-and-set. `expected = None` means "key must not exist".
    /// Returns false if the current value didn't match.
    async fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool>;

    /// Apply a target transition only when both its expected state and an
    /// independent guard value match. Both checks and the write are atomic.
    ///
    /// Backends must override this with a native transaction. The default is
    /// deliberately fail-closed; a read followed by [`Self::cas`] is not an
    /// implementation of this contract.
    async fn guarded_mutate(&self, mutation: GuardedMutation<'_>) -> Result<bool> {
        mutation.validate()?;
        Err(MetaError::GuardedMutationUnsupported)
    }

    /// List keys under a prefix, prefix stripped.
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;

    /// Scan key-value entries under a prefix, returning stripped keys.
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let keys = self.list_prefix(prefix).await?;
        let mut entries = Vec::with_capacity(keys.len());
        for stripped in keys {
            let key = format!("{prefix}{stripped}");
            if let Some(value) = self.get(&key).await? {
                entries.push((stripped, value));
            }
        }
        Ok(entries)
    }

    /// Scan at most `limit` entries. Backends override this to avoid loading
    /// an unbounded prefix; the default preserves compatibility for test
    /// stores while still presenting the same cursor contract.
    async fn scan_prefix_page(
        &self,
        prefix: &str,
        continuation: Option<&str>,
        limit: usize,
    ) -> Result<MetaScanPage> {
        if limit == 0 {
            return Ok(MetaScanPage::new(Vec::new(), None));
        }
        let mut entries = self.scan_prefix(prefix).await?;
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut eligible = entries.into_iter().filter(|(key, _)| {
            continuation.is_none_or(|cursor| format!("{prefix}{key}").as_str() > cursor)
        });
        let page = eligible.by_ref().take(limit).collect::<Vec<_>>();
        let next = eligible
            .next()
            .and_then(|_| page.last().map(|(key, _)| format!("{prefix}{key}")));
        Ok(MetaScanPage::new(page, next))
    }

    /// Delete `key` only when its current value exactly matches `expected`.
    /// Returns false when the key is absent or has changed.
    async fn delete(&self, key: &str, expected: &[u8]) -> Result<bool>;
}
