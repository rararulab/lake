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

//! Production metastore view that fences every target publication with the
//! latest live metadata lease.

use std::sync::Arc;

use async_trait::async_trait;
use lake_meta::{
    GuardedMutation, MetaError, MetaScanPage, MetaStore, MetaStoreRef, Result, SignaledMutation,
};

use crate::leadership::Leadership;

pub(crate) struct FencedMetaStore {
    inner:      MetaStoreRef,
    leadership: Arc<Leadership>,
}

impl FencedMetaStore {
    pub(crate) fn new(inner: MetaStoreRef, leadership: Arc<Leadership>) -> Self {
        Self { inner, leadership }
    }

    fn guard(&self) -> Result<crate::election::LeaseGuard> {
        self.leadership
            .current_guard()
            .ok_or(MetaError::MutationGuardUnavailable)
    }
}

#[async_trait]
impl MetaStore for FencedMetaStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> { self.inner.get(key).await }

    async fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool> {
        let _publication = self.leadership.begin_publication().await;
        let guard = self.guard()?;
        let mutation = match expected {
            None => GuardedMutation::create(crate::election::LEASE_KEY, guard.bytes(), key, new),
            Some(value) => {
                GuardedMutation::update(crate::election::LEASE_KEY, guard.bytes(), key, value, new)
            }
        };
        self.inner.guarded_mutate(mutation).await
    }

    async fn signaled_mutate(&self, mutation: SignaledMutation<'_>) -> Result<bool> {
        let _publication = self.leadership.begin_publication().await;
        let guard = self.guard()?;
        self.inner
            .guarded_mutate(mutation.guarded_by(crate::election::LEASE_KEY, guard.bytes()))
            .await
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        self.inner.scan_prefix(prefix).await
    }

    async fn scan_prefix_page(
        &self,
        prefix: &str,
        continuation: Option<&str>,
        limit: usize,
    ) -> Result<MetaScanPage> {
        self.inner
            .scan_prefix_page(prefix, continuation, limit)
            .await
    }

    async fn delete(&self, key: &str, expected: &[u8]) -> Result<bool> {
        let _publication = self.leadership.begin_publication().await;
        let guard = self.guard()?;
        self.inner
            .guarded_mutate(GuardedMutation::delete(
                crate::election::LEASE_KEY,
                guard.bytes(),
                key,
                expected,
            ))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use lake_meta::{MetaError, MetaStore, MetaStoreRef, RocksMeta, SignaledMutation};

    use super::FencedMetaStore;
    use crate::{
        election::{LeaseElection, LeaseGuard, LeaseStatus},
        leadership::Leadership,
    };

    fn leader_guard(status: LeaseStatus) -> LeaseGuard {
        let LeaseStatus::Leader { guard, .. } = status else {
            panic!("expected leader status");
        };
        guard
    }

    #[tokio::test]
    async fn stale_leader_cannot_publish_after_takeover() {
        let dir = tempfile::tempdir().unwrap();
        let raw: MetaStoreRef = Arc::new(RocksMeta::open(dir.path()).unwrap());
        let a = LeaseElection::new(raw.clone(), "a", Duration::from_millis(10));
        let b = LeaseElection::new(raw.clone(), "b", Duration::from_millis(10));
        let stale = leader_guard(a.campaign_at(0).await.unwrap());
        let leadership = Arc::new(Leadership::new());
        leadership.assume_guarded_leader("a", stale, Duration::from_mins(1));
        let fenced = FencedMetaStore::new(raw.clone(), leadership);
        assert!(raw.cas("target", None, b"old").await.unwrap());

        let takeover = leader_guard(b.campaign_at(20).await.unwrap());
        assert_eq!(takeover.epoch(), 2);
        assert!(!fenced.cas("target", Some(b"old"), b"stale").await.unwrap());
        assert_eq!(
            raw.get("target").await.unwrap().as_deref(),
            Some(&b"old"[..])
        );
    }

    #[tokio::test]
    async fn stale_leader_cannot_publish_directory_generation() {
        let dir = tempfile::tempdir().unwrap();
        let raw: MetaStoreRef = Arc::new(RocksMeta::open(dir.path()).unwrap());
        let a = LeaseElection::new(raw.clone(), "a", Duration::from_millis(10));
        let b = LeaseElection::new(raw.clone(), "b", Duration::from_millis(10));
        let stale = leader_guard(a.campaign_at(0).await.unwrap());
        let leadership = Arc::new(Leadership::new());
        leadership.assume_guarded_leader("a", stale, Duration::from_mins(1));
        let fenced = FencedMetaStore::new(raw.clone(), leadership);

        let takeover = leader_guard(b.campaign_at(20).await.unwrap());
        assert_eq!(takeover.epoch(), 2);
        let published = fenced
            .signaled_mutate(SignaledMutation::create(
                "tbl/robots/episodes",
                b"registration",
                "__lake_internal/catalog-directory-generation",
                b"generation",
            ))
            .await
            .unwrap();

        assert!(!published);
        assert_eq!(raw.get("tbl/robots/episodes").await.unwrap(), None);
        assert_eq!(
            raw.get("__lake_internal/catalog-directory-generation")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn publication_uses_fresh_guard_after_renewal() {
        let dir = tempfile::tempdir().unwrap();
        let raw: MetaStoreRef = Arc::new(RocksMeta::open(dir.path()).unwrap());
        let election = LeaseElection::new(raw.clone(), "a", Duration::from_millis(100));
        let first = leader_guard(election.campaign_at(0).await.unwrap());
        let leadership = Arc::new(Leadership::new());
        leadership.assume_guarded_leader("a", first.clone(), Duration::from_mins(1));
        let fenced = FencedMetaStore::new(raw.clone(), leadership.clone());

        let renewed = leader_guard(election.campaign_at(1).await.unwrap());
        assert_eq!(renewed.epoch(), first.epoch());
        assert_ne!(renewed.bytes(), first.bytes());
        leadership.assume_guarded_leader("a", renewed, Duration::from_mins(1));

        assert!(fenced.cas("target", None, b"published").await.unwrap());
        assert_eq!(
            raw.get("target").await.unwrap().as_deref(),
            Some(&b"published"[..])
        );
    }

    #[tokio::test]
    async fn publication_without_live_guard_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let raw: MetaStoreRef = Arc::new(RocksMeta::open(dir.path()).unwrap());
        let fenced = FencedMetaStore::new(raw.clone(), Arc::new(Leadership::new()));

        let error = fenced
            .cas("target", None, b"value")
            .await
            .expect_err("no local authority");
        assert!(matches!(error, MetaError::MutationGuardUnavailable));
        assert_eq!(raw.get("target").await.unwrap(), None);
    }
}
