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

//! Lease-in-KV leader election for the metadata tier.
//!
//! This gives the stateful metadata tier HA (leader + standby) without any
//! self-built consensus: it rides entirely on the [`lake_meta::MetaStore`] CAS
//! primitive, adapting the GreptimeDB election pattern to our KV. See the HA
//! section of `docs/architecture.md` and the election study in
//! `docs/design/meta-server.md`.
//!
//! A single lease record lives at [`LEASE_KEY`]. A node campaigns to become
//! leader by CAS-installing (or renewing, or stealing an expired) lease keyed
//! to its own id. Only the lease holder is permitted to serve the write path;
//! a standby that observes an expired lease steals it and takes over. Because
//! every state transition is a single compare-and-set, two nodes racing for
//! the same lease resolve deterministically: exactly one CAS wins.
//!
//! The core [`LeaseElection::campaign_at`] takes an injected `now_ms` clock so
//! the whole protocol is testable without real sleeps;
//! [`LeaseElection::campaign`] wraps it with the wall clock.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lake_meta::{MetaError, MetaStoreRef};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

/// KV key under which the single leader lease record is stored.
pub const LEASE_KEY: &str = "election/leader";

/// Errors raised by the lease election.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ElectionError {
    /// The underlying metastore CAS/get failed.
    #[snafu(display("election metastore operation failed"))]
    Store { source: MetaError },

    /// A lease value could not be encoded to JSON.
    #[snafu(display("failed to encode lease value"))]
    Encode { source: serde_json::Error },

    /// The stored lease value could not be decoded from JSON.
    #[snafu(display("failed to decode lease value at '{LEASE_KEY}'"))]
    Decode { source: serde_json::Error },

    /// The fencing epoch cannot advance without wrapping.
    #[snafu(display("lease fencing epoch exhausted"))]
    EpochExhausted,
}

/// Election result alias.
pub type Result<T> = std::result::Result<T, ElectionError>;

/// The durable lease record stored at [`LEASE_KEY`].
///
/// `expires_at_ms` is an absolute millisecond timestamp on the same clock the
/// campaign is driven with. A lease whose `expires_at_ms <= now_ms` is up for
/// grabs by any node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseValue {
    /// Node id of the current lease holder.
    pub holder:        String,
    /// Absolute expiry, in milliseconds since the Unix epoch.
    pub expires_at_ms: u64,
    /// Monotonic fencing token. Missing legacy values deserialize as zero and
    /// are upgraded to one by the next successful campaign from the holder or
    /// a successor.
    #[serde(default)]
    pub epoch:         u64,
}

/// Exact durable lease value authorizing one metadata publication.
///
/// The bytes are the value successfully installed at [`LEASE_KEY`]. Keeping
/// them opaque prevents callers from re-encoding legacy JSON and weakening an
/// exact DynamoDB/RocksDB guard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseGuard {
    epoch: u64,
    bytes: Vec<u8>,
}

impl LeaseGuard {
    pub(crate) fn new(epoch: u64, bytes: Vec<u8>) -> Self { Self { epoch, bytes } }

    #[must_use]
    pub const fn epoch(&self) -> u64 { self.epoch }

    #[must_use]
    pub fn bytes(&self) -> &[u8] { &self.bytes }
}

/// The outcome of a single campaign round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseStatus {
    /// This node holds the lease until `expires_at_ms`.
    Leader {
        /// Absolute expiry of the lease this node now holds.
        expires_at_ms: u64,
        /// Exact durable lease value held by this node.
        guard:         LeaseGuard,
    },
    /// Another node holds the lease.
    Follower {
        /// Node id of the observed lease holder.
        current_holder: String,
    },
}

impl LeaseStatus {
    /// Whether this status means the local node is the leader.
    #[must_use]
    pub const fn is_leader(&self) -> bool { matches!(self, Self::Leader { .. }) }
}

/// A participant in the lease-in-KV leader election.
///
/// Cheap to clone-by-construction: it holds a shared [`MetaStoreRef`], the
/// local `node_id`, and the lease `ttl`.
pub struct LeaseElection {
    meta:    MetaStoreRef,
    node_id: String,
    ttl:     Duration,
}

impl LeaseElection {
    /// Build an election participant for `node_id` with lease lifetime `ttl`.
    pub fn new(meta: MetaStoreRef, node_id: impl Into<String>, ttl: Duration) -> Self {
        Self {
            meta,
            node_id: node_id.into(),
            ttl,
        }
    }

    /// This node's id.
    #[must_use]
    pub fn node_id(&self) -> &str { &self.node_id }

    /// Configured lease lifetime.
    #[must_use]
    pub(crate) fn ttl(&self) -> Duration { self.ttl }

    /// Read and decode the current lease, if any.
    async fn read_lease(&self) -> Result<Option<(LeaseValue, Vec<u8>)>> {
        match self.meta.get(LEASE_KEY).await.context(StoreSnafu)? {
            Some(bytes) => {
                let value = serde_json::from_slice(&bytes).context(DecodeSnafu)?;
                Ok(Some((value, bytes)))
            }
            None => Ok(None),
        }
    }

    /// Build the lease this node would install at `now_ms`.
    fn our_lease(&self, now_ms: u64, epoch: u64) -> LeaseValue {
        LeaseValue {
            holder: self.node_id.clone(),
            expires_at_ms: now_ms.saturating_add(self.ttl_ms()),
            epoch,
        }
    }

    fn ttl_ms(&self) -> u64 { u64::try_from(self.ttl.as_millis()).unwrap_or(u64::MAX) }

    /// Run one campaign round against the injected clock `now_ms`.
    ///
    /// Every branch resolves to a single CAS, so concurrent campaigns are
    /// serialized by the store:
    /// - no lease → install one (`None` → ours); a lost race re-reads and
    ///   reports the winner as [`LeaseStatus::Follower`].
    /// - we already hold it → renew in place.
    /// - it is held by another but expired → steal it.
    /// - it is held by another and still valid → [`LeaseStatus::Follower`].
    pub async fn campaign_at(&self, now_ms: u64) -> Result<LeaseStatus> {
        match self.read_lease().await? {
            // No lease yet: try to install ours from the empty state.
            None => {
                let new_lease = self.our_lease(now_ms, 1);
                let new_bytes = serde_json::to_vec(&new_lease).context(EncodeSnafu)?;
                if self
                    .meta
                    .cas(LEASE_KEY, None, &new_bytes)
                    .await
                    .context(StoreSnafu)?
                {
                    Ok(LeaseStatus::Leader {
                        expires_at_ms: new_lease.expires_at_ms,
                        guard:         LeaseGuard::new(new_lease.epoch, new_bytes),
                    })
                } else {
                    // Lost the install race: someone else got in first.
                    self.follower_after_lost_race().await
                }
            }
            Some((current, old_bytes)) => {
                let held_by_us = current.holder == self.node_id;
                let expired = current.expires_at_ms <= now_ms;
                if held_by_us || expired {
                    let epoch = if held_by_us {
                        current.epoch.max(1)
                    } else if current.epoch == 0 {
                        1
                    } else {
                        current
                            .epoch
                            .checked_add(1)
                            .ok_or(ElectionError::EpochExhausted)?
                    };
                    let new_lease = self.our_lease(now_ms, epoch);
                    let new_bytes = serde_json::to_vec(&new_lease).context(EncodeSnafu)?;
                    // Renew (ours) or steal (expired other): CAS from the
                    // exact observed bytes so a concurrent writer can't be
                    // clobbered.
                    if self
                        .meta
                        .cas(LEASE_KEY, Some(&old_bytes), &new_bytes)
                        .await
                        .context(StoreSnafu)?
                    {
                        Ok(LeaseStatus::Leader {
                            expires_at_ms: new_lease.expires_at_ms,
                            guard:         LeaseGuard::new(new_lease.epoch, new_bytes),
                        })
                    } else {
                        self.follower_after_lost_race().await
                    }
                } else {
                    // Held by another node and still valid.
                    Ok(LeaseStatus::Follower {
                        current_holder: current.holder,
                    })
                }
            }
        }
    }

    /// Resolve the current holder after we lost a CAS race. Re-reads the lease
    /// and reports whoever now holds it; a lease that vanished under us is
    /// reported as a follower of the empty holder so the caller retries.
    async fn follower_after_lost_race(&self) -> Result<LeaseStatus> {
        let current_holder = self
            .read_lease()
            .await?
            .map_or_else(String::new, |(lease, _)| lease.holder);
        Ok(LeaseStatus::Follower { current_holder })
    }

    /// Campaign using the wall clock.
    pub async fn campaign(&self) -> Result<LeaseStatus> { self.campaign_at(now_ms()).await }

    /// The address of the node currently holding a valid lease, if any.
    ///
    /// Returns `Some(holder)` when a lease exists and has not expired
    /// (`expires_at_ms > now_ms`), otherwise `None`. Followers use this to
    /// locate the leader to forward writes to.
    pub async fn current_leader(&self) -> Result<Option<String>> {
        self.current_leader_at(now_ms()).await
    }

    /// [`current_leader`](Self::current_leader) against the injected clock
    /// `now_ms`.
    async fn current_leader_at(&self, now_ms: u64) -> Result<Option<String>> {
        Ok(self
            .read_lease()
            .await?
            .and_then(|(lease, _)| (lease.expires_at_ms > now_ms).then_some(lease.holder)))
    }

    /// Resign at the injected clock `now_ms`: if this node holds the lease,
    /// CAS it to an already-expired record so a standby steals it on its next
    /// campaign instead of waiting out the full TTL. Returns whether we held
    /// and released the lease.
    pub async fn resign_at(&self, now_ms: u64) -> Result<bool> {
        let Some((current, old_bytes)) = self.read_lease().await? else {
            return Ok(false);
        };
        if current.holder != self.node_id {
            return Ok(false);
        }
        let released = LeaseValue {
            holder:        self.node_id.clone(),
            expires_at_ms: now_ms,
            epoch:         current.epoch,
        };
        let new_bytes = serde_json::to_vec(&released).context(EncodeSnafu)?;
        self.meta
            .cas(LEASE_KEY, Some(&old_bytes), &new_bytes)
            .await
            .context(StoreSnafu)
    }

    /// Resign using the wall clock.
    pub async fn resign(&self) -> Result<bool> { self.resign_at(now_ms()).await }
}

/// Current wall clock in milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_millis()
        .try_into()
        .expect("milliseconds since the Unix epoch exceed u64")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lake_meta::{MetaStoreRef, RocksMeta};
    use tempfile::TempDir;

    use super::{Duration, LEASE_KEY, LeaseElection, LeaseStatus, LeaseValue};

    const TTL_MS: u64 = 10_000;

    fn shared_store() -> (TempDir, MetaStoreRef) {
        let dir = tempfile::tempdir().expect("create tempdir");
        let store = RocksMeta::open(dir.path()).expect("open RocksMeta");
        (dir, Arc::new(store) as MetaStoreRef)
    }

    fn election(meta: &MetaStoreRef, node_id: &str) -> LeaseElection {
        LeaseElection::new(Arc::clone(meta), node_id, Duration::from_millis(TTL_MS))
    }

    fn assert_leader(status: LeaseStatus, expires_at_ms: u64, epoch: u64) {
        let LeaseStatus::Leader {
            expires_at_ms: actual_expiry,
            guard,
        } = status
        else {
            panic!("expected leader status");
        };
        assert_eq!(actual_expiry, expires_at_ms);
        assert_eq!(guard.epoch(), epoch);
    }

    #[tokio::test]
    async fn lease_lifecycle_over_injected_clock() {
        let (_dir, meta) = shared_store();
        let a = election(&meta, "a");
        let b = election(&meta, "b");

        // At now=0, a wins the empty install and b sees a valid holder.
        assert_leader(a.campaign_at(0).await.expect("a campaigns"), TTL_MS, 1);
        assert!(a.campaign_at(0).await.expect("a status").is_leader());
        assert_eq!(
            b.campaign_at(0).await.expect("b campaigns"),
            LeaseStatus::Follower {
                current_holder: "a".to_string(),
            }
        );

        // a renews in place; expiry advances with the clock.
        assert_leader(
            a.campaign_at(1000).await.expect("a renews"),
            1000 + TTL_MS,
            1,
        );

        // Before expiry (a's lease runs to 11_000), b stays a follower.
        assert_eq!(
            b.campaign_at(5000).await.expect("b before expiry"),
            LeaseStatus::Follower {
                current_holder: "a".to_string(),
            }
        );

        // After expiry, b steals the lease; a then becomes a follower of b.
        assert_leader(
            b.campaign_at(20_000).await.expect("b steals"),
            20_000 + TTL_MS,
            2,
        );
        assert_eq!(
            a.campaign_at(20_001).await.expect("a demoted"),
            LeaseStatus::Follower {
                current_holder: "b".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn lease_epoch_advances_only_on_takeover() {
        let (_dir, meta) = shared_store();
        let a = election(&meta, "a");
        let b = election(&meta, "b");

        let legacy = br#"{"holder":"a","expires_at_ms":10000}"#;
        assert!(
            meta.cas(LEASE_KEY, None, legacy)
                .await
                .expect("seed legacy")
        );
        assert_leader(
            a.campaign_at(100).await.expect("upgrade legacy renewal"),
            100 + TTL_MS,
            1,
        );
        assert_leader(
            a.campaign_at(200).await.expect("renew epoch 1"),
            200 + TTL_MS,
            1,
        );
        assert_leader(
            b.campaign_at(20_000).await.expect("take over"),
            20_000 + TTL_MS,
            2,
        );

        let current_bytes = meta.get(LEASE_KEY).await.expect("read lease").unwrap();
        let exhausted = LeaseValue {
            holder:        "b".to_owned(),
            expires_at_ms: 20_000,
            epoch:         u64::MAX,
        };
        let exhausted_bytes = serde_json::to_vec(&exhausted).unwrap();
        assert!(
            meta.cas(LEASE_KEY, Some(&current_bytes), &exhausted_bytes)
                .await
                .expect("install exhausted lease")
        );
        let error = a
            .campaign_at(20_001)
            .await
            .expect_err("epoch exhaustion must fail closed");
        assert!(matches!(error, super::ElectionError::EpochExhausted));
        assert_eq!(
            meta.get(LEASE_KEY)
                .await
                .expect("read unchanged")
                .as_deref(),
            Some(exhausted_bytes.as_slice())
        );
    }

    #[tokio::test]
    async fn campaign_returns_exact_installed_lease_guard() {
        let (_dir, meta) = shared_store();
        let a = election(&meta, "a");

        let status = a.campaign_at(123).await.expect("acquire lease");
        let LeaseStatus::Leader { guard, .. } = status else {
            panic!("first campaign must lead");
        };
        assert_eq!(guard.epoch(), 1);
        assert_eq!(
            meta.get(LEASE_KEY).await.expect("read lease").as_deref(),
            Some(guard.bytes())
        );
    }

    #[tokio::test]
    async fn current_leader_reports_holder_until_expiry() {
        let (_dir, meta) = shared_store();
        let a = election(&meta, "a");

        // No lease installed yet: nobody leads.
        assert_eq!(a.current_leader_at(0).await.expect("read empty"), None);

        // a wins the lease at now=0; it runs until TTL_MS.
        assert!(a.campaign_at(0).await.expect("a leads").is_leader());
        assert_eq!(
            a.current_leader_at(1000).await.expect("fresh lease"),
            Some("a".to_string())
        );

        // At and past expiry the lease no longer names a live leader.
        assert_eq!(a.current_leader_at(TTL_MS).await.expect("at expiry"), None);
        assert_eq!(
            a.current_leader_at(TTL_MS + 1).await.expect("past expiry"),
            None
        );
    }

    #[tokio::test]
    async fn resign_lets_standby_take_over_immediately() {
        let (_dir, meta) = shared_store();
        let a = election(&meta, "a");
        let b = election(&meta, "b");

        assert!(a.campaign_at(0).await.expect("a leads").is_leader());
        // A non-holder resigning is a no-op.
        assert!(!b.resign_at(100).await.expect("b resign no-op"));
        // The holder resigns, releasing the lease well before its TTL.
        assert!(a.resign_at(200).await.expect("a resigns"));
        // b now steals it immediately, without waiting out the TTL.
        assert_leader(
            b.campaign_at(300).await.expect("b takes over"),
            300 + TTL_MS,
            2,
        );
    }
}
