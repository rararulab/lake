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
//! self-built consensus: it rides entirely on the [`MetaStore`] CAS
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
}

/// The outcome of a single campaign round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseStatus {
    /// This node holds the lease until `expires_at_ms`.
    Leader {
        /// Absolute expiry of the lease this node now holds.
        expires_at_ms: u64,
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

    /// Read and decode the current lease, if any.
    async fn read_lease(&self) -> Result<Option<LeaseValue>> {
        match self.meta.get(LEASE_KEY).await.context(StoreSnafu)? {
            Some(bytes) => {
                let value = serde_json::from_slice(&bytes).context(DecodeSnafu)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Build the lease this node would install at `now_ms`.
    fn our_lease(&self, now_ms: u64) -> LeaseValue {
        LeaseValue {
            holder:        self.node_id.clone(),
            expires_at_ms: now_ms.saturating_add(self.ttl_ms()),
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
        let new_lease = self.our_lease(now_ms);
        let new_bytes = serde_json::to_vec(&new_lease).context(EncodeSnafu)?;

        match self.read_lease().await? {
            // No lease yet: try to install ours from the empty state.
            None => {
                if self
                    .meta
                    .cas(LEASE_KEY, None, &new_bytes)
                    .await
                    .context(StoreSnafu)?
                {
                    Ok(LeaseStatus::Leader {
                        expires_at_ms: new_lease.expires_at_ms,
                    })
                } else {
                    // Lost the install race: someone else got in first.
                    self.follower_after_lost_race().await
                }
            }
            Some(current) => {
                let held_by_us = current.holder == self.node_id;
                let expired = current.expires_at_ms <= now_ms;
                if held_by_us || expired {
                    // Renew (ours) or steal (expired other): CAS from the
                    // exact observed bytes so a concurrent writer can't be
                    // clobbered.
                    let old_bytes = serde_json::to_vec(&current).context(EncodeSnafu)?;
                    if self
                        .meta
                        .cas(LEASE_KEY, Some(&old_bytes), &new_bytes)
                        .await
                        .context(StoreSnafu)?
                    {
                        Ok(LeaseStatus::Leader {
                            expires_at_ms: new_lease.expires_at_ms,
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
            .map_or_else(String::new, |lease| lease.holder);
        Ok(LeaseStatus::Follower { current_holder })
    }

    /// Campaign using the wall clock.
    pub async fn campaign(&self) -> Result<LeaseStatus> { self.campaign_at(now_ms()).await }

    /// Resign at the injected clock `now_ms`: if this node holds the lease,
    /// CAS it to an already-expired record so a standby steals it on its next
    /// campaign instead of waiting out the full TTL. Returns whether we held
    /// and released the lease.
    pub async fn resign_at(&self, now_ms: u64) -> Result<bool> {
        let Some(current) = self.read_lease().await? else {
            return Ok(false);
        };
        if current.holder != self.node_id {
            return Ok(false);
        }
        let released = LeaseValue {
            holder:        self.node_id.clone(),
            expires_at_ms: now_ms,
        };
        let old_bytes = serde_json::to_vec(&current).context(EncodeSnafu)?;
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

    use super::{Duration, LeaseElection, LeaseStatus};

    const TTL_MS: u64 = 10_000;

    fn shared_store() -> (TempDir, MetaStoreRef) {
        let dir = tempfile::tempdir().expect("create tempdir");
        let store = RocksMeta::open(dir.path()).expect("open RocksMeta");
        (dir, Arc::new(store) as MetaStoreRef)
    }

    fn election(meta: &MetaStoreRef, node_id: &str) -> LeaseElection {
        LeaseElection::new(Arc::clone(meta), node_id, Duration::from_millis(TTL_MS))
    }

    #[tokio::test]
    async fn lease_lifecycle_over_injected_clock() {
        let (_dir, meta) = shared_store();
        let a = election(&meta, "a");
        let b = election(&meta, "b");

        // At now=0, a wins the empty install and b sees a valid holder.
        assert_eq!(
            a.campaign_at(0).await.expect("a campaigns"),
            LeaseStatus::Leader {
                expires_at_ms: TTL_MS,
            }
        );
        assert!(a.campaign_at(0).await.expect("a status").is_leader());
        assert_eq!(
            b.campaign_at(0).await.expect("b campaigns"),
            LeaseStatus::Follower {
                current_holder: "a".to_string(),
            }
        );

        // a renews in place; expiry advances with the clock.
        assert_eq!(
            a.campaign_at(1000).await.expect("a renews"),
            LeaseStatus::Leader {
                expires_at_ms: 1000 + TTL_MS,
            }
        );

        // Before expiry (a's lease runs to 11_000), b stays a follower.
        assert_eq!(
            b.campaign_at(5000).await.expect("b before expiry"),
            LeaseStatus::Follower {
                current_holder: "a".to_string(),
            }
        );

        // After expiry, b steals the lease; a then becomes a follower of b.
        assert_eq!(
            b.campaign_at(20_000).await.expect("b steals"),
            LeaseStatus::Leader {
                expires_at_ms: 20_000 + TTL_MS,
            }
        );
        assert_eq!(
            a.campaign_at(20_001).await.expect("a demoted"),
            LeaseStatus::Follower {
                current_holder: "b".to_string(),
            }
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
        assert_eq!(
            b.campaign_at(300).await.expect("b takes over"),
            LeaseStatus::Leader {
                expires_at_ms: 300 + TTL_MS,
            }
        );
    }
}
