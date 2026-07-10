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

//! Background leadership: the campaign loop and the shared lease deadline.
//!
//! The control plane's write path gates on leadership. Rather than campaign
//! inline on every write, a single background task ([`run_campaign_loop`])
//! renews the lease on a fixed interval and publishes its local monotonic
//! deadline for the Flight service to read via [`Leadership::is_leader`].
//! Reads never consult the deadline; only writes do (see
//! `docs/architecture.md`).

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::Instant;

use crate::election::{LeaseElection, LeaseStatus};

/// How often the campaign loop renews the lease. Half the 10s TTL used by
/// [`serve`](crate::serve), so a renew is attempted well before expiry.
const RENEW_INTERVAL: Duration = Duration::from_secs(5);

/// The shared leadership state read by the control-plane service.
///
/// Holds both whether *this* node currently leads (the fast-path write gate)
/// and the address of whoever the last campaign round observed leading, so a
/// follower can forward writes to the current leader. [`run_campaign_loop`]
/// writes both; the Flight service reads them.
pub(crate) struct Leadership {
    state: Mutex<LeadershipState>,
}

struct LeadershipState {
    /// Address of the observed lease holder, or `None` when no leader is known.
    leader:         Option<String>,
    /// Local monotonic deadline for a lease held by this node.
    local_deadline: Option<Instant>,
}

impl Leadership {
    /// A fresh leadership state: not leading, no known leader yet.
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(LeadershipState {
                leader:         None,
                local_deadline: None,
            }),
        }
    }

    /// Whether this node currently holds an unexpired local lease.
    pub(crate) fn is_leader(&self) -> bool {
        self.state
            .lock()
            .expect("leadership mutex poisoned")
            .local_deadline
            .is_some_and(|deadline| Instant::now() < deadline)
    }

    /// The address of the currently observed leader, if any.
    pub(crate) fn leader(&self) -> Option<String> {
        self.state
            .lock()
            .expect("leadership mutex poisoned")
            .leader
            .clone()
    }

    /// Atomically publish the observed leader and our local lease deadline.
    fn publish(&self, leader: Option<String>, local_deadline: Option<Instant>) -> bool {
        let mut state = self.state.lock().expect("leadership mutex poisoned");
        let was_leader = state
            .local_deadline
            .is_some_and(|deadline| Instant::now() < deadline);
        state.leader = leader;
        state.local_deadline = local_deadline;
        was_leader
    }
}

/// Drive `election` forever, publishing each round's outcome into `leadership`.
///
/// Runs one [`campaign`](LeaseElection::campaign) per [`RENEW_INTERVAL`],
/// storing whether we hold the lease and the address of the observed leader. A
/// campaign error is logged and treated as "not leader, leader unknown" for
/// that round rather than crashing the loop, so a transient store hiccup
/// demotes us to standby instead of taking the process down. Leadership
/// transitions (acquire / lose) are logged via `tracing`.
pub(crate) async fn run_campaign_loop(election: LeaseElection, leadership: Arc<Leadership>) {
    loop {
        // Bound local authority from the start of the store round-trip. This
        // is conservative when the store is slow and immune to wall-clock
        // jumps after the lease is written.
        let campaign_started = Instant::now();
        let (leader, local_deadline) = match election.campaign().await {
            Ok(LeaseStatus::Leader { .. }) => (
                Some(election.node_id().to_string()),
                Some(campaign_started + election.ttl()),
            ),
            Ok(LeaseStatus::Follower { current_holder }) => {
                // An empty holder means the lease vanished under a lost race;
                // report "no known leader" so writes fail fast rather than
                // forwarding to nowhere.
                let leader = (!current_holder.is_empty()).then_some(current_holder);
                (leader, None)
            }
            Err(err) => {
                tracing::warn!(
                    node_id = election.node_id(),
                    error = %err,
                    "leadership campaign failed; treating as not leader this round"
                );
                (None, None)
            }
        };

        let now_leader = local_deadline.is_some_and(|deadline| Instant::now() < deadline);
        let was_leader = leadership.publish(leader, local_deadline);
        if now_leader != was_leader {
            if now_leader {
                tracing::info!(node_id = election.node_id(), "acquired metasrv leadership");
            } else {
                tracing::warn!(node_id = election.node_id(), "lost metasrv leadership");
            }
        }

        tokio::time::sleep(RENEW_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use lake_meta::{MetaStoreRef, RocksMeta};

    use super::*;

    #[tokio::test]
    async fn write_gate_expires_without_a_completed_renewal() {
        let dir = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(dir.path()).unwrap());
        let election = LeaseElection::new(meta, "node-a", Duration::from_millis(20));
        let leadership = Arc::new(Leadership::new());
        let campaign = tokio::spawn(run_campaign_loop(election, leadership.clone()));

        tokio::time::timeout(Duration::from_secs(1), async {
            while !leadership.is_leader() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("initial campaign acquires lease");

        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            !leadership.is_leader(),
            "a completed campaign must not authorize writes past its lease deadline"
        );
        campaign.abort();
    }
}
