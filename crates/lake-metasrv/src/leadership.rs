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

//! Background leadership: the campaign loop and the shared leader flag.
//!
//! The control plane's write path gates on leadership. Rather than campaign
//! inline on every write, a single background task ([`run_campaign_loop`])
//! renews the lease on a fixed interval and publishes the outcome through an
//! [`AtomicBool`] that the Flight service reads via [`Leadership::is_leader`].
//! Reads never consult the flag; only writes do (see `docs/architecture.md`).

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

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
    /// Set to `true` while this node holds the lease, `false` otherwise.
    is_leader: Arc<AtomicBool>,
    /// Address of the observed lease holder, or `None` when no leader is known.
    leader:    Mutex<Option<String>>,
}

impl Leadership {
    /// A fresh leadership state: not leading, no known leader yet.
    pub(crate) fn new() -> Self {
        Self {
            is_leader: Arc::new(AtomicBool::new(false)),
            leader:    Mutex::new(None),
        }
    }

    /// Whether this node currently holds leadership.
    pub(crate) fn is_leader(&self) -> bool { self.is_leader.load(Ordering::Relaxed) }

    /// The address of the currently observed leader, if any.
    pub(crate) fn leader(&self) -> Option<String> {
        self.leader
            .lock()
            .expect("leadership mutex poisoned")
            .clone()
    }

    /// The shared leader flag, for background tasks (e.g. the maintenance
    /// sweep) that gate on leadership without touching the Flight service.
    pub(crate) fn is_leader_flag(&self) -> Arc<AtomicBool> { self.is_leader.clone() }

    /// Publish the address of the observed leader.
    fn set_leader(&self, addr: Option<String>) {
        *self.leader.lock().expect("leadership mutex poisoned") = addr;
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
        let (now_leader, leader) = match election.campaign().await {
            Ok(LeaseStatus::Leader { .. }) => (true, Some(election.node_id().to_string())),
            Ok(LeaseStatus::Follower { current_holder }) => {
                // An empty holder means the lease vanished under a lost race;
                // report "no known leader" so writes fail fast rather than
                // forwarding to nowhere.
                let leader = (!current_holder.is_empty()).then_some(current_holder);
                (false, leader)
            }
            Err(err) => {
                tracing::warn!(
                    node_id = election.node_id(),
                    error = %err,
                    "leadership campaign failed; treating as not leader this round"
                );
                (false, None)
            }
        };

        leadership.set_leader(leader);
        let was_leader = leadership.is_leader.swap(now_leader, Ordering::Relaxed);
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
