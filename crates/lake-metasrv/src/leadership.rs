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
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::election::LeaseElection;

/// How often the campaign loop renews the lease. Half the 10s TTL used by
/// [`serve`](crate::serve), so a renew is attempted well before expiry.
const RENEW_INTERVAL: Duration = Duration::from_secs(5);

/// The shared leadership flag read by the control-plane service.
///
/// Cheap to clone-share: it wraps an [`Arc<AtomicBool>`] that
/// [`run_campaign_loop`] writes and the Flight service reads.
pub(crate) struct Leadership {
    /// Set to `true` while this node holds the lease, `false` otherwise.
    pub(crate) is_leader: Arc<AtomicBool>,
}

impl Leadership {
    /// Whether this node currently holds leadership.
    pub(crate) fn is_leader(&self) -> bool { self.is_leader.load(Ordering::Relaxed) }
}

/// Drive `election` forever, publishing each round's outcome into `is_leader`.
///
/// Runs one [`campaign`](LeaseElection::campaign) per [`RENEW_INTERVAL`],
/// storing whether we hold the lease. A campaign error is logged and treated
/// as "not leader" for that round rather than crashing the loop, so a
/// transient store hiccup demotes us to standby instead of taking the process
/// down. Leadership transitions (acquire / lose) are logged via `tracing`.
pub(crate) async fn run_campaign_loop(election: LeaseElection, is_leader: Arc<AtomicBool>) {
    loop {
        let now_leader = match election.campaign().await {
            Ok(status) => status.is_leader(),
            Err(err) => {
                tracing::warn!(
                    node_id = election.node_id(),
                    error = %err,
                    "leadership campaign failed; treating as not leader this round"
                );
                false
            }
        };

        let was_leader = is_leader.swap(now_leader, Ordering::Relaxed);
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
