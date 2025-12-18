// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Persistent storage for the trust quorum task
//!
//! We write two pieces of data to M.2 devices in production via
//! [`omicron_common::ledger::Ledger`]:
//!
//!    1. [`trust_quorum_protocol::PersistentState`] for trust quorum state
//!    2. A network config blob required for pre-rack-unlock configuration

use camino::Utf8PathBuf;
use omicron_common::ledger::{Ledger, Ledgerable};
use serde::{Deserialize, Serialize};
use slog::{Logger, info};
use trust_quorum_protocol::PersistentState;

/// A wrapper type around [`PersistentState`] for use as a [`Ledger`]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentStateLedger {
    pub generation: u64,
    pub state: PersistentState,
}

impl Ledgerable for PersistentStateLedger {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.generation > other.generation
    }

    fn generation_bump(&mut self) {
        self.generation += 1;
    }
}

impl PersistentStateLedger {
    /// Save the persistent state to a ledger and return the new generation
    /// number.
    ///
    /// Panics if the ledger cannot be saved.
    ///
    ///   The trust quorum protocol relies on persisting state to disk, such
    ///   as whether a node has prepared or committed a configuration, before
    ///   responding to a coordinator node or Nexus. This is necessary in order
    ///   to ensure that enough nodes actually have performed an operation and
    ///   not have the overall state of the protocol go backward in the case of
    ///   a crash and restart of a node. In this manner, trust quorum is similar
    ///   to consensus protocols like Raft and Paxos.
    ///
    ///   If for any reason we cannot persist trust quorum state to the ledger,
    ///   we must panic to ensure that the node does not take any further
    ///   action incorrectly, like acknowledging a `Prepare` to a coordinator.
    ///   Panicking is the simplest mechanism to ensure that a given node will
    ///   not violate the invariants of the trust quorum protocol in the case
    ///   of internal disk failures. It also ensures a very obvious failure that
    ///   will allow support to get involved and replace internal disks.
    pub async fn save(
        log: &Logger,
        paths: Vec<Utf8PathBuf>,
        generation: u64,
        state: PersistentState,
    ) -> u64 {
        let persistent_state = PersistentStateLedger { generation, state };
        let mut ledger = Ledger::new_with(log, paths, persistent_state);
        ledger
            .commit()
            .await
            .expect("Critical: Failed to save ledger for persistent state");
        ledger.data().generation
    }

    /// Return Some(`PersistentStateLedger`) if it exists on disk, otherwise
    /// return `None`.
    pub async fn load(
        log: &Logger,
        paths: Vec<Utf8PathBuf>,
    ) -> Option<PersistentStateLedger> {
        let Some(ledger) =
            Ledger::<PersistentStateLedger>::new(&log, paths).await
        else {
            return None;
        };
        let persistent_state = ledger.into_inner();
        info!(
            log,
            "Loaded persistent state from ledger with generation {}",
            persistent_state.generation
        );
        Some(persistent_state)
    }
}
