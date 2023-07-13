// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Storage for the v0 bootstore scheme
//!
//! We write two pieces of data to M.2 devices in production via
//! [`omicron_common::Ledger`]:
//!
//!    1. [`super::Fsm::State`] for bootstore state itself
//!    2. A network config blob required for pre-rack-unlock configuration
//!

use super::{Fsm, FsmConfig, State};
use camino::Utf8PathBuf;
use omicron_common::ledger::{Ledger, Ledgerable};
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use slog::{info, Logger};

/// A persistent version of `Fsm::State`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentFsmState {
    pub generation: u64,
    pub state: State,
}

impl Ledgerable for PersistentFsmState {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.generation >= other.generation
    }

    fn generation_bump(&mut self) {
        self.generation += 1;
    }
}

impl PersistentFsmState {
    /// Save the persistent state to a ledger and return the new generation
    /// number.
    ///
    /// Panics if the ledger cannot be saved.
    pub async fn save(
        log: &Logger,
        paths: Vec<Utf8PathBuf>,
        generation: u64,
        state: State,
    ) -> u64 {
        let persistent_state = PersistentFsmState { generation, state };
        let mut ledger = Ledger::new_with(log, paths, persistent_state);
        ledger
            .commit()
            .await
            .expect("Critical: Failed to save bootstore ledger.");
        ledger.data().generation
    }
    /// If the Ledger that stores the Fsm::State exists, then initialize the Fsm
    /// in the saved state, otherwise start out in `State::Uninitialized`.
    ///
    /// Return a pair of `Fsm` and Ledger generation number
    pub async fn load(
        log: &Logger,
        paths: Vec<Utf8PathBuf>,
        node_id: Baseboard,
        config: FsmConfig,
    ) -> (Fsm, u64) {
        if let Some(ledger) =
            Ledger::<PersistentFsmState>::new(&log, paths).await
        {
            let persistent_state = ledger.into_inner();
            info!(
                log,
                "Loading Fsm::State from ledger in state {} with generation {}",
                persistent_state.state.name(),
                persistent_state.generation
            );
            (
                Fsm::new(node_id, config, persistent_state.state),
                persistent_state.generation,
            )
        } else {
            info!(log, "No ledger found. Loading Fsm::State as Uninitialized");
            (Fsm::new_uninitialized(node_id, config), 0)
        }
    }
}

/// Network configuration required before before rack unlock
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub generation: u64,
    // A serialized blob of configuration data. Network APIs must know how to
    // appropriately serialize/deserialize
    pub blob: Vec<u8>,
}

impl Ledgerable for NetworkConfig {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.generation >= other.generation
    }

    fn generation_bump(&mut self) {
        self.generation += 1;
    }
}
