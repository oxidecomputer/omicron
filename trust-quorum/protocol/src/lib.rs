// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the oxide rack trust quorum protocol
//!
//! This protocol is written as a
//! [no-IO](https://sans-io.readthedocs.io/how-to-sans-io.html) implementation.
//! All persistent state and all networking is managed outside of this
//! implementation.

use daft::Diffable;
use gfss::shamir::Share;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;
use slog::{Logger, error, warn};
use trust_quorum_types::configuration::Configuration;
use trust_quorum_types::crypto::Sha3_256Digest;
use trust_quorum_types::types::Epoch;

mod compute_key_share;
mod configuration;
mod coordinator_state;
pub(crate) mod crypto;
mod messages;
mod node;
mod node_ctx;
mod persistent_state;
#[allow(unused)]
mod rack_secret_loader;
mod validators;

pub use coordinator_state::{
    CoordinatingMsg, CoordinatorOperation, CoordinatorState,
    CoordinatorStateDiff,
};
pub use rack_secret_loader::{LoadRackSecretError, RackSecretLoaderDiff};
pub use validators::{
    LrtqUpgradeError, ReconfigurationError, ValidatedLrtqUpgradeMsgDiff,
    ValidatedReconfigureMsgDiff,
};

// These crypto types and functions are NOT in trust-quorum-types because they
// contain sensitive data or have complex implementations tied to this crate.
#[cfg(feature = "testing")]
pub use configuration::configurations_equal_except_for_crypto_data;
pub use configuration::new_configuration;
pub use crypto::{
    PlaintextRackSecrets, RackSecret, ReconstructedRackSecret, SECRET_LEN,
    decrypt_rack_secrets,
};
pub use messages::{PeerMsg, PeerMsgKind};
pub use node::{CommitError, Node, NodeDiff, PrepareAndCommitError};
// public only for docs.
pub use node_ctx::NodeHandlerCtx;
pub use node_ctx::{NodeCallerCtx, NodeCommonCtx, NodeCtx, NodeCtxDiff};
pub use persistent_state::PersistentState;

/// A container to make messages between trust quorum nodes routable
#[derive(Debug, Clone, Serialize, Deserialize, Diffable)]
#[cfg_attr(feature = "danger_partial_eq_ct_wrapper", derive(PartialEq, Eq))]
#[daft(leaf)]
pub struct Envelope {
    pub to: BaseboardId,
    pub from: BaseboardId,
    pub msg: PeerMsg,
}

#[cfg(feature = "testing")]
impl Envelope {
    pub fn equal_except_for_crypto_data(&self, other: &Self) -> bool {
        self.to == other.to
            && self.from == other.from
            && self.msg.equal_except_for_crypto_data(&other.msg)
    }
}

/// Check if a received share is valid for a given configuration
///
/// Return true if valid, false otherwise.
pub fn validate_share(
    log: &Logger,
    config: &Configuration,
    from: &BaseboardId,
    epoch: Epoch,
    share: &Share,
) -> bool {
    // Are we trying to retrieve shares for `epoch`?
    if epoch != config.epoch {
        warn!(
            log,
            "Received Share from node with wrong epoch";
            "received_epoch" => %epoch,
            "from" => %from
        );
        return false;
    }

    // Is the sender a member of the configuration `epoch`?
    // Was the sender a member of the configuration at `old_epoch`?
    let Some(expected_digest) = config.members.get(&from) else {
        warn!(
            log,
            "Received Share from unexpected node";
            "epoch" => %epoch,
            "from" => %from
        );
        return false;
    };

    // Does the share hash match what we expect?
    let mut digest = Sha3_256Digest::default();
    share.digest::<sha3::Sha3_256>(&mut digest.0);
    if digest != *expected_digest {
        error!(
            log,
            "Received share with invalid digest";
            "epoch" => %epoch,
            "from" => %from
        );
        return false;
    }

    true
}
