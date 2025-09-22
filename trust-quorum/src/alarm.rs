// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanism for reporting protocol invariant violations

use serde::{Deserialize, Serialize};

use crate::{
    Configuration, Epoch,
    crypto::{DecryptionError, RackSecretReconstructError},
};

#[allow(clippy::large_enum_variant)]
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum Alarm {
    /// Different configurations found for the same epoch
    ///
    /// Reason: Nexus creates configurations and stores them in CRDB before
    /// sending them to a coordinator of its choosing. Nexus will not send the
    /// same reconfiguration request to different coordinators. If it does those
    /// coordinators will generate different key shares. However, since Nexus
    /// will not tell different nodes to coordinate the same configuration, this
    /// state should be impossible to reach.
    MismatchedConfigurations {
        config1: Configuration,
        config2: Configuration,
        // Either a stringified `PlatformId` or "Nexus"
        from: String,
    },

    /// The `keyShareComputer` could not compute this node's share
    ///
    /// Reason: A threshold of valid key shares were received based on the the
    /// share digests in the Configuration. However, computation of the share
    /// still failed. This should be impossible.
    ShareComputationFailed { epoch: Epoch, err: gfss::shamir::CombineError },

    /// We started collecting shares for a committed configuration,
    /// but we no longer have that configuration in our persistent state.
    CommittedConfigurationLost {
        latest_committed_epoch: Epoch,
        collecting_epoch: Epoch,
    },

    /// Decrypting the encrypted rack secrets failed when presented with a
    /// `valid` RackSecret.
    ///
    /// `Configuration` membership contains the hashes of each valid share. All
    /// shares utilized to reconstruct the rack secret were validated against
    /// these hashes, and the rack seceret was reconstructed. However, using
    /// the rack secret to derive encryption keys and decrypt the secrets from
    /// old configurations still failed. This should never be possible, and
    /// therefore we raise an alarm.
    RackSecretDecryptionFailed { epoch: Epoch, err: DecryptionError },

    /// Reconstructing the rack secret failed when presented with `valid` shares.
    ///
    /// `Configuration` membership contains the hashes of each valid share. All
    /// shares utilized to reconstruct the rack secret were validated against
    /// these hashes, and yet, the reconstruction still failed. This indicates
    /// either a bit flip in a share after validation, or, more likely, an
    /// invalid hash.
    RackSecretReconstructionFailed {
        epoch: Epoch,
        err: RackSecretReconstructError,
    },
}
