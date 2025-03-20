// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A configuration of a trust quroum

use crate::crypto::{EncryptedRackSecret, EncryptedShares, ShareDigestGf256};
use crate::{Epoch, PlatformId, RackId, Threshold};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The configuration for a given epoch
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Configuration {
    /// Unique Id of the rack
    pub rack_id: RackId,

    // Unique, monotonically increasing identifier for a configuration
    pub epoch: Epoch,

    /// Who was the coordinator of this reconfiguration?
    pub coordinator: PlatformId,

    // All members of the current configuration and the hash of their key shares
    pub members: BTreeMap<PlatformId, ShareDigestGf256>,

    /// The number of sleds required to reconstruct the rack secret
    pub threshold: Threshold,

    /// Encrypted key shares for this configuration. This is used to generate
    /// `Prepare` messages to send to members of this configuration in case they
    /// were offline during initial distribution.
    pub encrypted_shares: EncryptedShares,

    // There is no previous configuration for the initial configuration
    pub previous_configuration: Option<PreviousConfiguration>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct PreviousConfiguration {
    /// The epoch of the last committed configuration
    pub epoch: Epoch,

    /// Is this configuration an LRTQ configuration?
    pub is_lrtq: bool,

    /// The encrypted rack secret for the last committed epoch
    pub encrypted_last_committed_rack_secret: EncryptedRackSecret,

    /// A random value used to derive the key to encrypt the rack secret from
    /// the last committed epoch.
    ///
    /// We only encrypt the rack secret once and so we use a nonce of all zeros
    pub encrypted_last_committed_rack_secret_salt: [u8; 32],
}
