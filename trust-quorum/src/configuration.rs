// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A configuration of a trust quroum

use crate::crypto::{
    EncryptedRackSecret, EncryptedShares, KeyShareGf256, RackSecret, Salt,
    ShareDigestGf256,
};
use crate::{Epoch, Error, PlatformId, RackId, ReconfigureMsg, Threshold};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The configuration for a given epoch.
///
/// Only valid for non-lrtq configurations
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

impl Configuration {
    /// Create a new configuration for the trust quorum
    ///
    /// `previous_configuration` is never filled in upon construction. A
    /// coordinator will fill this in as necessary after retrieving shares for
    /// the last committed epoch.
    pub fn new(
        coordinator: PlatformId,
        reconfigure_msg: &ReconfigureMsg,
    ) -> Result<(Configuration, BTreeMap<PlatformId, KeyShareGf256>), Error>
    {
        let rack_secret = RackSecret::new();
        let shares = rack_secret
            .split(reconfigure_msg.threshold, reconfigure_msg.members.len())?;

        let share_digests = shares.iter().map(|s| s.digest());
        let members = reconfigure_msg
            .members
            .iter()
            .cloned()
            .zip(share_digests)
            .collect();

        let shares_by_member: BTreeMap<PlatformId, KeyShareGf256> =
            reconfigure_msg
                .members
                .iter()
                .cloned()
                .zip(shares.into_iter())
                .collect();

        let rack_id = reconfigure_msg.rack_id;
        let encrypted_shares =
            EncryptedShares::new(&rack_id, &rack_secret, &shares_by_member)?;

        Ok((
            Configuration {
                rack_id,
                epoch: reconfigure_msg.epoch,
                coordinator,
                members,
                threshold: reconfigure_msg.threshold,
                encrypted_shares,
                previous_configuration: None,
            },
            shares_by_member,
        ))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct PreviousConfiguration {
    /// The epoch of the last committed configuration
    pub epoch: Epoch,

    /// Is the previous configuration LRTQ?
    pub is_lrtq: bool,

    /// The encrypted rack secret for the last committed epoch
    pub encrypted_last_committed_rack_secret: EncryptedRackSecret,

    /// A random value used to derive the key to encrypt the rack secret from
    /// the last committed epoch.
    ///
    /// We only encrypt the rack secret once and so we use a nonce of all zeros
    pub encrypted_last_committed_rack_secret_salt: Salt,
}
