// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A configuration of a trust quroum at a given epoch

use crate::crypto::{EncryptedRackSecret, RackSecret, Salt, Sha3_256Digest};
use crate::validators::ValidatedReconfigureMsg;
use crate::{Epoch, PlatformId, Threshold};
use gfss::shamir::{Share, SplitError};
use iddqd::{IdOrdItem, id_upcast};
use omicron_uuid_kinds::RackUuid;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use slog_error_chain::SlogInlineError;
use std::collections::BTreeMap;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq, SlogInlineError)]
pub enum ConfigurationError {
    #[error("rack secret split error")]
    RackSecretSplit(
        #[from]
        #[source]
        SplitError,
    ),
    #[error("too many members: must be fewer than 255")]
    TooManyMembers,
}

/// The configuration for a given epoch.
///
/// Only valid for non-lrtq configurations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Configuration {
    /// Unique Id of the rack
    pub rack_id: RackUuid,

    // Unique, monotonically increasing identifier for a configuration
    pub epoch: Epoch,

    /// Who was the coordinator of this reconfiguration?
    pub coordinator: PlatformId,

    // All members of the current configuration and the hash of their key shares
    pub members: BTreeMap<PlatformId, Sha3_256Digest>,

    /// The number of sleds required to reconstruct the rack secret
    pub threshold: Threshold,

    // There is no previous configuration for the initial configuration
    pub previous_configuration: Option<PreviousConfiguration>,
}

impl IdOrdItem for Configuration {
    type Key<'a> = Epoch;

    fn key(&self) -> Self::Key<'_> {
        self.epoch
    }

    id_upcast!();
}

impl Configuration {
    /// Create a new configuration for the trust quorum
    ///
    /// `previous_configuration` is never filled in upon construction. A
    /// coordinator will fill this in as necessary after retrieving shares for
    /// the last committed epoch.
    pub fn new(
        reconfigure_msg: &ValidatedReconfigureMsg,
    ) -> Result<(Configuration, BTreeMap<PlatformId, Share>), ConfigurationError>
    {
        let coordinator = reconfigure_msg.coordinator_id().clone();
        let rack_secret = RackSecret::new();
        let shares = rack_secret.split(
            reconfigure_msg.threshold(),
            reconfigure_msg
                .members()
                .len()
                .try_into()
                .map_err(|_| ConfigurationError::TooManyMembers)?,
        )?;

        let shares_and_digests =
            shares.shares.expose_secret().iter().map(|s| {
                let mut digest = Sha3_256Digest::default();
                s.digest::<sha3::Sha3_256>(&mut digest.0);
                (s.clone(), digest)
            });

        let mut members: BTreeMap<PlatformId, Sha3_256Digest> = BTreeMap::new();
        let mut shares: BTreeMap<PlatformId, Share> = BTreeMap::new();
        for (platform_id, (share, digest)) in
            reconfigure_msg.members().iter().cloned().zip(shares_and_digests)
        {
            members.insert(platform_id.clone(), digest);
            shares.insert(platform_id, share);
        }

        Ok((
            Configuration {
                rack_id: reconfigure_msg.rack_id(),
                epoch: reconfigure_msg.epoch(),
                coordinator,
                members,
                threshold: reconfigure_msg.threshold(),
                previous_configuration: None,
            },
            shares,
        ))
    }
}

/// Information for the last committed configuration that is necessary to track
/// in the next `Configuration`.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct PreviousConfiguration {
    /// The epoch of the last committed configuration
    pub epoch: Epoch,

    /// Is the previous configuration LRTQ?
    pub is_lrtq: bool,

    /// The encrypted rack secret for the last committed epoch
    ///
    /// This allows us to derive old encryption keys so they can be rotated
    pub encrypted_last_committed_rack_secret: EncryptedRackSecret,

    /// A random value used to derive the key to encrypt the rack secret from
    /// the last committed epoch.
    ///
    /// We only encrypt the rack secret once and so we use a nonce of all zeros.
    /// This is why there is no corresponding `nonce` field.
    pub encrypted_last_committed_rack_secret_salt: Salt,
}
