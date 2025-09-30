// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A configuration of a trust quroum at a given epoch

use crate::crypto::{EncryptedRackSecrets, RackSecret, Sha3_256Digest};
use crate::{Epoch, BaseboardId, Threshold};
use daft::Diffable;
use gfss::shamir::{Share, SplitError};
use iddqd::{IdOrdItem, id_upcast};
use omicron_uuid_kinds::RackUuid;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use slog_error_chain::SlogInlineError;
use std::collections::{BTreeMap, BTreeSet};

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
#[serde_as]
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Diffable,
)]
pub struct Configuration {
    /// Unique Id of the rack
    pub rack_id: RackUuid,

    // Unique, monotonically increasing identifier for a configuration
    pub epoch: Epoch,

    /// Who was the coordinator of this reconfiguration?
    pub coordinator: BaseboardId,

    // All members of the current configuration and the hash of their key shares
    #[serde_as(as = "Vec<(_, _)>")]
    pub members: BTreeMap<BaseboardId, Sha3_256Digest>,

    /// The number of sleds required to reconstruct the rack secret
    pub threshold: Threshold,

    // There are no encrypted rack secrets for the initial configuration
    pub encrypted_rack_secrets: Option<EncryptedRackSecrets>,
}

impl IdOrdItem for Configuration {
    type Key<'a> = Epoch;

    fn key(&self) -> Self::Key<'_> {
        self.epoch
    }

    id_upcast!();
}

pub struct NewConfigParams<'a> {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub members: &'a BTreeSet<BaseboardId>,
    pub threshold: Threshold,
    pub coordinator_id: &'a BaseboardId,
}

impl Configuration {
    /// Create a new configuration for the trust quorum
    ///
    /// `previous_configuration` is never filled in upon construction. A
    /// coordinator will fill this in as necessary after retrieving shares for
    /// the last committed epoch.
    pub fn new(
        params: NewConfigParams<'_>,
    ) -> Result<(Configuration, BTreeMap<BaseboardId, Share>), ConfigurationError>
    {
        let coordinator = params.coordinator_id.clone();
        let rack_secret = RackSecret::new();
        let shares = rack_secret.split(
            params.threshold,
            params
                .members
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

        let mut members: BTreeMap<BaseboardId, Sha3_256Digest> = BTreeMap::new();
        let mut shares: BTreeMap<BaseboardId, Share> = BTreeMap::new();
        for (platform_id, (share, digest)) in
            params.members.iter().cloned().zip(shares_and_digests)
        {
            members.insert(platform_id.clone(), digest);
            shares.insert(platform_id, share);
        }

        Ok((
            Configuration {
                rack_id: params.rack_id,
                epoch: params.epoch,
                coordinator,
                members,
                threshold: params.threshold,
                encrypted_rack_secrets: None,
            },
            shares,
        ))
    }

    #[cfg(feature = "testing")]
    pub fn equal_except_for_crypto_data(&self, other: &Self) -> bool {
        let encrypted_rack_secrets_match =
            match (&self.encrypted_rack_secrets, &other.encrypted_rack_secrets)
            {
                (None, None) => true,
                (Some(_), Some(_)) => true,
                _ => false,
            };
        self.rack_id == other.rack_id
            && self.epoch == other.epoch
            && self.coordinator == other.coordinator
            && self
                .members
                .keys()
                .zip(other.members.keys())
                .all(|(id1, id2)| id1 == id2)
            && self.threshold == other.threshold
            && encrypted_rack_secrets_match
    }
}
