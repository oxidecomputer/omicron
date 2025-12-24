// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration creation for trust quorum.
//!
//! The `Configuration` type itself is defined in `trust-quorum-types` for API versioning akin to
//! RFD 619. This module provides the `new_configuration` function which creates configurations (and
//! cannot be an inherent method on a foreign type due to `Configuration` being defined in another crate).

use gfss::shamir::Share;
use secrecy::ExposeSecret;
use std::collections::BTreeMap;

use crate::crypto::RackSecret;
use trust_quorum_types::configuration::{
    BaseboardId, Configuration, ConfigurationError, NewConfigParams,
};
use trust_quorum_types::crypto::Sha3_256Digest;

/// Create a new configuration for the trust quorum.
///
/// This is a free function because `Configuration` is defined in
/// `trust-quorum-types` (for API versioning per RFD 619) and Rust doesn't
/// allow adding inherent methods to types from other crates.
///
/// `previous_configuration` is never filled in upon construction. A
/// coordinator will fill this in as necessary after retrieving shares for
/// the last committed epoch.
pub fn new_configuration(
    params: NewConfigParams<'_>,
) -> Result<(Configuration, BTreeMap<BaseboardId, Share>), ConfigurationError> {
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

    let shares_and_digests = shares.shares.expose_secret().iter().map(|s| {
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

/// Check if two configurations are equal except for crypto data.
///
/// This is a free function because `Configuration` is defined in
/// `trust-quorum-types` (for API versioning per RFD 619).
#[cfg(feature = "testing")]
pub fn configurations_equal_except_for_crypto_data(
    a: &Configuration,
    b: &Configuration,
) -> bool {
    let encrypted_rack_secrets_match =
        match (&a.encrypted_rack_secrets, &b.encrypted_rack_secrets) {
            (None, None) => true,
            (Some(_), Some(_)) => true,
            _ => false,
        };
    a.rack_id == b.rack_id
        && a.epoch == b.epoch
        && a.coordinator == b.coordinator
        && a.members.keys().zip(b.members.keys()).all(|(id1, id2)| id1 == id2)
        && a.threshold == b.threshold
        && encrypted_rack_secrets_match
}
