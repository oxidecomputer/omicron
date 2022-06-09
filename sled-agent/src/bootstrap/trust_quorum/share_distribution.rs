// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use serde::{Deserialize, Serialize};
use sprockets_host::Ed25519Certificate;
use std::fmt;
use vsss_rs::Share;

use super::rack_secret::Verifier;

/// A ShareDistribution is an individual share of a secret along with all the
/// metadata required to allow a server in possession of the share to know how
/// to correctly recreate a split secret.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ShareDistribution {
    pub threshold: usize,
    pub total_shares: usize,
    pub verifier: Verifier,
    pub share: Share,
    pub member_device_id_certs: Vec<Ed25519Certificate>,
}

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually and omit sensitive fields.
impl fmt::Debug for ShareDistribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShareDistribution")
            .field("threshold", &self.threshold)
            .field("total_shares", &self.total_shares)
            .field("verifier", &"Verifier")
            .field("share", &"Share")
            .field("member_device_id_certs", &self.member_device_id_certs)
            .finish()
    }
}
