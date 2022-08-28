// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use serde::Deserialize;
use serde::Serialize;
use sprockets_host::Ed25519Certificate;
use std::fmt;
use vsss_rs::Share;

use super::rack_secret::Verifier;

/// A ShareDistribution is an individual share of a secret along with all the
/// metadata required to allow a server in possession of the share to know how
/// to correctly recreate a split secret.
// We intentionally DO NOT derive `Debug` or `Serialize`; both provide avenues
// by which we may accidentally log the contents of our `share`.
#[derive(Clone, PartialEq, Deserialize)]
pub struct ShareDistribution {
    pub threshold: usize,
    pub verifier: Verifier,
    pub share: Share,
    pub member_device_id_certs: Vec<Ed25519Certificate>,
}

impl ShareDistribution {
    pub fn total_shares(&self) -> usize {
        self.member_device_id_certs.len()
    }
}

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually and omit sensitive fields.
impl fmt::Debug for ShareDistribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShareDistribution")
            .field("threshold", &self.threshold)
            .field("verifier", &"Verifier")
            .field("share", &"Share")
            .field("member_device_id_certs", &self.member_device_id_certs)
            .finish()
    }
}

/// This type is equivalent to `ShareDistribution` but implements `Serialize`.
/// It should be used _very carefully_; `ShareDistribution` should be preferred
/// in almost all cases to avoid accidental spillage of our `Share` contents.
/// This type should only be used to build careful serialization routines that
/// need to deal with trust quorum shares; e.g.,
/// `RequestEnvelope::danger_serialize_as_json()`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct SerializableShareDistribution {
    pub threshold: usize,
    pub share: Share,
    pub member_device_id_certs: Vec<Ed25519Certificate>,
    pub verifier: Verifier,
}

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually and omit sensitive fields.
impl fmt::Debug for SerializableShareDistribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SerializableShareDistribution")
            .field("threshold", &self.threshold)
            .field("verifier", &"Verifier")
            .field("share", &"Share")
            .field("member_device_id_certs", &self.member_device_id_certs)
            .finish()
    }
}

impl From<ShareDistribution> for SerializableShareDistribution {
    fn from(dist: ShareDistribution) -> Self {
        Self {
            threshold: dist.threshold,
            verifier: dist.verifier,
            share: dist.share,
            member_device_id_certs: dist.member_device_id_certs,
        }
    }
}

impl From<SerializableShareDistribution> for ShareDistribution {
    fn from(dist: SerializableShareDistribution) -> Self {
        Self {
            threshold: dist.threshold,
            verifier: dist.verifier,
            share: dist.share,
            member_device_id_certs: dist.member_device_id_certs,
        }
    }
}
