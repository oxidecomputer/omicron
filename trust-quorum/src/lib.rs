// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the oxide rack trust quorum protocol
//!
//! This protocol is written as a
//! [no-IO](https://sans-io.readthedocs.io/how-to-sans-io.html) implementation.
//! All persistent state and all networking is managed outside of this
//! implementation. Callers interact with the protocol via the [`Node`] api.

use bootstore::trust_quorum::RackSecret as LrtqRackSecret;
use secrecy::{ExposeSecret, Secret};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};

mod configuration;
mod error;
mod messages;
mod node;
mod persistent_state;
pub use configuration::Configuration;
pub use error::Error;
pub use messages::*;
pub use node::Node;

// Each share is a point on a polynomial (Curve25519). Each share is 33 bytes
// - one identifier (x-coordinate) byte, and one 32-byte y-coordinate.
const SHARE_SIZE: usize = 33;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct RackId(Uuid);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Epoch(u64);

/// The number of shares required to reconstruct the rack secret
///
/// Typically referred to as `k` in the docs
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Threshold(pub u8);

/// A unique identifier for a given trust quorum member.
//
/// This data is derived from the subject common name in the platform identity
/// certificate that makes up part of the certificate chain used to establish
/// [sprockets](https://github.com/oxidecomputer/sprockets) connections.
///
/// See RFDs 303 and 308 for more details.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct PlatformId {
    part_number: String,
    serial_number: String,
}

/// A container to make messages between trust quorum nodes routable
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Envelope {
    to: PlatformId,
    from: PlatformId,
    msg: PeerMsg,
}

/// We don't distinguish whether this is an Ed25519 Scalar or set of GF256
/// polynomials as both can be treated as 32 byte blobs when decrypted.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct EncryptedRackSecret(pub Vec<u8>);

/// The key share used for our "real" trust quorum
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ZeroizeOnDrop,
)]
pub struct KeyShareGf256(Vec<u8>);

// The key share format used for LRTQ
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ZeroizeOnDrop,
)]
pub struct KeyShareEd25519(Vec<u8>);

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually.
impl std::fmt::Debug for KeyShareEd25519 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyShareEd25519").finish()
    }
}
impl std::fmt::Debug for KeyShareGf256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyShareGf256").finish()
    }
}

impl KeyShareEd25519 {
    pub fn new(share: Vec<u8>) -> Self {
        assert_eq!(share.len(), SHARE_SIZE);
        Self(share)
    }

    pub fn digest(&self) -> ShareDigestEd25519 {
        ShareDigestEd25519(Sha3_256Digest(
            Sha3_256::digest(&self.0).as_slice().try_into().unwrap(),
        ))
    }
}

impl KeyShareGf256 {
    pub fn new(share: Vec<u8>) -> Self {
        assert_eq!(share.len(), SHARE_SIZE);
        Self(share)
    }

    pub fn digest(&self) -> ShareDigestGf256 {
        ShareDigestGf256(Sha3_256Digest(
            Sha3_256::digest(&self.0).as_slice().try_into().unwrap(),
        ))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareDigestGf256(Sha3_256Digest);

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareDigestEd25519(Sha3_256Digest);

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Sha3_256Digest([u8; 32]);

/// A boxed array containing rack secret data
///
/// This should never be used directly, and always wrapped in a `Secret`.
/// upon construction. We sparate the two types, because a `Secret` must contain
/// `Zeroizable` data, and a `Box<[u8; 32]>` is not zeroizable on its own.
///
/// We explicitly choose to box the data so that it is not littered around
/// memory via moves, and also so that it is not accidentally growable like
/// a `Vec`.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct RackSecretData(Box<[u8; 32]>);

/// A rack secret reconstructed via share combination.
///
/// This secret must be treated as a generic array of 32 bytes. We don't
/// differentiate between whether or not this secret was recreated via Ed25519
/// + Ristretto key shares (LRTQ) or GF256 Key Shares (current protocol).
/// Therfore, this rack secret should never be split back into key shares.
pub struct ReconstructedRackSecret {
    secret: Secret<RackSecretData>,
}

pub struct InvalidRackSecretSize;

impl TryFrom<Vec<u8>> for ReconstructedRackSecret {
    type Error = InvalidRackSecretSize;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let data: Box<[u8; 32]> =
            value.try_into().map_err(|_| InvalidRackSecretSize)?;
        Ok(ReconstructedRackSecret {
            secret: Secret::new(RackSecretData(data)),
        })
    }
}

impl From<LrtqRackSecret> for ReconstructedRackSecret {
    fn from(value: LrtqRackSecret) -> Self {
        let secret = value.expose_secret().as_bytes();
        ReconstructedRackSecret {
            secret: Secret::new(RackSecretData(Box::new(*secret))),
        }
    }
}
