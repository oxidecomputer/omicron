// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Various cryptographic constructs used by trust quroum.

use bootstore::trust_quorum::RackSecret as LrtqRackSecret;
use derive_more::From;
use rand::RngCore;
use rand::rngs::OsRng;
use secrecy::{ExposeSecret, Secret};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use vsss_rs::{Gf256, subtle::ConstantTimeEq};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::{Error, Threshold};

// Each share is a point on a polynomial (Curve25519). Each share is 33 bytes
// - one identifier (x-coordinate) byte, and one 32-byte y-coordinate.
const SHARE_SIZE: usize = 33;

/// We don't distinguish whether this is an Ed25519 Scalar or set of GF256
/// polynomials as both can be treated as 32 byte blobs when decrypted.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct EncryptedRackSecret(pub Vec<u8>);

/// The key share used for our "real" trust quorum
#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Zeroize,
    ZeroizeOnDrop,
    From,
)]
#[repr(transparent)]
pub struct KeyShareGf256(Vec<u8>);

// The key share format used for LRTQ
#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Zeroize,
    ZeroizeOnDrop,
    From,
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

#[derive(Debug)]
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

/// A shared secret based on GF256
pub struct RackSecret {
    secret: Secret<RackSecretData>,
}

impl RackSecret {
    /// Create a random 32 byte secret
    pub fn new() -> RackSecret {
        let mut rng = OsRng;
        let mut data = Box::new([0u8; 32]);
        while data.ct_eq(&[0u8; 32]).into() {
            rng.fill_bytes(&mut *data);
        }
        RackSecret { secret: Secret::new(RackSecretData(data)) }
    }

    /// Split a secert into `total_shares` number of shares, where combining
    /// `threshold` of the shares can be used to recover the secret.
    pub fn split(
        &self,
        threshold: Threshold,
        total_shares: usize,
    ) -> Result<Secret<Vec<KeyShareGf256>>, Error> {
        let rng = OsRng;
        let shares = Gf256::split_array(
            threshold.0 as usize,
            total_shares,
            &*self.secret.expose_secret().0,
            rng,
        )?;
        Ok(Secret::new(shares.into_iter().map(KeyShareGf256).collect()))
    }

    pub fn reconstruct(
        shares: &[KeyShareGf256],
    ) -> Result<ReconstructedRackSecret, Error> {
        // Safety: We're casting from a transparent newtype wrapper,
        // so `KeyShareGf256` and `Vec<u8>` are the same size.
        let shares = unsafe {
            std::slice::from_raw_parts(
                shares.as_ptr() as *const Vec<u8>,
                shares.len(),
            )
        };
        let secret = Gf256::combine_array(shares)?;
        Ok(secret.try_into().expect("valid rack secret size"))
    }
}

impl PartialEq for RackSecret {
    fn eq(&self, other: &Self) -> bool {
        self.secret
            .expose_secret()
            .0
            .ct_eq(&*other.secret.expose_secret().0)
            .into()
    }
}

impl Eq for RackSecret {}
