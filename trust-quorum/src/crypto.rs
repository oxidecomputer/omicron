// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Various cryptographic constructs used by trust quroum.

use bootstore::trust_quorum::RackSecret as LrtqRackSecret;
use derive_more::From;
use gfss::shamir::{self, CombineError, SecretShares, Share, SplitError};
use rand::RngCore;
use rand::rngs::OsRng;
use secrecy::{ExposeSecret, SecretBox};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use slog_error_chain::SlogInlineError;
use std::fmt::Debug;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::Threshold;

/// Each share contains a byte for the y-coordinate of 32 points on 32 different
/// polynomials over Ed25519. All points share an x-coordinate, which is the 0th
/// byte in a 33 byte Vec.
///
/// This is enough information to share a secret 32 bytes long.
const LRTQ_SHARE_SIZE: usize = 33;

/// We don't distinguish whether this is an Ed25519 Scalar or set of GF(256)
/// polynomials points with an x-coordinate of 0. Both can be treated as 32 byte
/// blobs when decrypted, as they are immediately fed into HKDF.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct EncryptedRackSecret(pub Vec<u8>);

// The key share format used for LRTQ
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop, From)]
pub struct LrtqShare(Vec<u8>);

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually.
impl std::fmt::Debug for LrtqShare {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyShareEd25519").finish()
    }
}

impl LrtqShare {
    pub fn new(share: Vec<u8>) -> Self {
        assert_eq!(share.len(), LRTQ_SHARE_SIZE);
        Self(share)
    }

    pub fn digest(&self) -> ShareDigestLrtq {
        ShareDigestLrtq(Sha3_256Digest(
            Sha3_256::digest(&self.0).as_slice().try_into().unwrap(),
        ))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareDigestLrtq(Sha3_256Digest);

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub struct Sha3_256Digest(pub [u8; 32]);

/// A boxed array containing rack secret data
///
/// We explicitly choose to box the data so that it is not littered around
/// memory via moves, and also so that it is not accidentally growable like
/// a `Vec`.
#[derive(Debug)]
struct RackSecretData(SecretBox<[u8; 32]>);

/// A rack secret reconstructed via share combination.
///
/// This secret must be treated as a generic array of 32 bytes. We don't
/// differentiate between whether or not this secret was recreated via Ed25519
/// +Ristretto key shares (LRTQ) or GF256 Key Shares (current protocol).
/// Therefore, this rack secret should never be split back into key shares.
#[derive(Debug)]
pub struct ReconstructedRackSecret {
    secret: RackSecretData,
}

impl ExposeSecret<[u8; 32]> for ReconstructedRackSecret {
    fn expose_secret(&self) -> &[u8; 32] {
        &self.secret.0.expose_secret()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid rack secret size")]
pub struct InvalidRackSecretSizeError;

impl TryFrom<SecretBox<[u8]>> for ReconstructedRackSecret {
    type Error = InvalidRackSecretSizeError;
    fn try_from(value: SecretBox<[u8]>) -> Result<Self, Self::Error> {
        let data: Box<[u8; 32]> = Box::new(
            value
                .expose_secret()
                .try_into()
                .map_err(|_| InvalidRackSecretSizeError)?,
        );
        Ok(ReconstructedRackSecret {
            secret: RackSecretData(SecretBox::new(data)),
        })
    }
}

impl From<LrtqRackSecret> for ReconstructedRackSecret {
    fn from(value: LrtqRackSecret) -> Self {
        let secret = value.expose_secret().as_bytes();
        ReconstructedRackSecret {
            secret: RackSecretData(SecretBox::new(Box::new(*secret))),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq, SlogInlineError)]
pub enum RackSecretReconstructError {
    #[error("share combine error")]
    Combine(
        #[from]
        #[source]
        CombineError,
    ),
    #[error(transparent)]
    Size(#[from] InvalidRackSecretSizeError),
}

/// A shared secret based on GF256
#[derive(Debug)]
pub struct RackSecret {
    secret: RackSecretData,
}

impl ExposeSecret<[u8; 32]> for RackSecret {
    fn expose_secret(&self) -> &[u8; 32] {
        &self.secret.0.expose_secret()
    }
}

impl Default for RackSecret {
    fn default() -> Self {
        Self::new()
    }
}

impl RackSecret {
    /// Create a random 32 byte secret
    pub fn new() -> RackSecret {
        let mut rng = OsRng;
        let mut data = Box::new([0u8; 32]);
        while data.ct_eq(&[0u8; 32]).into() {
            rng.fill_bytes(&mut *data);
        }
        RackSecret { secret: RackSecretData(SecretBox::new(data)) }
    }

    /// Split a secret into `total_shares` number of shares, where combining
    /// `threshold` of the shares can be used to recover the secret.
    pub fn split(
        &self,
        threshold: Threshold,
        total_shares: u8,
    ) -> Result<SecretShares, SplitError> {
        let shares = shamir::split_secret(
            self.expose_secret(),
            total_shares,
            threshold.0,
        )?;
        Ok(shares)
    }

    pub fn reconstruct(
        shares: &[Share],
    ) -> Result<ReconstructedRackSecret, RackSecretReconstructError> {
        let secret = shamir::compute_secret(shares)?.try_into()?;
        Ok(secret)
    }
}

impl PartialEq for RackSecret {
    fn eq(&self, other: &Self) -> bool {
        self.expose_secret().ct_eq(other.expose_secret()).into()
    }
}

impl Eq for RackSecret {}

/// Some public randomness for cryptographic operations
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Salt(pub [u8; 32]);

impl Salt {
    pub fn new() -> Salt {
        let mut rng = OsRng;
        let mut salt = [0u8; 32];
        rng.fill_bytes(&mut salt);
        Salt(salt)
    }
}

impl Default for Salt {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{
        prelude::TestCaseError, prop_assert, prop_assert_eq, prop_assert_ne,
    };
    use test_strategy::{Arbitrary, proptest};

    #[derive(Arbitrary, Debug)]
    pub struct TestRackSecretInput {
        #[strategy(2..255u8)]
        threshold: u8,
        #[strategy(2..255u8)]
        total_shares: u8,
    }

    fn check_rack_secret_invariants(
        input: &TestRackSecretInput,
        original: RackSecret,
        res: Result<SecretShares, SplitError>,
    ) -> Result<(), TestCaseError> {
        if input.threshold > input.total_shares {
            prop_assert!(res.is_err());
            return Ok(());
        }

        let shares = res.unwrap();

        // Fewer than threshold shares generates a nonsense secret
        let res = RackSecret::reconstruct(
            &shares.shares.expose_secret()[0..(input.threshold - 2) as usize],
        );
        if res.is_ok() {
            let rs = res.unwrap();
            prop_assert_ne!(rs.expose_secret(), original.expose_secret());
        }

        // Can unlock with at least `threshold` shares
        let rack_secret = RackSecret::reconstruct(
            &shares.shares.expose_secret()[..input.threshold as usize],
        )
        .unwrap();
        prop_assert_eq!(rack_secret.expose_secret(), original.expose_secret());

        Ok(())
    }

    #[proptest]
    fn rack_secret_test(input: TestRackSecretInput) {
        let rs = RackSecret::new();
        let res = rs.split(Threshold(input.threshold), input.total_shares);
        check_rack_secret_invariants(&input, rs, res)?;
    }
}
