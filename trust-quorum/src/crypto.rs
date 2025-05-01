// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Various cryptographic constructs used by trust quroum.

use crate::{Epoch, Threshold};
use bootstore::trust_quorum::RackSecret as LrtqRackSecret;
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, aead, aead::Aead};
use derive_more::From;
use gfss::shamir::{self, CombineError, SecretShares, Share, SplitError};
use hkdf::Hkdf;
use omicron_uuid_kinds::{GenericUuid, RackUuid};
use rand::RngCore;
use rand::rngs::OsRng;
use secrecy::{DebugSecret, ExposeSecret, Secret};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use slog_error_chain::SlogInlineError;
use std::fmt::Debug;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// Each share contains a byte for the y-coordinate of 32 points on 32 different
/// polynomials over Ed25519. All points share an x-coordinate, which is the 0th
/// byte in a 33 byte Vec.
///
/// This is enough information to share a secret 32 bytes long.
const LRTQ_SHARE_SIZE: usize = 33;

/// We don't distinguish whether this is an Ed25519 Scalar or set of GF(256)
/// polynomials' points with an x-coordinate of 0. Both can be treated as 32
/// byte blobs when decrypted, as they are immediately fed into HKDF.
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
/// This should never be used directly, and always wrapped in a `Secret` upon
/// construction. We sparate the two types, because a `Secret` must contain
/// `Zeroizable` data, and a `Box<[u8; 32]>` is not zeroizable on its own.
///
/// We explicitly choose to box the data so that it is not littered around
/// memory via moves, and also so that it is not accidentally growable like
/// a `Vec`.
#[derive(Zeroize, ZeroizeOnDrop)]
struct RackSecretData(Box<[u8; 32]>);

/// A rack secret reconstructed via share combination.
///
/// This secret must be treated as a generic array of 32 bytes. We don't
/// differentiate between whether or not this secret was recreated via Ed25519
/// +Ristretto key shares (LRTQ) or GF256 Key Shares (current protocol).
/// Therefore, this rack secret should never be split back into key shares.
pub struct ReconstructedRackSecret {
    secret: Secret<RackSecretData>,
}

impl DebugSecret for ReconstructedRackSecret {}

impl Debug for ReconstructedRackSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Self::debug_secret(f)
    }
}

impl ExposeSecret<[u8; 32]> for ReconstructedRackSecret {
    fn expose_secret(&self) -> &[u8; 32] {
        &self.secret.expose_secret().0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid rack secret size")]
pub struct InvalidRackSecretSizeError;

impl TryFrom<Secret<Box<[u8]>>> for ReconstructedRackSecret {
    type Error = InvalidRackSecretSizeError;
    fn try_from(value: Secret<Box<[u8]>>) -> Result<Self, Self::Error> {
        // This is somewhat janky
        //
        // We explicitly clone the secret out, then put it in a boxed array.
        // This should avoid an intermediate allocation that we'd have to zero.
        let v: Vec<u8> = value.expose_secret().clone().into();
        let data: Box<[u8; 32]> =
            v.try_into().map_err(|_| InvalidRackSecretSizeError)?;
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
pub struct RackSecret {
    secret: Secret<RackSecretData>,
}

impl ExposeSecret<[u8; 32]> for RackSecret {
    fn expose_secret(&self) -> &[u8; 32] {
        &self.secret.expose_secret().0
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
        RackSecret { secret: Secret::new(RackSecretData(data)) }
    }

    /// Split a secert into `total_shares` number of shares, where combining
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

    pub fn reconstruct_from_iter<'a>(
        shares: impl Iterator<Item = &'a Share>,
    ) -> Result<ReconstructedRackSecret, RackSecretReconstructError> {
        let mut shares: Vec<Share> = shares.cloned().collect();
        let res = RackSecret::reconstruct(&shares);
        shares.zeroize();
        res
    }
}

impl DebugSecret for RackSecret {}

impl Debug for RackSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Self::debug_secret(f)
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

/// Encrypt the old rack secret with a key derived from the new rack secret.
///
/// A random salt is generated and returned along with the encrypted secret. Key
/// derivation context includes `rack_id`, `old_epoch`, and `new_epoch`.
pub fn encrypt_old_rack_secret(
    old_rack_secret: ReconstructedRackSecret,
    new_rack_secret: ReconstructedRackSecret,
    rack_id: RackUuid,
    old_epoch: Epoch,
    new_epoch: Epoch,
) -> aead::Result<(EncryptedRackSecret, Salt)> {
    let salt = Salt::new();
    let cipher = derive_encryption_key_for_rack_secret(
        new_rack_secret,
        salt,
        rack_id,
        old_epoch,
        new_epoch,
    );

    // This key is only used to encrypt one plaintext. A nonce of all zeroes is
    // all that's required.
    let nonce = [0u8; 12].into();
    let encrypted_rack_secret = EncryptedRackSecret(
        cipher.encrypt(&nonce, old_rack_secret.expose_secret().as_ref())?,
    );

    Ok((encrypted_rack_secret, salt))
}

fn derive_encryption_key_for_rack_secret(
    new_rack_secret: ReconstructedRackSecret,
    salt: Salt,
    rack_id: RackUuid,
    old_epoch: Epoch,
    new_epoch: Epoch,
) -> ChaCha20Poly1305 {
    let prk = Hkdf::<Sha3_256>::new(
        Some(&salt.0[..]),
        new_rack_secret.expose_secret(),
    );

    // The "info" string is context to bind the key to its purpose
    let mut key = Zeroizing::new([0u8; 32]);
    prk.expand_multi_info(
        &[
            b"trust-quorum-v1-rack-secret",
            rack_id.as_untyped_uuid().as_ref(),
            &new_epoch.0.to_be_bytes(),
            &old_epoch.0.to_be_bytes(),
        ],
        key.as_mut(),
    )
    .unwrap();
    ChaCha20Poly1305::new(Key::from_slice(key.as_ref()))
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
