// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Various cryptographic constructs used by trust quroum.

use bootstore::trust_quorum::RackSecret as LrtqRackSecret;
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, aead, aead::Aead};
use derive_more::From;
use gfss::shamir::{self, CombineError, SecretShares, Share, SplitError};
use hkdf::Hkdf;
use omicron_uuid_kinds::RackUuid;
use rand::TryRngCore;
use rand::rngs::OsRng;
use secrecy::{ExposeSecret, SecretBox};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use slog_error_chain::SlogInlineError;
use static_assertions::const_assert_eq;
use std::collections::BTreeMap;
use std::fmt::Debug;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::{Epoch, Threshold};

/// Each share contains a byte for the y-coordinate of 32 points on 32 different
/// polynomials over Ed25519. All points share an x-coordinate, which is the 0th
/// byte in a 33 byte Vec.
///
/// This is enough information to share a secret 32 bytes long.
const LRTQ_SHARE_SIZE: usize = 33;

// The size in bytes of a single rack secret
//
// Public only for docs
pub const SECRET_LEN: usize = 32;

// The size in bytes of an `Epoch`
const EPOCH_LEN: usize = size_of::<Epoch>();
const_assert_eq!(EPOCH_LEN, 8);

// The size of a ChaCha20Poly1305 nonce in bytes
const CHACHA20POLY1305_NONCE_LEN: usize = 12;

// The key share format used for LRTQ
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop, From)]
#[cfg_attr(feature = "danger_partial_eq_ct_wrapper", derive(PartialEq, Eq))]
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

    pub fn inner(&self) -> &Vec<u8> {
        &self.0
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareDigestLrtq(Sha3_256Digest);

#[derive(
    Default, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Sha3_256Digest(pub [u8; 32]);

impl std::fmt::Debug for Sha3_256Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sha3 digest: ")?;
        for v in self.0.as_slice() {
            write!(f, "{:x?}", v)?;
        }
        Ok(())
    }
}

impl From<ShareDigestLrtq> for bootstore::Sha3_256Digest {
    fn from(value: ShareDigestLrtq) -> Self {
        bootstore::Sha3_256Digest::new(value.0.0)
    }
}

/// A boxed array containing rack secret data
///
/// We explicitly choose to box the data so that it is not littered around
/// memory via moves, and also so that it is not accidentally growable like
/// a `Vec`.
#[derive(Debug)]
struct RackSecretData(SecretBox<[u8; SECRET_LEN]>);

/// A rack secret reconstructed via share combination.
///
/// This secret must be treated as a generic array of `SECRET_LEN` bytes. We
/// don't differentiate between whether or not this secret was recreated via
/// Ed25519 +Ristretto key shares (LRTQ) or GF256 Key Shares (current protocol).
/// Therefore, this rack secret should never be split back into key shares.
#[derive(Debug)]
pub struct ReconstructedRackSecret {
    secret: RackSecretData,
}

impl ExposeSecret<[u8; SECRET_LEN]> for ReconstructedRackSecret {
    fn expose_secret(&self) -> &[u8; SECRET_LEN] {
        &self.secret.0.expose_secret()
    }
}

impl Clone for ReconstructedRackSecret {
    fn clone(&self) -> Self {
        self.expose_secret().as_slice().try_into().unwrap()
    }
}

#[cfg(any(test, feature = "testing"))]
impl PartialEq for ReconstructedRackSecret {
    fn eq(&self, other: &Self) -> bool {
        self.expose_secret().ct_eq(other.expose_secret()).into()
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    thiserror::Error,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
#[error("invalid rack secret size")]
pub struct InvalidRackSecretSizeError;

impl TryFrom<SecretBox<[u8]>> for ReconstructedRackSecret {
    type Error = InvalidRackSecretSizeError;
    fn try_from(value: SecretBox<[u8]>) -> Result<Self, Self::Error> {
        let data: Box<[u8; SECRET_LEN]> = Box::new(
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

impl TryFrom<&[u8]> for ReconstructedRackSecret {
    type Error = InvalidRackSecretSizeError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let data: Box<[u8; SECRET_LEN]> =
            Box::new(value.try_into().map_err(|_| InvalidRackSecretSizeError)?);
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

impl From<RackSecret> for ReconstructedRackSecret {
    fn from(value: RackSecret) -> Self {
        let RackSecret { secret } = value;
        ReconstructedRackSecret { secret }
    }
}

#[derive(
    Debug,
    Clone,
    thiserror::Error,
    PartialEq,
    Eq,
    SlogInlineError,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
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

impl ExposeSecret<[u8; SECRET_LEN]> for RackSecret {
    fn expose_secret(&self) -> &[u8; SECRET_LEN] {
        &self.secret.0.expose_secret()
    }
}

impl Default for RackSecret {
    fn default() -> Self {
        Self::new()
    }
}

impl RackSecret {
    /// Create a random `SECRET_LEN` byte secret
    pub fn new() -> RackSecret {
        let mut rng = OsRng;
        let mut data = Box::new([0u8; SECRET_LEN]);
        while data.ct_eq(&[0u8; SECRET_LEN]).into() {
            rng.try_fill_bytes(&mut *data)
                .expect("fetched random bytes from OsRng");
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
        rng.try_fill_bytes(&mut salt).expect("fetched random bytes from OsRng");
        Salt(salt)
    }
}

impl Default for Salt {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
/// All possibly relevant __encrypted__ rack secrets for _prior_ committed
/// configurations
pub struct EncryptedRackSecrets {
    /// A random value used to derive the key to encrypt the rack secrets for
    /// prior committed epochs.
    salt: Salt,
    data: Box<[u8]>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    thiserror::Error,
    SlogInlineError,
    Serialize,
    Deserialize,
)]
pub enum DecryptionError {
    // An opaque error indicating decryption failed
    #[error("Failed to decrypt rack secrets")]
    Aead,

    // The length of the plaintext is not the correct size and cannot
    // be decoded.
    #[error("Plaintext length is invalid")]
    InvalidLength,
}

impl From<aead::Error> for DecryptionError {
    fn from(_: aead::Error) -> Self {
        DecryptionError::Aead
    }
}

impl EncryptedRackSecrets {
    pub fn new(salt: Salt, data: Box<[u8]>) -> Self {
        EncryptedRackSecrets { salt, data }
    }

    pub fn decrypt(
        &self,
        rack_id: RackUuid,
        epoch: Epoch,
        rack_secret: &ReconstructedRackSecret,
    ) -> Result<PlaintextRackSecrets, DecryptionError> {
        let key = derive_encryption_key_for_rack_secrets(
            rack_id,
            epoch,
            self.salt,
            rack_secret,
        );

        // This key only encrypts a single plaintext and so a nonce of all zeroes
        // is all that's required.
        let nonce = [0u8; CHACHA20POLY1305_NONCE_LEN].into();

        let plaintext =
            Zeroizing::new(key.decrypt(&nonce, self.data.as_ref())?);

        if plaintext.len() % (SECRET_LEN + EPOCH_LEN) != 0 {
            return Err(DecryptionError::InvalidLength);
        }

        let mut plaintext_secrets = PlaintextRackSecrets::new();

        for p in plaintext.chunks_exact(SECRET_LEN + EPOCH_LEN) {
            // SAFETY: Neither of these unwraps will ever fail as we've checked
            // the validity of plaintext length above.
            let epoch =
                Epoch(u64::from_be_bytes(p[0..EPOCH_LEN].try_into().unwrap()));
            let secret: ReconstructedRackSecret =
                p[EPOCH_LEN..].try_into().unwrap();
            plaintext_secrets.insert(epoch, secret);
        }

        Ok(plaintext_secrets)
    }
}

/// All possibly relevant __unencrypted__ rack secrets for _prior_ committed
/// configurations
///
/// For secret rotations we need to maintain all prior rack secrets for
/// configurations that any current trust quorum members may be stuck
/// at. Eventually they will learn the latest configuration and decrypt
/// `EncryptedRackSecrets` in order to get these plaintext versions. They will
/// then use the rack secret for their currently committed configuration for key
/// rotation.
pub struct PlaintextRackSecrets {
    secrets: BTreeMap<Epoch, ReconstructedRackSecret>,
}

impl PlaintextRackSecrets {
    pub fn new() -> PlaintextRackSecrets {
        PlaintextRackSecrets { secrets: BTreeMap::new() }
    }

    pub fn insert(&mut self, epoch: Epoch, secret: ReconstructedRackSecret) {
        assert!(self.secrets.insert(epoch, secret).is_none());
    }

    pub fn get(&self, epoch: Epoch) -> Option<&ReconstructedRackSecret> {
        self.secrets.get(&epoch)
    }

    pub fn into_inner(self) -> BTreeMap<Epoch, ReconstructedRackSecret> {
        self.secrets
    }

    /// Consume the plaintext and return an `EncryptedRackSecrets`
    ///
    /// We are encrypting rack secrets for prior epochs with the latest
    /// rack secret.
    pub fn encrypt(
        self,
        rack_id: RackUuid,
        new_epoch: Epoch,
        new_rack_secret: &ReconstructedRackSecret,
    ) -> aead::Result<EncryptedRackSecrets> {
        assert!(!self.secrets.is_empty());

        // We generate a fresh salt because we should only be encrypting
        // once for this epoch. This is also why we consume `self`.
        let salt = Salt::new();
        let key = derive_encryption_key_for_rack_secrets(
            rack_id,
            new_epoch,
            salt,
            new_rack_secret,
        );

        // Figure out the size of our plaintext and create a zeroizable buffer
        // for it.
        let plaintext_size = (SECRET_LEN + EPOCH_LEN) * self.secrets.len();
        let mut plaintext =
            Zeroizing::new(Vec::<u8>::with_capacity(plaintext_size));

        // Write each epoch as a big endian u64 followed by the plaintext secret
        for (epoch, secret) in &self.secrets {
            plaintext.extend_from_slice(&epoch.0.to_be_bytes());
            plaintext.extend_from_slice(secret.secret.0.expose_secret());
        }

        // This key only encrypts a single plaintext and so a nonce of all zeroes
        // is all that's required.
        let nonce = [0u8; CHACHA20POLY1305_NONCE_LEN].into();
        let encrypted =
            key.encrypt(&nonce, plaintext.as_ref())?.into_boxed_slice();
        Ok(EncryptedRackSecrets { salt, data: encrypted })
    }
}

/// Derive an encryption key from a rack secret used to decrypt
/// `EncryptedRackSecrets` and encrypt `PlaintextRackSecrets`
fn derive_encryption_key_for_rack_secrets(
    rack_id: RackUuid,
    epoch: Epoch,
    salt: Salt,
    rack_secret: &ReconstructedRackSecret,
) -> ChaCha20Poly1305 {
    const ALL_ZEROES: [u8; 32] = [0u8; SECRET_LEN];
    let prk =
        Hkdf::<Sha3_256>::new(Some(&salt.0[..]), rack_secret.expose_secret());
    let mut key = Zeroizing::new(ALL_ZEROES);

    // Limit where and how this key can be used as part of its construction.
    // It's a defense in depth practice to prevent confused deputy attacks.
    //
    // In this particular case, we are saying that this key is intended for
    // the first iteration of the trust quorum protocol and it is being used to
    // encrypt rack secrets. In addition, the key is only valid for this rack
    // id and epoch, but those are separate parameters in addition to the fixed
    // context for the protocol.
    //
    // The constant is intentionally local to this method to prevent reusing it
    // for other key derivations. Please do not move its definition.
    const KEY_CONTEXT: &'static [u8] = b"trust-quorum-v1-rack-secrets";

    // SAFETY: `key` is sized such that `prk.expand_multi_info` will never fail.
    prk.expand_multi_info(
        &[KEY_CONTEXT, rack_id.as_ref(), &epoch.0.to_be_bytes()],
        key.as_mut(),
    )
    .unwrap();

    let is_key_all_zeroes: bool = ALL_ZEROES.ct_eq(key.as_ref()).into();
    assert!(!is_key_all_zeroes);

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
        if let Ok(rs) = res {
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

    #[test]
    fn rack_secrets_encrypt_decrypt_roundtrip() {
        let mut plaintext = PlaintextRackSecrets::new();
        let mut plaintext2 = PlaintextRackSecrets::new();
        for i in 1..5u64 {
            let rs: ReconstructedRackSecret = RackSecret::new().into();
            let rs2 = rs.clone();
            plaintext.insert(Epoch(i), rs);
            plaintext2.insert(Epoch(i), rs2);
        }
        let rack_id = RackUuid::new_v4();
        let new_epoch = Epoch(7);
        let new_rack_secret = RackSecret::new().into();
        let encrypted =
            plaintext.encrypt(rack_id, new_epoch, &new_rack_secret).unwrap();
        let decrypted =
            encrypted.decrypt(rack_id, new_epoch, &new_rack_secret).unwrap();

        // We don't actually do any comparisons of rack secrets outside this
        // test and we don't want to derive `PartialEq` due to data dependent
        // timing. Therefore we do a basic loop comparison here, which is a
        // bit tedious.
        for i in 1..5u64 {
            assert_eq!(
                decrypted.get(Epoch(i)).unwrap().expose_secret(),
                plaintext2.get(Epoch(i)).unwrap().expose_secret()
            );
        }
    }

    #[test]
    fn rack_secrets_decryption_failure() {
        let mut plaintext = PlaintextRackSecrets::new();
        for i in 1..5u64 {
            let rs: ReconstructedRackSecret = RackSecret::new().into();
            plaintext.insert(Epoch(i), rs);
        }
        let rack_id = RackUuid::new_v4();
        let new_epoch = Epoch(7);
        let new_rack_secret = RackSecret::new().into();
        let mut encrypted =
            plaintext.encrypt(rack_id, new_epoch, &new_rack_secret).unwrap();

        // Decrypting with wrong rack_id fails.
        assert!(
            encrypted
                .decrypt(RackUuid::new_v4(), new_epoch, &new_rack_secret)
                .is_err()
        );

        // Decrypting with wrong epoch fails.
        assert!(
            encrypted
                .decrypt(RackUuid::new_v4(), Epoch(99), &new_rack_secret)
                .is_err()
        );

        // Decrypting with the wrong secret fails
        assert!(
            encrypted
                .decrypt(rack_id, new_epoch, &RackSecret::new().into())
                .is_err()
        );

        // Decrypting with corrupted plaintext is invalid
        encrypted.data = vec![0u8, 1u8].into_boxed_slice();
        assert!(
            encrypted.decrypt(rack_id, new_epoch, &new_rack_secret).is_err()
        );
    }
}
