// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Distributable data packages containing key shares and metadata

use crate::Sha3_256Digest;
use crate::trust_quorum::{RackSecret, TrustQuorumError};
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, aead::Aead};
use hkdf::Hkdf;
use rand08::{RngCore, rngs::OsRng};
use secrecy::{ExposeSecret, SecretBox};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use sled_hardware_types::Baseboard;
use std::collections::BTreeSet;
use std::fmt;
use uuid::Uuid;
use zeroize::Zeroizing;
use zeroize::{Zeroize, ZeroizeOnDrop};

// Each share is a point on a polynomial (Curve25519).  Each share is 33 bytes
// - one identifier (x-coordinate) byte, and one 32-byte y-coordinate.
const SHARE_SIZE: usize = 33;

/// A common container of fields used inside both [`SharePkg`] and
/// [`LearnedSharePkg`]
#[derive(
    Clone, PartialEq, Eq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop,
)]
pub struct SharePkgCommon {
    /// Unique Id of the rack
    #[zeroize(skip)]
    pub rack_uuid: Uuid,
    // We aren't planning on doing any reconfigurations with this version of
    // the protocol
    pub epoch: u32,

    /// The number of shares required to recompute the [`RackSecret`]
    pub threshold: u8,

    /// This sled's unencrypted share
    pub share: Vec<u8>,

    // Digests of all 256 shares so that each sled can verify the integrity
    // of received shares.
    //
    // No need for expensive nonsense
    #[zeroize(skip)]
    pub share_digests: BTreeSet<Sha3_256Digest>,
}

/// A container distributed among trust quorum participants for
/// trust quorum scheme version 0.
///
/// Security Note: This scheme does not verify membership, and will hand out
/// shares to whomever asks.
#[derive(
    Clone, PartialEq, Eq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop,
)]
pub struct SharePkg {
    pub common: SharePkgCommon,
    /// The initial group membership
    #[zeroize(skip)]
    pub initial_membership: BTreeSet<Baseboard>,

    /// Salt used for key derivation
    /// Since we use the same key for all pkgs, we share the same salt
    pub salt: [u8; 32],

    /// Nonce used for encryption.
    ///
    /// We generate all pkgs during rack init and ensure uniqueness of all nonces
    pub nonce: [u8; 12],

    /// We include a distinct subset of unused shares for each sled in the
    /// initial group. This allows the sled to hand out unique shares when a
    /// new sled joins the cluster. We keep these encrypted so that a single
    /// sled does not have enough unencrypted shares to unlock the rack
    /// without participating in trust quorum.
    ///
    /// Each sled should keep local track of which shares it has already handed
    /// out.
    ///
    /// Each sled derives the same encryption key, but uses the unique nonce
    /// for encrypting its unique shares.
    ///
    /// No need for expensive zeroizing
    #[zeroize(skip)]
    pub encrypted_shares: Vec<u8>,
}

impl SharePkg {
    pub fn decrypt_shares(
        &self,
        rack_secret: &RackSecret,
    ) -> Result<SecretBox<Vec<Vec<u8>>>, TrustQuorumError> {
        let cipher = derive_encryption_key(
            &self.common.rack_uuid,
            &rack_secret,
            &self.salt,
        );
        decrypt_shares(self.nonce, &cipher, &self.encrypted_shares)
    }
}

/// An analog to [`SharePkg`] for nodes that were added after rack
/// initialization. There are no encrypted_shares or nonces because of this.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop,
)]
pub struct LearnedSharePkg {
    pub common: SharePkgCommon,
}

/// Create a package for each sled
pub fn create_pkgs(
    rack_uuid: Uuid,
    initial_membership: BTreeSet<Baseboard>,
) -> Result<SecretBox<Vec<SharePkg>>, TrustQuorumError> {
    // There are only up to 32 sleds in a rack.
    let n = u8::try_from(initial_membership.len()).unwrap();
    let rack_secret = RackSecret::new();
    let threshold = n / 2 + 1;
    let epoch = 0;
    // We always generate 255 shares to allow new sleds to come online
    let total_shares = 255;
    let shares_per_sled = (total_shares / n) as usize;
    let shares = rack_secret.split(threshold, total_shares)?;
    let share_digests = share_digests(&shares);
    let mut salt = [0u8; 32];
    OsRng.try_fill_bytes(&mut salt).expect("fetched random bytes");
    let cipher = derive_encryption_key(&rack_uuid, &rack_secret, &salt);
    let mut pkgs = Vec::with_capacity(n as usize);
    for i in 0..n {
        // Each pkg gets a distinct subset of shares
        let mut iter = shares
            .expose_secret()
            .iter()
            .skip(i as usize * shares_per_sled)
            .take(shares_per_sled);
        let share = iter.next().unwrap();
        let plaintext_len = (shares_per_sled - 1) * SHARE_SIZE;
        let plaintext: SecretBox<[u8]> = SecretBox::new(
            iter.fold(Vec::with_capacity(plaintext_len), |mut acc, x| {
                acc.extend_from_slice(x);
                acc
            })
            .into_boxed_slice(),
        );
        let nonce = new_nonce(i);
        let encrypted_shares = cipher
            .encrypt((&nonce).into(), plaintext.expose_secret().as_ref())
            .map_err(|_| TrustQuorumError::FailedToEncrypt)?;

        let common = SharePkgCommon {
            rack_uuid,
            epoch,
            threshold,
            share: share.clone(),
            share_digests: share_digests.clone(),
        };

        let pkg = SharePkg {
            common,
            initial_membership: initial_membership.clone(),
            salt,
            nonce,
            encrypted_shares,
        };
        pkgs.push(pkg);
    }
    Ok(SecretBox::new(Box::new(pkgs)))
}

// This is a fairly standard nonce construction consisting of a random part and
// a counter.
//
// We know we only have up to 32 packages (1 for each sled), so we fill the
// first 11 bytes of the nonce with random bytes, and the last byte with
// a counter.
fn new_nonce(i: u8) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    OsRng.try_fill_bytes(&mut nonce[..11]).expect("fetched random bytes");
    nonce[11] = i;
    nonce
}

fn share_digests(shares: &SecretBox<Vec<Vec<u8>>>) -> BTreeSet<Sha3_256Digest> {
    shares
        .expose_secret()
        .iter()
        .map(|s| {
            Sha3_256Digest(Sha3_256::digest(s).as_slice().try_into().unwrap())
        })
        .collect()
}

// Return a cipher
fn derive_encryption_key(
    rack_uuid: &Uuid,
    rack_secret: &RackSecret,
    salt: &[u8; 32],
) -> ChaCha20Poly1305 {
    let prk = Hkdf::<Sha3_256>::new(
        Some(&salt[..]),
        rack_secret.expose_secret().as_bytes(),
    );

    // The "info" string is context to bind the key to its purpose
    let mut key = Zeroizing::new([0u8; 32]);
    prk.expand_multi_info(
        &[b"trust-quorum-v0-key-shares-", rack_uuid.as_ref()],
        key.as_mut(),
    )
    .unwrap();
    ChaCha20Poly1305::new(Key::from_slice(key.as_ref()))
}

// Return a set of decrypted shares, given a cipher and nonce
#[allow(unused)]
fn decrypt_shares(
    nonce: [u8; 12],
    cipher: &ChaCha20Poly1305,
    ciphertext: &[u8],
) -> Result<SecretBox<Vec<Vec<u8>>>, TrustQuorumError> {
    let plaintext = Zeroizing::new(
        cipher
            .decrypt((&nonce).into(), ciphertext)
            .map_err(|_| TrustQuorumError::FailedToDecrypt)?,
    );
    Ok(SecretBox::new(Box::new(
        plaintext.chunks(SHARE_SIZE).map(|share| share.into()).collect(),
    )))
}

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually and omit sensitive fields. This also allows us avoid printing
// large amounts of data unnecessarily.
impl fmt::Debug for SharePkgCommon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharePkgCommon")
            .field("rack_uuid", &self.rack_uuid)
            .field("epoch", &self.epoch)
            .field("threshold", &self.threshold)
            .field("share", &"Share")
            .field("share_digests", &"Digests")
            .finish()
    }
}

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually and omit sensitive fields. This also allows us avoid printing
// large amounts of data unnecessarily.
impl fmt::Debug for SharePkg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharePkg")
            .field("common", &self.common)
            .field("salt", &hex::encode(&self.salt))
            .field("nonce", &hex::encode(self.nonce))
            .field("encrypted_shares", &"Encrypted")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand08::seq::SliceRandom;
    use secrecy::ExposeSecret;

    #[test]
    fn create_packages() {
        let uuid = Uuid::new_v4();
        let initial_members: BTreeSet<Baseboard> =
            [("a", "1"), ("b", "1"), ("c", "1"), ("d", "1")]
                .iter()
                .map(|(id, model)| {
                    Baseboard::new_pc(id.to_string(), model.to_string())
                })
                .collect();
        let num_sleds = initial_members.len();
        let packages = create_pkgs(uuid, initial_members).unwrap();

        let mut threshold_of_shares = vec![];
        // Verify basic properties of each pkg
        for pkg in packages.expose_secret() {
            assert_eq!(uuid, pkg.common.rack_uuid);
            assert_eq!(0, pkg.common.epoch);
            assert_eq!(3, pkg.common.threshold); // n/2 + 1
            assert_eq!(255, pkg.common.share_digests.len());
            threshold_of_shares.push(pkg.common.share.clone());
        }

        let rack_secret =
            RackSecret::combine_shares(&threshold_of_shares).unwrap();

        let salt = packages.expose_secret()[0].salt;
        let cipher = derive_encryption_key(&uuid, &rack_secret, &salt);

        // We divide all shares among each package, and leave one of them
        // unencrypted
        let num_encrypted_shares_per_pkg = 255 / num_sleds - 1;

        // Decrypt shares for each package
        let mut decrypted_shares: Vec<Vec<u8>> = vec![];
        for pkg in packages.expose_secret() {
            let shares =
                decrypt_shares(pkg.nonce, &cipher, &pkg.encrypted_shares)
                    .unwrap();
            assert_eq!(
                num_encrypted_shares_per_pkg,
                shares.expose_secret().len()
            );
            decrypted_shares.push(pkg.common.share.clone());
            decrypted_shares.extend_from_slice(shares.expose_secret());
        }

        assert_eq!(
            decrypted_shares.len(),
            (num_encrypted_shares_per_pkg + 1) * num_sleds
        );

        let hashes = packages.expose_secret()[0].common.share_digests.clone();

        // Compute share hashes and ensure they are valid
        for share in &decrypted_shares {
            assert_eq!(share.len(), SHARE_SIZE);
            let computed = Sha3_256Digest(
                Sha3_256::digest(share).as_slice().try_into().unwrap(),
            );
            assert!(hashes.contains(&computed));
        }

        // Grab a threshold of random shares and ensure that we can recompute
        // the rack secret
        let mut rng = &mut rand08::thread_rng();
        let random_shares: Vec<_> =
            decrypted_shares.choose_multiple(&mut rng, 3).cloned().collect();
        let rack_secret2 = RackSecret::combine_shares(&random_shares).unwrap();
        assert_eq!(rack_secret, rack_secret2);
    }
}
