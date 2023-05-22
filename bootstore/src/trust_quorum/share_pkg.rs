// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::RackSecret;
use super::TrustQuorumError;
use crate::Sha3_256Digest;
use chacha20poly1305::{aead::Aead, ChaCha20Poly1305, Key, KeyInit};
use hkdf::Hkdf;
use rand::{rngs::OsRng, RngCore};
use secrecy::Secret;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::collections::BTreeMap;
use std::fmt;
use uuid::Uuid;
use zeroize::Zeroizing;
use zeroize::{Zeroize, ZeroizeOnDrop};

// Each share is a point on a polynomial (Curve25519).  Each share is 33 bytes
// - one identifier (x-coordinate) byte, and one 32-byte y-coordinate.
const SHARE_SIZE: usize = 33;

/// Details about a `SharePkg` or analog, that can be read first to allow
/// us to know what version and structure we are dealing with.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct SharePkgHeader {
    version: u32,
    metadata: BTreeMap<String, String>,
}

#[allow(unused)]
impl SharePkgHeader {
    pub fn new_v0() -> SharePkgHeader {
        SharePkgHeader {
            version: 0,
            metadata: BTreeMap::from([("learned".into(), "false".into())]),
        }
    }

    pub fn new_v0_learned() -> SharePkgHeader {
        SharePkgHeader {
            version: 0,
            metadata: BTreeMap::from([("learned".into(), "true".into())]),
        }
    }
}

/// A container distributed among trust quorum participants for
/// trust quorum scheme version 0: [`crate::SchemeV0`].
///
/// This scheme does not verify membership, and will hand out shares
/// to whomever asks.
#[derive(Clone, PartialEq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct SharePkgV0 {
    #[zeroize(skip)]
    pub rack_uuid: Uuid,
    // We aren't planning on doing any reconfigurations with this version of
    // the protocol. This is here in case we decide otherwise, and because an
    // epoch is used for disk encryption purposes.
    pub epoch: u32,
    pub threshold: u8,
    pub share: Vec<u8>,

    /// Digests of all 256 shares so that each sled can verify the integrity
    /// of received shares.
    ///
    /// No need for expensive zeroizing
    #[zeroize(skip)]
    pub share_digests: Vec<Sha3_256Digest>,

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

/// An analog to [`SharePkgV0`] for nodes that were added after rack
/// initialization. There is no encrypted_shares or nonces because of this.
#[derive(Clone, PartialEq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct LearnedSharePkgV0 {
    #[zeroize(skip)]
    pub rack_uuid: Uuid,
    // We aren't planning on doing any reconfigurations with this version of
    // the protocol
    pub epoch: u32,
    pub threshold: u8,
    pub share: Vec<u8>,

    // Digests of all 256 shares so that each sled can verify the integrity
    // of received shares.
    //
    // No need for expensive nonsense
    #[zeroize(skip)]
    pub share_digests: Vec<Sha3_256Digest>,
}

/// Create a package for each sled
pub fn create_pkgs(
    rack_uuid: Uuid,
    n: usize,
) -> Result<Secret<Vec<SharePkgV0>>, TrustQuorumError> {
    let rack_secret = RackSecret::new();
    let threshold = n / 2 + 1;
    let epoch = 0;
    // We always generate 255 shares to allow new sleds to come online
    let total_shares = 255;
    let shares_per_sled = 255 / n;
    let shares = rack_secret.split(threshold, total_shares)?;
    let share_digests = share_digests(&shares);
    let mut salt = [0u8; 32];
    OsRng.fill_bytes(&mut salt);
    let cipher = derive_encryption_key(&rack_uuid, &rack_secret, &salt);
    let mut pkgs = Vec::with_capacity(n);
    for i in 0..n {
        // Each pkg gets a distinct subset of shares
        let mut iter =
            shares.iter().skip(i * shares_per_sled).take(shares_per_sled);
        let share = iter.next().unwrap();
        let plaintext: Vec<u8> = iter.fold(Vec::new(), |mut acc, x| {
            acc.extend_from_slice(x);
            acc
        });
        let nonce = new_nonce(i);
        let encrypted_shares = cipher
            .encrypt((&nonce).into(), plaintext.as_ref())
            .map_err(|_| TrustQuorumError::FailedToEncrypt)?;

        let pkg = SharePkgV0 {
            rack_uuid: rack_uuid.clone(),
            epoch,
            threshold: threshold.try_into().unwrap(),
            share: share.clone(),
            share_digests: share_digests.clone(),
            salt,
            nonce,
            encrypted_shares,
        };
        pkgs.push(pkg);
    }
    Ok(Secret::new(pkgs))
}

// This is a fairly standard nonce construction consisting of a random part and
// a counter.
//
// We know we only have up to 32 packages (1 for each sled), so we fill the
// first 11 bytes of the nonce with random bytes, and the last byte with
// a counter.
fn new_nonce(i: usize) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    OsRng.fill_bytes(&mut nonce[..11]);
    nonce[11] = u8::try_from(i).unwrap();
    nonce
}

fn share_digests(shares: &Vec<Vec<u8>>) -> Vec<Sha3_256Digest> {
    shares
        .iter()
        .map(|s| {
            Sha3_256Digest(Sha3_256::digest(&s).as_slice().try_into().unwrap())
        })
        .collect()
}

// Return a cipher
fn derive_encryption_key(
    rack_uuid: &Uuid,
    rack_secret: &RackSecret,
    salt: &[u8; 32],
) -> ChaCha20Poly1305 {
    let prk =
        Hkdf::<Sha3_256>::new(Some(&salt[..]), rack_secret.as_ref().as_bytes());

    // The "info" string is context to bind the key to its purpose
    let mut key = Zeroizing::new([0u8; 32]);
    prk.expand_multi_info(
        &[b"trust-quorum-v0-key-shares-", &rack_uuid.as_ref()],
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
) -> Result<Vec<Vec<u8>>, TrustQuorumError> {
    let plaintext = Zeroizing::new(
        cipher
            .decrypt((&nonce).into(), ciphertext)
            .map_err(|_| TrustQuorumError::FailedToDecrypt)?,
    );
    Ok(plaintext.chunks(SHARE_SIZE).map(|share| share.into()).collect())
}

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually and omit sensitive fields.
impl fmt::Debug for SharePkgV0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharePkgV0")
            .field("rack_uuid", &self.rack_uuid)
            .field("epoch", &self.epoch)
            .field("threshold", &self.threshold)
            .field("share", &"Share")
            .field("share_digests", &self.share_digests)
            .field("salt", &self.salt)
            .field("nonce", &self.nonce)
            .field("encrypted_shares", &"Encrypted")
            .finish()
    }
}

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually and omit sensitive fields.
impl fmt::Debug for LearnedSharePkgV0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharePkgV0")
            .field("rack_uuid", &self.rack_uuid)
            .field("epoch", &self.epoch)
            .field("threshold", &self.threshold)
            .field("share", &"Share")
            .field("share_digests", &self.share_digests)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use secrecy::ExposeSecret;

    #[test]
    fn create_packages() {
        let uuid = Uuid::new_v4();
        let num_sleds = 4;
        let packages = create_pkgs(uuid, num_sleds).unwrap();

        let mut threshold_of_shares = vec![];
        // Verify basic properties of each pkg
        for pkg in packages.expose_secret() {
            assert_eq!(uuid, pkg.rack_uuid);
            assert_eq!(0, pkg.epoch);
            assert_eq!(3, pkg.threshold); // n/2 + 1
            assert_eq!(255, pkg.share_digests.len());
            threshold_of_shares.push(pkg.share.clone());
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
            assert_eq!(num_encrypted_shares_per_pkg, shares.len());
            decrypted_shares.push(pkg.share.clone());
            decrypted_shares.extend_from_slice(&shares);
        }

        assert_eq!(
            decrypted_shares.len(),
            (num_encrypted_shares_per_pkg + 1) * num_sleds
        );

        let hashes = packages.expose_secret()[0].share_digests.clone();
        for (share, hash) in decrypted_shares.iter().zip(hashes.iter()) {
            assert_eq!(share.len(), SHARE_SIZE);

            // Ensure the hash of each share matches what is stored in hashes
            // Iteration guarantees that shares match hashes in order
            let computed_hash = Sha3_256Digest(
                Sha3_256::digest(share).as_slice().try_into().unwrap(),
            );
            assert_eq!(computed_hash, *hash);
        }

        // Grab a threshold of random shares and ensure that we can recompute
        // the rack secret
        let mut rng = &mut thread_rng();
        let random_shares: Vec<_> =
            decrypted_shares.choose_multiple(&mut rng, 3).cloned().collect();
        let rack_secret2 = RackSecret::combine_shares(&random_shares).unwrap();
        assert_eq!(rack_secret, rack_secret2);
    }
}
