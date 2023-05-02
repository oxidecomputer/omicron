// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A crate used to derive keys useful for the Oxide control plane

use async_trait::async_trait;
use hkdf::Hkdf;
use secrecy::Secret;
use sha3::Sha3_256;
use sled_hardware::DiskIdentity;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Input Key Material
///
/// This should never be used directly, and always wrapped in a `Secret<Ikm>`
/// upon construction.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Ikm(Box<[u8; 32]>);

/// Secret Input Key Material for a given rack reconfiguration epoch
pub struct VersionedIkm {
    epoch: u64,
    ikm: Secret<Ikm>,
}

/// A derived PRK for a given rack reconfiguration epoch
pub struct VersionedPrk {
    epoch: u64,

    /// The wrapper around a pseudo-random key(PRK) created via HKDF-Extract.
    /// This key is used to derive application level keys via HKDF-Expand
    prk: Hkdf<Sha3_256>,
}

/// An error returned by the [`KeyManager`]
pub enum KeyManagerError {
    SecretNotLoaded { epoch: u64 },
}

/// Derived Disk Encryption key
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Aes256GcmDiskEncryptionKey(Box<[u8; 32]>);

pub struct VersionedAes256GcmDiskEncryptionKey {
    epoch: u64,
    key: Secret<Aes256GcmDiskEncryptionKey>,
}

/// The main mechanism used to derive keys from a shared secret for the oxide
/// control plane.
///
///
/// In the full interest of time the shared secret will be computed from trust-
/// quorum share reconstruction. However, the mechanism
pub struct KeyManager<S: SecretRetriever> {
    // A mechanism for retrieving input key material
    secret_retriever: S,

    /// In the common case, we will only have a single `prk1` for everything.
    ///
    /// If there is an ongoing reconfiguration, we will have at least 2 prks.
    /// In some cases of failure while multiple reconfigurations have taken
    /// place, we may have multiple Prks. Policy should limit the number of
    /// allowed reconfigurations before a node is no longer part of the trust
    /// quorum. This should most likely be a small number like `3`.
    prks: Vec<VersionedPrk>,
}

impl<S: SecretRetriever> KeyManager<S> {
    fn new(secret_retriever: S) -> KeyManager<S> {
        KeyManager { secret_retriever, prks: vec![] }
    }

    /// Load latest version of the input key material into the key manager.
    async fn load_latest_secret(&mut self) -> Result<(), S::Error> {
        unimplemented!()
    }

    /// Load input key material for the given epoch into the key manager.
    async fn load_secret(&mut self, epoch: u64) -> Result<(), S::Error> {
        unimplemented!()
    }

    /// Derive an encryption key for the given [`sled_hardware::DiskIdentity`]
    fn disk_encryption_key(
        &self,
        epoch: u64,
        disk_id: DiskIdentity,
    ) -> Result<VersionedAes256GcmDiskEncryptionKey, KeyManagerError> {
        unimplemented!()
    }

    /// Clear the PRKs
    fn clear(&mut self) {
        // PRKs should be zeroized on drop
        self.prks = vec![];
    }
}

/// The current state returned from a [`SecretRetriever`]
pub enum SecretState {
    /// A reconfiguration is not ongoing
    Current(VersionedIkm),

    /// A reconfiguration is ongoing
    Reconfiguration { old: VersionedIkm, new: VersionedIkm },
}

/// A mechanism for retrieving a secrets to use as input key material to HKDF-
/// Extract.
#[async_trait]
pub trait SecretRetriever {
    type Error;

    /// Return the latest secret
    ////
    /// This is useful when a new entity is being encrypted and there is no need
    /// for a reconfiguration. When an entity is already encrypted, and needs to
    /// be decrypted, the user should instead call the [`SecretRetriever::get`].
    async fn get_latest(&self) -> Result<VersionedIkm, Self::Error>;

    /// Get the secret for the given epoch
    ///
    /// If the requested epoch is not the latest one, then return the secret for
    /// the latest epoch, along with the secret for the requested epoch so that
    /// the key can be rotated. Note that is is not necessary for the latest
    /// epoch to be exactly 1 greater than the requested epoch. Multiple epochs
    /// can pass, without a reconfiguration taking place due to a node being
    /// temporarily offline.
    ///
    /// Return an error if its not possible to recover the old secret given the
    /// latest secret.
    ///
    /// TODO(AJS): Ensure that we store the epoch of the actual key protecting
    /// data in a ZFS property for each drive. This will allow us to retrieve the correct
    /// keys for rotation and as needed.

    async fn get(&self, epoch: u64) -> Result<SecretState, Self::Error>;
}
