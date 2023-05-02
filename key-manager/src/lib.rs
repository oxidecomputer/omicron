// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A crate used to derive keys useful for the Oxide control plane

use std::collections::BTreeMap;

use async_trait::async_trait;
use hkdf::Hkdf;
use secrecy::{ExposeSecret, Secret};
use sha3::Sha3_256;
use sled_hardware::DiskIdentity;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Input Key Material
///
/// This should never be used directly, and always wrapped in a `Secret<Ikm>`
/// upon construction. We sparate the two types, because a `Secret` must contain
/// `Zeroizable` data, and a `Box<[u8; 32]>` is not zeroizable on its own.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Ikm(pub Box<[u8; 32]>);

/// Secret Input Key Material for a given rack reconfiguration epoch
pub struct VersionedIkm {
    pub epoch: u64,
    pub salt: [u8; 32],
    pub ikm: Secret<Ikm>,
}

impl VersionedIkm {
    pub fn new(epoch: u64, salt: [u8; 32], data: &[u8; 32]) -> VersionedIkm {
        let ikm = Secret::new(Ikm(Box::new(*data)));
        VersionedIkm { epoch, salt, ikm }
    }
}

/// An error returned by the [`KeyManager`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Secret not loaded for {epoch}")]
    SecretNotLoaded { epoch: u64 },

    #[error("Failed to retreive secret: {0}")]
    SecretRetreival(#[from] SecretRetrieverError),
}

/// Derived Disk Encryption key
#[derive(Zeroize, ZeroizeOnDrop, Default)]
pub struct Aes256GcmDiskEncryptionKey(pub Box<[u8; 32]>);

pub struct VersionedAes256GcmDiskEncryptionKey {
    pub epoch: u64,
    pub key: Secret<Aes256GcmDiskEncryptionKey>,
}

/// The main mechanism used to derive keys from a shared secret for the Oxide
/// control plane.
pub struct KeyManager<S: SecretRetriever> {
    // A mechanism for retrieving input key material
    secret_retriever: S,

    /// Pseudo-Random-Keys (PRKs) wrapped in an `Hkdf` structure so that we can
    /// create keys from them with `HKDF-Expand` (`prk.expand_multi_info`).
    ///
    /// In the common case, we will only have a single PRK for everything.
    ///
    /// If there is an ongoing reconfiguration, we will have at least 2 PRKs. In
    /// some cases of failure while multiple reconfigurations have taken place,
    /// we may have multiple PRKs. Policy should limit the number of allowed
    /// reconfigurations before a node is no longer part of the trust quorum.
    /// This should most likely be a small number like `3`.
    prks: BTreeMap<u64, Hkdf<Sha3_256>>,
}

impl<S: SecretRetriever> KeyManager<S> {
    pub fn new(secret_retriever: S) -> KeyManager<S> {
        KeyManager { secret_retriever, prks: BTreeMap::new() }
    }

    /// Load latest version of the input key material into the key manager.
    pub async fn load_latest_secret(&mut self) -> Result<(), Error> {
        let ikm = self.secret_retriever.get_latest().await?;
        self.insert_prk(ikm);
        Ok(())
    }

    /// Load input key material for the given epoch into the key manager.
    pub async fn load_secret(&mut self, epoch: u64) -> Result<(), Error> {
        match self.secret_retriever.get(epoch).await? {
            SecretState::Current(ikm) => self.insert_prk(ikm),
            SecretState::Reconfiguration { old, new } => {
                self.insert_prk(old);
                self.insert_prk(new);
            }
        }
        Ok(())
    }

    /// Derive an encryption key for the given [`sled_hardware::DiskIdentity`]
    pub async fn disk_encryption_key(
        &mut self,
        epoch: u64,
        disk_id: &DiskIdentity,
    ) -> Result<VersionedAes256GcmDiskEncryptionKey, Error> {
        let prk = if let Some(prk) = self.prks.get(&epoch) {
            prk
        } else {
            self.load_secret(epoch).await?;
            self.prks.get(&epoch).unwrap()
        };

        let mut key = Aes256GcmDiskEncryptionKey::default();

        // Unwrap is safe because we know our buffer is large enough to hold the output key
        prk.expand_multi_info(
            &[
                b"U.2-zfs-",
                disk_id.vendor.as_bytes(),
                disk_id.model.as_bytes(),
                disk_id.serial.as_bytes(),
            ],
            key.0.as_mut(),
        )
        .unwrap();

        Ok(VersionedAes256GcmDiskEncryptionKey { epoch, key: Secret::new(key) })
    }

    /// Return the epochs for all secrets which are loaded
    pub fn loaded_secrets(&self) -> Vec<u64> {
        self.prks.keys().copied().collect()
    }

    /// Clear the PRKs
    ///
    /// TODO(AJS): From what I can tell, the internal PRKs inside
    /// Hkdf instances are not zeroized, and so will remain in memory
    /// after drop. We should fix this one way or another.
    pub fn clear(&mut self) {
        self.prks = BTreeMap::new();
    }

    fn insert_prk(&mut self, ikm: VersionedIkm) {
        let prk =
            Hkdf::new(Some(&ikm.salt), ikm.ikm.expose_secret().0.as_ref());
        self.prks.insert(ikm.epoch, prk);
    }
}

/// The current state returned from a [`SecretRetriever`]
pub enum SecretState {
    /// A reconfiguration is not ongoing
    Current(VersionedIkm),

    /// A reconfiguration is ongoing
    Reconfiguration { old: VersionedIkm, new: VersionedIkm },
}

#[derive(thiserror::Error, Debug)]
pub enum SecretRetrieverError {
    #[error("Secret does not exist for {0}")]
    NoSuchEpoch(u64),
}

/// A mechanism for retrieving a secrets to use as input key material to HKDF-
/// Extract.
#[async_trait]
pub trait SecretRetriever {
    /// Return the latest secret
    ////
    /// This is useful when a new entity is being encrypted and there is no need
    /// for a reconfiguration. When an entity is already encrypted, and needs to
    /// be decrypted, the user should instead call the [`SecretRetriever::get`].
    async fn get_latest(&self) -> Result<VersionedIkm, SecretRetrieverError>;

    /// Get the secret for the given epoch
    ///
    /// If the requested epoch is not the latest one, then return the secret for
    /// the latest epoch, along with the secret for the requested epoch so that
    /// the key can be rotated. Note that it is not necessary for the latest
    /// epoch to be exactly 1 epoch greater than the requested epoch. Multiple
    /// epochs can pass, without a reconfiguration taking place due to a node
    /// being temporarily offline.
    ///
    /// Return an error if its not possible to recover the old secret given the
    /// latest secret.
    ///
    /// TODO(AJS): Ensure that we store the epoch of the actual key protecting
    /// data in a ZFS property for each drive. This will allow us to retrieve
    /// the correct keys when needed.
    async fn get(
        &self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    pub struct TestSecretRetriever {
        ikms: BTreeMap<u64, [u8; 32]>,
    }

    impl TestSecretRetriever {
        pub fn new() -> TestSecretRetriever {
            TestSecretRetriever { ikms: BTreeMap::from([(0, [0u8; 32])]) }
        }

        pub fn insert(&mut self, epoch: u64, bytes: [u8; 32]) {
            self.ikms.insert(epoch, bytes);
        }
    }

    #[async_trait]
    impl SecretRetriever for TestSecretRetriever {
        async fn get_latest(
            &self,
        ) -> Result<VersionedIkm, SecretRetrieverError> {
            let salt = [0u8; 32];
            let (epoch, bytes) = self.ikms.last_key_value().unwrap();
            Ok(VersionedIkm::new(*epoch, salt, bytes))
        }

        async fn get(
            &self,
            epoch: u64,
        ) -> Result<SecretState, SecretRetrieverError> {
            let salt = [0u8; 32];
            let bytes = self
                .ikms
                .get(&epoch)
                .ok_or(SecretRetrieverError::NoSuchEpoch(epoch))?;
            let ikm = VersionedIkm::new(epoch, salt, bytes);
            let latest = self.get_latest().await.unwrap();
            if ikm.epoch != latest.epoch {
                Ok(SecretState::Reconfiguration { old: ikm, new: latest })
            } else {
                Ok(SecretState::Current(ikm))
            }
        }
    }

    #[tokio::test]
    async fn disk_encryption_key_epoch_0() {
        let mut km = KeyManager::new(TestSecretRetriever::new());
        km.load_latest_secret().await.unwrap();
        let disk_id = DiskIdentity {
            vendor: "a".to_string(),
            model: "b".to_string(),
            serial: "c".to_string(),
        };
        let epoch = 0;
        let key = km.disk_encryption_key(epoch, &disk_id).await.unwrap();
        assert_eq!(key.epoch, epoch);

        // Key derivation is deterministic based on disk_id and loaded secrets
        let key2 = km.disk_encryption_key(epoch, &disk_id).await.unwrap();
        assert_eq!(key.epoch, key2.epoch);
        assert_eq!(key.key.expose_secret().0, key2.key.expose_secret().0);

        // There is no secret for epoch 1
        let epoch = 1;
        assert!(km.disk_encryption_key(epoch, &disk_id).await.is_err());
        assert_eq!(vec![0], km.loaded_secrets());
    }

    #[tokio::test]
    async fn different_disks_produce_different_keys() {
        let mut km = KeyManager::new(TestSecretRetriever::new());
        km.load_latest_secret().await.unwrap();
        let id_1 = DiskIdentity {
            vendor: "a".to_string(),
            model: "b".to_string(),
            serial: "c".to_string(),
        };
        let id_2 = DiskIdentity {
            vendor: "a".to_string(),
            model: "b".to_string(),
            serial: "d".to_string(),
        };

        let epoch = 0;
        let key1 = km.disk_encryption_key(epoch, &id_1).await.unwrap();
        let key2 = km.disk_encryption_key(epoch, &id_2).await.unwrap();
        assert_eq!(key1.epoch, epoch);
        assert_eq!(key2.epoch, epoch);
        assert_ne!(key1.key.expose_secret().0, key2.key.expose_secret().0);
    }

    #[tokio::test]
    async fn different_ikm_produces_different_keys() {
        let mut retriever = TestSecretRetriever::new();

        // Load a distinct secret (IKM) for epoch 1
        retriever.insert(1, [1u8; 32]);

        let mut km = KeyManager::new(retriever);
        km.load_latest_secret().await.unwrap();
        let disk_id = DiskIdentity {
            vendor: "a".to_string(),
            model: "b".to_string(),
            serial: "c".to_string(),
        };
        let epoch = 0;
        let key0 = km.disk_encryption_key(epoch, &disk_id).await.unwrap();

        let epoch = 1;
        let key1 = km.disk_encryption_key(epoch, &disk_id).await.unwrap();
        assert_ne!(key0.key.expose_secret().0, key1.key.expose_secret().0);
    }

    #[tokio::test]
    async fn loading_key_for_old_epoch_loads_latest_epoch() {
        let mut retriever = TestSecretRetriever::new();

        // Load a distinct secret (IKM) for epoch 1
        retriever.insert(1, [1u8; 32]);

        let mut km = KeyManager::new(retriever);

        let disk_id = DiskIdentity {
            vendor: "a".to_string(),
            model: "b".to_string(),
            serial: "c".to_string(),
        };
        let epoch = 0;

        assert_eq!(0, km.loaded_secrets().len());

        // This will load secrets if they are not already loaded.
        // Note that we never called `km.load_latest_secret()`
        let _ = km.disk_encryption_key(epoch, &disk_id).await.unwrap();
        assert_eq!(2, km.loaded_secrets().len());

        // Loading just the latest secret will not load any other secrets
        km.clear();
        let epoch = 1;
        let _ = km.disk_encryption_key(epoch, &disk_id).await.unwrap();
        assert_eq!(1, km.loaded_secrets().len());
    }
}
