// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A crate used to derive keys useful for the Oxide control plane

use std::collections::BTreeMap;
use std::fmt::Debug;

use async_trait::async_trait;
use hkdf::Hkdf;
use secrecy::{ExposeSecret, ExposeSecretMut, SecretBox};
use sha3::Sha3_256;
use slog::{Logger, o, warn};
use tokio::sync::{mpsc, oneshot};

use omicron_common::disk::DiskIdentity;

/// Secret Input Key Material for a given rack reconfiguration epoch
pub struct VersionedIkm {
    epoch: u64,
    salt: [u8; 32],
    ikm: SecretBox<[u8; 32]>,
}

impl VersionedIkm {
    pub fn new(epoch: u64, salt: [u8; 32], data: &[u8; 32]) -> VersionedIkm {
        let ikm = SecretBox::new(Box::new(*data));
        VersionedIkm { epoch, salt, ikm }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn salt(&self) -> &[u8; 32] {
        &self.salt
    }

    pub fn expose_secret(&self) -> &[u8; 32] {
        &self.ikm.expose_secret()
    }
}

/// An error returned by the [`KeyManager`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Secret not loaded for {epoch}")]
    SecretNotLoaded { epoch: u64 },

    #[error("Failed to retrieve secret: {0}")]
    SecretRetrieval(#[from] SecretRetrieverError),
}

/// Derived Disk Encryption key
#[derive(Default)]
struct Aes256GcmDiskEncryptionKey(SecretBox<[u8; 32]>);

/// A Disk encryption key for a given epoch to be used with ZFS datasets for
/// U.2 devices
pub struct VersionedAes256GcmDiskEncryptionKey {
    epoch: u64,
    key: Aes256GcmDiskEncryptionKey,
}

impl VersionedAes256GcmDiskEncryptionKey {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn expose_secret(&self) -> &[u8; 32] {
        &self.key.0.expose_secret()
    }
}

/// A request sent from a [`StorageKeyRequester`] to the [`KeyManager`].
enum StorageKeyRequest {
    GetKey {
        epoch: u64,
        disk_id: DiskIdentity,
        responder:
            oneshot::Sender<Result<VersionedAes256GcmDiskEncryptionKey, Error>>,
    },
    LoadLatestSecret {
        responder: oneshot::Sender<Result<u64, Error>>,
    },
}

/// A client of [`KeyManager`] that can request generation of storage related keys
///
/// The StorageKeyRequester only derives `Clone` because the `HardwareMonitor`
/// requires it to get passed in and the `HardwareMonitor` gets recreated when
/// the sled-agent starts. The `HardwareMonitor` gets the StorageKeyRequester
/// from the bootstrap agent. If this changes, we should remove the `Clone` to
/// limit who has access to the storage keys.
#[derive(Debug, Clone)]
pub struct StorageKeyRequester {
    tx: mpsc::Sender<StorageKeyRequest>,
}

impl StorageKeyRequester {
    /// Get a disk encryption key from the [`KeyManager`] for the given epoch
    pub async fn get_key(
        &self,
        epoch: u64,
        disk_id: DiskIdentity,
    ) -> Result<VersionedAes256GcmDiskEncryptionKey, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageKeyRequest::GetKey { epoch, disk_id, responder: tx })
            .await
            .map_err(|e| e.to_string())
            .expect("Failed to send GetKey request to KeyManager");

        rx.await.expect("KeyManager bug (dropped responder without responding)")
    }

    /// Loads the rack secret for the latest epoch into the [`KeyManager`]
    ///
    /// Return the latest epoch on success
    pub async fn load_latest_secret(&self) -> Result<u64, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageKeyRequest::LoadLatestSecret { responder: tx })
            .await
            .map_err(|e| e.to_string())
            .expect("Failed to send LoadLatestSecret request to KeyManager");

        rx.await.expect("KeyManager bug (dropped responder without responding)")
    }
}

/// The main mechanism used to derive keys from a shared secret for the Oxide
/// control plane.
pub struct KeyManager<S: SecretRetriever> {
    // A mechanism for retrieving input key material
    secret_retriever: S,

    /// Rack configuration epochs mapped to Pseudo-Random-Keys (PRKs) wrapped
    /// in an `Hkdf` structure so that we can create keys from them with `HKDF-
    /// Expand` (`prk.expand_multi_info`).
    ///
    /// In the common case, we will only have a single PRK for everything, mapped
    /// from the latest epoch.
    ///
    /// If there is an ongoing reconfiguration, we will have at least 2 PRKs. In
    /// some cases of failure while multiple reconfigurations have taken place,
    /// we may have multiple PRKs. Policy should limit the number of allowed
    /// reconfigurations before a node is no longer part of the trust quorum.
    /// This should most likely be a small number like `3`.
    prks: BTreeMap<u64, Hkdf<Sha3_256>>,

    // Receives requests from a `StorageKeyRequester`, which is expected to run
    // in the `StorageWorker` task.
    storage_rx: mpsc::Receiver<StorageKeyRequest>,

    log: Logger,
}

impl<S: SecretRetriever> KeyManager<S> {
    pub fn new(
        log: &Logger,
        secret_retriever: S,
    ) -> (KeyManager<S>, StorageKeyRequester) {
        // There are up to 10 U.2 drives per sleds, but only one request should
        // come in at a time from a single worker. We leave a small buffer for
        // the possibility of asynchronous requests without responses that we
        // may want to send in series.
        let (tx, rx) = mpsc::channel(10);

        let storage_key_requester = StorageKeyRequester { tx };
        let key_manager = KeyManager {
            secret_retriever,
            prks: BTreeMap::new(),
            storage_rx: rx,
            log: log.new(o!("component" => "KeyManager")),
        };

        (key_manager, storage_key_requester)
    }

    /// Run the main receive loop of the `KeyManager`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(&mut self) {
        loop {
            if let Some(request) = self.storage_rx.recv().await {
                use StorageKeyRequest::*;
                match request {
                    GetKey { epoch, disk_id, responder } => {
                        let rsp =
                            self.disk_encryption_key(epoch, &disk_id).await;
                        let _ = responder.send(rsp);
                    }
                    LoadLatestSecret { responder } => {
                        let rsp = self.load_latest_secret().await;
                        let _ = responder.send(rsp);
                    }
                }
            } else {
                warn!(
                    self.log,
                    concat!(
                        "KeyManager shutting down: ",
                        "all storage key requesters dropped.",
                    )
                );
                return;
            }
        }
    }

    /// Load latest version of the input key material into the key manager.
    ///
    /// Return the latest epoch on success
    async fn load_latest_secret(&mut self) -> Result<u64, Error> {
        let ikm = self.secret_retriever.get_latest().await?;
        let epoch = ikm.epoch();
        self.insert_prk(ikm);
        Ok(epoch)
    }

    /// Load input key material for the given epoch into the key manager.
    async fn load_secret(&mut self, epoch: u64) -> Result<(), Error> {
        match self.secret_retriever.get(epoch).await? {
            SecretState::Current(ikm) => self.insert_prk(ikm),
            SecretState::Reconfiguration { old, new } => {
                self.insert_prk(old);
                self.insert_prk(new);
            }
        }
        Ok(())
    }

    /// Derive an encryption key for the given [`DiskIdentity`]
    async fn disk_encryption_key(
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
            key.0.expose_secret_mut(),
        )
        .unwrap();

        Ok(VersionedAes256GcmDiskEncryptionKey { epoch, key })
    }

    /// Return the epochs for all secrets which are loaded
    #[cfg(test)]
    fn loaded_epochs(&self) -> Vec<u64> {
        self.prks.keys().copied().collect()
    }

    /// Clear the PRKs
    ///
    /// TODO(AJS): From what I can tell, the internal PRKs inside
    /// Hkdf instances are not zeroized, and so will remain in memory
    /// after drop. We should fix this one way or another.
    ///
    /// TODO(AJS): We should have a timeout mechanism so that we can clear
    /// unused secrets from memory.
    #[allow(unused)]
    fn clear(&mut self) {
        self.prks = BTreeMap::new();
    }

    fn insert_prk(&mut self, ikm: VersionedIkm) {
        let prk = Hkdf::new(Some(ikm.salt()), ikm.expose_secret());
        self.prks.insert(ikm.epoch(), prk);
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

    #[error("Rack must be initialized for secret access")]
    RackNotInitialized,

    #[error("Bootstore error: {0}")]
    Bootstore(String),
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
    async fn get(
        &self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    pub fn log() -> slog::Logger {
        let drain = slog::Discard;
        slog::Logger::root(drain, o!())
    }

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
            if ikm.epoch() != latest.epoch() {
                Ok(SecretState::Reconfiguration { old: ikm, new: latest })
            } else {
                Ok(SecretState::Current(ikm))
            }
        }
    }

    #[tokio::test]
    async fn disk_encryption_key_epoch_0() {
        let (mut km, _) = KeyManager::new(&log(), TestSecretRetriever::new());
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
        assert_eq!(key.epoch(), key2.epoch());
        assert_eq!(key.expose_secret(), key2.expose_secret());

        // There is no secret for epoch 1
        let epoch = 1;
        assert!(km.disk_encryption_key(epoch, &disk_id).await.is_err());
        assert_eq!(vec![0], km.loaded_epochs());
    }

    #[tokio::test]
    async fn different_disks_produce_different_keys() {
        let (mut km, _) = KeyManager::new(&log(), TestSecretRetriever::new());
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
        assert_ne!(key1.expose_secret(), key2.expose_secret());
    }

    #[tokio::test]
    async fn different_ikm_produces_different_keys() {
        let mut retriever = TestSecretRetriever::new();

        // Load a distinct secret (IKM) for epoch 1
        retriever.insert(1, [1u8; 32]);

        let (mut km, _) = KeyManager::new(&log(), retriever);
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
        assert_ne!(key0.expose_secret(), key1.expose_secret());
    }

    #[tokio::test]
    async fn loading_key_for_old_epoch_loads_latest_epoch() {
        let mut retriever = TestSecretRetriever::new();

        // Load a distinct secret (IKM) for epoch 1
        retriever.insert(1, [1u8; 32]);

        let (mut km, _) = KeyManager::new(&log(), retriever);

        let disk_id = DiskIdentity {
            vendor: "a".to_string(),
            model: "b".to_string(),
            serial: "c".to_string(),
        };
        let epoch = 0;

        assert_eq!(0, km.loaded_epochs().len());

        // This will load secrets if they are not already loaded.
        // Note that we never called `km.load_latest_secret()`
        let _ = km.disk_encryption_key(epoch, &disk_id).await.unwrap();
        assert_eq!(2, km.loaded_epochs().len());

        // Loading just the latest secret will not load any other secrets
        km.clear();
        let epoch = 1;
        let _ = km.disk_encryption_key(epoch, &disk_id).await.unwrap();
        assert_eq!(1, km.loaded_epochs().len());
    }
}
