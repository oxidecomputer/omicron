// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for disk encryption keys used by the key-manager crate.

use secrecy::{ExposeSecret, ExposeSecretMut, SecretBox};

/// Derived Disk Encryption key
#[derive(Debug, Default)]
pub struct Aes256GcmDiskEncryptionKey(SecretBox<[u8; 32]>);

impl Aes256GcmDiskEncryptionKey {
    /// Expose the secret key bytes mutably for writing.
    ///
    /// This is intended for use by the key-manager crate during key derivation.
    pub fn expose_secret_mut(&mut self) -> &mut [u8; 32] {
        self.0.expose_secret_mut()
    }
}

/// A Disk encryption key for a given epoch to be used with ZFS datasets for
/// U.2 devices
#[derive(Debug)]
pub struct VersionedAes256GcmDiskEncryptionKey {
    epoch: u64,
    key: Aes256GcmDiskEncryptionKey,
}

impl VersionedAes256GcmDiskEncryptionKey {
    /// Create a new versioned disk encryption key.
    ///
    /// This is intended for use by the key-manager crate during key derivation.
    pub fn new(epoch: u64, key: Aes256GcmDiskEncryptionKey) -> Self {
        Self { epoch, key }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn expose_secret(&self) -> &[u8; 32] {
        &self.key.0.expose_secret()
    }
}
