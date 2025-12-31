// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Cryptographic types for trust quorum.

use gfss::shamir::CombineError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::{hex::Hex, serde_as};
use slog_error_chain::SlogInlineError;

/// A SHA3-256 digest (32 bytes).
#[serde_as]
#[derive(
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[schemars(transparent)]
pub struct Sha3_256Digest(
    #[serde_as(as = "Hex")]
    #[schemars(with = "String")]
    pub [u8; 32],
);

impl std::fmt::Debug for Sha3_256Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sha3 digest: ")?;
        for v in self.0.as_slice() {
            write!(f, "{:x?}", v)?;
        }
        Ok(())
    }
}

/// Some public randomness for cryptographic operations.
#[serde_as]
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[schemars(transparent)]
pub struct Salt(
    #[serde_as(as = "Hex")]
    #[schemars(with = "String")]
    pub [u8; 32],
);

/// All possibly relevant __encrypted__ rack secrets for _prior_ committed
/// configurations.
#[serde_as]
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct EncryptedRackSecrets {
    /// A random value used to derive the key to encrypt the rack secrets for
    /// prior committed epochs.
    pub salt: Salt,
    #[serde_as(as = "Hex")]
    #[schemars(with = "String")]
    pub data: Box<[u8]>,
}

impl EncryptedRackSecrets {
    pub fn new(salt: Salt, data: Box<[u8]>) -> Self {
        EncryptedRackSecrets { salt, data }
    }
}

/// Error indicating the rack secret has an invalid size.
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
    JsonSchema,
)]
#[error("invalid rack secret size")]
pub struct InvalidRackSecretSizeError;

/// Error reconstructing a rack secret from shares.
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
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RackSecretReconstructError {
    #[error("share combine error")]
    #[schemars(with = "CombineError")]
    Combine(
        #[from]
        #[source]
        CombineError,
    ),
    #[error(transparent)]
    #[schemars(with = "InvalidRackSecretSizeError")]
    Size(#[from] InvalidRackSecretSizeError),
}

/// Error decrypting rack secrets.
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
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DecryptionError {
    /// An opaque error indicating decryption failed.
    #[error("Failed to decrypt rack secrets")]
    Aead,

    /// The length of the plaintext is not the correct size and cannot
    /// be decoded.
    #[error("Plaintext length is invalid")]
    InvalidLength,
}
