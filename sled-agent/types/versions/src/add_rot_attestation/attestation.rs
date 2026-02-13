// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! RoT attestation types for the Sled Agent API.

use std::fmt;

use omicron_common::BytesToHexDebug;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A Root of Trust (RoT) which provides measurments and signed attestations.
#[derive(Deserialize, Serialize, JsonSchema, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Rot {
    /// The per-sled RoT in an Oxide rack.
    Oxide,
}

/// Path parameters for RoT attestation requests.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct RotPathParams {
    /// Which RoT to use
    pub rot: Rot,
}

const SHA3_256_LEN: usize = 32;

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct Sha3_256Digest(
    #[serde(with = "serde_human_bytes::hex_array")]
    #[schemars(schema_with = "omicron_common::hex_schema::<SHA3_256_LEN>")]
    pub [u8; SHA3_256_LEN],
);

impl fmt::Debug for Sha3_256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Sha3_256Digest")
            .field(&BytesToHexDebug(&self.0))
            .finish()
    }
}

/// An RoT provided measurement which represents a digest of some component
/// in the trusted computing base (TCB) for the attestor.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Measurement {
    /// A SHA3-256 digest.
    Sha3_256(Sha3_256Digest),
}

/// The set of measurments provided by the RoT.
#[derive(Default, Deserialize, Serialize, JsonSchema)]
pub struct MeasurementLog(pub Vec<Measurement>);

/// A chain of PEM-encoded X.509 certificates (RFC5280) that link an
/// attestation signing key to a trusted PKI root.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct CertificateChain(pub Vec<String>);

/// A random nonce provided as part of an attestation challenge to guarantee
/// freshness thereby preventing replay attacks.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum Nonce {
    /// A 32-byte nonce.
    #[serde(with = "serde_human_bytes::hex_array")]
    #[schemars(schema_with = "omicron_common::hex_schema::<32>")]
    N32([u8; 32]),
}

const ED25519_SIG_LEN: usize = 64;

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct Ed25519Signature(
    #[serde(with = "serde_human_bytes::hex_array")]
    #[schemars(schema_with = "omicron_common::hex_schema::<ED25519_SIG_LEN>")]
    pub [u8; ED25519_SIG_LEN],
);

impl fmt::Debug for Ed25519Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Ed25519Signature")
            .field(&BytesToHexDebug(&self.0))
            .finish()
    }
}

/// An RoT produced attestation that represents a signature over the provided
/// [`Nonce`] combined with the [`MeasurementLog`] and signed by a key certified
/// by the [`CertificateChain`].
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Attestation {
    /// An Ed25519 signature.
    Ed25519(Ed25519Signature),
}
