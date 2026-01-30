// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! RoT attestation types for the Sled Agent API.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use x509_cert::der::EncodePem;

const SHA3_256_LEN: usize = attest_data::Sha3_256Digest::LENGTH;

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct Sha3_256Digest(
    #[serde(with = "serde_human_bytes::hex_array")]
    #[schemars(schema_with = "omicron_common::hex_schema::<SHA3_256_LEN>")]
    pub [u8; SHA3_256_LEN],
);

impl fmt::Debug for Sha3_256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Sha3_256Digest").field(&hex::encode(self.0)).finish()
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

impl From<attest_data::Measurement> for Measurement {
    fn from(m: attest_data::Measurement) -> Self {
        match m {
            attest_data::Measurement::Sha3_256(d) => {
                Measurement::Sha3_256(Sha3_256Digest(d.0))
            }
        }
    }
}

/// The set of measurments provided by the RoT.
#[derive(Default, Deserialize, Serialize, JsonSchema)]
pub struct MeasurementLog(
    #[schemars(length(max = "attest_data::LOG_CAPACITY"))] pub Vec<Measurement>,
);

impl From<attest_data::Log> for MeasurementLog {
    fn from(log: attest_data::Log) -> Self {
        MeasurementLog(log.iter().copied().map(Into::into).collect())
    }
}

/// A chain of PEM-encoded X.509 certificates (RFC5280) that link an
/// attestation signing key to a trusted PKI root.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct CertificateChain(pub Vec<String>);

impl TryFrom<x509_cert::PkiPath> for CertificateChain {
    type Error = x509_cert::der::Error;

    fn try_from(chain: x509_cert::PkiPath) -> Result<Self, Self::Error> {
        use x509_cert::der::pem::LineEnding;
        let certs: Result<Vec<_>, _> =
            chain.into_iter().map(|cert| cert.to_pem(LineEnding::LF)).collect();
        Ok(CertificateChain(certs?))
    }
}

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

impl From<Nonce> for attest_data::Nonce {
    fn from(n: Nonce) -> Self {
        match n {
            Nonce::N32(n32) => attest_data::Nonce::N32(n32.into()),
        }
    }
}

const ED25519_SIG_LEN: usize = attest_data::Ed25519Signature::LENGTH;

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct Ed25519Signature(
    #[serde(with = "serde_human_bytes::hex_array")]
    #[schemars(schema_with = "omicron_common::hex_schema::<ED25519_SIG_LEN>")]
    pub [u8; ED25519_SIG_LEN],
);

impl fmt::Debug for Ed25519Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Ed25519Signature").field(&hex::encode(self.0)).finish()
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

impl From<attest_data::Attestation> for Attestation {
    fn from(att: attest_data::Attestation) -> Self {
        match att {
            attest_data::Attestation::Ed25519(sig) => {
                Attestation::Ed25519(Ed25519Signature(sig.0))
            }
        }
    }
}
