// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! RoT attestation types for the Sled Agent API.

use std::fmt;

use omicron_common::BytesToHexDebug;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v19;
use crate::v19::attestation::Ed25519Signature;
use crate::v19::attestation::MeasurementLog;

/// A Root of Trust (RoT) which provides measurments and signed attestations.
#[derive(Debug, Deserialize, Serialize, JsonSchema, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Rot {
    /// The per-sled RoT in an Oxide rack.
    Oxide,
    /// The RoT provided by Helios.
    Helios,
}

impl From<v19::attestation::Rot> for Rot {
    fn from(old: v19::attestation::Rot) -> Self {
        match old {
            v19::attestation::Rot::Oxide => Rot::Oxide,
        }
    }
}

/// Path parameters for RoT attestation requests.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct RotPathParams {
    /// Which RoT to use
    pub rot: Rot,
}

impl From<v19::attestation::RotPathParams> for RotPathParams {
    fn from(old: v19::attestation::RotPathParams) -> Self {
        RotPathParams { rot: old.rot.into() }
    }
}

/// The measurement log from an RoT, if it provides one.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct MeasurementLogResponse {
    /// The set of measurements recorded by the RoT, or `None` if the RoT does
    /// not provide a measurement log.
    pub log: Option<MeasurementLog>,
}

/// Error converting a [`MeasurementLogResponse`] to a prior version.
#[derive(Debug, thiserror::Error)]
#[error("the RoT did not provide a measurement log")]
pub struct MissingMeasurementLogError;

impl TryFrom<MeasurementLogResponse> for v19::attestation::MeasurementLog {
    type Error = MissingMeasurementLogError;

    fn try_from(new: MeasurementLogResponse) -> Result<Self, Self::Error> {
        new.log.ok_or(MissingMeasurementLogError)
    }
}

/// Data provided by the challenger in an attestation challenge. This value is
/// opaque to the responder & so we place no restrictions on its use. Callers
/// typically provide a random value / nonce as a means to prevent replay attacks.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum QualifyingData {
    /// 32 bytes of qualifying data.
    #[serde(with = "byte_wrapper::HexArray::<32>")]
    Q32([u8; 32]),
    /// 48 bytes of qualifying data.
    #[serde(with = "byte_wrapper::HexArray::<48>")]
    Q48([u8; 48]),
}

impl From<v19::attestation::Nonce> for QualifyingData {
    fn from(old: v19::attestation::Nonce) -> Self {
        match old {
            v19::attestation::Nonce::N32(n) => QualifyingData::Q32(n),
        }
    }
}

const P384_SIG_LEN: usize = 96;

/// An ECDSA P-384 signature.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct P384Signature(
    #[serde(with = "byte_wrapper::HexArray::<P384_SIG_LEN>")]
    pub  [u8; P384_SIG_LEN],
);

impl fmt::Debug for P384Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("P384Signature").field(&BytesToHexDebug(&self.0)).finish()
    }
}

/// An RoT produced attestation that represents a signature over the provided
/// [`QualifyingData`] (combined with the measurement log, for RoTs that provide one)
/// and signed by a key certified by the RoT's certificate chain.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Attestation {
    /// An Ed25519 signature.
    Ed25519(Ed25519Signature),
    /// An ECDSA P-384 signature.
    P384(P384Signature),
}

/// Error converting an [`Attestation`] to a prior version.
#[derive(Debug, thiserror::Error)]
#[error("P-384 attestations are not supported by this API version")]
pub struct P384AttestationError;

impl TryFrom<Attestation> for v19::attestation::Attestation {
    type Error = P384AttestationError;

    fn try_from(new: Attestation) -> Result<Self, Self::Error> {
        match new {
            Attestation::Ed25519(sig) => {
                Ok(v19::attestation::Attestation::Ed25519(sig))
            }
            Attestation::P384(_) => Err(P384AttestationError),
        }
    }
}
