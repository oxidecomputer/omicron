// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, str::FromStr};

use daft::Diffable;
use hex::FromHexError;
use schemars::{
    JsonSchema,
    gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
};
use serde::{Deserialize, Serialize};
use tufaceous_artifact::{Artifact, ArtifactKind, ArtifactVersion};

/// An identifier for an artifact.
///
/// The kind is [`ArtifactKind`], indicating that it might represent an artifact
/// whose kind is unknown.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct ArtifactId {
    /// The artifact's name.
    pub name: String,

    /// The artifact's version.
    pub version: ArtifactVersion,

    /// The kind of artifact this is.
    pub kind: ArtifactKind,
}

/// Used for user-friendly messages.
impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} v{} ({})", self.name, self.version, self.kind)
    }
}

impl From<Artifact> for ArtifactId {
    fn from(artifact: Artifact) -> Self {
        ArtifactId {
            name: artifact.name,
            version: artifact.version,
            kind: artifact.kind,
        }
    }
}

/// A hash-based identifier for an artifact.
///
/// Some places, e.g. the installinator, request artifacts by hash rather than
/// by name and version. This type indicates that.
#[derive(
    Debug,
    Diffable,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct ArtifactHashId {
    /// The kind of artifact this is.
    pub kind: ArtifactKind,

    /// The hash of the artifact.
    pub hash: ArtifactHash,
}

/// The hash of an artifact.
#[derive(
    Copy,
    Clone,
    Diffable,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[daft(leaf)]
#[serde(transparent)]
#[cfg_attr(feature = "testing", derive(test_strategy::Arbitrary))]
pub struct ArtifactHash(
    #[serde(with = "serde_human_bytes::hex_array")]
    #[schemars(schema_with = "hex_schema::<32>")]
    pub [u8; 32],
);

impl AsRef<[u8]> for ArtifactHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for ArtifactHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ArtifactHash").field(&hex::encode(self.0)).finish()
    }
}

impl fmt::Display for ArtifactHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl FromStr for ArtifactHash {
    type Err = FromHexError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out)?;
        Ok(Self(out))
    }
}

/// Produce an OpenAPI schema describing a hex array of a specific length (e.g.,
/// a hash digest).
pub fn hex_schema<const N: usize>(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(gen).into();
    schema.format = Some(format!("hex string ({N} bytes)"));
    schema.into()
}
