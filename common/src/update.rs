// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{borrow::Cow, convert::Infallible, fmt, str::FromStr};

use crate::api::{external::SemverVersion, internal::nexus::KnownArtifactKind};
use hex::FromHexError;
use schemars::{
    gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
    JsonSchema,
};
use serde::{Deserialize, Serialize};

/// Description of the `artifacts.json` target found in rack update
/// repositories.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ArtifactsDocument {
    pub system_version: SemverVersion,
    pub artifacts: Vec<Artifact>,
}

impl ArtifactsDocument {
    /// Creates an artifacts document with the provided system version and an
    /// empty list of artifacts.
    pub fn empty(system_version: SemverVersion) -> Self {
        Self { system_version, artifacts: Vec::new() }
    }
}

/// Describes an artifact available in the repository.
///
/// See also [`crate::api::internal::nexus::UpdateArtifactId`], which is used
/// internally in Nexus and Sled Agent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Artifact {
    /// Used to differentiate between different series of artifacts of the same
    /// kind. This is used by the control plane to select the correct artifact.
    ///
    /// For SP and ROT images ([`KnownArtifactKind::GimletSp`],
    /// [`KnownArtifactKind::GimletRot`], [`KnownArtifactKind::PscSp`],
    /// [`KnownArtifactKind::PscRot`], [`KnownArtifactKind::SwitchSp`],
    /// [`KnownArtifactKind::SwitchRot`]), `name` is the value of the board
    /// (`BORD`) tag in the image caboose.
    ///
    /// In the future when [`KnownArtifactKind::ControlPlane`] is split up into
    /// separate zones, `name` will be the zone name.
    pub name: String,
    pub version: SemverVersion,
    pub kind: ArtifactKind,
    pub target: String,
}

impl Artifact {
    /// Returns the artifact ID for this artifact.
    pub fn id(&self) -> ArtifactId {
        ArtifactId {
            name: self.name.clone(),
            version: self.version.clone(),
            kind: self.kind.clone(),
        }
    }

    /// Returns the artifact ID for this artifact without clones.
    pub fn into_id(self) -> ArtifactId {
        ArtifactId { name: self.name, version: self.version, kind: self.kind }
    }
}

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
    pub version: SemverVersion,

    /// The kind of artifact this is.
    pub kind: ArtifactKind,
}

/// Used for user-friendly messages.
impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} v{} ({})", self.name, self.version, self.kind)
    }
}

/// A hash-based identifier for an artifact.
///
/// Some places, e.g. the installinator, request artifacts by hash rather than
/// by name and version. This type indicates that.
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
pub struct ArtifactHashId {
    /// The kind of artifact this is.
    pub kind: ArtifactKind,

    /// The hash of the artifact.
    pub hash: ArtifactHash,
}

/// The kind of artifact we are dealing with.
///
/// To ensure older versions of Nexus can work with update repositories that
/// describe artifact kinds it is not yet aware of, this is a newtype wrapper
/// around a string. The set of known artifact kinds is described in
/// [`KnownArtifactKind`], and this type has conversions to and from it.
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
#[serde(transparent)]
pub struct ArtifactKind(Cow<'static, str>);

impl ArtifactKind {
    /// Creates a new `ArtifactKind` from a string.
    pub fn new(kind: String) -> Self {
        Self(kind.into())
    }

    /// Creates a new `ArtifactKind` from a static string.
    pub const fn from_static(kind: &'static str) -> Self {
        Self(Cow::Borrowed(kind))
    }

    /// Creates a new `ArtifactKind` from a known kind.
    pub fn from_known(kind: KnownArtifactKind) -> Self {
        Self::new(kind.to_string())
    }

    /// Returns the kind as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Converts self to a `KnownArtifactKind`, if it is known.
    pub fn to_known(&self) -> Option<KnownArtifactKind> {
        self.0.parse().ok()
    }
}

/// These artifact kinds are not stored anywhere, but are derived from stored
/// kinds and used as internal identifiers.
impl ArtifactKind {
    /// Gimlet root of trust bootloader slot image identifier.
    ///
    /// Derived from [`KnownArtifactKind::GimletRotBootloader`].
    pub const GIMLET_ROT_STAGE0: Self =
        Self::from_static("gimlet_rot_bootloader");

    /// Gimlet root of trust A slot image identifier.
    ///
    /// Derived from [`KnownArtifactKind::GimletRot`].
    pub const GIMLET_ROT_IMAGE_A: Self =
        Self::from_static("gimlet_rot_image_a");

    /// Gimlet root of trust B slot image identifier.
    ///
    /// Derived from [`KnownArtifactKind::GimletRot`].
    pub const GIMLET_ROT_IMAGE_B: Self =
        Self::from_static("gimlet_rot_image_b");

    /// PSC root of trust stage0 image identifier.
    ///
    /// Derived from [`KnownArtifactKind::PscRotBootloader`].
    pub const PSC_ROT_STAGE0: Self = Self::from_static("psc_rot_bootloader");

    /// PSC root of trust A slot image identifier.
    ///
    /// Derived from [`KnownArtifactKind::PscRot`].
    pub const PSC_ROT_IMAGE_A: Self = Self::from_static("psc_rot_image_a");

    /// PSC root of trust B slot image identifier.
    ///
    /// Derived from [`KnownArtifactKind::PscRot`].
    pub const PSC_ROT_IMAGE_B: Self = Self::from_static("psc_rot_image_b");

    /// Switch root of trust A slot image identifier.
    ///
    /// Derived from [`KnownArtifactKind::SwitchRotBootloader`].
    pub const SWITCH_ROT_STAGE0: Self =
        Self::from_static("switch_rot_bootloader");

    /// Switch root of trust A slot image identifier.
    ///
    /// Derived from [`KnownArtifactKind::SwitchRot`].
    pub const SWITCH_ROT_IMAGE_A: Self =
        Self::from_static("switch_rot_image_a");

    /// Switch root of trust B slot image identifier.
    ///
    /// Derived from [`KnownArtifactKind::SwitchRot`].
    pub const SWITCH_ROT_IMAGE_B: Self =
        Self::from_static("switch_rot_image_b");

    /// Host phase 1 identifier.
    ///
    /// Derived from [`KnownArtifactKind::Host`].
    pub const HOST_PHASE_1: Self = Self::from_static("host_phase_1");

    /// Host phase 2 identifier.
    ///
    /// Derived from [`KnownArtifactKind::Host`].
    pub const HOST_PHASE_2: Self = Self::from_static("host_phase_2");

    /// Trampoline phase 1 identifier.
    ///
    /// Derived from [`KnownArtifactKind::Trampoline`].
    pub const TRAMPOLINE_PHASE_1: Self =
        Self::from_static("trampoline_phase_1");

    /// Trampoline phase 2 identifier.
    ///
    /// Derived from [`KnownArtifactKind::Trampoline`].
    pub const TRAMPOLINE_PHASE_2: Self =
        Self::from_static("trampoline_phase_2");
}

impl From<KnownArtifactKind> for ArtifactKind {
    fn from(kind: KnownArtifactKind) -> Self {
        Self::from_known(kind)
    }
}

impl fmt::Display for ArtifactKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ArtifactKind {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s.to_owned()))
    }
}

/// The hash of an artifact.
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(transparent)]
#[cfg_attr(feature = "testing", derive(test_strategy::Arbitrary))]
pub struct ArtifactHash(
    #[serde(with = "serde_human_bytes::hex_array")]
    #[schemars(schema_with = "artifact_hash_schema")]
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

fn artifact_hash_schema(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = hex_schema::<32>(gen).into();
    schema.extensions.insert(
        "x-rust-type".into(),
        serde_json::json!({
            "crate": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "path": concat!(module_path!(), "::ArtifactHash"),
        }),
    );
    schema.into()
}

#[cfg(test)]
mod tests {
    use crate::api::internal::nexus::KnownArtifactKind;
    use crate::update::ArtifactKind;

    #[test]
    fn serde_artifact_kind() {
        assert_eq!(
            serde_json::from_str::<ArtifactKind>("\"gimlet_sp\"")
                .unwrap()
                .to_known(),
            Some(KnownArtifactKind::GimletSp)
        );
        assert_eq!(
            serde_json::from_str::<ArtifactKind>("\"fhqwhgads\"")
                .unwrap()
                .to_known(),
            None,
        );
        assert!(serde_json::from_str::<ArtifactKind>("null").is_err());

        assert_eq!(
            serde_json::to_string(&ArtifactKind::from_known(
                KnownArtifactKind::GimletSp
            ))
            .unwrap(),
            "\"gimlet_sp\""
        );
        assert_eq!(
            serde_json::to_string(&ArtifactKind::new("fhqwhgads".to_string()))
                .unwrap(),
            "\"fhqwhgads\""
        );
    }
}
