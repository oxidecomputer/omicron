// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use anyhow::{bail, Context, Result};
use bytesize::ByteSize;
use camino::{Utf8Path, Utf8PathBuf};
use omicron_common::api::{
    external::SemverVersion, internal::nexus::KnownArtifactKind,
};
use serde::{de::Visitor, Deserialize};

use crate::ArtifactSource;

/// A list of components in a TUF repo representing a single update.
#[derive(Clone, Debug)]
pub struct ArtifactManifest {
    pub system_version: SemverVersion,
    pub artifacts: BTreeMap<KnownArtifactKind, ArtifactData>,
}

impl ArtifactManifest {
    /// Reads a manifest in from a TOML file.
    pub fn from_path(path: &Utf8Path) -> Result<Self> {
        let input = fs_err::read_to_string(path)?;
        let base_dir = path
            .parent()
            .with_context(|| format!("path `{path}` did not have a parent"))?;
        Self::from_str(base_dir, &input)
    }

    /// Deserializes a manifest from an input string.
    pub fn from_str(base_dir: &Utf8Path, input: &str) -> Result<Self> {
        let de = toml::Deserializer::new(input);
        let manifest: DeserializedManifest =
            serde_path_to_error::deserialize(de)?;

        // Replace all paths in the deserialized manifest with absolute ones.
        Ok(ArtifactManifest {
            system_version: manifest.system_version,
            artifacts: manifest
                .artifacts
                .into_iter()
                .map(|(kind, data)| {
                    let source = match data.source {
                        DeserializedArtifactSource::File(path) => {
                            ArtifactSource::File(base_dir.join(&path))
                        }
                        DeserializedArtifactSource::Fake { size } => {
                            ArtifactSource::Fake { size }
                        }
                    };
                    let data = ArtifactData {
                        name: data.name,
                        version: data.version,
                        source,
                    };
                    (kind, data)
                })
                .collect(),
        })
    }

    /// Returns a fake manifest. Useful for testing.
    pub fn new_fake() -> Self {
        static FAKE_MANIFEST_TOML: &str =
            include_str!("../../../tufaceous/manifests/fake.toml");
        // The base directory doesn't matter for fake manifests.
        Self::from_str(".".into(), FAKE_MANIFEST_TOML)
            .expect("the fake manifest is a valid manifest")
    }

    /// Checks that all expected artifacts are present, returning an error with
    /// details if any artifacts are missing.
    pub fn verify_all_present(&self) -> Result<()> {
        let all_artifacts: BTreeSet<_> = KnownArtifactKind::iter().collect();
        let present_artifacts: BTreeSet<_> =
            self.artifacts.keys().copied().collect();

        let missing = &all_artifacts - &present_artifacts;
        if !missing.is_empty() {
            bail!(
                "manifest has missing artifacts: {}",
                itertools::join(missing, ", ")
            );
        }

        Ok(())
    }
}

/// Information about an individual artifact.
#[derive(Clone, Debug)]
pub struct ArtifactData {
    pub name: String,
    pub version: SemverVersion,
    pub source: ArtifactSource,
}

/// Deserializable version of [`ArtifactManifest`].
///
/// Since manifests require a base directory to be deserialized properly,
/// we don't expose the `Deserialize` impl on `ArtifactManifest, forcing
/// consumers to go through [`ArtifactManifest::from_path`] or
/// [`ArtifactManifest::from_str`].
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct DeserializedManifest {
    system_version: SemverVersion,
    #[serde(rename = "artifact")]
    artifacts: BTreeMap<KnownArtifactKind, DeserializedArtifactData>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct DeserializedArtifactData {
    pub name: String,
    pub version: SemverVersion,
    pub source: DeserializedArtifactSource,
}

#[derive(Clone, Debug)]
enum DeserializedArtifactSource {
    File(Utf8PathBuf),
    Fake { size: u64 },
}

impl<'de> Deserialize<'de> for DeserializedArtifactSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // This is similar to DeserializedArtifactSource except with an
        // auto-derived impl, which we'll use in visit_map below.
        #[derive(Clone, Debug, Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case")]
        enum AsMap {
            File { path: Utf8PathBuf },
            Fake { size: ByteSize },
        }

        impl From<AsMap> for DeserializedArtifactSource {
            fn from(value: AsMap) -> Self {
                match value {
                    AsMap::File { path } => Self::File(path),
                    AsMap::Fake { size } => Self::Fake { size: size.0 },
                }
            }
        }

        struct V;

        impl<'de2> Visitor<'de2> for V {
            type Value = DeserializedArtifactSource;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    formatter,
                    "a file name or a table {{ fake = true, size = 1048576 }}"
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // Currently, a string always represents a file name.
                Ok(DeserializedArtifactSource::File(v.into()))
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de2>,
            {
                AsMap::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )
                .map(DeserializedArtifactSource::from)
            }
        }

        deserializer.deserialize_any(V)
    }
}
