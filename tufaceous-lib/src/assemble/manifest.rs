// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use omicron_common::api::{
    external::SemverVersion, internal::nexus::KnownArtifactKind,
};
use parse_size::parse_size;
use serde::{Deserialize, Serialize};

use crate::{
    make_filler_text, ArtifactSource, CompositeControlPlaneArchiveBuilder,
    CompositeHostArchiveBuilder, CompositeRotArchiveBuilder,
};

static FAKE_MANIFEST_TOML: &str =
    include_str!("../../../tufaceous/manifests/fake.toml");

/// A list of components in a TUF repo representing a single update.
#[derive(Clone, Debug)]
pub struct ArtifactManifest {
    pub system_version: SemverVersion,
    pub artifacts: BTreeMap<KnownArtifactKind, Vec<ArtifactData>>,
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
        let manifest = DeserializedManifest::from_str(input)?;
        Self::from_deserialized(base_dir, manifest)
    }

    /// Creates a manifest from a [`DeserializedManifest`].
    pub fn from_deserialized(
        base_dir: &Utf8Path,
        manifest: DeserializedManifest,
    ) -> Result<Self> {
        // Replace all paths in the deserialized manifest with absolute ones,
        // and do some processing to support flexible manifests:
        //
        // 1. assemble any composite artifacts from their pieces
        // 2. replace any "fake" artifacts with in-memory buffers
        //
        // Currently both of those transformations produce
        // `ArtifactSource::Memory(_)` variants (i.e., composite and fake
        // artifacts all sit in-memory until we're done with the manifest),
        // which puts some limits on how large the inputs to the manifest can
        // practically be. If this becomes onerous, we could instead write the
        // transformed artifacts to temporary files.
        //
        // We do some additional error checking here to make sure the
        // `CompositeZZZ` variants are only used with their corresponding
        // `KnownArtifactKind`s. It would be nicer to enforce this more
        // statically and let serde do these checks, but that seems relatively
        // tricky in comparison to these checks.

        Ok(ArtifactManifest {
            system_version: manifest.system_version,
            artifacts: manifest
                .artifacts
                .into_iter()
                .map(|(kind, entries)| {
                    Self::parse_deserialized_entries(base_dir, kind, entries)
                })
                .collect::<Result<_, _>>()?,
        })
    }

    fn parse_deserialized_entries(
        base_dir: &Utf8Path,
        kind: KnownArtifactKind,
        entries: Vec<DeserializedArtifactData>,
    ) -> Result<(KnownArtifactKind, Vec<ArtifactData>)> {
        let entries = entries
            .into_iter()
            .map(|data| {
                let source = match data.source {
                    DeserializedArtifactSource::File { path } => {
                        ArtifactSource::File(base_dir.join(path))
                    }
                    DeserializedArtifactSource::Fake { size } => {
                        let fake_data = FakeDataAttributes::new(
                            &data.name,
                            kind,
                            &data.version,
                        )
                        .make_data(size as usize);
                        ArtifactSource::Memory(fake_data.into())
                    }
                    DeserializedArtifactSource::CompositeHost {
                        phase_1,
                        phase_2,
                    } => {
                        ensure!(
                            matches!(
                                kind,
                                KnownArtifactKind::Host
                                    | KnownArtifactKind::Trampoline
                            ),
                            "`composite_host` source cannot be used with \
                             artifact kind {kind:?}"
                        );

                        let mut builder =
                            CompositeHostArchiveBuilder::new(Vec::new())?;
                        phase_1.with_data(
                            FakeDataAttributes::new(
                                "fake-phase-1",
                                kind,
                                &data.version,
                            ),
                            |buf| {
                                builder
                                    .append_phase_1(buf.len(), buf.as_slice())
                            },
                        )?;
                        phase_2.with_data(
                            FakeDataAttributes::new(
                                "fake-phase-2",
                                kind,
                                &data.version,
                            ),
                            |buf| {
                                builder
                                    .append_phase_2(buf.len(), buf.as_slice())
                            },
                        )?;
                        ArtifactSource::Memory(builder.finish()?.into())
                    }
                    DeserializedArtifactSource::CompositeRot {
                        archive_a,
                        archive_b,
                    } => {
                        ensure!(
                            matches!(
                                kind,
                                KnownArtifactKind::GimletRot
                                    | KnownArtifactKind::SwitchRot
                                    | KnownArtifactKind::PscRot
                            ),
                            "`composite_rot` source cannot be used with \
                             artifact kind {kind:?}"
                        );

                        let mut builder =
                            CompositeRotArchiveBuilder::new(Vec::new())?;
                        archive_a.with_data(
                            FakeDataAttributes::new(
                                "fake-rot-archive-a",
                                kind,
                                &data.version,
                            ),
                            |buf| {
                                builder
                                    .append_archive_a(buf.len(), buf.as_slice())
                            },
                        )?;
                        archive_b.with_data(
                            FakeDataAttributes::new(
                                "fake-rot-archive-b",
                                kind,
                                &data.version,
                            ),
                            |buf| {
                                builder
                                    .append_archive_b(buf.len(), buf.as_slice())
                            },
                        )?;
                        ArtifactSource::Memory(builder.finish()?.into())
                    }
                    DeserializedArtifactSource::CompositeControlPlane {
                        zones,
                    } => {
                        ensure!(
                            kind == KnownArtifactKind::ControlPlane,
                            "`composite_control_plane` source cannot be \
                             used with artifact kind {kind:?}"
                        );

                        let data = Vec::new();
                        let mut builder =
                            CompositeControlPlaneArchiveBuilder::new(data)?;

                        for zone in zones {
                            zone.with_name_and_data(|name, data| {
                                builder.append_zone(
                                    name,
                                    data.len(),
                                    data.as_slice(),
                                )
                            })?;
                        }
                        ArtifactSource::Memory(builder.finish()?.into())
                    }
                };
                let data = ArtifactData {
                    name: data.name,
                    version: data.version,
                    source,
                };
                Ok(data)
            })
            .collect::<Result<_, _>>()?;
        Ok((kind, entries))
    }

    /// Returns a fake manifest. Useful for testing.
    pub fn new_fake() -> Self {
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

#[derive(Debug)]
struct FakeDataAttributes<'a> {
    name: &'a str,
    kind: KnownArtifactKind,
    version: &'a SemverVersion,
}

impl<'a> FakeDataAttributes<'a> {
    fn new(
        name: &'a str,
        kind: KnownArtifactKind,
        version: &'a SemverVersion,
    ) -> Self {
        Self { name, kind, version }
    }

    fn make_data(&self, size: usize) -> Vec<u8> {
        use hubtools::{CabooseBuilder, HubrisArchiveBuilder};

        let board = match self.kind {
            // non-Hubris artifacts: just make fake data
            KnownArtifactKind::Host
            | KnownArtifactKind::Trampoline
            | KnownArtifactKind::ControlPlane => return make_filler_text(size),

            // hubris artifacts: build a fake archive (SimGimletSp and
            // SimGimletRot are used by sp-sim)
            KnownArtifactKind::GimletSp => "SimGimletSp",
            KnownArtifactKind::GimletRot => "SimGimletRot",
            KnownArtifactKind::PscSp => "fake-psc-sp",
            KnownArtifactKind::PscRot => "fake-psc-rot",
            KnownArtifactKind::SwitchSp => "fake-sidecar-sp",
            KnownArtifactKind::SwitchRot => "fake-sidecar-rot",
        };

        let caboose = CabooseBuilder::default()
            .git_commit("this-is-fake-data")
            .board(board)
            .version(self.version.to_string())
            .name(self.name)
            .build();

        let mut builder = HubrisArchiveBuilder::with_fake_image();
        builder.write_caboose(caboose.as_slice()).unwrap();
        builder.build_to_vec().unwrap()
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
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DeserializedManifest {
    pub system_version: SemverVersion,
    #[serde(rename = "artifact")]
    pub artifacts: BTreeMap<KnownArtifactKind, Vec<DeserializedArtifactData>>,
}

impl DeserializedManifest {
    pub fn from_path(path: &Utf8Path) -> Result<Self> {
        let input = fs_err::read_to_string(path)?;
        Self::from_str(&input).with_context(|| {
            format!("error deserializing manifest from {path}")
        })
    }

    pub fn from_str(input: &str) -> Result<Self> {
        let de = toml::Deserializer::new(input);
        serde_path_to_error::deserialize(de)
            .context("error deserializing manifest")
    }

    /// Returns the fake manifest.
    pub fn fake() -> Self {
        Self::from_str(FAKE_MANIFEST_TOML).unwrap()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DeserializedArtifactData {
    pub name: String,
    pub version: SemverVersion,
    pub source: DeserializedArtifactSource,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DeserializedArtifactSource {
    File {
        path: Utf8PathBuf,
    },
    Fake {
        #[serde(deserialize_with = "deserialize_byte_size")]
        size: u64,
    },
    CompositeHost {
        phase_1: DeserializedFileArtifactSource,
        phase_2: DeserializedFileArtifactSource,
    },
    CompositeRot {
        archive_a: DeserializedFileArtifactSource,
        archive_b: DeserializedFileArtifactSource,
    },
    CompositeControlPlane {
        zones: Vec<DeserializedControlPlaneZoneSource>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeserializedFileArtifactSource {
    File {
        path: Utf8PathBuf,
    },
    Fake {
        #[serde(deserialize_with = "deserialize_byte_size")]
        size: u64,
    },
}

impl DeserializedFileArtifactSource {
    fn with_data<F, T>(&self, fake_attr: FakeDataAttributes, f: F) -> Result<T>
    where
        F: FnOnce(Vec<u8>) -> Result<T>,
    {
        let data = match self {
            DeserializedFileArtifactSource::File { path } => {
                std::fs::read(path)
                    .with_context(|| format!("failed to read {path}"))?
            }
            DeserializedFileArtifactSource::Fake { size } => {
                fake_attr.make_data(*size as usize)
            }
        };
        f(data)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeserializedControlPlaneZoneSource {
    File {
        path: Utf8PathBuf,
    },
    Fake {
        name: String,
        #[serde(deserialize_with = "deserialize_byte_size")]
        size: u64,
    },
}

impl DeserializedControlPlaneZoneSource {
    fn with_name_and_data<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&str, Vec<u8>) -> Result<T>,
    {
        let (name, data) = match self {
            DeserializedControlPlaneZoneSource::File { path } => {
                let data = std::fs::read(path)
                    .with_context(|| format!("failed to read {path}"))?;
                let name = path.file_name().with_context(|| {
                    format!("zone path missing file name: {path}")
                })?;
                (name, data)
            }
            DeserializedControlPlaneZoneSource::Fake { name, size } => {
                let data = make_filler_text(*size as usize);
                (name.as_str(), data)
            }
        };
        f(name, data)
    }
}

fn deserialize_byte_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Attempt to deserialize the size as either a string or an integer.

    struct Visitor;

    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = u64;

        fn expecting(
            &self,
            formatter: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            formatter
                .write_str("a string representing a byte size or an integer")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            parse_size(value).map_err(|_| {
                serde::de::Error::invalid_value(
                    serde::de::Unexpected::Str(value),
                    &self,
                )
            })
        }

        // TOML uses i64, not u64
        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value as u64)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }
    }

    deserializer.deserialize_any(Visitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ensure that the fake manifest roundtrips after serialization and
    // deserialization.
    #[test]
    fn fake_roundtrip() {
        let manifest = DeserializedManifest::fake();
        let toml = toml::to_string(&manifest).unwrap();
        let deserialized = DeserializedManifest::from_str(&toml)
            .expect("fake manifest is a valid manifest");
        assert_eq!(manifest, deserialized);
    }
}
