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
    CompositeEntry, CompositeHostArchiveBuilder, CompositeRotArchiveBuilder,
    MtimeSource,
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
                        let fake_data =
                            FakeDataAttributes::new(kind, &data.version)
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

                        let mtime_source =
                            if phase_1.is_fake() && phase_2.is_fake() {
                                // Ensure stability of fake artifacts.
                                MtimeSource::Zero
                            } else {
                                MtimeSource::Now
                            };

                        let mut builder = CompositeHostArchiveBuilder::new(
                            Vec::new(),
                            mtime_source,
                        )?;
                        phase_1.with_entry(
                            FakeDataAttributes::new(kind, &data.version),
                            |entry| builder.append_phase_1(entry),
                        )?;
                        phase_2.with_entry(
                            FakeDataAttributes::new(kind, &data.version),
                            |entry| builder.append_phase_2(entry),
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

                        let mtime_source =
                            if archive_a.is_fake() && archive_b.is_fake() {
                                // Ensure stability of fake artifacts.
                                MtimeSource::Zero
                            } else {
                                MtimeSource::Now
                            };

                        let mut builder = CompositeRotArchiveBuilder::new(
                            Vec::new(),
                            mtime_source,
                        )?;
                        archive_a.with_entry(
                            FakeDataAttributes::new(kind, &data.version),
                            |entry| builder.append_archive_a(entry),
                        )?;
                        archive_b.with_entry(
                            FakeDataAttributes::new(kind, &data.version),
                            |entry| builder.append_archive_b(entry),
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

                        // Ensure stability of fake artifacts.
                        let mtime_source = if zones.iter().all(|z| z.is_fake())
                        {
                            MtimeSource::Zero
                        } else {
                            MtimeSource::Now
                        };

                        let data = Vec::new();
                        let mut builder =
                            CompositeControlPlaneArchiveBuilder::new(
                                data,
                                mtime_source,
                            )?;

                        for zone in zones {
                            zone.with_name_and_entry(|name, entry| {
                                builder.append_zone(name, entry)
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
    kind: KnownArtifactKind,
    version: &'a SemverVersion,
}

impl<'a> FakeDataAttributes<'a> {
    fn new(kind: KnownArtifactKind, version: &'a SemverVersion) -> Self {
        Self { kind, version }
    }

    fn make_data(&self, size: usize) -> Vec<u8> {
        use hubtools::{CabooseBuilder, HubrisArchiveBuilder};

        let board = match self.kind {
            KnownArtifactKind::GimletRotBootloader
            | KnownArtifactKind::PscRotBootloader
            | KnownArtifactKind::SwitchRotBootloader => "SimRotStage0",
            // non-Hubris artifacts: just make fake data
            KnownArtifactKind::Host
            | KnownArtifactKind::Trampoline
            | KnownArtifactKind::ControlPlane => return make_filler_text(size),

            // hubris artifacts: build a fake archive (SimGimletSp and
            // SimGimletRot are used by sp-sim)
            KnownArtifactKind::GimletSp => "SimGimletSp",
            KnownArtifactKind::GimletRot => "SimRot",
            KnownArtifactKind::PscSp => "fake-psc-sp",
            KnownArtifactKind::PscRot => "fake-psc-rot",
            KnownArtifactKind::SwitchSp => "SimSidecarSp",
            KnownArtifactKind::SwitchRot => "SimRot",
        };

        // For our purposes sign = board represents what we want for the RoT
        // and we don't care about the sign value for the SP
        // We now have an assumption that board == name for our production
        // images
        let caboose = CabooseBuilder::default()
            .git_commit("this-is-fake-data")
            .board(board)
            .version(self.version.to_string())
            .name(board)
            .sign(board)
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

    pub fn to_toml(&self) -> Result<String> {
        toml::to_string(self).context("error serializing manifest to TOML")
    }

    /// For fake manifests, applies a set of changes to them.
    ///
    /// Intended for testing.
    pub fn apply_tweaks(&mut self, tweaks: &[ManifestTweak]) -> Result<()> {
        for tweak in tweaks {
            match tweak {
                ManifestTweak::SystemVersion(version) => {
                    self.system_version = version.clone();
                }
                ManifestTweak::ArtifactVersion { kind, version } => {
                    let entries =
                        self.artifacts.get_mut(kind).with_context(|| {
                            format!(
                                "manifest does not have artifact kind \
                                 {kind}",
                            )
                        })?;
                    for entry in entries {
                        entry.version = version.clone();
                    }
                }
                ManifestTweak::ArtifactContents { kind, size_delta } => {
                    let entries =
                        self.artifacts.get_mut(kind).with_context(|| {
                            format!(
                                "manifest does not have artifact kind \
                                 {kind}",
                            )
                        })?;

                    for entry in entries {
                        entry.source.apply_size_delta(*size_delta)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns the fake manifest.
    pub fn fake() -> Self {
        Self::from_str(FAKE_MANIFEST_TOML).unwrap()
    }

    /// Returns a version of the fake manifest with a set of changes applied.
    ///
    /// This is primarily intended for testing.
    pub fn tweaked_fake(tweaks: &[ManifestTweak]) -> Self {
        let mut manifest = Self::fake();
        manifest
            .apply_tweaks(tweaks)
            .expect("builtin fake manifest should accept all tweaks");

        manifest
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

impl DeserializedArtifactSource {
    fn apply_size_delta(&mut self, size_delta: i64) -> Result<()> {
        match self {
            DeserializedArtifactSource::File { .. } => {
                bail!("cannot apply size delta to `file` source")
            }
            DeserializedArtifactSource::Fake { size } => {
                *size = (*size).saturating_add_signed(size_delta);
                Ok(())
            }
            DeserializedArtifactSource::CompositeHost { phase_1, phase_2 } => {
                phase_1.apply_size_delta(size_delta)?;
                phase_2.apply_size_delta(size_delta)?;
                Ok(())
            }
            DeserializedArtifactSource::CompositeRot {
                archive_a,
                archive_b,
            } => {
                archive_a.apply_size_delta(size_delta)?;
                archive_b.apply_size_delta(size_delta)?;
                Ok(())
            }
            DeserializedArtifactSource::CompositeControlPlane { zones } => {
                for zone in zones {
                    zone.apply_size_delta(size_delta)?;
                }
                Ok(())
            }
        }
    }
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
    fn is_fake(&self) -> bool {
        matches!(self, DeserializedFileArtifactSource::Fake { .. })
    }

    fn with_entry<F, T>(&self, fake_attr: FakeDataAttributes, f: F) -> Result<T>
    where
        F: FnOnce(CompositeEntry<'_>) -> Result<T>,
    {
        let (data, mtime_source) = match self {
            DeserializedFileArtifactSource::File { path } => {
                let data = std::fs::read(path)
                    .with_context(|| format!("failed to read {path}"))?;
                // For now, always use the current time as the source. (Maybe
                // change this to use the mtime on disk in the future?)
                (data, MtimeSource::Now)
            }
            DeserializedFileArtifactSource::Fake { size } => {
                (fake_attr.make_data(*size as usize), MtimeSource::Zero)
            }
        };
        let entry = CompositeEntry { data: &data, mtime_source };
        f(entry)
    }

    fn apply_size_delta(&mut self, size_delta: i64) -> Result<()> {
        match self {
            DeserializedFileArtifactSource::File { .. } => {
                bail!("cannot apply size delta to `file` source")
            }
            DeserializedFileArtifactSource::Fake { size } => {
                *size = (*size).saturating_add_signed(size_delta);
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeserializedControlPlaneZoneSource {
    File {
        path: Utf8PathBuf,
        #[serde(skip_serializing_if = "Option::is_none")]
        file_name: Option<String>,
    },
    Fake {
        name: String,
        #[serde(deserialize_with = "deserialize_byte_size")]
        size: u64,
    },
}

impl DeserializedControlPlaneZoneSource {
    fn is_fake(&self) -> bool {
        matches!(self, DeserializedControlPlaneZoneSource::Fake { .. })
    }

    fn with_name_and_entry<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&str, CompositeEntry<'_>) -> Result<T>,
    {
        let (name, data, mtime_source) = match self {
            DeserializedControlPlaneZoneSource::File { path, file_name } => {
                let data = std::fs::read(path)
                    .with_context(|| format!("failed to read {path}"))?;
                let name = file_name
                    .as_deref()
                    .or_else(|| path.file_name())
                    .with_context(|| {
                        format!("zone path missing file name: {path}")
                    })?;
                // For now, always use the current time as the source. (Maybe
                // change this to use the mtime on disk in the future?)
                (name, data, MtimeSource::Now)
            }
            DeserializedControlPlaneZoneSource::Fake { name, size } => {
                let data = make_filler_text(*size as usize);
                (name.as_str(), data, MtimeSource::Zero)
            }
        };
        let entry = CompositeEntry { data: &data, mtime_source };
        f(name, entry)
    }

    fn apply_size_delta(&mut self, size_delta: i64) -> Result<()> {
        match self {
            DeserializedControlPlaneZoneSource::File { .. } => {
                bail!("cannot apply size delta to `file` source")
            }
            DeserializedControlPlaneZoneSource::Fake { size, .. } => {
                (*size) = (*size).saturating_add_signed(size_delta);
                Ok(())
            }
        }
    }
}
/// A change to apply to a manifest.
#[derive(Clone, Debug)]
pub enum ManifestTweak {
    /// Update the system version.
    SystemVersion(SemverVersion),

    /// Update the versions for this artifact.
    ArtifactVersion { kind: KnownArtifactKind, version: SemverVersion },

    /// Update the contents of this artifact (only support changing the size).
    ArtifactContents { kind: KnownArtifactKind, size_delta: i64 },
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
