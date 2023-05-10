// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    io::{self, BufReader, Read, Write},
    path::Path,
};

use anyhow::{bail, Context, Result};
use buf_list::BufList;
use bytes::Bytes;
use camino::Utf8PathBuf;
use fs_err::File;
use omicron_common::{api::external::SemverVersion, update::ArtifactKind};

use crate::oxide_metadata;

mod composite;

pub use composite::CompositeControlPlaneArchiveBuilder;
pub use composite::CompositeHostArchiveBuilder;
pub use composite::CompositeRotArchiveBuilder;

/// The location a artifact will be obtained from.
#[derive(Clone, Debug)]
pub enum ArtifactSource {
    File(Utf8PathBuf),
    Memory(BufList),
    // We might need to support downloading data over HTTP as well
}

/// Describes a new artifact to be added.
pub struct AddArtifact {
    kind: ArtifactKind,
    name: String,
    version: SemverVersion,
    source: ArtifactSource,
}

impl AddArtifact {
    /// Creates an [`AddArtifact`] from the provided source.
    pub fn new(
        kind: ArtifactKind,
        name: String,
        version: SemverVersion,
        source: ArtifactSource,
    ) -> Self {
        Self { kind, name, version, source }
    }

    /// Creates an [`AddArtifact`] from the path, name and version.
    ///
    /// If the name is `None`, it is derived from the filename of the path
    /// without matching extensions.
    pub fn from_path(
        kind: ArtifactKind,
        name: Option<String>,
        version: SemverVersion,
        path: Utf8PathBuf,
    ) -> Result<Self> {
        let name = match name {
            Some(name) => name,
            None => path
                .file_name()
                .context("artifact path is a directory")?
                .split('.')
                .next()
                .expect("str::split has at least 1 element")
                .to_owned(),
        };

        Ok(Self { kind, name, version, source: ArtifactSource::File(path) })
    }

    /// Returns the kind of artifact this is.
    pub fn kind(&self) -> &ArtifactKind {
        &self.kind
    }

    /// Returns the name of the new artifact.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the version of the new artifact.
    pub fn version(&self) -> &SemverVersion {
        &self.version
    }

    /// Returns the source for this artifact.
    pub fn source(&self) -> &ArtifactSource {
        &self.source
    }

    /// Writes this artifact to the specified writer.
    pub(crate) fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        match &self.source {
            ArtifactSource::File(path) => {
                let mut reader = File::open(path)?;
                std::io::copy(&mut reader, writer)?;
            }
            ArtifactSource::Memory(buf_list) => {
                for chunk in buf_list {
                    writer.write_all(chunk)?;
                }
            }
        }

        Ok(())
    }
}

pub(crate) fn make_filler_text(length: usize) -> Vec<u8> {
    std::iter::repeat(FILLER_TEXT).flatten().copied().take(length).collect()
}

/// Represents host phase images.
///
/// The host and trampoline artifacts are actually tarballs, with phase 1 and
/// phase 2 images inside them. This code extracts those images out of the
/// tarballs.
#[derive(Clone, Debug)]
pub struct HostPhaseImages {
    pub phase_1: Bytes,
    pub phase_2: Bytes,
}

impl HostPhaseImages {
    pub fn extract<R: io::Read>(reader: R) -> Result<Self> {
        let uncompressed =
            flate2::bufread::GzDecoder::new(BufReader::new(reader));
        let mut archive = tar::Archive::new(uncompressed);

        let mut oxide_json_found = false;
        let mut phase_1 = None;
        let mut phase_2 = None;
        for entry in archive
            .entries()
            .context("error building list of entries from archive")?
        {
            let entry = entry.context("error reading entry from archive")?;
            let path = entry
                .header()
                .path()
                .context("error reading path from archive")?;
            if path == Path::new(OXIDE_JSON_FILE_NAME) {
                let json_bytes = read_entry(entry, OXIDE_JSON_FILE_NAME)?;
                let metadata: oxide_metadata::Metadata =
                    serde_json::from_slice(&json_bytes).with_context(|| {
                        format!(
                            "error deserializing JSON from {OXIDE_JSON_FILE_NAME}"
                        )
                    })?;
                if !metadata.is_os() {
                    bail!(
                        "unexpected archive type: expected os, found {:?}",
                        metadata.archive_type(),
                    )
                }
                oxide_json_found = true;
            } else if path == Path::new(HOST_PHASE_1_FILE_NAME) {
                phase_1 = Some(read_entry(entry, HOST_PHASE_1_FILE_NAME)?);
            } else if path == Path::new(HOST_PHASE_2_FILE_NAME) {
                phase_2 = Some(read_entry(entry, HOST_PHASE_2_FILE_NAME)?);
            }

            if oxide_json_found && phase_1.is_some() && phase_2.is_some() {
                break;
            }
        }

        let mut not_found = Vec::new();
        if !oxide_json_found {
            not_found.push(OXIDE_JSON_FILE_NAME);
        }
        if phase_1.is_none() {
            not_found.push(HOST_PHASE_1_FILE_NAME);
        }
        if phase_2.is_none() {
            not_found.push(HOST_PHASE_2_FILE_NAME);
        }
        if !not_found.is_empty() {
            bail!("required files not found: {}", not_found.join(", "))
        }

        Ok(Self { phase_1: phase_1.unwrap(), phase_2: phase_2.unwrap() })
    }
}

fn read_entry<R: io::Read>(
    mut entry: tar::Entry<R>,
    file_name: &str,
) -> Result<Bytes> {
    let entry_type = entry.header().entry_type();
    if entry_type != tar::EntryType::Regular {
        bail!("for {file_name}, expected regular file, found {entry_type:?}");
    }
    let size = entry.size();
    let mut buf = Vec::with_capacity(size as usize);
    entry
        .read_to_end(&mut buf)
        .with_context(|| format!("error reading {file_name} from archive"))?;
    Ok(buf.into())
}

/// Represents RoT A/B hubris archives.
///
/// RoT artifacts are actually tarballs, with both A and B hubris archives
/// inside them. This code extracts those archives out of the tarballs.
#[derive(Clone, Debug)]
pub struct RotArchives {
    pub archive_a: Bytes,
    pub archive_b: Bytes,
}

impl RotArchives {
    pub fn extract<R: io::Read>(reader: R) -> Result<Self> {
        let uncompressed =
            flate2::bufread::GzDecoder::new(BufReader::new(reader));
        let mut archive = tar::Archive::new(uncompressed);

        let mut oxide_json_found = false;
        let mut archive_a = None;
        let mut archive_b = None;
        for entry in archive
            .entries()
            .context("error building list of entries from archive")?
        {
            let entry = entry.context("error reading entry from archive")?;
            let path = entry
                .header()
                .path()
                .context("error reading path from archive")?;
            if path == Path::new(OXIDE_JSON_FILE_NAME) {
                let json_bytes = read_entry(entry, OXIDE_JSON_FILE_NAME)?;
                let metadata: oxide_metadata::Metadata =
                    serde_json::from_slice(&json_bytes).with_context(|| {
                        format!(
                            "error deserializing JSON from {OXIDE_JSON_FILE_NAME}"
                        )
                    })?;
                if !metadata.is_rot() {
                    bail!(
                        "unexpected archive type: expected rot, found {:?}",
                        metadata.archive_type(),
                    )
                }
                oxide_json_found = true;
            } else if path == Path::new(ROT_ARCHIVE_A_FILE_NAME) {
                archive_a = Some(read_entry(entry, ROT_ARCHIVE_A_FILE_NAME)?);
            } else if path == Path::new(ROT_ARCHIVE_B_FILE_NAME) {
                archive_b = Some(read_entry(entry, ROT_ARCHIVE_B_FILE_NAME)?);
            }

            if oxide_json_found && archive_a.is_some() && archive_b.is_some() {
                break;
            }
        }

        let mut not_found = Vec::new();
        if !oxide_json_found {
            not_found.push(OXIDE_JSON_FILE_NAME);
        }
        if archive_a.is_none() {
            not_found.push(ROT_ARCHIVE_A_FILE_NAME);
        }
        if archive_b.is_none() {
            not_found.push(ROT_ARCHIVE_B_FILE_NAME);
        }
        if !not_found.is_empty() {
            bail!("required files not found: {}", not_found.join(", "))
        }

        Ok(Self {
            archive_a: archive_a.unwrap(),
            archive_b: archive_b.unwrap(),
        })
    }
}

static FILLER_TEXT: &[u8; 16] = b"tufaceousfaketxt";
static OXIDE_JSON_FILE_NAME: &str = "oxide.json";
static HOST_PHASE_1_FILE_NAME: &str = "image/rom";
static HOST_PHASE_2_FILE_NAME: &str = "image/zfs.img";
static ROT_ARCHIVE_A_FILE_NAME: &str = "archive-a.zip";
static ROT_ARCHIVE_B_FILE_NAME: &str = "archive-b.zip";
static CONTROL_PLANE_ARCHIVE_ZONE_DIRECTORY: &str = "zones";
