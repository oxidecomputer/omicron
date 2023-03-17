// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
};

use anyhow::{bail, Context, Result};
use buf_list::BufList;
use bytes::Bytes;
use camino::Utf8PathBuf;
use flate2::Compression;
use fs_err::File;
use omicron_common::{
    api::{external::SemverVersion, internal::nexus::KnownArtifactKind},
    update::ArtifactKind,
};
use serde::{Deserialize, Serialize};

/// The location a artifact will be obtained from.
#[derive(Clone, Debug)]
pub enum ArtifactSource {
    File(Utf8PathBuf),
    Memory(BufList),
    Fake { size: u64 },
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
            ArtifactSource::Fake { size } => {
                let known = self.kind.to_known();
                if known == Some(KnownArtifactKind::Host)
                    || known == Some(KnownArtifactKind::Trampoline)
                {
                    write_host_tarball_fake_artifact(*size, writer)?;
                } else {
                    write_generic_fake_artifact(*size, writer)?;
                }
            }
        }

        Ok(())
    }
}

/// Writes a fake artifact with no internal structure.
fn write_generic_fake_artifact<W: Write>(
    size: u64,
    writer: &mut W,
) -> io::Result<()> {
    let mut buf_writer = BufWriter::new(writer);
    // Don't need to get the size exactly right, capping to the nearest 16 is fine.
    let times = (size as usize) / FILLER_TEXT.len();
    for _ in 0..times {
        buf_writer.write_all(FILLER_TEXT)?;
    }
    buf_writer.flush()
}

/// Writes a fake artifact that looks like a host or trampoline tarball.
fn write_host_tarball_fake_artifact<W: Write>(
    size: u64,
    writer: &mut W,
) -> Result<()> {
    let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
        BufWriter::new(writer),
        Compression::fast(),
    ));

    let times = (size as usize) / FILLER_TEXT.len();
    let phase_1_times = times / 8;
    let phase_2_times = times - phase_1_times;

    {
        let oxide_metadata_json =
            serde_json::to_string(&OxideMetadata::os_metadata())
                .expect("oxide metadata should serialize correctly");
        let mut header = tar::Header::new_gnu();
        header.set_path(OXIDE_JSON_FILE_NAME).unwrap();
        header.set_size(oxide_metadata_json.len() as u64);
        header.set_cksum();

        builder
            .append(&header, oxide_metadata_json.as_bytes())
            .with_context(|| format!("error writing {OXIDE_JSON_FILE_NAME}"))?;
    }

    {
        let mut header = tar::Header::new_gnu();
        header.set_path(PHASE_1_FILE_NAME).unwrap();
        header.set_size((phase_1_times * FILLER_TEXT.len()) as u64);
        header.set_cksum();

        builder
            .append(&header, FillerReader::new(phase_1_times))
            .with_context(|| format!("error writing `{PHASE_1_FILE_NAME}`"))?;
    }

    {
        let mut header = tar::Header::new_gnu();
        header.set_path(PHASE_2_FILE_NAME).unwrap();
        header.set_size((phase_2_times * FILLER_TEXT.len()) as u64);
        header.set_cksum();

        builder
            .append(&header, FillerReader::new(phase_2_times))
            .with_context(|| format!("error writing `{PHASE_1_FILE_NAME}`"))?;
    }

    let gz_encoder =
        builder.into_inner().context("error finalizing archive")?;
    let buf_writer =
        gz_encoder.finish().context("error finishing gz encoder")?;
    buf_writer
        .into_inner()
        .map_err(|_| anyhow::anyhow!("error flushing archive writer"))?;

    Ok(())
}

/// A simple `Read` implementation used to generate filler text.
///
/// This is used by [`write_host_tarball_fake_artifact`] above. Ideally, we'd
/// just be able to write the filler text directly to some sort of handle
/// provided by tar. However, `tar::Builder` only exposes `append` methods that
/// take a `Read` impl, so instead we hand-write a `Read` impl that achieves the
/// same goal.
///
/// This is really a bug in upstream tar, since providing a writer is more
/// generic than providing a reader (you can always use `std::io::copy` to copy
/// data from a reader to a writer). The bug is tracked at
/// https://github.com/alexcrichton/tar-rs/issues/304.
struct FillerReader {
    remaining_times: usize,
    // Current position within the text.
    pos: usize,
}

impl FillerReader {
    fn new(times: usize) -> Self {
        Self { remaining_times: times, pos: 0 }
    }
}

impl io::Read for FillerReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining_times == 0 {
            // Signal the end of the reader.
            return Ok(0);
        }

        let bytes_to_write = (FILLER_TEXT.len() - self.pos).min(buf.len());
        buf[..bytes_to_write].copy_from_slice(
            &FILLER_TEXT[self.pos..(self.pos + bytes_to_write)],
        );

        if self.pos + bytes_to_write == FILLER_TEXT.len() {
            self.remaining_times -= 1;
            self.pos = 0;
        }

        Ok(bytes_to_write)
    }
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
                let oxide_json: OxideMetadata =
                    serde_json::from_slice(&json_bytes).with_context(|| {
                        format!(
                        "error deserializing JSON from {OXIDE_JSON_FILE_NAME}"
                    )
                    })?;

                let expected = OxideMetadata::os_metadata();
                if oxide_json != expected {
                    // The metadata didn't match.
                    bail!(
                        "unexpected metadata for {OXIDE_JSON_FILE_NAME}:\
                         expected {expected:?}, found {oxide_json:?}",
                    );
                }
                oxide_json_found = true;
            } else if path == Path::new(PHASE_1_FILE_NAME) {
                phase_1 = Some(read_entry(entry, PHASE_1_FILE_NAME)?);
            } else if path == Path::new(PHASE_2_FILE_NAME) {
                phase_2 = Some(read_entry(entry, PHASE_2_FILE_NAME)?);
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
            not_found.push(PHASE_1_FILE_NAME);
        }
        if phase_2.is_none() {
            not_found.push(PHASE_2_FILE_NAME);
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
    let size = entry.size();
    let mut buf = Vec::with_capacity(size as usize);
    entry
        .read_to_end(&mut buf)
        .with_context(|| format!("error reading {file_name} from archive"))?;
    Ok(buf.into())
}

static FILLER_TEXT: &[u8; 16] = b"tufaceousfaketxt";
static OXIDE_JSON_FILE_NAME: &str = "oxide.json";
static PHASE_1_FILE_NAME: &str = "image/rom";
static PHASE_2_FILE_NAME: &str = "image/zfs.img";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct OxideMetadata {
    v: String,
    t: String,
}

impl OxideMetadata {
    fn os_metadata() -> Self {
        Self { v: "1".to_owned(), t: "os".to_owned() }
    }
}
