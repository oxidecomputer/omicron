// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::CONTROL_PLANE_ARCHIVE_ZONE_DIRECTORY;
use super::HOST_PHASE_1_FILE_NAME;
use super::HOST_PHASE_2_FILE_NAME;
use super::ROT_ARCHIVE_A_FILE_NAME;
use super::ROT_ARCHIVE_B_FILE_NAME;
use crate::oxide_metadata;
use crate::oxide_metadata::Metadata;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use camino::Utf8Path;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;

pub struct CompositeControlPlaneArchiveBuilder<W: Write> {
    inner: CompositeTarballBuilder<W>,
}

impl<W: Write> CompositeControlPlaneArchiveBuilder<W> {
    pub fn new(writer: W) -> Result<Self> {
        let metadata = oxide_metadata::MetadataBuilder::new(
            oxide_metadata::ArchiveType::ControlPlane,
        )
        .build()
        .context("error building oxide metadata")?;
        let inner = CompositeTarballBuilder::new(writer, metadata)?;
        Ok(Self { inner })
    }

    pub fn append_zone<R: Read>(
        &mut self,
        name: &str,
        size: usize,
        data: R,
    ) -> Result<()> {
        let name_path = Utf8Path::new(name);
        if name_path.file_name() != Some(name) {
            bail!("control plane zone filenames should not contain paths");
        }
        let path =
            Utf8Path::new(CONTROL_PLANE_ARCHIVE_ZONE_DIRECTORY).join(name_path);
        self.inner.append_file(path.as_str(), size, data)
    }

    pub fn finish(self) -> Result<W> {
        self.inner.finish()
    }
}

pub struct CompositeRotArchiveBuilder<W: Write> {
    inner: CompositeTarballBuilder<W>,
}

impl<W: Write> CompositeRotArchiveBuilder<W> {
    pub fn new(writer: W) -> Result<Self> {
        let metadata = oxide_metadata::MetadataBuilder::new(
            oxide_metadata::ArchiveType::Rot,
        )
        .build()
        .context("error building oxide metadata")?;
        let inner = CompositeTarballBuilder::new(writer, metadata)?;
        Ok(Self { inner })
    }

    pub fn append_archive_a<R: Read>(
        &mut self,
        size: usize,
        data: R,
    ) -> Result<()> {
        self.inner.append_file(ROT_ARCHIVE_A_FILE_NAME, size, data)
    }

    pub fn append_archive_b<R: Read>(
        &mut self,
        size: usize,
        data: R,
    ) -> Result<()> {
        self.inner.append_file(ROT_ARCHIVE_B_FILE_NAME, size, data)
    }

    pub fn finish(self) -> Result<W> {
        self.inner.finish()
    }
}

pub struct CompositeHostArchiveBuilder<W: Write> {
    inner: CompositeTarballBuilder<W>,
}

impl<W: Write> CompositeHostArchiveBuilder<W> {
    pub fn new(writer: W) -> Result<Self> {
        let metadata = oxide_metadata::MetadataBuilder::new(
            oxide_metadata::ArchiveType::Os,
        )
        .build()
        .context("error building oxide metadata")?;
        let inner = CompositeTarballBuilder::new(writer, metadata)?;
        Ok(Self { inner })
    }

    pub fn append_phase_1<R: Read>(
        &mut self,
        size: usize,
        data: R,
    ) -> Result<()> {
        self.inner.append_file(HOST_PHASE_1_FILE_NAME, size, data)
    }

    pub fn append_phase_2<R: Read>(
        &mut self,
        size: usize,
        data: R,
    ) -> Result<()> {
        self.inner.append_file(HOST_PHASE_2_FILE_NAME, size, data)
    }

    pub fn finish(self) -> Result<W> {
        self.inner.finish()
    }
}

struct CompositeTarballBuilder<W: Write> {
    builder: tar::Builder<GzEncoder<BufWriter<W>>>,
}

impl<W: Write> CompositeTarballBuilder<W> {
    fn new(writer: W, metadata: Metadata) -> Result<Self> {
        let mut builder = tar::Builder::new(GzEncoder::new(
            BufWriter::new(writer),
            Compression::fast(),
        ));
        metadata.append_to_tar(&mut builder)?;
        Ok(Self { builder })
    }

    fn append_file<R: Read>(
        &mut self,
        path: &str,
        size: usize,
        data: R,
    ) -> Result<()> {
        let header = make_tar_header(path, size);
        self.builder
            .append(&header, data)
            .with_context(|| format!("error append {path:?}"))
    }

    fn finish(self) -> Result<W> {
        let gz_encoder =
            self.builder.into_inner().context("error finalizing archive")?;
        let buf_writer =
            gz_encoder.finish().context("error finishing gz encoder")?;
        buf_writer
            .into_inner()
            .map_err(|_| anyhow!("error flushing buffered archive writer"))
    }
}

fn make_tar_header(path: &str, size: usize) -> tar::Header {
    use std::time::{SystemTime, UNIX_EPOCH};

    let mtime = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    let mut header = tar::Header::new_ustar();
    header.set_username("root").unwrap();
    header.set_uid(0);
    header.set_groupname("root").unwrap();
    header.set_gid(0);
    header.set_path(path).unwrap();
    header.set_size(size as u64);
    header.set_mode(0o444);
    header.set_entry_type(tar::EntryType::Regular);
    header.set_mtime(mtime);
    header.set_cksum();

    header
}
