// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanics for interacting with the OS phase 2 images stored on M.2
//! partitions.

use crate::InternalDisks;
use crate::ResolverStatusExt;
use crate::SledAgentArtifactStore;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use installinator_common::RawDiskWriter;
use nexus_sled_agent_shared::inventory::BootPartitionContents as BootPartitionContentsInventory;
use nexus_sled_agent_shared::inventory::BootPartitionDetails;
use nexus_sled_agent_shared::inventory::HostPhase2DesiredContents;
use nexus_sled_agent_shared::inventory::HostPhase2DesiredSlots;
use omicron_common::disk::M2Slot;
use sled_agent_types::zone_images::ResolverStatus;
use sled_hardware::PooledDiskError;
use slog::Logger;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::io;
use std::io::BufRead as _;
use std::io::BufReader;
use std::io::Read as _;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tufaceous_artifact::ArtifactHash;

#[derive(Debug, thiserror::Error)]
#[error("boot disk not found")]
pub struct BootDiskNotFound;

#[derive(Debug, thiserror::Error)]
pub enum BootPartitionError {
    #[error("no disk found in this slot")]
    NoDiskInSlot,
    #[error("could not determine raw devfs path")]
    DetermineDevfsPath(#[source] Arc<PooledDiskError>),
    #[error("failed opening disk at {path}")]
    OpenDevfs {
        path: Utf8PathBuf,
        #[source]
        err: io::Error,
    },
    #[error("failed fetching disk's extended media info at {path}")]
    MediaInfoExtended {
        path: Utf8PathBuf,
        #[source]
        err: io::Error,
    },
    #[error("failed reading image header at {path}")]
    ReadImageHeader {
        path: Utf8PathBuf,
        #[source]
        err: io::Error,
    },
    #[error("failed parsing image header at {path}")]
    ParseImageHeader {
        path: Utf8PathBuf,
        #[source]
        err: ImageHeaderParseError,
    },
    #[error("failed reading image contents at {path} offset {offset}")]
    ReadImageContents {
        path: Utf8PathBuf,
        offset: usize,
        #[source]
        err: io::Error,
    },
    #[error(
        "sha256 of image contents don't match header at {path}; \
         expected {expected} but got {got}"
    )]
    Sha256Mismatch {
        path: Utf8PathBuf,
        // These aren't really artifact hashes, exactly, but we use that type
        // here to get the nice Display impl
        expected: ArtifactHash,
        got: ArtifactHash,
    },
    #[error("failed to open {path} for writing")]
    OpenForWriting {
        path: Utf8PathBuf,
        #[source]
        err: io::Error,
    },
    #[error("could not find desired artifact {desired}")]
    MissingDesiredArtifact {
        desired: ArtifactHash,
        #[source]
        err: anyhow::Error,
    },
    #[error("failed to write to path {path}")]
    Write {
        path: Utf8PathBuf,
        #[source]
        err: io::Error,
    },
    #[error("failed to finalize writes to path {path}")]
    FinalizeWrite {
        path: Utf8PathBuf,
        #[source]
        err: io::Error,
    },
}

#[derive(Debug, Default)]
pub(crate) struct BootPartitionReconciler {
    // Any time the config reconciler runs, we want to report the contents of
    // our boot partitions, which requires reading and hashing a full OS image.
    // This isn't super expensive, but does take a handful of seconds, and it's
    // really easy for us to cache previous results.
    //
    // Cache invalidation is hard; this assumes:
    //
    // * What disk we booted from never changes; once we've read it once, we
    //   know it forever.
    // * We are the only entity writing to the boot partition. (Therefore, if
    //   we've successfully read the current contents and we haven't changed
    //   them, we can return our cached value.)
    cached_boot_disk: Option<M2Slot>,
    cached_slot_a: Option<BootPartitionDetails>,
    cached_slot_b: Option<BootPartitionDetails>,
}

impl BootPartitionReconciler {
    pub(crate) async fn reconcile<T: SledAgentArtifactStore>(
        &mut self,
        resolver_status: &ResolverStatus,
        internal_disks: &InternalDisks,
        desired: &HostPhase2DesiredSlots,
        artifact_store: &T,
        log: &Logger,
    ) -> BootPartitionContents {
        let prepared_slot_a = resolver_status.prepare_host_phase_2_contents(
            &log.new(o!("slot" => "A")),
            &desired.slot_a,
        );
        let prepared_slot_b = resolver_status.prepare_host_phase_2_contents(
            &log.new(o!("slot" => "B")),
            &desired.slot_b,
        );

        let (slot_a, slot_b) = futures::join!(
            Self::reconcile_slot::<_, RawDiskReader, RawDiskWriter>(
                M2Slot::A,
                internal_disks,
                &mut self.cached_slot_a,
                prepared_slot_a.desired_contents(),
                artifact_store,
                log,
            ),
            Self::reconcile_slot::<_, RawDiskReader, RawDiskWriter>(
                M2Slot::B,
                internal_disks,
                &mut self.cached_slot_b,
                prepared_slot_b.desired_contents(),
                artifact_store,
                log,
            ),
        );
        BootPartitionContents {
            boot_disk: self.determine_boot_disk(internal_disks),
            slot_a,
            slot_b,
        }
    }

    fn determine_boot_disk(
        &mut self,
        internal_disks: &InternalDisks,
    ) -> Result<M2Slot, BootDiskNotFound> {
        if let Some(slot) = self.cached_boot_disk {
            return Ok(slot);
        }
        self.cached_boot_disk = internal_disks.boot_disk_slot();
        self.cached_boot_disk.ok_or(BootDiskNotFound)
    }

    async fn reconcile_slot<
        T: SledAgentArtifactStore,
        R: DiskReader,
        W: DiskWriter,
    >(
        slot: M2Slot,
        internal_disks: &InternalDisks,
        cache: &mut Option<BootPartitionDetails>,
        desired: &HostPhase2DesiredContents,
        artifact_store: &T,
        log: &Logger,
    ) -> Result<BootPartitionDetails, BootPartitionError> {
        // If we don't know what's there, try to read it first.
        if cache.is_none() {
            info!(
                log,
                "reading M.2 slot to determine current contents";
                "slot" => ?slot,
            );
            match R::read(slot, internal_disks).await {
                Ok(details) => {
                    *cache = Some(details);
                }
                Err(err) => {
                    // We couldn't read this slot; if we have a target we want
                    // to write, that's okay and we'll fall through. If we
                    // don't, there's nothing else we can do here.
                    warn!(
                        log,
                        "failed to read M.2 slot contents";
                        "slot" => ?slot,
                        InlineErrorChain::new(&err),
                    );
                    match desired {
                        HostPhase2DesiredContents::CurrentContents => {
                            return Err(err);
                        }
                        HostPhase2DesiredContents::Artifact { .. } => (),
                    }
                }
            }
        }

        // See if we need to write: does what we have already match what it's
        // supposed to be?
        match (&cache, desired) {
            // Two common cases: we know what's there, and either don't have a
            // desired target or it already matches what we have. In both cases,
            // we can return our cached details.
            (Some(details), HostPhase2DesiredContents::CurrentContents) => {
                Ok(details.clone())
            }
            (Some(details), HostPhase2DesiredContents::Artifact { hash })
                if details.artifact_hash == *hash =>
            {
                Ok(details.clone())
            }
            // Less common: we need to write the desired artifact to this slot.
            // It doesn't matter whether we failed to read the slot (hopefully
            // because it doesn't have an image - if read failed due to I/O
            // problems, our write will probably fail too, but we can at least
            // try) or whether we read the slot and the contents don't match.
            (_, HostPhase2DesiredContents::Artifact { hash }) => {
                Self::write_artifact::<_, R, W>(
                    slot,
                    internal_disks,
                    cache,
                    *hash,
                    artifact_store,
                    log,
                )
                .await
            }
            // Impossible: this case is handled above. If we have no cached
            // details, we must have failed to read the slot above, and if we
            // aren't supposed to write anything, we would have returned the
            // error we got from trying to read.
            (None, HostPhase2DesiredContents::CurrentContents) => {
                unreachable!("covered by read case above")
            }
        }
    }

    async fn write_artifact<
        T: SledAgentArtifactStore,
        R: DiskReader,
        W: DiskWriter,
    >(
        slot: M2Slot,
        internal_disks: &InternalDisks,
        cache: &mut Option<BootPartitionDetails>,
        desired: ArtifactHash,
        artifact_store: &T,
        log: &Logger,
    ) -> Result<BootPartitionDetails, BootPartitionError> {
        info!(
            log,
            "attempting to write artifact to M.2 slot";
            "slot" => ?slot,
            "artifact" => %desired,
        );
        let artifact =
            artifact_store.get_artifact(desired).await.map_err(|err| {
                BootPartitionError::MissingDesiredArtifact { desired, err }
            })?;
        let mut reader = tokio::io::BufReader::new(artifact);

        let path = match internal_disks.boot_image_raw_devfs_path(slot) {
            Some(Ok(path)) => path,
            Some(Err(err)) => {
                return Err(BootPartitionError::DetermineDevfsPath(err));
            }
            None => return Err(BootPartitionError::NoDiskInSlot),
        };

        let mut writer = W::open(&path).await.map_err(|err| {
            BootPartitionError::OpenForWriting { path: path.clone(), err }
        })?;

        // We're about to write to the disk; invalidate our cache of what it
        // contains.
        *cache = None;

        tokio::io::copy(&mut reader, &mut writer).await.map_err(|err| {
            BootPartitionError::Write { path: path.clone(), err }
        })?;

        // Ensure the writes are sync'd to disk.
        writer
            .finalize()
            .await
            .map_err(|err| BootPartitionError::FinalizeWrite { path, err })?;

        // Re-read the disk we just wrote; this both gets us the metadata
        // describing it and confirms we wrote a valid image.
        info!(
            log,
            "re-reading M.2 slot we just successfully wrote";
            "slot" => ?slot,
        );
        let details = R::read(slot, internal_disks).await?;

        // Check whether the artifact we just wrote is the one we think we
        // should have. This failing would be _extremely_ strange. If we have
        // `details`, we successfully read and parsed a disk image, and we just
        // wrote a disk image we got from the artifact store with the hash
        // `desired`. We don't need to fail here, though; if we have a mismatch
        // but have a valid boot image, we can still report that valid disk
        // image, and upstack software will recognize that it doesn't match what
        // it wanted.
        if details.artifact_hash != desired {
            warn!(
                log,
                "successfully wrote and reread boot image, but got unexpected \
                 artifact hash during reread";
                "slot" => ?slot,
                "expected-hash" => %desired,
                "computed-hash" => %details.artifact_hash,
            );
        }

        *cache = Some(details.clone());
        Ok(details)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostPhase2PreparedContents<'a> {
    /// No mupdate override was found, so the desired host phase 2 contents were
    /// used.
    NoMupdateOverride(&'a HostPhase2DesiredContents),

    /// A mupdate override was found, so the contents are always set to
    /// `CurrentContents`.
    WithMupdateOverride,
}

impl<'a> HostPhase2PreparedContents<'a> {
    pub fn desired_contents(&self) -> &'a HostPhase2DesiredContents {
        match self {
            Self::NoMupdateOverride(contents) => contents,
            Self::WithMupdateOverride => {
                &HostPhase2DesiredContents::CurrentContents
            }
        }
    }
}

/// Traits to allow unit tests to run without accessing real raw disks; these
/// are trivially implemented by the real types below and by unit tests inside
/// the test module.
trait DiskWriter {
    type F: DiskWriterFile + Unpin;

    async fn open(path: &Utf8Path) -> io::Result<Self::F>;
}

impl DiskWriter for RawDiskWriter {
    type F = RawDiskWriter;

    async fn open(path: &Utf8Path) -> io::Result<Self::F> {
        RawDiskWriter::open(path.as_std_path()).await
    }
}

trait DiskWriterFile: AsyncWrite {
    async fn finalize(self) -> io::Result<()>;
}

impl DiskWriterFile for RawDiskWriter {
    async fn finalize(self) -> io::Result<()> {
        RawDiskWriter::finalize(self).await
    }
}

trait DiskReader {
    async fn read(
        slot: M2Slot,
        internal_disks: &InternalDisks,
    ) -> Result<BootPartitionDetails, BootPartitionError>;
}

struct RawDiskReader;

impl DiskReader for RawDiskReader {
    async fn read(
        slot: M2Slot,
        internal_disks: &InternalDisks,
    ) -> Result<BootPartitionDetails, BootPartitionError> {
        boot_partition_details::read(slot, internal_disks).await
    }
}

#[derive(Debug)]
pub(crate) struct BootPartitionContents {
    pub(crate) boot_disk: Result<M2Slot, BootDiskNotFound>,
    pub(crate) slot_a: Result<BootPartitionDetails, BootPartitionError>,
    pub(crate) slot_b: Result<BootPartitionDetails, BootPartitionError>,
}

impl BootPartitionContents {
    pub(crate) fn into_inventory(self) -> BootPartitionContentsInventory {
        let err_to_string = |err: &dyn std::error::Error| {
            InlineErrorChain::new(err).to_string()
        };
        BootPartitionContentsInventory {
            boot_disk: self.boot_disk.map_err(|e| err_to_string(&e)),
            slot_a: self.slot_a.map_err(|e| err_to_string(&e)),
            slot_b: self.slot_b.map_err(|e| err_to_string(&e)),
        }
    }
}

// These would be methods on `BootPartitionDetails` if we defined it,
// but it's defined in `nexus_sled_agent_shared`. Use a module instead.
mod boot_partition_details {
    use super::*;
    use illumos_utils::dkio::MediaInfoExtended;
    use sha2::Digest as _;
    use std::cmp;
    use std::fs::File;
    use std::os::fd::AsRawFd as _;

    pub(super) async fn read(
        slot: M2Slot,
        internal_disks: &InternalDisks,
    ) -> Result<BootPartitionDetails, BootPartitionError> {
        match internal_disks.boot_image_raw_devfs_path(slot) {
            Some(Ok(path)) => {
                tokio::task::spawn_blocking(|| read_blocking(path))
                    .await
                    .expect("read_blocking() did not panic")
            }
            Some(Err(err)) => Err(BootPartitionError::DetermineDevfsPath(err)),
            None => Err(BootPartitionError::NoDiskInSlot),
        }
    }

    fn read_blocking(
        path: Utf8PathBuf,
    ) -> Result<BootPartitionDetails, BootPartitionError> {
        const ONE_MIB: usize = 1024 * 1024;

        let f = File::open(&path).map_err(|err| {
            BootPartitionError::OpenDevfs { path: path.clone(), err }
        })?;

        // Determine the disk's block size.
        let mut block_size = MediaInfoExtended::from_fd(f.as_raw_fd())
            .map_err(|err| BootPartitionError::MediaInfoExtended {
                path: path.clone(),
                err,
            })?
            .logical_block_size as usize;

        // We expect a block_size of 512 or 4096 in practice, but that's a
        // pretty small amount to read at once. If we have a block size that
        // evenly divides 1 MiB, bump up to that. (Going larger doesn't seem to
        // matter much on the disks we have at hand when this is written, and
        // this isn't a performance-criticial path anyway.)
        //
        // Alternatively, guard against something going very wrong in
        // `MediaInfoExtended`; we'll allocate a buffer of this size when we
        // read, so if we get back something wild as the logical block size,
        // we'll assume that's wrong and cap it at 128 MiB.
        if block_size < ONE_MIB && ONE_MIB % block_size == 0 {
            block_size = ONE_MIB;
        } else if block_size > 128 * ONE_MIB {
            block_size = 128 * ONE_MIB;
        }

        read_blocking_with_buf_size(
            &mut BufReaderExactSize::with_capacity(block_size, f),
            path,
        )
    }

    // This is separated from `read_blocking()` so we can write unit tests over
    // this function without needing real disks that respond to the
    // `MediaInfoExtended` ioctl.
    //
    // This is `pub(super)` only so it can be tested.
    pub(super) fn read_blocking_with_buf_size<R: io::Read>(
        f: &mut BufReaderExactSize<R>,
        path: Utf8PathBuf,
    ) -> Result<BootPartitionDetails, BootPartitionError> {
        // Compute two SHA256 hashes as we read the contents of this boot image.
        // The `image_header` contains a sha256 of the data _after_ the header
        // itself, but the artifact hash that lands in the TUF repo depo
        // _includes_ the header. Compute both here: we'll validate the data
        // hash against the header, and report the artifact hash.
        let mut artifact_hasher = sha2::Sha256::new();
        let mut data_hasher = sha2::Sha256::new();

        // Read the image header and accumulate it into artifact_hasher but not
        // data_hasher.
        let image_header = {
            let mut buf = [0; boot_image_header::SIZE];
            f.read_exact(&mut buf).map_err(|err| {
                BootPartitionError::ReadImageHeader { path: path.clone(), err }
            })?;
            let header = boot_image_header::parse(&buf).map_err(|err| {
                BootPartitionError::ParseImageHeader { path: path.clone(), err }
            })?;
            artifact_hasher.update(&buf);
            header
        };

        let mut nleft = image_header.data_size as usize;
        let mut offset = boot_image_header::SIZE;
        let artifact_size = nleft + offset;
        while nleft > 0 {
            // Read the rest of the image in block-sized chunks by filling the
            // underlying `BufReader`'s buffer and consuming it.
            let buf = f.fill_buf().map_err(|err| {
                BootPartitionError::ReadImageContents {
                    path: path.clone(),
                    offset,
                    err,
                }
            })?;

            // Our last block may have too much data; only hash to the end of
            // the image.
            let nread = buf.len();
            let nvalid = cmp::min(nleft, nread);

            artifact_hasher.update(&buf[..nvalid]);
            data_hasher.update(&buf[..nvalid]);

            nleft -= nvalid;
            offset += nvalid;
            f.consume(nread);
        }

        let artifact_hash = artifact_hasher.finalize();
        let data_hash: [u8; 32] = data_hasher.finalize().into();

        if image_header.sha256 != data_hash {
            return Err(BootPartitionError::Sha256Mismatch {
                path,
                expected: ArtifactHash(image_header.sha256),
                got: ArtifactHash(data_hash),
            });
        }

        Ok(BootPartitionDetails {
            artifact_hash: ArtifactHash(artifact_hash.into()),
            artifact_size,
            header: image_header,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ImageHeaderParseError {
    #[error("not enough data to parse header")]
    TooSmall,
    #[error("bad magic (expected {expected:#x}, got {got:#x})")]
    BadMagic { expected: u32, got: u32 },
    #[error("bad version (expected {expected}, got {got})")]
    BadVersion { expected: u32, got: u32 },
    #[error(
        "image size ({image_size}) is greater than target size ({target_size})"
    )]
    BadImageTargetSize { image_size: u64, target_size: u64 },
}

// These would be constants and methods on `BootImageHeader` if we defined it,
// but it's defined in `nexus_sled_agent_shared`. Use a module instead.
mod boot_image_header {
    use super::ImageHeaderParseError;
    use bytes::Buf as _;
    use nexus_sled_agent_shared::inventory::BootImageHeader;

    pub(super) const SIZE: usize = 4096;
    pub(super) const DATASET_NAME_SIZE: usize = 128;
    pub(super) const IMAGE_NAME_SIZE: usize = 128;
    pub(super) const MAGIC: u32 = 0x1deb0075;
    pub(super) const VERSION: u32 = 2;

    pub(super) fn parse(
        mut buf: &[u8],
    ) -> Result<BootImageHeader, ImageHeaderParseError> {
        // The `get_*_le()` methods below (from `bytes::Buf`) panic if the slice
        // isn't long enough. We can check once that we have enough data for a
        // full header, guaranteeing none of the `get_*`s below will panic.
        if buf.len() < SIZE {
            return Err(ImageHeaderParseError::TooSmall);
        }

        let magic = buf.get_u32_le();
        if magic != MAGIC {
            return Err(ImageHeaderParseError::BadMagic {
                expected: MAGIC,
                got: magic,
            });
        }

        let version = buf.get_u32_le();
        if version != VERSION {
            return Err(ImageHeaderParseError::BadVersion {
                expected: VERSION,
                got: version,
            });
        }

        let flags = buf.get_u64_le();
        let data_size = buf.get_u64_le();
        let image_size = buf.get_u64_le();
        let target_size = buf.get_u64_le();
        if image_size > target_size {
            return Err(ImageHeaderParseError::BadImageTargetSize {
                image_size,
                target_size,
            });
        }

        let mut sha256 = [0; 32];
        buf.copy_to_slice(&mut sha256);

        // skip the dataset name field
        buf.advance(DATASET_NAME_SIZE);

        // read the image name field (this is a 0-terminated string, so trim to
        // the first 0)
        let image_name = {
            let field = &buf[..IMAGE_NAME_SIZE];
            buf.advance(IMAGE_NAME_SIZE);

            let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());

            String::from_utf8_lossy(&field[..end]).to_string()
        };

        Ok(BootImageHeader {
            flags,
            data_size,
            image_size,
            target_size,
            sha256,
            image_name,
        })
    }
}

/// The std lib [`BufReader`]'s [`io::Read`] implementation will bypass the
/// buffer and issue reads directly to the inner reader if the requested buffer
/// size is large enough. When reading raw disks, we don't want to do that: we
/// always want to issue reads that are a multiple of the disk's block size, so
/// this wrapper limits reads to at most the configured block size (which is
/// expected to be such a multiple).
struct BufReaderExactSize<R> {
    inner: BufReader<R>,
    block_size: usize,
}

impl<R: io::Read> BufReaderExactSize<R> {
    fn with_capacity(block_size: usize, inner: R) -> Self {
        Self { inner: BufReader::with_capacity(block_size, inner), block_size }
    }

    #[cfg(test)]
    fn into_inner(self) -> R {
        self.inner.into_inner()
    }
}

impl<R: io::Read> io::Read for BufReaderExactSize<R> {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        // This is our extension of `BufReader`; if we're asked for a large
        // read, restrict it to at most our block size to avoid `BufReader`
        // bypassing its buffer. (If buf.len() is exactly block_size, it will
        // still bypass the buffer, but that's fine - it'll issue an exactly
        // block_size read.)
        if buf.len() > self.block_size {
            buf = &mut buf[..self.block_size];
        }
        self.inner.read(buf)
    }
}

impl<R: io::Read> io::BufRead for BufReaderExactSize<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amount: usize) {
        self.inner.consume(amount)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InternalDiskDetails;
    use crate::InternalDisksReceiver;
    use anyhow::Context as _;
    use assert_matches::assert_matches;
    use bytes::BufMut as _;
    use camino_tempfile::Utf8TempDir;
    use omicron_common::disk::DiskIdentity;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::InternalZpoolUuid;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::prop_oneof;
    use sha2::Digest as _;
    use sled_storage::config::MountConfig;
    use slog_error_chain::InlineErrorChain;
    use std::io::Write;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use test_strategy::proptest;
    use tokio::io::AsyncWriteExt;

    // We're reading a raw disk that is (presumably) much larger than any phase
    // 2 image written to them, which allows us to read past the end of the
    // image. This is important because it allows us to always issue reads that
    // are a multiple of the disk's block size, even if the OS image itself is
    // not. We emulate the disk being larger than the image in tests by wrapping
    // the fake OS image in this `NeverEndingReader`, which returns 0s after the
    // end of the inner reader.
    struct NeverEndingReader<R> {
        inner: R,
        inner_done: bool,
        read_sizes_requested: Vec<usize>,
    }

    impl<R: io::Read> NeverEndingReader<R> {
        fn new(inner: R) -> Self {
            Self { inner, inner_done: false, read_sizes_requested: Vec::new() }
        }
    }

    impl<R: io::Read> io::Read for NeverEndingReader<R> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.read_sizes_requested.push(buf.len());
            loop {
                if self.inner_done {
                    buf.fill(0);
                    return Ok(buf.len());
                } else {
                    let n = self.inner.read(buf)?;
                    if n == 0 {
                        self.inner_done = true;
                    } else {
                        return Ok(n);
                    }
                }
            }
        }
    }

    fn prepend_valid_image_hader(data: &mut Vec<u8>) -> [u8; 32] {
        let sha256 = sha2::Sha256::digest(&data);
        let mut header = [0; boot_image_header::SIZE];
        let mut buf = header.as_mut_slice();
        buf.put_u32_le(boot_image_header::MAGIC);
        buf.put_u32_le(boot_image_header::VERSION);
        buf.put_u64_le(0); // flags
        buf.put_u64_le(data.len() as u64);
        buf.put_u64_le(data.len() as u64);
        buf.put_u64_le(data.len() as u64);
        buf.put_slice(&sha256);
        data.splice(0..0, header);
        sha256.into()
    }

    /// Custom strategy for our disk block size: 500, 4097, 1 MiB, or random
    /// between 1 MiB and 2 MiB (25% each). These are chosen to exercise
    /// different edge cases:
    ///
    /// * 500 is smaller than the image header and will require multiple reads
    /// * 4097 is 1 byte larger than the image header
    /// * 1 MiB is the common case we expect in practice; any real disk block
    ///   size that divides 1 MiB will induce a 1 MiB read size
    /// * Random large block sizes
    fn block_size_strategy() -> impl Strategy<Value = usize> {
        const ONE_MIB: usize = 1024 * 1024;
        prop_oneof![
            Just(500usize),
            Just(4097usize),
            Just(ONE_MIB),
            (ONE_MIB..=2 * ONE_MIB),
        ]
    }

    #[proptest]
    fn proptest_read_valid_host_phase2(
        #[strategy(vec(any::<u8>(), 0..1024*1024))] mut data: Vec<u8>,
        #[strategy(block_size_strategy())] block_size: usize,
    ) {
        let expected_header_sha256 = prepend_valid_image_hader(&mut data);
        let expected_artifact_hash =
            ArtifactHash(sha2::Sha256::digest(&data).into());
        let expected_artifact_size = data.len();

        let mut reader = BufReaderExactSize::with_capacity(
            block_size,
            NeverEndingReader::new(&*data),
        );
        match boot_partition_details::read_blocking_with_buf_size(
            &mut reader,
            "/does-not-matter".into(),
        ) {
            Ok(BootPartitionDetails {
                artifact_hash,
                artifact_size,
                header,
            }) => {
                assert_eq!(artifact_hash, expected_artifact_hash);
                assert_eq!(artifact_size, expected_artifact_size);
                assert_eq!(header.sha256, expected_header_sha256);
            }
            Err(err) => {
                panic!("unexpected error: {}", InlineErrorChain::new(&err));
            }
        }

        // All reads to the underlying disk should have been exactly our block
        // size.
        assert!(
            reader
                .into_inner()
                .read_sizes_requested
                .iter()
                .all(|&sz| sz == block_size)
        );
    }

    /// Helper for setting up tests of
    /// [`BootPartitionReconciler::reconcile_slot()`].
    struct ReconcileTestHarness {
        tempdir: Utf8TempDir,
        disk_a: Option<BootPartitionDetails>,
        disk_b: Option<BootPartitionDetails>,
    }

    impl ReconcileTestHarness {
        fn new() -> Self {
            let tempdir = Utf8TempDir::new().expect("created tempdir");
            Self { tempdir, disk_a: None, disk_b: None }
        }

        fn path(&self, slot: M2Slot) -> Utf8PathBuf {
            let filename = match slot {
                M2Slot::A => "disk-a",
                M2Slot::B => "disk-b",
            };
            self.tempdir.path().join(filename)
        }

        fn with_valid_slot(
            &mut self,
            slot: M2Slot,
            after_header_contents: &[u8],
        ) -> BootPartitionDetails {
            let path = self.path(slot);
            let mut data = after_header_contents.to_vec();
            prepend_valid_image_hader(&mut data);

            {
                let mut f =
                    std::fs::File::create(&path).expect("made output file");
                f.write_all(&data).expect("wrote output file");
            }

            let details = self.read_valid_disk(slot);
            match slot {
                M2Slot::A => {
                    self.disk_a = Some(details.clone());
                }
                M2Slot::B => {
                    self.disk_b = Some(details.clone());
                }
            }
            details
        }

        fn read_valid_disk(&self, slot: M2Slot) -> BootPartitionDetails {
            let path = self.path(slot);
            let f = std::fs::File::open(&path).expect("opened file");
            let mut r = BufReaderExactSize::with_capacity(
                boot_image_header::SIZE,
                NeverEndingReader::new(f),
            );
            boot_partition_details::read_blocking_with_buf_size(
                &mut r,
                "/does-not-matter".into(),
            )
            .expect("read boot partition")
        }

        fn internal_disks(&self) -> InternalDisks {
            let mut disk_details = Vec::new();
            for (i, slot) in [M2Slot::A, M2Slot::B].into_iter().enumerate() {
                disk_details.push(InternalDiskDetails::fake_details(
                    DiskIdentity {
                        vendor: "fake".to_string(),
                        model: "fake".to_string(),
                        serial: format!("fake-{i}"),
                    },
                    InternalZpoolUuid::new_v4(),
                    false,
                    Some(slot),
                    Some(self.path(slot)),
                ));
            }
            InternalDisksReceiver::fake_static(
                Arc::new(MountConfig::default()),
                disk_details.into_iter(),
            )
            .current()
        }
    }

    struct TempFileWriter {
        file: tokio::fs::File,
    }

    impl DiskWriter for TempFileWriter {
        type F = TempFileWriter;

        async fn open(path: &Utf8Path) -> io::Result<Self::F> {
            let file = tokio::fs::File::create(path).await?;
            Ok(Self { file })
        }
    }

    impl DiskWriterFile for TempFileWriter {
        async fn finalize(mut self) -> io::Result<()> {
            self.file.flush().await
        }
    }

    impl AsyncWrite for TempFileWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            let f = &mut self.as_mut().file;
            Pin::new(f).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            let f = &mut self.as_mut().file;
            Pin::new(f).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            let f = &mut self.as_mut().file;
            Pin::new(f).poll_shutdown(cx)
        }
    }

    struct TempFileReader;

    impl DiskReader for TempFileReader {
        async fn read(
            slot: M2Slot,
            internal_disks: &InternalDisks,
        ) -> Result<BootPartitionDetails, BootPartitionError> {
            let path = internal_disks
                .boot_image_raw_devfs_path(slot)
                .ok_or(BootPartitionError::NoDiskInSlot)?
                .expect("fake disks constructed with valid boot paths");
            let f = std::fs::File::open(&path)
                .map_err(|err| BootPartitionError::OpenDevfs { path, err })?;
            let mut r = BufReaderExactSize::with_capacity(
                boot_image_header::SIZE,
                NeverEndingReader::new(f),
            );
            boot_partition_details::read_blocking_with_buf_size(
                &mut r,
                "/does-not-matter".into(),
            )
        }
    }

    #[derive(Debug)]
    struct FakeArtifactStore {
        tempdir: Utf8TempDir,
    }

    impl FakeArtifactStore {
        fn new() -> Self {
            Self { tempdir: Utf8TempDir::new().expect("made tempdir") }
        }

        fn insert(&self, data: &[u8]) {
            let hash = sha2::Sha256::digest(data);
            let artifact = ArtifactHash(hash.into());
            let path = self.tempdir.path().join(artifact.to_string());
            std::fs::write(path, data)
                .expect("wrote data to fake artifact store");
        }
    }

    impl SledAgentArtifactStore for FakeArtifactStore {
        async fn get_artifact(
            &self,
            artifact: ArtifactHash,
        ) -> anyhow::Result<tokio::fs::File> {
            let path = self.tempdir.path().join(artifact.to_string());
            tokio::fs::File::open(&path)
                .await
                .with_context(|| format!("couldn't open {path}"))
        }
    }

    #[tokio::test]
    async fn reconcile_slot_reads_disk_as_needed() {
        let logctx = dev::test_setup_log("reconcile_slot_reads_disk_as_needed");
        let log = &logctx.log;

        // We don't use any artifacts in this test, but still need an artifact
        // store.
        let artifact_store = FakeArtifactStore::new();

        // Create a valid slot A.
        let slot = M2Slot::A;
        let mut harness = ReconcileTestHarness::new();
        harness.with_valid_slot(slot, b"some data");

        // Start with an empty cache.
        let mut cache = None;

        // Try to reconcile with `CurrentContents`; this should read the disk,
        // return the expected details, and populate the cache.
        let details = BootPartitionReconciler::reconcile_slot::<
            _,
            TempFileReader,
            TempFileWriter,
        >(
            slot,
            &harness.internal_disks(),
            &mut cache,
            &HostPhase2DesiredContents::CurrentContents,
            &artifact_store,
            log,
        )
        .await
        .expect("reconciled slot");
        assert_eq!(Some(&details), cache.as_ref());
        assert_eq!(Some(&details), harness.disk_a.as_ref());

        // We should get the same result if we ask it to reconcile an artifact
        // whose hash matches what's already there. Our artifact store is empty,
        // which is fine: since we already have matching data, we shouldn't even
        // try to access artifacts from it.
        let mut cache = None;
        let artifact = details.artifact_hash;
        let details = BootPartitionReconciler::reconcile_slot::<
            _,
            TempFileReader,
            TempFileWriter,
        >(
            slot,
            &harness.internal_disks(),
            &mut cache,
            &HostPhase2DesiredContents::Artifact { hash: artifact },
            &artifact_store,
            log,
        )
        .await
        .expect("reconciled slot");
        assert_eq!(Some(&details), cache.as_ref());
        assert_eq!(Some(&details), harness.disk_a.as_ref());

        // Confirm we don't try to read the disk if the cache is populated: blow
        // away the underlying file out from under the reconciler, but keep the
        // cache populated. It should still claim success. (Part of our type's
        // invariants is that it's the only thing that touches the disks, so
        // this is exploiting that invariant to test something else.)
        std::fs::remove_file(harness.path(slot)).expect("removed temp file");

        // Reconcile with both kinds of desired contents and a populated cache.
        let details = BootPartitionReconciler::reconcile_slot::<
            _,
            TempFileReader,
            TempFileWriter,
        >(
            slot,
            &harness.internal_disks(),
            &mut cache,
            &HostPhase2DesiredContents::CurrentContents,
            &artifact_store,
            log,
        )
        .await
        .expect("reconciled slot");
        assert_eq!(Some(&details), cache.as_ref());
        assert_eq!(Some(&details), harness.disk_a.as_ref());

        let details = BootPartitionReconciler::reconcile_slot::<
            _,
            TempFileReader,
            TempFileWriter,
        >(
            slot,
            &harness.internal_disks(),
            &mut cache,
            &HostPhase2DesiredContents::Artifact { hash: artifact },
            &artifact_store,
            log,
        )
        .await
        .expect("reconciled slot");
        assert_eq!(Some(&details), cache.as_ref());
        assert_eq!(Some(&details), harness.disk_a.as_ref());

        // Invalidate the cache and try to reconcile with `CurrentContents`;
        // this should now fail, because there's no valid disk image to read.
        let mut cache = None;
        let err = BootPartitionReconciler::reconcile_slot::<
            _,
            TempFileReader,
            TempFileWriter,
        >(
            slot,
            &harness.internal_disks(),
            &mut cache,
            &HostPhase2DesiredContents::CurrentContents,
            &artifact_store,
            log,
        )
        .await
        .expect_err("failed to reconcile slot");
        assert_eq!(None, cache.as_ref());
        assert_matches!(err, BootPartitionError::OpenDevfs { .. });

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn reconcile_slot_writes_disk_as_needed() {
        let logctx =
            dev::test_setup_log("reconcile_slot_writes_disk_as_needed");
        let log = &logctx.log;

        // Start with an empty artifact store.
        let artifact_store = FakeArtifactStore::new();

        // Insert a valid image into slot A.
        let mut harness = ReconcileTestHarness::new();
        let slot_a_details =
            harness.with_valid_slot(M2Slot::A, b"slot A contents");
        let slot_a_data =
            std::fs::read(harness.path(M2Slot::A)).expect("read slot A");
        let artifact = slot_a_details.artifact_hash;

        // Try to reconcile this image into slot B; this should realize it needs
        // to write an image (because it can't read a valid, matching image),
        // but will fail because we haven't inserted it into our artifact store.
        let mut cache = None;
        let err = BootPartitionReconciler::reconcile_slot::<
            _,
            TempFileReader,
            TempFileWriter,
        >(
            M2Slot::B,
            &harness.internal_disks(),
            &mut cache,
            &HostPhase2DesiredContents::Artifact { hash: artifact },
            &artifact_store,
            log,
        )
        .await
        .expect_err("failed to reconcile slot");
        assert_eq!(None, cache.as_ref());
        assert_matches!(err, BootPartitionError::MissingDesiredArtifact { .. });

        // Insert it into our store.
        artifact_store.insert(&slot_a_data);

        // Reconciliation should now work and write disk B.
        let details = BootPartitionReconciler::reconcile_slot::<
            _,
            TempFileReader,
            TempFileWriter,
        >(
            M2Slot::B,
            &harness.internal_disks(),
            &mut cache,
            &HostPhase2DesiredContents::Artifact { hash: artifact },
            &artifact_store,
            log,
        )
        .await
        .expect("reconciled slot");
        assert_eq!(Some(&details), cache.as_ref());
        assert_eq!(details, slot_a_details);

        let slot_b_data =
            std::fs::read(harness.path(M2Slot::B)).expect("read slot B");
        assert_eq!(slot_a_data, slot_b_data);

        // Now insert a different, valid image in slot B and the artifact store.
        let slot_b_details =
            harness.with_valid_slot(M2Slot::B, b"slot B contents");
        let slot_b_data =
            std::fs::read(harness.path(M2Slot::B)).expect("read slot B");
        assert_ne!(slot_a_data, slot_b_data);
        artifact_store.insert(&slot_b_data);

        // Our cache still has the valid description of what's in slot A. Ask it
        // to reconcile with a desired artifact of what's in slot B.
        assert_eq!(Some(&slot_a_details), cache.as_ref());
        let details = BootPartitionReconciler::reconcile_slot::<
            _,
            TempFileReader,
            TempFileWriter,
        >(
            M2Slot::A,
            &harness.internal_disks(),
            &mut cache,
            &HostPhase2DesiredContents::Artifact {
                hash: slot_b_details.artifact_hash,
            },
            &artifact_store,
            log,
        )
        .await
        .expect("reconciled slot");
        assert_eq!(Some(&details), cache.as_ref());
        assert_eq!(details, slot_b_details);

        // Slot A should now contain the new thing we wrote to slot B.
        let slot_a_data =
            std::fs::read(harness.path(M2Slot::A)).expect("read slot A");
        assert_eq!(slot_a_data, slot_b_data);

        logctx.cleanup_successful();
    }
}
