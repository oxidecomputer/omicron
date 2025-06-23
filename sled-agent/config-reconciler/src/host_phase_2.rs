// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanics for interacting with the OS phase 2 images stored on M.2
//! partitions.

use crate::InternalDisks;
use camino::Utf8PathBuf;
use nexus_sled_agent_shared::inventory::BootPartitionDetails;
use omicron_common::disk::M2Slot;
use sled_hardware::PooledDiskError;
use std::io;
use std::io::BufRead as _;
use std::io::BufReader;
use std::io::Read as _;
use std::sync::Arc;
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
}

#[derive(Debug)]
pub struct BootPartitionContents {
    pub boot_disk: Result<M2Slot, BootDiskNotFound>,
    pub slot_a: Result<BootPartitionDetails, BootPartitionError>,
    pub slot_b: Result<BootPartitionDetails, BootPartitionError>,
}

impl BootPartitionContents {
    pub async fn read(internal_disks: &InternalDisks) -> Self {
        let (slot_a, slot_b) = futures::join!(
            boot_partition_details::read(M2Slot::A, internal_disks),
            boot_partition_details::read(M2Slot::B, internal_disks),
        );
        Self {
            boot_disk: internal_disks.boot_disk_slot().ok_or(BootDiskNotFound),
            slot_a,
            slot_b,
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
        match internal_disks.image_raw_devfs_path(slot) {
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
        sha256.copy_from_slice(&buf[..32]);

        Ok(BootImageHeader {
            flags,
            data_size,
            image_size,
            target_size,
            sha256,
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
    use bytes::BufMut as _;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::prop_oneof;
    use sha2::Digest as _;
    use slog_error_chain::InlineErrorChain;
    use test_strategy::proptest;

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
}
