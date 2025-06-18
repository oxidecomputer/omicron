// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanics for interacting with the OS phase 2 images stored on M.2
//! partitions.

use bytes::Buf as _;
use camino::Utf8PathBuf;
use illumos_utils::dkio::MediaInfoExtended;
use omicron_common::disk::M2Slot;
use sha2::Digest as _;
use slog_error_chain::InlineErrorChain;
use std::cmp;
use std::fs::File;
use std::io::Read as _;
use std::os::fd::AsRawFd as _;
use tufaceous_artifact::ArtifactHash;

use crate::InternalDisks;

pub struct BootPartitionContents {
    pub slot_a: BootPartitionDetails,
    pub slot_b: BootPartitionDetails,
}

impl BootPartitionContents {
    pub async fn read(internal_disks: &InternalDisks) -> Self {
        let (slot_a, slot_b) = futures::join!(
            BootPartitionDetails::read(M2Slot::A, internal_disks),
            BootPartitionDetails::read(M2Slot::B, internal_disks),
        );
        Self { slot_a, slot_b }
    }
}

pub enum BootPartitionDetails {
    NoDiskFound,
    ErrorDeterminingDiskPath(String),
    ErrorOpeningDisk {
        path: Utf8PathBuf,
        err: String,
    },
    ErrorDeterminingBlockSize {
        path: Utf8PathBuf,
        err: String,
    },
    ErrorReadingDisk {
        path: Utf8PathBuf,
        err: String,
    },
    ErrorParsingImageHeader(String),
    HeaderSha256Mismatch {
        header: BootImageHeader,
        calculated_sha256: [u8; 32],
    },
    Phase2Image {
        artifact_hash: ArtifactHash,
        artifact_size: usize,
        header: BootImageHeader,
    },
}

impl BootPartitionDetails {
    async fn read(slot: M2Slot, internal_disks: &InternalDisks) -> Self {
        match internal_disks.image_raw_devfs_path(slot) {
            Some(Ok(path)) => {
                tokio::task::spawn_blocking(|| Self::read_blocking(path))
                    .await
                    .expect("read_blocking() did not panic")
            }
            Some(Err(err)) => Self::ErrorDeterminingDiskPath(
                InlineErrorChain::new(&err).to_string(),
            ),
            None => Self::NoDiskFound,
        }
    }

    fn read_blocking(path: Utf8PathBuf) -> Self {
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(err) => {
                return Self::ErrorOpeningDisk {
                    path,
                    err: InlineErrorChain::new(&err).to_string(),
                };
            }
        };

        // Determine the disk's block size.
        let block_size = match MediaInfoExtended::from_fd(f.as_raw_fd()) {
            Ok(media_info) => media_info.logical_block_size as usize,
            Err(err) => {
                return Self::ErrorDeterminingBlockSize {
                    path,
                    err: InlineErrorChain::new(&err).to_string(),
                };
            }
        };

        // In practice we expect block sizes of 512 or 4096, but we can read
        // bigger chunks; we'll choose 1 MiB (and fall back to the block size if
        // we somehow have a disk with block sizes > 1 MiB).
        let buf_size = cmp::max(block_size, 1024 * 1024);
        let mut buf = vec![0; buf_size];

        if let Err(err) = f.read_exact(&mut buf) {
            return Self::ErrorReadingDisk {
                path,
                err: InlineErrorChain::new(&err).to_string(),
            };
        }

        let image_header = match BootImageHeader::parse(&buf) {
            Ok(header) => header,
            Err(err) => {
                return Self::ErrorParsingImageHeader(
                    InlineErrorChain::new(&err).to_string(),
                );
            }
        };

        let artifact_size =
            image_header.data_size as usize + BootImageHeader::SIZE;

        // Compute two SHA256 hashes as we read the contents of this boot image.
        // The `image_header` contains a sha256 of the data _after_ the header
        // itself, but the artifact hash that lands in the TUF repo depo
        // _includes_ the header. Compute both here: we'll validate the data
        // hash against the header, and report the artifact hash.
        let mut artifact_hasher = sha2::Sha256::new();
        let mut data_hasher = sha2::Sha256::new();

        // This should probably never happen, but isn't fatal...
        if artifact_size < buf.len() {
            artifact_hasher.update(&buf[..artifact_size]);
            data_hasher.update(&buf[BootImageHeader::SIZE..artifact_size]);
        } else {
            artifact_hasher.update(&buf[..]);
            data_hasher.update(&buf[BootImageHeader::SIZE..]);
            let mut nleft = artifact_size - buf.len();
            while nleft > 0 {
                // Always read a full buffer...
                if let Err(err) = f.read_exact(&mut buf) {
                    return Self::ErrorReadingDisk {
                        path,
                        err: InlineErrorChain::new(&err).to_string(),
                    };
                }

                // ...but the last such read might have extra data.
                let nvalid = cmp::min(nleft, buf.len());
                artifact_hasher.update(&buf[..nvalid]);
                data_hasher.update(&buf[..nvalid]);

                nleft -= nvalid;
            }
        }

        let artifact_hash = artifact_hasher.finalize();
        let data_hash: [u8; 32] = data_hasher.finalize().into();

        if image_header.sha256 != data_hash {
            return Self::HeaderSha256Mismatch {
                header: image_header,
                calculated_sha256: data_hash,
            };
        }

        Self::Phase2Image {
            artifact_hash: ArtifactHash(artifact_hash.into()),
            artifact_size,
            header: image_header,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ImageHeaderParseError {
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

// There are several other fields in the header that we either parse and discard
// or ignore completely; see https://github.com/oxidecomputer/boot-image-tools
// for more thorough support.
#[derive(Debug)]
pub struct BootImageHeader {
    pub flags: u64,
    pub data_size: u64,
    pub image_size: u64,
    pub target_size: u64,
    pub sha256: [u8; 32],
}

impl BootImageHeader {
    const SIZE: usize = 4096;
    const MAGIC: u32 = 0x1deb0075;
    const VERSION: u32 = 2;

    fn parse(mut buf: &[u8]) -> Result<Self, ImageHeaderParseError> {
        // The `get_*_le()` methods below (from `bytes::Buf`) panic if the slice
        // isn't long enough. We can check once that we have enough data for a
        // full header, guaranteeing none of the `get_*`s below will panic.
        if buf.len() < Self::SIZE {
            return Err(ImageHeaderParseError::TooSmall);
        }

        let magic = buf.get_u32_le();
        if magic != Self::MAGIC {
            return Err(ImageHeaderParseError::BadMagic {
                expected: Self::MAGIC,
                got: magic,
            });
        }

        let version = buf.get_u32_le();
        if version != Self::VERSION {
            return Err(ImageHeaderParseError::BadVersion {
                expected: Self::VERSION,
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

        Ok(Self { flags, data_size, image_size, target_size, sha256 })
    }
}
