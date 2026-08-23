// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Async writer for raw disks on illumos (e.g., host OS phase 2 images written
//! to M.2 drives).

use crate::BlockSizeBufWriter;
use illumos_utils::dkio;
use illumos_utils::dkio::MediaInfoExtended;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::fs::File;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

/// Writer for illumos raw disks.
///
/// Construct an instance via [`RawDiskWriter::open()`], write to it just like
/// any other async writer (it will handle passing writes down to the device in
/// chunks of length [`RawDiskWriter::block_size()`]), and then call
/// [`RawDiskWriter::finalize()`]. It is **critical** to call `finalize()`;
/// failure to do so will likely lead to data loss.
///
/// `RawDiskWriter` attempts to be as conservative as it can about ensuring data
/// is written:
///
/// * The device is opened with `O_SYNC`
/// * In `finalize()`, the file is `fsync`'d after any remaining data is flushed
/// * In `finalize()`, the disk write cache is flushed (if supported by the
///   target device)
///
/// Writing an amount of data that is not a multiple of the device's
/// `block_size()` will likely result in a failure when writing / flushing the
/// final not-correctly-sized chunk.
///
/// This type is illumos-specific due to using dkio for two things:
///
/// 1. Determining the logical block size of the device
/// 2. Flushing the disk write cache
pub struct RawDiskWriter {
    inner: BlockSizeBufWriter<File>,
}

impl RawDiskWriter {
    /// Open the disk device at `path` for writing, and attempt to determine its
    /// logical block size via [`MediaInfoExtended`].
    pub async fn open(path: &Path) -> io::Result<Self> {
        let f = tokio::fs::OpenOptions::new()
            .create(false)
            .write(true)
            .truncate(false)
            .custom_flags(libc::O_SYNC)
            .open(path)
            .await?;

        // Synthetic boot images in test environments are regular files, which
        // reject the media info ioctl. Writes are O_SYNC, so a device-sized
        // block would fsync every few hundred bytes; use 1 MiB, matching what
        // the boot partition reader settles on.
        let block_size = if f.metadata().await?.is_file() {
            1024 * 1024
        } else {
            let media_info = MediaInfoExtended::from_fd(f.as_raw_fd())?;
            media_info.logical_block_size as usize
        };

        let inner = BlockSizeBufWriter::with_block_size(block_size, f);

        Ok(Self { inner })
    }

    /// The logical block size of the underlying device.
    pub fn block_size(&self) -> usize {
        self.inner.block_size()
    }

    /// Flush any remaining data and attempt to ensure synchronization with the
    /// device.
    pub async fn finalize(mut self) -> io::Result<()> {
        // Flush any remaining data in our buffer
        self.inner.flush().await?;

        // `fsync` the file...
        let f = self.inner.into_inner();
        f.sync_all().await?;

        // ...and also attempt to flush the disk write cache
        tokio::task::spawn_blocking(move || {
            match dkio::flush_write_cache(f.as_raw_fd()) {
                Ok(()) => Ok(()),
                // Some drives don't support `flush_write_cache`; we don't want
                // to fail in this case. Regular files (synthetic boot images
                // in test environments) reject the ioctl entirely; sync_all
                // above already covers them.
                Err(err) if err.raw_os_error() == Some(libc::ENOTSUP) => Ok(()),
                Err(err) if err.raw_os_error() == Some(libc::ENOTTY) => Ok(()),
                Err(err) => Err(err),
            }
        })
        .await
        .expect("task panicked")
    }
}

impl AsyncWrite for RawDiskWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino_tempfile::tempdir;

    // Synthetic boot images are regular files, which reject the media info
    // ioctl. Writes are O_SYNC, so the fallback block size is what keeps a
    // 1 GiB-scale phase 2 write from fsyncing itself to a crawl.
    #[tokio::test]
    async fn regular_file_uses_one_mib_blocks() {
        let dir = tempdir().expect("created tempdir");
        let path = dir.path().join("boot_image");
        std::fs::write(&path, b"synthetic boot image").expect("wrote file");

        let writer = RawDiskWriter::open(path.as_std_path())
            .await
            .expect("opened regular file");
        assert_eq!(writer.block_size(), 1024 * 1024);
    }
}
