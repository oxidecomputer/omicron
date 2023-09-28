// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Key file support for ZFS dataset encryption

use illumos_utils::zfs::Keypath;
use slog::{info, Logger};
use tokio::fs::{remove_file, File};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

/// This path is intentionally on a `tmpfs` to prevent copy-on-write behavior
/// and to ensure it goes away on power off.
///
/// We want minimize the time the key files are in memory, and so we rederive
/// the keys and recreate the files on demand when creating and mounting
/// encrypted filesystems. We then zero them and unlink them.
pub const KEYPATH_ROOT: &str = "/var/run/oxide/";

/// A file that wraps a zfs encryption key.
///
/// We put this in a RAM backed filesystem and zero and delete it when we are
/// done with it. Unfortunately we cannot do this inside `Drop` because there is no
/// equivalent async drop.
pub struct KeyFile {
    path: Keypath,
    file: File,
    log: Logger,
}

impl KeyFile {
    pub async fn create(
        path: Keypath,
        key: &[u8; 32],
        log: &Logger,
    ) -> std::io::Result<KeyFile> {
        // TODO: fix this to not truncate
        // We want to overwrite any existing contents.
        // If we truncate we may leave dirty pages around
        // containing secrets.
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path.0)
            .await?;
        file.write_all(key).await?;
        info!(log, "Created keyfile {}", path);
        Ok(KeyFile { path, file, log: log.clone() })
    }

    /// These keyfiles live on a tmpfs and we zero the file so the data doesn't
    /// linger on the page in memory.
    ///
    /// It'd be nice to `impl Drop for `KeyFile` and then call `zero`
    /// from within the drop handler, but async `Drop` isn't supported.
    pub async fn zero_and_unlink(&mut self) -> std::io::Result<()> {
        let zeroes = [0u8; 32];
        let _ = self.file.seek(SeekFrom::Start(0)).await?;
        self.file.write_all(&zeroes).await?;
        info!(self.log, "Zeroed and unlinked keyfile {}", self.path);
        remove_file(&self.path().0).await?;
        Ok(())
    }

    pub fn path(&self) -> &Keypath {
        &self.path
    }
}
