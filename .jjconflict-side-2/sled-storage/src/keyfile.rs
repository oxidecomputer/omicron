// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Key file support for ZFS dataset encryption

use illumos_utils::zfs::Keypath;
use slog::{Logger, error, info};
use tokio::fs::{File, remove_file};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

/// A file that wraps a zfs encryption key.
///
/// We put this in a RAM backed filesystem and zero and delete it when we are
/// done with it. Unfortunately we cannot do this inside `Drop` because there is no
/// equivalent async drop.
pub struct KeyFile {
    path: Keypath,
    file: File,
    log: Logger,
    zero_and_unlink_called: bool,
}

impl KeyFile {
    pub async fn create(
        path: Keypath,
        key: &[u8; 32],
        log: &Logger,
    ) -> std::io::Result<KeyFile> {
        info!(log, "About to create keyfile"; "path" => ?path);
        // We want to overwrite any existing contents.
        let mut file = tokio::fs::File::create(&path.0).await?;
        file.write_all(key).await?;
        file.flush().await?;
        info!(log, "Created keyfile"; "path" => ?path);
        Ok(KeyFile {
            path,
            file,
            log: log.clone(),
            zero_and_unlink_called: false,
        })
    }

    /// These keyfiles live on a tmpfs and we zero the file so the data doesn't
    /// linger on the page in memory.
    ///
    /// It'd be nice to `impl Drop for `KeyFile` and then call `zero`
    /// from within the drop handler, but async `Drop` isn't supported.
    pub async fn zero_and_unlink(&mut self) -> std::io::Result<()> {
        self.zero_and_unlink_called = true;
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

impl Drop for KeyFile {
    fn drop(&mut self) {
        if !self.zero_and_unlink_called {
            error!(
                self.log,
                "Failed to call zero_and_unlink for keyfile";
                "path" => %self.path
            );
        }
    }
}
