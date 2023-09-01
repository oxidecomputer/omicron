// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper for calling fstyp.

use camino::Utf8Path;
use helios_fusion::{
    zpool::ZpoolName, BoxedExecutor, ExecutionError, FSTYP, PFEXEC,
};
use std::str::FromStr;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("fstype output is not valid UTF-8: {0}")]
    NotValidUtf8(#[from] std::string::FromUtf8Error),

    #[error("fstyp execution error: {0}")]
    Execution(#[from] ExecutionError),

    #[error("Failed to find zpool name from fstyp")]
    NotFound,

    #[error("Failed to parse zpool name from fstyp: {0}")]
    ZpoolName(String),
}

/// Wraps 'fstyp' command.
pub struct Fstyp {}

impl Fstyp {
    /// Executes the 'fstyp' command and parses the name of a zpool from it, if
    /// one exists.
    pub fn get_zpool(
        executor: &BoxedExecutor,
        path: &Utf8Path,
    ) -> Result<ZpoolName, Error> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");

        let cmd = cmd.arg(FSTYP).arg("-a").arg(path);

        let output = executor.execute(cmd).map_err(Error::from)?;
        let stdout = String::from_utf8(output.stdout)?;

        let mut seen_zfs_marker = false;
        for line in stdout.lines() {
            let line = line.trim();
            if line == "zfs" {
                seen_zfs_marker = true;
            }

            if seen_zfs_marker {
                if let Some(name) = line.strip_prefix("name: ") {
                    let name = name.trim_matches('\'');
                    return ZpoolName::from_str(name)
                        .map_err(|e| Error::ZpoolName(e));
                }
            }
        }
        return Err(Error::NotFound);
    }
}
