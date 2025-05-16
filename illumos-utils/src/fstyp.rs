// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper for calling fstyp.

use crate::zpool::ZpoolName;
use crate::{PFEXEC, execute_async};
use camino::Utf8Path;
use std::str::FromStr;
use tokio::process::Command;

const FSTYP: &str = "/usr/sbin/fstyp";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("fstype output is not valid UTF-8: {0}")]
    NotValidUtf8(#[from] std::string::FromUtf8Error),

    #[error("fstyp execution error: {0}")]
    Execution(#[from] crate::ExecutionError),

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
    pub async fn get_zpool(path: &Utf8Path) -> Result<ZpoolName, Error> {
        let mut cmd = Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");

        let cmd = cmd.arg(FSTYP).arg("-a").arg(path);

        let output = execute_async(cmd).await.map_err(Error::from)?;
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
