// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper for calling fstyp.

use crate::illumos::execute;
use crate::illumos::zpool::ZpoolName;
use std::path::Path;
use std::str::FromStr;

const FSTYP: &str = "/usr/sbin/fstyp";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("fstyp execution error: {0}")]
    Execution(#[from] crate::illumos::ExecutionError),

    #[error("Failed to find zpool name from fstyp")]
    NotFound,

    #[error("Failed to parse zpool name from fstyp: {0}")]
    ZpoolName(String),
}

/// Wraps 'fstyp' command.
pub struct Fstyp {}

#[cfg_attr(test, mockall::automock)]
impl Fstyp {
    /// Executes the 'fstyp' command and parses the name of a zpool from it, if
    /// one exists.
    pub fn get_zpool(path: &Path) -> Result<ZpoolName, Error> {
        let mut command = std::process::Command::new(FSTYP);
        let cmd = command.arg("-a").arg(path);

        let output = execute(cmd).map_err(Error::from)?;
        let stdout = String::from_utf8_lossy(&output.stdout);

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
