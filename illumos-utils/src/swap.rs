// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper for calling swap.

use crate::{execute, PFEXEC};
use std::path::{Path, PathBuf};

const SWAP: &str = "/usr/sbin/swap";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("swap output is not valid UTF-8: {0}")]
    NotValidUtf8(#[from] std::string::FromUtf8Error),

    #[error("swap execution error: {0}")]
    Execution(#[from] crate::ExecutionError),

    #[error("Failed to parse swap devices: {0}")]
    SwapListParse(String),
}

/// Wraps 'swap' command.
pub struct Swap {}

#[cfg_attr(test, mockall::automock)]
impl Swap {
    /// Executes the 'swap -a (device)' command
    pub fn set_swap(path: &Path) -> Result<(), Error> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");

        let cmd = cmd.arg(SWAP).arg("-a").arg(path);

        execute(cmd).map_err(Error::from)?;
        Ok(())
    }

    /// Executes the 'swap -l' command
    pub fn list_swap_devices() -> Result<Vec<PathBuf>, Error> {
        let mut cmd = std::process::Command::new(PFEXEC);
        cmd.env_clear();
        cmd.env("LC_ALL", "C.UTF-8");

        let cmd = cmd.arg(SWAP).arg("-l");

        let output = execute(cmd).map_err(Error::from)?;
        let stdout = String::from_utf8(output.stdout)?;

        let lines = stdout.lines().skip(1);

        let mut swapdevs = vec![];
        for line in lines {
            if let Some(device) = line.trim().split(' ').next() {
                swapdevs.push(PathBuf::from(device));
            } else {
                return Err(Error::SwapListParse(stdout));
            }
        }
        Ok(swapdevs)
    }
}
