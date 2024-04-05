// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for interacting with chronyd.

use crate::zone::CHRONYD;
use crate::{execute, ExecutionError, PFEXEC};

/// Wraps commands for interacting with chronyd.
pub struct Chronyd {}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
impl Chronyd {
    pub fn start_daemon(file: &str) -> Result<(), ExecutionError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        // TODO: This doesn't seem to be working. I think `execute()`
        // doesn't like the "&", and it immediately exits after running.
        // find a way to keep the process going.
        let cmd = cmd.args(&[CHRONYD, "-d", "-f", file, "&"]);
        execute(cmd)?;
        Ok(())
    }
}
