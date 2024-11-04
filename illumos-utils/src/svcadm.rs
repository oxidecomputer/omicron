// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for manipulating SMF services.

use crate::zone::SVCADM;
use crate::{execute, ExecutionError, PFEXEC};

/// Wraps commands for interacting with svcadm.
pub struct Svcadm {}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
impl Svcadm {
    pub fn refresh_logadm_upgrade() -> Result<(), ExecutionError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[SVCADM, "refresh", "logadm-upgrade"]);
        execute(cmd)?;
        Ok(())
    }

    pub fn enable_service(fmri: String) -> Result<(), ExecutionError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[SVCADM, "enable", "-s", &fmri]);
        execute(cmd)?;
        Ok(())
    }
}
