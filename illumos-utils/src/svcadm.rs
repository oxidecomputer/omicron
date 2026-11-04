// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for manipulating SMF services.

use crate::zone::SVCADM;
use crate::{ExecutionError, PFEXEC, execute_async};
use tokio::process::Command;

/// Wraps commands for interacting with svcadm.
pub struct Svcadm {}

impl Svcadm {
    pub async fn refresh_logadm_upgrade() -> Result<(), ExecutionError> {
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[SVCADM, "refresh", "logadm-upgrade"]);
        execute_async(cmd).await?;
        Ok(())
    }

    pub async fn enable_service(fmri: String) -> Result<(), ExecutionError> {
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[SVCADM, "enable", "-s", &fmri]);
        execute_async(cmd).await?;
        Ok(())
    }
}
