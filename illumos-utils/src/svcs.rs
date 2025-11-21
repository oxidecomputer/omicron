// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for reporting SMF services' status.

use crate::zone::SVCS;
use crate::{ExecutionError, PFEXEC, execute_async};
use tokio::process::Command;

/// Wraps commands for interacting with interfaces.
pub struct Svcs {}

impl Svcs {
    /// Lists SMF services in maintenance
    // TODO-K: Do not return a string
    pub async fn in_maintenance() -> Result<String, ExecutionError> {
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[SVCS, "-Zxv"]);
        let output = execute_async(cmd).await?;
        // TODO-K: handle stderr and acutally parse the output
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}
