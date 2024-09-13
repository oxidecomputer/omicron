// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common xtask command helpers

use anyhow::{bail, Context, Result};
use std::process::Command;

/// Runs the given command, printing some basic debug information around it, and
/// failing with an error message if the command does not exit successfully
pub fn run_subcmd(mut command: Command) -> Result<()> {
    eprintln!(
        "running: {} {}",
        command.get_program().to_str().unwrap(),
        command
            .get_args()
            .map(|arg| format!("{:?}", arg.to_str().unwrap()))
            .collect::<Vec<_>>()
            .join(" ")
    );

    let exit_status = command
        .spawn()
        .context("failed to spawn child process")?
        .wait()
        .context("failed to wait for child process")?;

    if !exit_status.success() {
        bail!("failed: {}", exit_status);
    }

    Ok(())
}
