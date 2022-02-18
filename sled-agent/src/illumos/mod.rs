// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Wrappers around illumos-specific commands.

pub mod addrobj;
pub mod dladm;
pub mod running_zone;
pub mod svc;
pub mod vnic;
pub mod zfs;
pub mod zone;
pub mod zpool;

const PFEXEC: &str = "/usr/bin/pfexec";

#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("Failed to start execution of process: {0}")]
    ExecutionStart(std::io::Error),

    #[error(
        "Command executed and failed with status: {status}. Output: {stderr}"
    )]
    CommandFailure { status: std::process::ExitStatus, stderr: String },
}

// Helper function for starting the process and checking the
// exit code result.
fn execute(
    command: &mut std::process::Command,
) -> Result<std::process::Output, ExecutionError> {
    let output =
        command.output().map_err(|e| ExecutionError::ExecutionStart(e))?;

    if !output.status.success() {
        return Err(ExecutionError::CommandFailure {
            status: output.status,
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }

    Ok(output)
}
