// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Wrappers around illumos-specific commands.

use cfg_if::cfg_if;

pub mod addrobj;
pub mod dladm;
pub mod running_zone;
pub mod svc;
pub mod vnic;
pub mod zfs;
pub mod zone;
pub mod zpool;

pub const PFEXEC: &str = "/usr/bin/pfexec";

#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("Failed to start execution of [{command}]: {err}")]
    ExecutionStart { command: String, err: std::io::Error },

    #[error(
        "Command [{command}] executed and failed with status: {status}. Stdout: {stdout}, Stderr: {stderr}"
    )]
    CommandFailure {
        command: String,
        status: std::process::ExitStatus,
        stdout: String,
        stderr: String,
    },
}

// We wrap this method in an inner module to make it possible to mock
// these free functions.
#[cfg_attr(test, mockall::automock, allow(dead_code))]
mod inner {
    use super::*;

    fn to_string(command: &mut std::process::Command) -> String {
        command
            .get_args()
            .map(|s| s.to_string_lossy().into())
            .collect::<Vec<String>>()
            .join(" ")
    }

    // Helper function for starting the process and checking the
    // exit code result.
    pub fn execute(
        command: &mut std::process::Command,
    ) -> Result<std::process::Output, ExecutionError> {
        let output = command.output().map_err(|err| {
            ExecutionError::ExecutionStart { command: to_string(command), err }
        })?;

        if !output.status.success() {
            return Err(ExecutionError::CommandFailure {
                command: command
                    .get_args()
                    .map(|s| s.to_string_lossy().into())
                    .collect::<Vec<String>>()
                    .join(" "),
                status: output.status,
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            });
        }

        Ok(output)
    }
}

cfg_if! {
    if #[cfg(test)] {
        pub use mock_inner::*;
    } else {
        pub use inner::*;
    }
}
