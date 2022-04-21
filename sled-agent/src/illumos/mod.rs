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

const PFEXEC: &str = "/usr/bin/pfexec";

#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("Failed to start execution of process: {0}")]
    ExecutionStart(std::io::Error),

    #[error(
        "Command [{command}] executed and failed with status: {status}. Output: {stderr}"
    )]
    CommandFailure {
        command: String,
        status: std::process::ExitStatus,
        stderr: String,
    },
}

// We wrap this method in an inner module to make it possible to mock
// these free functions.
#[cfg_attr(test, mockall::automock, allow(dead_code))]
mod inner {
    use super::*;

    // Helper function for starting the process and checking the
    // exit code result.
    pub fn execute(
        command: &mut std::process::Command,
    ) -> Result<std::process::Output, ExecutionError> {
        let output =
            command.output().map_err(|e| ExecutionError::ExecutionStart(e))?;

        if !output.status.success() {
            return Err(ExecutionError::CommandFailure {
                command: command.get_args().map(|s| s.to_string_lossy().into()).collect::<Vec<String>>().join(" "),
                status: output.status,
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
