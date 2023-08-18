// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use slog::error;

#[derive(Debug)]
pub struct FailureInfo {
    pub command: String,
    pub status: std::process::ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

impl std::fmt::Display for FailureInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Command [{}] executed and failed with status: {}",
            self.command, self.status
        )?;
        write!(f, "  stdout: {}", self.stdout)?;
        write!(f, "  stderr: {}", self.stderr)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("Failed to start execution of [{command}]: {err}")]
    ExecutionStart { command: String, err: std::io::Error },

    #[error("{0}")]
    CommandFailure(Box<FailureInfo>),

    #[error("Failed to enter zone: {err}")]
    ZoneEnter { err: std::io::Error },

    #[error("Zone not running")]
    NotRunning,
}

impl ExecutionError {
    pub fn from_output<S: Into<String>>(
        command_str: S,
        output: &std::process::Output,
    ) -> Self {
        Self::CommandFailure(Box::new(FailureInfo {
            command: command_str.into(),
            status: output.status,
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        }))
    }
}
