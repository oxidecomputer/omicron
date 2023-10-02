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

    #[error("Failed to manipulate process contract: {err}")]
    ContractFailure { err: std::io::Error },

    #[error("Zone not running")]
    NotRunning,
}

/// Convenience trait for turning [std::process::Command] into a String.
pub trait AsCommandStr {
    fn into_str(&self) -> String;
}

impl AsCommandStr for String {
    fn into_str(&self) -> String {
        self.into()
    }
}

impl AsCommandStr for &std::process::Command {
    fn into_str(&self) -> String {
        shlex::join(
            std::iter::once(self.get_program())
                .chain(self.get_args())
                .map(|s| s.to_str().expect("Invalid UTF-8")),
        )
    }
}

impl ExecutionError {
    pub fn from_output<S: AsCommandStr>(
        command: S,
        output: &std::process::Output,
    ) -> Self {
        Self::CommandFailure(Box::new(FailureInfo {
            command: command.into_str(),
            status: output.status,
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        }))
    }
}
