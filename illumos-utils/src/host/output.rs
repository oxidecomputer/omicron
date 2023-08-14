// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::host::error::{ExecutionError, FailureInfo};

use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;

pub type Output = std::process::Output;

/// Convenience functions for usage in tests, to perform common operations
/// with minimal boilerplate.
pub trait OutputExt: Sized {
    fn success() -> Self;
    fn failure() -> Self;
    fn set_stdout<S: AsRef<str>>(self, stdout: S) -> Self;
    fn set_stderr<S: AsRef<str>>(self, stderr: S) -> Self;
}

impl OutputExt for Output {
    fn success() -> Self {
        Output {
            status: ExitStatus::from_raw(0),
            stdout: vec![],
            stderr: vec![],
        }
    }

    fn failure() -> Self {
        Output {
            status: ExitStatus::from_raw(-1),
            stdout: vec![],
            stderr: vec![],
        }
    }

    fn set_stdout<S: AsRef<str>>(mut self, stdout: S) -> Self {
        self.stdout = stdout.as_ref().as_bytes().to_vec();
        self
    }

    fn set_stderr<S: AsRef<str>>(mut self, stderr: S) -> Self {
        self.stderr = stderr.as_ref().as_bytes().to_vec();
        self
    }
}

pub fn output_to_exec_error(
    command_str: String,
    output: &std::process::Output,
) -> ExecutionError {
    ExecutionError::CommandFailure(Box::new(FailureInfo {
        command: command_str,
        status: output.status,
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }))
}
