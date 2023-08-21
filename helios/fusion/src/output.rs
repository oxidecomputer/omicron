// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
