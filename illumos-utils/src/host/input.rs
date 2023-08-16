// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::VecDeque;
use std::process::Command;

/// Wrapper around the input of a [std::process::Command] as strings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Input {
    pub program: String,
    pub args: VecDeque<String>,
    pub envs: Vec<(String, String)>,
}

impl Input {
    pub fn new<S: AsRef<str>>(program: S, args: Vec<S>) -> Self {
        Self {
            program: program.as_ref().to_string(),
            args: args.into_iter().map(|s| s.as_ref().to_string()).collect(),
            envs: vec![],
        }
    }

    /// Short-hand for a whitespace-separated string, which can be provided
    /// "like a shell command".
    pub fn shell<S: AsRef<str>>(input: S) -> Self {
        let mut args = input.as_ref().split_whitespace();

        Self::new(
            args.next().expect("Needs at least a program"),
            args.collect(),
        )
    }
}

impl std::fmt::Display for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.program)?;
        for arg in &self.args {
            write!(f, " {}", arg)?;
        }
        Ok(())
    }
}

fn os_str_to_string(s: &std::ffi::OsStr) -> String {
    s.to_string_lossy().to_string()
}

impl From<&Command> for Input {
    fn from(command: &Command) -> Self {
        Self {
            program: os_str_to_string(command.get_program()),
            args: command.get_args().map(os_str_to_string).collect(),
            envs: command
                .get_envs()
                .map(|(k, v)| {
                    (
                        os_str_to_string(k),
                        os_str_to_string(v.unwrap_or_default()),
                    )
                })
                .collect(),
        }
    }
}
