// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A process executor

use itertools::Itertools;
use slog::{debug, error, info, Logger};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, ExitStatus, Output};
use std::str::from_utf8;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// NOTE: Is the "counter as ID" misleading?
//
// It's not actually possible to make an incrementing counter avoid the race of
// "log, do operation, log again" without making executing processes serialized
// (which seems bad).
//
// We could make this a UUID, but I don't like how hard-to-read those can be
// when trying to quickly parse logs.

pub type BoxedExecutor = Arc<dyn Executor>;

pub trait Executor: Send + Sync {
    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError>;
}

fn log_command(log: &Logger, id: u64, command: &Command) {
    info!(
        log,
        "{id} - Running Command: [{:?} {:?}]",
        command.get_program(),
        to_space_separated_string(command.get_args()),
    );
    debug!(
        log,
        "{id} - Environment: [{:?}]",
        to_space_separated_string(command.get_envs()),
    )
}

fn log_output(log: &Logger, id: u64, output: &Output) {
    info!(
        log,
        "{id} - {} (status code: {})",
        if output.status.success() { "OK" } else { "ERROR" },
        output
            .status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    if !output.stdout.is_empty() {
        debug!(
            log,
            "{id} - stdout: {}",
            from_utf8(&output.stdout).unwrap_or("<Not valid UTF-8>"),
        );
    }
    if !output.stderr.is_empty() {
        debug!(
            log,
            "{id} - stderr: {}",
            from_utf8(&output.stderr).unwrap_or("<Not valid UTF-8>"),
        );
    }
}

pub struct FakeExecutor {
    log: Logger,
    counter: AtomicU64,
    all_operations: Mutex<Vec<Command>>,
}

impl FakeExecutor {
    pub fn new(log: Logger) -> Arc<FakeExecutor> {
        Arc::new(Self {
            log,
            counter: AtomicU64::new(0),
            all_operations: Mutex::new(vec![]),
        })
    }

    pub fn as_executor(self: Arc<Self>) -> BoxedExecutor {
        self
    }
}

impl Executor for FakeExecutor {
    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError> {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        log_command(&self.log, id, command);

        // TODO: Environment variables?
        let mut record = Command::new(command.get_program());
        record.args(command.get_args());
        self.all_operations.lock().unwrap().push(record);

        // TODO: Control failure of the command?
        let output = Output {
            status: ExitStatus::from_raw(0),
            stdout: vec![],
            stderr: vec![],
        };
        log_output(&self.log, id, &output);
        Ok(output)
    }
}

pub struct RealExecutor {
    log: slog::Logger,
    counter: std::sync::atomic::AtomicU64,
}

impl RealExecutor {
    pub fn new(log: Logger) -> Arc<Self> {
        Arc::new(Self { log, counter: AtomicU64::new(0) })
    }

    pub fn as_executor(self: Arc<Self>) -> BoxedExecutor {
        self
    }
}

impl Executor for RealExecutor {
    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError> {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        log_command(&self.log, id, command);
        let output = command.output().map_err(|err| {
            error!(self.log, "{id} - Could not start program!");
            ExecutionError::ExecutionStart {
                command: to_space_separated_string(command.get_args()),
                err,
            }
        })?;
        log_output(&self.log, id, &output);

        if !output.status.success() {
            return Err(ExecutionError::CommandFailure(Box::new(
                FailureInfo {
                    command: command
                        .get_args()
                        .map(|s| s.to_string_lossy().into())
                        .collect::<Vec<String>>()
                        .join(" "),
                    status: output.status,
                    stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                    stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                },
            )));
        }
        Ok(output)
    }
}

pub const PFEXEC: &str = "/usr/bin/pfexec";

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
}

// We wrap this method in an inner module to make it possible to mock
// these free functions.
fn to_space_separated_string<T, I>(iter: T) -> String
where
    T: IntoIterator<Item = I>,
    I: std::fmt::Debug,
{
    Itertools::intersperse(
        iter.into_iter().map(|arg| format!("{arg:?}")),
        " ".into(),
    )
    .collect::<String>()
}
