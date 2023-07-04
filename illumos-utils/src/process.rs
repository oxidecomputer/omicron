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

/// Describes the commonly-used "safe-to-reference" type describing the
/// Executor as a trait object.
pub type BoxedExecutor = Arc<dyn Executor>;

/// Describes an "executor", which can run [Command]s and return a response.
///
/// - In production, this is usually simply a [HostExecutor].
/// - Under test, this can be customized, and a [FakeExecutor] may be used.
pub trait Executor: Send + Sync {
    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError>;
}

fn log_command(log: &Logger, id: u64, command: &Command) {
    info!(log, "{id} - Running Command: [{}]", Input::from(command),);
    debug!(
        log,
        "{id} - Environment: [{}]",
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

/// Wrapper around the input of a [std::process::Command] as strings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Input {
    pub program: String,
    pub args: Vec<String>,
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
            args: command
                .get_args()
                .into_iter()
                .map(os_str_to_string)
                .collect(),
            envs: command
                .get_envs()
                .into_iter()
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

/// Describes a fully-completed command.
#[derive(Clone)]
pub struct CompletedCommand {
    pub input: Input,
    pub output: Output,
}

impl CompletedCommand {
    fn new(command: &Command, output: Output) -> Self {
        Self { input: Input::from(command), output }
    }
}

/// A handler that may be used for setting inputs/outputs to the executor
/// when these commands are known ahead-of-time.
///
/// See: [FakeExecutor::set_static_handler] for usage.
pub struct StaticHandler {
    expected: Vec<(Input, Output)>,
    index: usize,
}

impl StaticHandler {
    pub fn new() -> Self {
        Self { expected: Vec::new(), index: 0 }
    }

    pub fn expect(&mut self, input: Input, output: Output) {
        self.expected.push((input, output));
    }

    pub fn expect_ok<S: AsRef<str>>(&mut self, input: S) {
        self.expect(Input::shell(input), Output::success())
    }

    pub fn expect_fail<S: AsRef<str>>(&mut self, input: S) {
        self.expect(Input::shell(input), Output::failure())
    }

    fn execute(&mut self, command: &Command) -> Output {
        let input = Input::from(command);
        let expected = &self
            .expected
            .get(self.index)
            .expect(&format!("Unexpected command: {input}"));
        self.index += 1;
        assert_eq!(input, expected.0);
        expected.1.clone()
    }
}

impl Drop for StaticHandler {
    fn drop(&mut self) {
        let expected = self.expected.len();
        let actual = self.index;
        if actual < expected {
            let next = &self.expected[actual].0;
            assert!(false, "Only saw {actual} calls, expected {expected}\nNext would have been: {next}");
        }
    }
}

pub type ExecutorFn = dyn FnMut(&Command) -> Output + Send + Sync;
pub type BoxedExecutorFn = Box<ExecutorFn>;

/// An executor which can expect certain inputs, and respond with specific outputs.
pub struct FakeExecutor {
    log: Logger,
    counter: AtomicU64,
    all_operations: Mutex<Vec<CompletedCommand>>,
    handler: Mutex<BoxedExecutorFn>,
}

impl FakeExecutor {
    pub fn new(log: Logger) -> Arc<FakeExecutor> {
        Arc::new(Self {
            log,
            counter: AtomicU64::new(0),
            all_operations: Mutex::new(vec![]),
            handler: Mutex::new(Box::new(|_cmd| Output::success())),
        })
    }

    /// Set the request handler to an arbitrary function.
    pub fn set_handler(&self, f: BoxedExecutorFn) {
        *self.handler.lock().unwrap() = f;
    }

    /// Set the request handler to a static set of inputs and outputs.
    pub fn set_static_handler(&self, mut handler: StaticHandler) {
        self.set_handler(Box::new(move |cmd| -> Output {
            handler.execute(cmd)
        }));
    }

    /// Perform some type coercion to access a commonly-used trait object.
    pub fn as_executor(self: Arc<Self>) -> BoxedExecutor {
        self
    }

    pub fn all_operations(&self) -> Vec<CompletedCommand> {
        (*self.all_operations.lock().unwrap()).clone()
    }
}

impl Executor for FakeExecutor {
    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError> {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        log_command(&self.log, id, command);

        // Call our handler function with the caller-provided function.
        let output = self.handler.lock().unwrap()(command);

        // TODO: De-duplicate this with the HostExecutor
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
        log_output(&self.log, id, &output);
        self.all_operations
            .lock()
            .unwrap()
            .push(CompletedCommand::new(command, output.clone()));
        Ok(output)
    }
}

pub struct HostExecutor {
    log: slog::Logger,
    counter: std::sync::atomic::AtomicU64,
}

impl HostExecutor {
    pub fn new(log: Logger) -> Arc<Self> {
        Arc::new(Self { log, counter: AtomicU64::new(0) })
    }

    pub fn as_executor(self: Arc<Self>) -> BoxedExecutor {
        self
    }
}

impl Executor for HostExecutor {
    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError> {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        log_command(&self.log, id, command);
        let output = command.output().map_err(|err| {
            error!(self.log, "{id} - Could not start program!");
            ExecutionError::ExecutionStart {
                command: Input::from(&*command).to_string(),
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

    #[error("Failed to enter zone: {err}")]
    ZoneEnter { err: std::io::Error },

    #[error("Zone not running")]
    NotRunning,
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
