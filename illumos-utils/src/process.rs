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

pub trait OutputExt: Sized {
    fn success() -> Self;
    fn silent_failure() -> Self;
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

    fn silent_failure() -> Self {
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

#[derive(Clone)]
pub struct CompletedCommand {
    pub input: Input,
    pub output: Output,
}

fn os_str_to_string(s: &std::ffi::OsStr) -> String {
    s.to_string_lossy().to_string()
}

impl CompletedCommand {
    fn new(command: &Command, output: Output) -> Self {
        Self { input: Input::from(command), output }
    }
}

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
        self.expect(Input::shell(input), Output::silent_failure())
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

pub type ExecutorFn = Box<dyn FnMut(&Command) -> Output + Send + Sync>;

pub struct FakeExecutor {
    log: Logger,
    counter: AtomicU64,
    all_operations: Mutex<Vec<CompletedCommand>>,
    handler: Mutex<ExecutorFn>,
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

    pub fn set_handler(&self, f: ExecutorFn) {
        *self.handler.lock().unwrap() = f;
    }

    pub fn set_static_handler(&self, mut handler: StaticHandler) {
        self.set_handler(Box::new(move |cmd| -> Output {
            handler.execute(cmd)
        }));
    }

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

        // TODO: De-duplicate this with the RealExecutor
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
