// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A process executor

use itertools::Itertools;
use slog::{debug, error, info, Logger};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, ExitStatus, Output, Stdio};
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
    /// Executes a task, waiting for it to complete, and returning output.
    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError>;

    /// Spawns a task, without waiting for it to complete.
    fn spawn(
        &self,
        command: &mut Command,
    ) -> Result<BoxedChild, ExecutionError>;
}

/// A wrapper around a spawned [Child] process.
pub type BoxedChild = Box<dyn Child>;

/// A child process spawned by the executor.
pub trait Child: Send {
    /// Accesses the stdin of the spawned child, as a Writer.
    fn stdin(&mut self) -> Option<Box<dyn Write + Send>>;

    /// Accesses the stdout of the spawned child, as a Reader.
    fn stdout(&mut self) -> Option<Box<dyn Read + Send>>;

    /// Accesses the stderr of the spawned child, as a Reader.
    fn stderr(&mut self) -> Option<Box<dyn Read + Send>>;

    /// Waits for the child to complete, and returns the output.
    fn wait(&mut self) -> Result<Output, ExecutionError>;
}

/// A real, host-controlled child process
pub struct SpawnedChild {
    command_str: String,
    child: Option<std::process::Child>,
}

impl Child for SpawnedChild {
    fn stdin(&mut self) -> Option<Box<dyn Write + Send>> {
        self.child
            .as_mut()?
            .stdin
            .take()
            .map(|s| Box::new(s) as Box<dyn Write + Send>)
    }

    fn stdout(&mut self) -> Option<Box<dyn Read + Send>> {
        self.child
            .as_mut()?
            .stdout
            .take()
            .map(|s| Box::new(s) as Box<dyn Read + Send>)
    }

    fn stderr(&mut self) -> Option<Box<dyn Read + Send>> {
        self.child
            .as_mut()?
            .stderr
            .take()
            .map(|s| Box::new(s) as Box<dyn Read + Send>)
    }

    fn wait(&mut self) -> Result<Output, ExecutionError> {
        let output =
            self.child.take().unwrap().wait_with_output().map_err(|err| {
                ExecutionError::ExecutionStart {
                    command: self.command_str.clone(),
                    err,
                }
            })?;

        if !output.status.success() {
            return Err(output_to_exec_error(
                self.command_str.clone(),
                &output,
            ));
        }

        Ok(output)
    }
}

/// A queue of bytes that can selectively act as a reader or writer,
/// which can also be cloned.
///
/// This is primarily used to emulate stdin / stdout / stderr.
#[derive(Clone)]
struct ByteQueue {
    buf: Arc<Mutex<VecDeque<u8>>>,
}

impl ByteQueue {
    fn new() -> Self {
        Self { buf: Arc::new(Mutex::new(VecDeque::new())) }
    }
}

impl std::io::Write for ByteQueue {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl std::io::Read for ByteQueue {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.buf.lock().unwrap().read(buf)
    }
}

/// A child spawned by a [FakeExecutor].
pub struct FakeChild {
    id: u64,
    command: Command,
    executor: Arc<FakeExecutorInner>,
    stdin: ByteQueue,
    stdout: ByteQueue,
    stderr: ByteQueue,
}

impl FakeChild {
    fn new(
        id: u64,
        command: &Command,
        executor: Arc<FakeExecutorInner>,
    ) -> Box<Self> {
        // std::process::Command -- somewhat reasonably - doesn't implement Copy
        // or Clone. However, we'd like to be able to reference it in the
        // FakeChild, independently of where it was spawned.
        //
        // Manually copy the relevant pieces of the incoming command.
        let mut copy_command = Command::new(command.get_program());
        copy_command.args(command.get_args());
        copy_command.envs(command.get_envs().filter_map(|(k, v)| {
            if let Some(v) = v {
                Some((k, v))
            } else {
                None
            }
        }));

        Box::new(FakeChild {
            id,
            command: copy_command,
            executor,
            stdin: ByteQueue::new(),
            stdout: ByteQueue::new(),
            stderr: ByteQueue::new(),
        })
    }

    fn command(&self) -> &Command {
        &self.command
    }
}

impl Child for FakeChild {
    fn stdin(&mut self) -> Option<Box<dyn Write + Send>> {
        Some(Box::new(self.stdin.clone()))
    }

    fn stdout(&mut self) -> Option<Box<dyn Read + Send>> {
        Some(Box::new(self.stdout.clone()))
    }

    fn stderr(&mut self) -> Option<Box<dyn Read + Send>> {
        Some(Box::new(self.stderr.clone()))
    }

    fn wait(&mut self) -> Result<Output, ExecutionError> {
        let executor = self.executor.clone();
        let output = executor.wait_handler.lock().unwrap()(self);
        log_output(&self.executor.log, self.id, &output);
        if !output.status.success() {
            return Err(output_to_exec_error(
                command_to_string(&self.command),
                &output,
            ));
        }
        Ok(output)
    }
}

pub fn command_to_string(command: &std::process::Command) -> String {
    command
        .get_args()
        .map(|s| s.to_string_lossy().into())
        .collect::<Vec<String>>()
        .join(" ")
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
            .unwrap_or_else(|| panic!("Unexpected command: {input}"));
        self.index += 1;
        assert_eq!(input, expected.0, "Unexpected input command");
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

/// Handler called when spawning a fake child process
pub type SpawnFn = dyn FnMut(&mut FakeChild) + Send + Sync;
pub type BoxedSpawnFn = Box<SpawnFn>;

/// Handler called when awaiting a fake child process
pub type WaitFn = dyn FnMut(&mut FakeChild) -> Output + Send + Sync;
pub type BoxedWaitFn = Box<WaitFn>;

struct FakeExecutorInner {
    log: Logger,
    counter: AtomicU64,
    spawn_handler: Mutex<BoxedSpawnFn>,
    wait_handler: Mutex<BoxedWaitFn>,
}

/// An executor which can expect certain inputs, and respond with specific outputs.
pub struct FakeExecutor {
    inner: Arc<FakeExecutorInner>,
}

impl FakeExecutor {
    pub fn new(log: Logger) -> Arc<FakeExecutor> {
        Arc::new(Self {
            inner: Arc::new(FakeExecutorInner {
                log,
                counter: AtomicU64::new(0),
                spawn_handler: Mutex::new(Box::new(|_cmd| ())),
                wait_handler: Mutex::new(Box::new(|_cmd| Output::success())),
            }),
        })
    }

    /// Set the spawn handler to an arbitrary function.
    pub fn set_spawn_handler(&self, f: BoxedSpawnFn) {
        *self.inner.spawn_handler.lock().unwrap() = f;
    }

    /// Set the request handler to an arbitrary function.
    pub fn set_wait_handler(&self, f: BoxedWaitFn) {
        *self.inner.wait_handler.lock().unwrap() = f;
    }

    /// Set the request handler to a static set of inputs and outputs.
    pub fn set_static_handler(&self, mut handler: StaticHandler) {
        self.set_wait_handler(Box::new(move |child| -> Output {
            handler.execute(child.command())
        }));
    }

    /// Perform some type coercion to access a commonly-used trait object.
    pub fn as_executor(self: Arc<Self>) -> BoxedExecutor {
        self
    }
}

impl Executor for FakeExecutor {
    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError> {
        let id = self.inner.counter.fetch_add(1, Ordering::SeqCst);
        log_command(&self.inner.log, id, command);

        let mut child = FakeChild::new(id, command, self.inner.clone());

        // Call our handler function with the caller-provided functions.
        //
        // This performs both the "spawn" and "wait" actions back-to-back.
        self.inner.spawn_handler.lock().unwrap()(&mut child);
        let output = self.inner.wait_handler.lock().unwrap()(&mut child);
        log_output(&self.inner.log, id, &output);

        if !output.status.success() {
            return Err(output_to_exec_error(
                command_to_string(command),
                &output,
            ));
        }
        Ok(output)
    }

    fn spawn(
        &self,
        command: &mut Command,
    ) -> Result<BoxedChild, ExecutionError> {
        let id = self.inner.counter.fetch_add(1, Ordering::SeqCst);
        log_command(&self.inner.log, id, command);

        Ok(FakeChild::new(id, command, self.inner.clone()))
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
            return Err(output_to_exec_error(
                command_to_string(&command),
                &output,
            ));
        }
        Ok(output)
    }

    fn spawn(
        &self,
        command: &mut Command,
    ) -> Result<BoxedChild, ExecutionError> {
        let command_str = command_to_string(&command);
        Ok(Box::new(SpawnedChild {
            child: Some(
                command
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .map_err(|err| ExecutionError::ExecutionStart {
                    command: command_str.clone(),
                    err,
                })?,
            ),
            command_str,
        }))
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
