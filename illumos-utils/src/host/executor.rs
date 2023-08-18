// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::host::{
    byte_queue::ByteQueue, error::ExecutionError, input::Input,
    output::output_to_exec_error, output::Output, output::OutputExt,
};

use async_trait::async_trait;
use itertools::Itertools;
use slog::{debug, error, info, Logger};
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::str::from_utf8;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub fn command_to_string(command: &std::process::Command) -> String {
    command
        .get_args()
        .map(|s| s.to_string_lossy().into())
        .collect::<Vec<String>>()
        .join(" ")
}

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

fn log_input(log: &Logger, id: u64, command: &Command) {
    info!(
        log,
        "running command via executor"; "id" => id, "command" => %Input::from(command)
    );
    debug!(
        log,
        "running command via executor"; "id" => id, "envs" => %to_space_separated_string(command.get_envs())
    );
}

fn log_output(log: &Logger, id: u64, output: &Output) {
    info!(
        log,
        "finished running command via executor";
        "id" => id,
        "result" => if output.status.success() { "OK" } else { "ERROR" },
        "status" => output
            .status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
    if !output.stdout.is_empty() {
        debug!(
            log,
            "finished command stdout";
            "id" => id,
            "stdout" => from_utf8(&output.stdout).unwrap_or("<Not valid UTF-8>"),
        );
    }
    if !output.stderr.is_empty() {
        debug!(
            log,
            "finished command stderr";
            "id" => id,
            "stderr" => from_utf8(&output.stderr).unwrap_or("<Not valid UTF-8>"),
        );
    }
}

/// Describes the commonly-used "safe-to-reference" type describing the
/// Executor as a trait object.
pub type BoxedExecutor = Arc<dyn Executor>;

/// Describes an "executor", which can run [Command]s and return a response.
///
/// - In production, this is usually simply a [HostExecutor].
/// - Under test, this can be customized, and a [FakeExecutor] may be used.
#[async_trait]
pub trait Executor: Send + Sync {
    /// Executes a task, waiting for it to complete, and returning output.
    async fn execute_async(
        &self,
        command: &mut tokio::process::Command,
    ) -> Result<Output, ExecutionError>;

    /// Executes a task, waiting for it to complete, and returning output.
    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError>;

    /// Spawns a task, without waiting for it to complete.
    fn spawn(
        &self,
        command: &mut Command,
    ) -> Result<BoxedChild, ExecutionError>;
}

/// Handler called when spawning a fake child process
pub type SpawnFn = dyn FnMut(&mut FakeChild) + Send + Sync;
pub type BoxedSpawnFn = Box<SpawnFn>;

/// Handler called when awaiting a fake child process
pub type WaitFn = dyn FnMut(&mut FakeChild) -> Output + Send + Sync;
pub type BoxedWaitFn = Box<WaitFn>;

pub(crate) struct FakeExecutorInner {
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

    /// Perform some type coercion to access a commonly-used trait object.
    pub fn as_executor(self: Arc<Self>) -> BoxedExecutor {
        self
    }

    fn execute_internal(
        &self,
        command: &Command,
    ) -> Result<Output, ExecutionError> {
        let id = self.inner.counter.fetch_add(1, Ordering::SeqCst);
        log_input(&self.inner.log, id, command);

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
}

#[async_trait]
impl Executor for FakeExecutor {
    // NOTE: We aren't actually performing any async operations -- it's up to
    // the caller to control the (synchronous) handlers.
    //
    // However, this still provides testability, while letting the "real
    // executor" make truly async calls while launching processes.
    async fn execute_async(
        &self,
        command: &mut tokio::process::Command,
    ) -> Result<Output, ExecutionError> {
        self.execute_internal(command.as_std())
    }

    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError> {
        self.execute_internal(command)
    }

    fn spawn(
        &self,
        command: &mut Command,
    ) -> Result<BoxedChild, ExecutionError> {
        let id = self.inner.counter.fetch_add(1, Ordering::SeqCst);
        log_input(&self.inner.log, id, command);

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

    fn prepare(&self, command: &Command) -> u64 {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        log_input(&self.log, id, command);
        id
    }

    fn finalize(
        &self,
        command: &Command,
        id: u64,
        output: Output,
    ) -> Result<Output, ExecutionError> {
        log_output(&self.log, id, &output);
        if !output.status.success() {
            return Err(output_to_exec_error(
                command_to_string(command),
                &output,
            ));
        }
        Ok(output)
    }
}

#[async_trait]
impl Executor for HostExecutor {
    async fn execute_async(
        &self,
        command: &mut tokio::process::Command,
    ) -> Result<Output, ExecutionError> {
        let id = self.prepare(command.as_std());
        let output = command.output().await.map_err(|err| {
            error!(self.log, "Could not start program asynchronously!"; "id" => id);
            ExecutionError::ExecutionStart {
                command: Input::from(command.as_std()).to_string(),
                err,
            }
        })?;
        self.finalize(command.as_std(), id, output)
    }

    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError> {
        let id = self.prepare(command);
        let output = command.output().map_err(|err| {
            error!(self.log, "Could not start program!"; "id" => id);
            ExecutionError::ExecutionStart {
                command: Input::from(&*command).to_string(),
                err,
            }
        })?;
        self.finalize(command, id, output)
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

    /// OS-assigned PID identifier for the child
    fn id(&self) -> u32;

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

    fn id(&self) -> u32 {
        self.child.as_ref().expect("No child").id()
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

    pub fn command(&self) -> &Command {
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

    fn id(&self) -> u32 {
        self.id.try_into().expect("u32 overflow")
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

type DynamicHandler = Box<dyn FnMut(Input) -> Output + Send + Sync>;

enum HandledCommand {
    Static { input: Input, output: Output },
    Dynamic { handler: DynamicHandler },
}

/// A handler that may be used for setting inputs/outputs to the executor
/// when these commands are known ahead-of-time.
///
/// See: [Self::register] for integration with a [FakeExecutor].
pub struct StaticHandler {
    expected: Vec<HandledCommand>,
    index: usize,
}

impl StaticHandler {
    pub fn new() -> Self {
        Self { expected: Vec::new(), index: 0 }
    }

    /// Convenience function to register the handler with a [FakeExecutor].
    pub fn register(mut self, executor: &FakeExecutor) {
        executor.set_wait_handler(Box::new(move |child| -> Output {
            self.execute(child.command())
        }));
    }

    /// Expects a static "input" to exactly produce some "output".
    pub fn expect(&mut self, input: Input, output: Output) {
        self.expected.push(HandledCommand::Static { input, output });
    }

    /// A helper for [Self::expect] which quietly succeeds.
    pub fn expect_ok<S: AsRef<str>>(&mut self, input: S) {
        self.expect(Input::shell(input), Output::success())
    }

    /// A helper for [Self::expect] which quietly fails.
    pub fn expect_fail<S: AsRef<str>>(&mut self, input: S) {
        self.expect(Input::shell(input), Output::failure())
    }

    /// Expects a dynamic handler to be invoked to dynamically
    /// determine the output of this call.
    pub fn expect_dynamic(&mut self, handler: DynamicHandler) {
        self.expected.push(HandledCommand::Dynamic { handler });
    }

    fn execute(&mut self, command: &Command) -> Output {
        let observed_input = Input::from(command);
        let expected = &mut self
            .expected
            .get_mut(self.index)
            .unwrap_or_else(|| panic!("Unexpected command: {observed_input}"));
        self.index += 1;

        match expected {
            HandledCommand::Static { input, output } => {
                assert_eq!(&observed_input, input, "Unexpected input command");
                output.clone()
            }
            HandledCommand::Dynamic { ref mut handler } => {
                handler(observed_input)
            }
        }
    }
}

impl Drop for StaticHandler {
    fn drop(&mut self) {
        let expected = self.expected.len();
        let actual = self.index;
        if actual < expected {
            let next = &self.expected[actual];
            let tip = match next {
                HandledCommand::Static { input, .. } => input.to_string(),
                HandledCommand::Dynamic { .. } => {
                    "<dynamic handler>".to_string()
                }
            };
            let errmsg = format!("Only saw {actual} calls, expected {expected}\nNext would have been: {tip}");
            if !std::thread::panicking() {
                assert!(false, "{errmsg}");
            } else {
                eprintln!("{errmsg}");
            }
        }
    }
}
