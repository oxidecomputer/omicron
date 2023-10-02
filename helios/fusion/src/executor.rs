// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces used to interact with the underlying host system.

use crate::{error::ExecutionError, input::Input, output::Output};

use async_trait::async_trait;
use itertools::Itertools;
use slog::{debug, info, Logger};
use std::io::{Read, Write};
use std::process::Command;
use std::str::from_utf8;
use std::sync::Arc;

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

pub fn log_input(log: &Logger, id: u64, command: &Command) {
    info!(
        log,
        "running command via executor"; "id" => id, "command" => %Input::from(command)
    );
    debug!(
        log,
        "running command via executor"; "id" => id, "envs" => %to_space_separated_string(command.get_envs())
    );
}

pub fn log_output(log: &Logger, id: u64, output: &Output) {
    info!(
        log,
        "finished running command via executor";
        "id" => id,
        "succeeded" => output.status.success(),
        "status" => output.status.code()
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
/// - In production, this is usually `helios_protostar`'s executor.
/// - Under test, this can be customized, and `helios_tokamak`'s executor may be used.
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

/// A wrapper around a spawned [Child] process.
pub type BoxedChild = Box<dyn Child>;

/// A child process spawned by the executor.
pub trait Child: Send {
    /// Accesses the stdin of the spawned child, as a Writer.
    fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>>;

    /// Accesses the stdout of the spawned child, as a Reader.
    fn take_stdout(&mut self) -> Option<Box<dyn Read + Send>>;

    /// Accesses the stderr of the spawned child, as a Reader.
    fn take_stderr(&mut self) -> Option<Box<dyn Read + Send>>;

    /// OS-assigned PID identifier for the child
    fn id(&self) -> u32;

    /// Waits for the child to complete, and returns the output.
    fn wait(self: Box<Self>) -> Result<Output, ExecutionError>;
}
