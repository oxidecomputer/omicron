// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::ffi::OsStr;
use std::path::Path;
use std::process::ExitStatus;
use std::process::Output;
use std::process::Stdio;
use std::time::Instant;

use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use slog::debug;
use slog::Logger;

/// Wrapper for `tokio::process::Command` where the builder methods take/return
/// `self`, plus a number of convenience methods.
pub(crate) struct Command {
    inner: tokio::process::Command,
}

impl Command {
    pub(crate) fn new(program: impl AsRef<OsStr>) -> Command {
        Command { inner: tokio::process::Command::new(program) }
    }

    pub(crate) fn arg(mut self, arg: impl AsRef<OsStr>) -> Command {
        self.inner.arg(arg);
        self
    }

    pub(crate) fn args(
        mut self,
        args: impl IntoIterator<Item = impl AsRef<OsStr>>,
    ) -> Command {
        self.inner.args(args);
        self
    }

    pub(crate) fn current_dir(mut self, dir: impl AsRef<Path>) -> Command {
        self.inner.current_dir(dir);
        self
    }

    pub(crate) fn env(
        mut self,
        key: impl AsRef<OsStr>,
        value: impl AsRef<OsStr>,
    ) -> Command {
        self.inner.env(key, value);
        self
    }

    pub(crate) fn env_remove(mut self, key: impl AsRef<OsStr>) -> Command {
        self.inner.env_remove(key);
        self
    }

    pub(crate) async fn is_success(mut self, logger: &Logger) -> Result<bool> {
        self.inner
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        Ok(xtrace(&mut self, logger).await?.status.success())
    }

    pub(crate) async fn ensure_success(
        mut self,
        logger: &Logger,
    ) -> Result<()> {
        self.inner
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        let status = xtrace(&mut self, logger).await?.status;
        check_status(self, status)
    }

    pub(crate) async fn ensure_stdout(
        mut self,
        logger: &Logger,
    ) -> Result<String> {
        self.inner
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let output = xtrace(&mut self, logger).await?;
        check_status(self, output.status)?;
        String::from_utf8(output.stdout).context("command stdout was not UTF-8")
    }

    pub(crate) fn into_parts(self) -> (Description, tokio::process::Command) {
        (Description { str: self.to_string() }, self.inner)
    }
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let command = self.inner.as_std();
        for (name, value) in command.get_envs() {
            if let Some(value) = value {
                write!(
                    f,
                    "{}={} ",
                    shell_words::quote(&name.to_string_lossy()),
                    shell_words::quote(&value.to_string_lossy())
                )?;
            }
        }
        write!(
            f,
            "{}",
            shell_words::quote(&command.get_program().to_string_lossy())
        )?;
        for arg in command.get_args() {
            write!(f, " {}", shell_words::quote(&arg.to_string_lossy()))?;
        }
        Ok(())
    }
}

/// Returned from [`Command::into_parts`] for use in the `job` module.
pub(crate) struct Description {
    str: String,
}

impl Description {
    pub(crate) fn check_status(&self, status: ExitStatus) -> Result<()> {
        check_status(self, status)
    }
}

impl std::fmt::Display for Description {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.str)
    }
}

fn check_status(
    command: impl std::fmt::Display,
    status: ExitStatus,
) -> Result<()> {
    ensure!(status.success(), "command `{}` exited with {}", command, status);
    Ok(())
}

async fn xtrace(command: &mut Command, logger: &Logger) -> Result<Output> {
    command.inner.stdin(Stdio::null()).kill_on_drop(true);
    debug!(logger, "running: {}", command);
    let start = Instant::now();
    let output = command
        .inner
        .spawn()
        .with_context(|| format!("failed to exec `{}`", command))?
        .wait_with_output()
        .await
        .with_context(|| format!("failed to wait on `{}`", command))?;
    debug!(
        logger,
        "process exited with {} ({:?})",
        output.status,
        Instant::now().saturating_duration_since(start)
    );
    Ok(output)
}
