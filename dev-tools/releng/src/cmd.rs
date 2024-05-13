// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::ffi::OsStr;
use std::fmt::Write;
use std::process::ExitStatus;
use std::process::Output;
use std::process::Stdio;
use std::time::Instant;

use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use slog::debug;
use slog::Logger;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

pub(crate) trait CommandExt {
    fn check_status(&self, status: ExitStatus) -> Result<()>;
    fn to_string(&self) -> String;

    async fn is_success(&mut self, logger: &Logger) -> Result<bool>;
    async fn ensure_success(&mut self, logger: &Logger) -> Result<()>;
    async fn ensure_stdout(&mut self, logger: &Logger) -> Result<String>;
}

impl CommandExt for Command {
    fn check_status(&self, status: ExitStatus) -> Result<()> {
        ensure!(
            status.success(),
            "command `{}` exited with {}",
            self.to_string(),
            status
        );
        Ok(())
    }

    fn to_string(&self) -> String {
        let command = self.as_std();
        let mut command_str = String::new();
        for (name, value) in command.get_envs() {
            if let Some(value) = value {
                write!(
                    command_str,
                    "{}={} ",
                    shell_words::quote(&name.to_string_lossy()),
                    shell_words::quote(&value.to_string_lossy())
                )
                .unwrap();
            }
        }
        write!(
            command_str,
            "{}",
            shell_words::join(
                std::iter::once(command.get_program())
                    .chain(command.get_args())
                    .map(OsStr::to_string_lossy)
            )
        )
        .unwrap();
        command_str
    }

    async fn is_success(&mut self, logger: &Logger) -> Result<bool> {
        Ok(xtrace(self, logger, Command::status).await?.success())
    }

    async fn ensure_success(&mut self, logger: &Logger) -> Result<()> {
        let status = xtrace(self, logger, Command::status).await?;
        self.check_status(status)
    }

    async fn ensure_stdout(&mut self, logger: &Logger) -> Result<String> {
        let output = xtrace(self, logger, Command::output).await?;

        // Obnoxiously, `tokio::process::Command::output` overrides
        // your stdout and stderr settings (because it doesn't use
        // std::process::Command::output).
        //
        // Compensate by dumping whatever is in `output.stderr` to stderr.
        tokio::io::stderr().write_all(&output.stderr).await?;

        self.check_status(output.status)?;
        String::from_utf8(output.stdout).context("command stdout was not UTF-8")
    }
}

trait AsStatus {
    fn as_status(&self) -> &ExitStatus;
}

impl AsStatus for ExitStatus {
    fn as_status(&self) -> &ExitStatus {
        &self
    }
}

impl AsStatus for Output {
    fn as_status(&self) -> &ExitStatus {
        &self.status
    }
}

async fn xtrace<F, Fut, T>(
    command: &mut Command,
    logger: &Logger,
    f: F,
) -> Result<T>
where
    F: FnOnce(&mut Command) -> Fut,
    Fut: std::future::Future<Output = std::io::Result<T>>,
    T: AsStatus,
{
    command.stdin(Stdio::null()).kill_on_drop(true);
    debug!(logger, "running: {}", command.to_string());
    let start = Instant::now();
    let result = f(command)
        .await
        .with_context(|| format!("failed to exec `{}`", command.to_string()))?;
    debug!(
        logger,
        "process exited with {} ({:?})",
        result.as_status(),
        Instant::now().saturating_duration_since(start)
    );
    Ok(result)
}
