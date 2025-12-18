// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code that manages command dispatch from a shell for wicket.

use std::net::{Ipv6Addr, SocketAddrV6};

use anyhow::{Context, Result, bail};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use omicron_common::{FileKv, address::WICKETD_PORT};
use slog::Drain;

use crate::{
    Runner,
    cli::{CommandOutput, ShellApp},
};

pub fn exec() -> Result<()> {
    let wicketd_addr =
        SocketAddrV6::new(Ipv6Addr::LOCALHOST, WICKETD_PORT, 0, 0);

    // SSH_ORIGINAL_COMMAND contains additional arguments, if any.
    match std::env::var("SSH_ORIGINAL_COMMAND") {
        Ok(ssh_args) => {
            let args = shell_words::split(&ssh_args).with_context(|| {
                format!("could not parse shell arguments from input {ssh_args}")
            })?;

            let runtime = tokio::runtime::Runtime::new()
                .context("creating tokio runtime")?;
            runtime.block_on(exec_with_args(
                wicketd_addr,
                args,
                OutputKind::Terminal,
            ))
        }
        Err(_) => {
            // Do not expose log messages via standard error since they'll show up
            // on top of the TUI.
            let log = setup_log(&log_path()?, WithStderr::No)?;
            Runner::new(log, wicketd_addr).run()
        }
    }
}

/// Enables capturing of wicket's output.
pub enum OutputKind<'a> {
    /// Captures output to the provided log, as well as a buffer.
    Captured {
        log: slog::Logger,
        stdout: &'a mut Vec<u8>,
        stderr: &'a mut Vec<u8>,
    },

    /// Writes output to a terminal.
    Terminal,
}

pub async fn exec_with_args<S>(
    wicketd_addr: SocketAddrV6,
    args: Vec<S>,
    output: OutputKind<'_>,
) -> Result<()>
where
    S: AsRef<str>,
{
    // parse_from uses the the first argument as the command name. Insert "wicket" as
    // the command name.
    let app = ShellApp::parse_from(
        std::iter::once("wicket").chain(args.iter().map(|s| s.as_ref())),
    );

    match output {
        OutputKind::Captured { log, stdout, stderr } => {
            let output = CommandOutput { stdout, stderr };
            app.exec(log, wicketd_addr, output).await
        }
        OutputKind::Terminal => {
            let log = setup_log(
                &log_path()?,
                WithStderr::Yes { use_color: app.global_opts.use_color() },
            )?;
            let mut stdout = std::io::stdout();
            let mut stderr = std::io::stderr();
            let output =
                CommandOutput { stdout: &mut stdout, stderr: &mut stderr };
            app.exec(log, wicketd_addr, output).await
        }
    }
}

fn setup_log(
    path: &Utf8Path,
    with_stderr: WithStderr,
) -> anyhow::Result<slog::Logger> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("error opening log file {path}"))?;

    let decorator = slog_term::PlainDecorator::new(file);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();

    let drain = match with_stderr {
        WithStderr::Yes { use_color } => {
            let stderr_drain = stderr_env_drain("RUST_LOG", use_color);
            let drain = slog::Duplicate::new(drain, stderr_drain).fuse();
            slog_async::Async::new(drain).build().fuse()
        }
        WithStderr::No => slog_async::Async::new(drain).build().fuse(),
    };

    Ok(slog::Logger::root(drain, slog::o!(FileKv)))
}

#[derive(Copy, Clone, Debug)]
enum WithStderr {
    Yes { use_color: bool },
    No,
}

fn log_path() -> Result<Utf8PathBuf> {
    match std::env::var("WICKET_LOG_PATH") {
        Ok(path) => Ok(path.into()),
        Err(std::env::VarError::NotPresent) => Ok("/tmp/wicket.log".into()),
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("WICKET_LOG_PATH is not valid unicode");
        }
    }
}

fn stderr_env_drain(
    env_var: &str,
    use_color: bool,
) -> impl Drain<Ok = (), Err = slog::Never> + use<> {
    let mut builder = slog_term::TermDecorator::new();
    if use_color {
        builder = builder.force_color();
    } else {
        builder = builder.force_plain();
    }
    let stderr_decorator = builder.build();
    let stderr_drain =
        slog_term::FullFormat::new(stderr_decorator).build().fuse();
    let mut builder = slog_envlogger::LogBuilder::new(stderr_drain);
    if let Ok(s) = std::env::var(env_var) {
        builder = builder.parse(&s);
    } else {
        // Log at the info level by default.
        builder = builder.filter(None, slog::FilterLevel::Info);
    }
    builder.build()
}
