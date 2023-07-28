// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code that manages command dispatch from a shell for wicket.

use std::net::{Ipv6Addr, SocketAddrV6};

use anyhow::{bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use omicron_common::{address::WICKETD_PORT, FileKv};
use slog::Drain;

use crate::{rack_setup::SetupArgs, upload::UploadArgs, Runner};

pub fn exec() -> Result<()> {
    let wicketd_addr =
        SocketAddrV6::new(Ipv6Addr::LOCALHOST, WICKETD_PORT, 0, 0);

    // SSH_ORIGINAL_COMMAND contains additional arguments, if any.
    if let Ok(ssh_args) = std::env::var("SSH_ORIGINAL_COMMAND") {
        // The argument is in a quoted form, so split it using Unix shell semantics.
        let args = shell_words::split(&ssh_args).with_context(|| {
            format!("could not parse shell arguments from input {ssh_args}")
        })?;

        let log = setup_log(&log_path()?, WithStderr::Yes)?;
        // parse_from uses the the first argument as the command name. Insert "wicket" as
        // the command name.
        let args = ShellCommand::parse_from(
            std::iter::once("wicket".to_owned()).chain(args),
        );
        match args {
            ShellCommand::UploadRepo(args) => args.exec(log, wicketd_addr),
            ShellCommand::Setup(args) => args.exec(log, wicketd_addr),
        }
    } else {
        // Do not expose log messages via standard error since they'll show up
        // on top of the TUI.
        let log = setup_log(&log_path()?, WithStderr::No)?;
        Runner::new(log, wicketd_addr).run()
    }
}

/// Arguments passed to wicket.
///
/// Wicket is designed to be used as a captive shell, set up via sshd
/// ForceCommand. If no arguments are specified, wicket behaves like a TUI.
/// However, if arguments are specified via SSH_ORIGINAL_COMMAND, wicketd
/// accepts an upload command.
#[derive(Debug, Parser)]
enum ShellCommand {
    /// Upload a TUF repository to wicketd.
    UploadRepo(UploadArgs),
    /// Interact with rack setup configuration.
    #[command(subcommand)]
    Setup(SetupArgs),
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
        WithStderr::Yes => {
            let stderr_drain = stderr_env_drain("RUST_LOG");
            let drain = slog::Duplicate::new(drain, stderr_drain).fuse();
            slog_async::Async::new(drain).build().fuse()
        }
        WithStderr::No => slog_async::Async::new(drain).build().fuse(),
    };

    Ok(slog::Logger::root(drain, slog::o!(FileKv)))
}

#[derive(Copy, Clone, Debug)]
enum WithStderr {
    Yes,
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

fn stderr_env_drain(env_var: &str) -> impl Drain<Ok = (), Err = slog::Never> {
    let stderr_decorator = slog_term::TermDecorator::new().build();
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
