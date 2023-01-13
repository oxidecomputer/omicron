// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code that manages command dispatch for wicket.

use std::net::SocketAddrV6;

use anyhow::{bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use slog::Drain;

use crate::{upload::UploadArgs, wizard::Wizard};

#[derive(Debug, Parser)]
#[command(version, author = "Oxide Computer Company")]
pub struct WicketApp {
    /// Login shell arguments.
    ///
    /// Wicket is designed to be a login shell for use over ssh. If no arguments are specified,
    /// wicket behaves like a TUI. However, if arguments are specified with "-c" (as in other login
    /// shells e.g. bash -c), wicketd accepts an upload command.
    ///
    /// Login shell arguments are provided in a quoted form, so we expect a single String here.
    /// This string is split using shell quoting logic to get the actual arguments.
    #[arg(short = 'c', allow_hyphen_values = true)]
    shell_args: Option<String>,
}

#[derive(Debug, Parser)]
enum ShellCommand {
    /// Upload an artifact to wicketd.
    Upload(UploadArgs),
}

impl WicketApp {
    /// Executes the command.
    pub fn exec(self) -> Result<()> {
        // TODO: make this configurable?
        let wicketd_addr: SocketAddrV6 = "[::1]:8000".parse().unwrap();

        match self.shell_args {
            Some(shell_args) => {
                let args =
                    shell_words::split(&shell_args).with_context(|| {
                        format!("could not parse shell arguments from input {shell_args}")
                    })?;
                let log = setup_log(&log_path()?, WithStderr::Yes)?;
                // parse_from uses the the first argument as the command name. Insert "wicket" as
                // the command name.
                let args = ShellCommand::parse_from(
                    std::iter::once("wicket".to_owned()).chain(args),
                );
                match args {
                    ShellCommand::Upload(args) => args.exec(log, wicketd_addr),
                }
            }
            None => {
                // Do not expose standard error since it'll be on top of the TUI.
                let log = setup_log(&log_path()?, WithStderr::No)?;
                // Not invoked with "-c" -- run the TUI wizard.
                Wizard::new(log, wicketd_addr).run()
            }
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
        .open(path)?;

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

    Ok(slog::Logger::root(drain, slog::o!()))
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
