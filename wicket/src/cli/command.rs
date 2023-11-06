// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code that manages command dispatch from a shell for wicket.

use std::net::SocketAddrV6;

use anyhow::{Context, Result};
use clap::Parser;

use super::{
    preflight::PreflightArgs, rack_setup::SetupArgs, upload::UploadArgs,
};

pub fn exec(
    log: slog::Logger,
    args: &str,
    wicketd_addr: SocketAddrV6,
) -> Result<()> {
    // The argument is in a quoted form, so split it using Unix shell semantics.
    let args = shell_words::split(&args).with_context(|| {
        format!("could not parse shell arguments from input {args}")
    })?;

    // parse_from uses the the first argument as the command name. Insert "wicket" as
    // the command name.
    let args = ShellCommand::parse_from(
        std::iter::once("wicket".to_owned()).chain(args),
    );
    match args {
        ShellCommand::UploadRepo(args) => args.exec(log, wicketd_addr),
        ShellCommand::Setup(args) => args.exec(log, wicketd_addr),
        ShellCommand::Preflight(args) => args.exec(log, wicketd_addr),
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
    #[command(visible_alias = "upload")]
    UploadRepo(UploadArgs),
    /// Interact with rack setup configuration.
    #[command(subcommand)]
    Setup(SetupArgs),
    /// Run checks prior to setting up the rack.
    #[command(subcommand)]
    Preflight(PreflightArgs),
}
