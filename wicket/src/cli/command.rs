// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code that manages command dispatch from a shell for wicket.

use std::net::SocketAddrV6;

use anyhow::Result;
use clap::{Args, ColorChoice, Parser, Subcommand};

use super::{
    inventory::InventoryArgs, preflight::PreflightArgs, rack_setup::SetupArgs,
    rack_update::RackUpdateArgs, upload::UploadArgs,
};

pub(crate) struct CommandOutput<'a> {
    pub(crate) stdout: &'a mut dyn std::io::Write,
    pub(crate) stderr: &'a mut dyn std::io::Write,
}

/// An app that represents wicket started with arguments over ssh.
#[derive(Debug, Parser)]
pub(crate) struct ShellApp {
    /// Global options.
    #[clap(flatten)]
    pub(crate) global_opts: GlobalOpts,

    /// The command to run.
    #[clap(subcommand)]
    command: ShellCommand,
}

impl ShellApp {
    pub(crate) async fn exec(
        self,
        log: slog::Logger,
        wicketd_addr: SocketAddrV6,
        output: CommandOutput<'_>,
    ) -> Result<()> {
        match self.command {
            ShellCommand::UploadRepo(args) => {
                args.exec(log, wicketd_addr).await
            }
            ShellCommand::RackUpdate(args) => {
                args.exec(log, wicketd_addr, self.global_opts, output).await
            }
            ShellCommand::Setup(args) => {
                args.exec(log, wicketd_addr, self.global_opts).await
            }
            ShellCommand::Preflight(args) => args.exec(log, wicketd_addr).await,
            ShellCommand::Inventory(args) => {
                args.exec(log, wicketd_addr, output).await
            }
        }
    }
}

#[derive(Debug, Args)]
#[clap(next_help_heading = "Global options")]
pub(crate) struct GlobalOpts {
    /// Color output
    ///
    /// This may not be obeyed everywhere at the moment.
    #[clap(long, value_enum, global = true, default_value_t)]
    pub(crate) color: ColorChoice,
}

impl GlobalOpts {
    /// Returns true if color should be used on standard error.
    pub(crate) fn use_color(&self) -> bool {
        match self.color {
            ColorChoice::Auto => {
                supports_color::on_cached(supports_color::Stream::Stderr)
                    .is_some()
            }
            ColorChoice::Always => true,
            ColorChoice::Never => false,
        }
    }
}

/// Arguments passed to wicket.
///
/// Wicket is designed to be used as a captive shell, set up via sshd
/// ForceCommand. If no arguments are specified, wicket behaves like a TUI.
/// However, if arguments are specified via SSH_ORIGINAL_COMMAND, wicketd
/// accepts an upload command.
#[derive(Debug, Subcommand)]
enum ShellCommand {
    /// Upload a TUF repository to wicketd.
    #[command(visible_alias = "upload")]
    UploadRepo(UploadArgs),

    /// Perform a rack update.
    #[command(subcommand)]
    RackUpdate(RackUpdateArgs),

    /// Interact with rack setup configuration.
    #[command(subcommand)]
    Setup(SetupArgs),

    /// Run checks prior to setting up the rack.
    #[command(subcommand)]
    Preflight(PreflightArgs),

    /// Enumerate rack components
    #[command(subcommand)]
    Inventory(InventoryArgs),
}
