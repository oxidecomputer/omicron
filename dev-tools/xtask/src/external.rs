// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External xtasks. (extasks?)

use std::ffi::{OsStr, OsString};
use std::os::unix::process::CommandExt;
use std::process::Command;

use anyhow::{Context, Result};
use clap::Parser;

/// Argument parser for external xtasks.
///
/// In general we want all developer tasks to be discoverable simply by running
/// `cargo xtask`, but some development tools end up with a particularly
/// large dependency tree. It's not ideal to have to pay the cost of building
/// our release engineering tooling if all the user wants to do is check for
/// workspace dependency issues.
///
/// `External` provides a pattern for creating xtasks that live in other crates.
/// An external xtask is defined on `crate::Cmds` as a tuple variant containing
/// `External`, which captures all arguments and options (even `--help`) as
/// a `Vec<OsString>`. The main function then calls `External::exec` with the
/// appropriate bin target name and any additional Cargo arguments.
#[derive(Parser)]
#[clap(
    disable_help_flag(true),
    disable_help_subcommand(true),
    disable_version_flag(true)
)]
pub struct External {
    #[clap(trailing_var_arg(true), allow_hyphen_values(true))]
    args: Vec<OsString>,

    #[clap(skip)]
    command: Option<Command>,
}

impl External {
    pub fn cargo_args(
        mut self,
        args: impl IntoIterator<Item = impl AsRef<OsStr>>,
    ) -> External {
        self.command.get_or_insert_with(default_command).args(args);
        self
    }

    pub fn exec(self, bin_target: impl AsRef<OsStr>) -> Result<()> {
        let error = self
            .command
            .unwrap_or_else(default_command)
            .arg("--bin")
            .arg(bin_target)
            .arg("--")
            .args(self.args)
            .exec();
        Err(error).context("failed to exec `cargo run`")
    }
}

fn default_command() -> Command {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut command = Command::new(cargo);
    command.arg("run");
    command
}
