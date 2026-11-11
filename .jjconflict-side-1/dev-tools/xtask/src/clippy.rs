// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask clippy

use crate::common::run_subcmd;
use anyhow::Result;
use clap::Parser;
use dev_tools_common::{CargoLocation, cargo_command};

#[derive(Parser)]
pub struct ClippyArgs {
    /// Automatically apply lint suggestions.
    #[clap(long)]
    fix: bool,
    /// Allow uncommitted changes
    #[clap(long)]
    allow_dirty: bool,
    /// Error format passed to `cargo clippy`.
    #[clap(long, value_name = "FMT")]
    message_format: Option<String>,
}

pub fn run_cmd(args: ClippyArgs) -> Result<()> {
    let mut command = cargo_command(CargoLocation::FromEnv);
    command.arg("clippy");

    if args.fix {
        command.arg("--fix");
    }

    if args.allow_dirty {
        command.arg("--allow-dirty");
    }

    // Pass along the `--message-format` flag if it was provided.
    //
    // We don't really care about validating that it's a valid argument to
    // `cargo check --message-format`, because `cargo check` will error out if
    // it's unrecognized, and repeating the validation here just presents an
    // opportunity to get out of sync with what Cargo actually accepts should a
    // new message format be added.
    if let Some(fmt) = args.message_format {
        command.args(["--message-format", &fmt]);
    }

    command
        // Make sure we check everything.
        .arg("--all-targets")
        .arg("--workspace")
        .arg("--")
        // For a list of lints, see
        // https://rust-lang.github.io/rust-clippy/master.
        //
        // We disallow warnings by default.
        .arg("--deny")
        .arg("warnings");

    run_subcmd(command)
}
