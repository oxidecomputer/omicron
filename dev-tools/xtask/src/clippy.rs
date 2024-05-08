// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask clippy

use anyhow::{bail, Context, Result};
use clap::Parser;
use std::process::Command;

#[derive(Parser)]
pub struct ClippyArgs {
    /// Automatically apply lint suggestions.
    #[clap(long)]
    fix: bool,
}

pub fn run_cmd(args: ClippyArgs) -> Result<()> {
    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let mut command = Command::new(&cargo);
    command.arg("clippy");

    if args.fix {
        command.arg("--fix");
    }

    command
        // Make sure we check everything.
        .arg("--all-targets")
        .arg("--")
        // For a list of lints, see
        // https://rust-lang.github.io/rust-clippy/master.
        //
        // We disallow warnings by default.
        .arg("--deny")
        .arg("warnings");

    eprintln!(
        "running: {:?} {}",
        &cargo,
        command
            .get_args()
            .map(|arg| format!("{:?}", arg.to_str().unwrap()))
            .collect::<Vec<_>>()
            .join(" ")
    );

    let exit_status = command
        .spawn()
        .context("failed to spawn child process")?
        .wait()
        .context("failed to wait for child process")?;

    if !exit_status.success() {
        bail!("clippy failed: {}", exit_status);
    }

    Ok(())
}
