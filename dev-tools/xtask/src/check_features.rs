// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask check-features

use anyhow::{bail, Context, Result};
use clap::Parser;
use std::process::Command;

/// The default version of `cargo-hack` to install.
/// We use a patch-floating version to avoid breaking the build when a new
/// version is released (locally).
const FLOAT_VERSION: &str = "~0.6.28";

#[derive(Parser)]
pub struct Args {
    /// Features to exclude from the check.
    #[clap(long)]
    exclude_features: Option<Vec<String>>,
    /// Depth of the feature powerset to check.
    #[clap(long)]
    depth: Option<usize>,
    /// Error format passed to `cargo hack check`.
    #[clap(long, value_name = "FMT")]
    message_format: Option<String>,
    /// Do not install `cargo-hack` before running the check.
    #[clap(long, default_value_t = false)]
    no_install: bool,
    /// Version of `cargo-hack` to install.
    #[clap(long)]
    version: Option<String>,
}

/// Run `cargo hack check`.
pub fn run_cmd(args: Args) -> Result<()> {
    if !args.no_install {
        install_cargo_hack(args.version).unwrap();
    }

    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let mut command = Command::new(&cargo);

    command.args(&["hack", "check"]);

    if let Some(features) = args.exclude_features {
        let ex = format!("--exclude-features={}", features.join(","));
        command.arg(ex);
    }

    if let Some(depth) = args.depth {
        let depth = format!("depth={}", depth);
        command.arg(depth);
    }

    // Pass along the `--message-format` flag if it was provided.
    if let Some(fmt) = args.message_format {
        command.args(["--message-format", &fmt]);
    }

    command
        // Make sure we check everything.
        .arg("--workspace")
        .arg("--bins")
        // We want to check the feature powerset.
        .arg("--feature-powerset")
        .arg("--no-dev-deps")
        .arg("--exclude-no-default-features");

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
        bail!("check-features failed: {}", exit_status);
    }

    Ok(())
}

/// Install `cargo-hack` at the specified version or the default version.
fn install_cargo_hack(version: Option<String>) -> Result<()> {
    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));

    let mut command = Command::new(&cargo);

    if let Some(version) = version {
        command.args(&["install", "cargo-hack", "--version", &version]);
    } else {
        command.args(&[
            "install",
            "cargo-hack",
            "--locked",
            "--version",
            FLOAT_VERSION,
        ]);
    }

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
        .expect("failed to spawn child process")
        .wait()
        .expect("failed to wait for child process");

    if !exit_status.success() {
        bail!("cargo-hack install failed: {}", exit_status);
    }

    Ok(())
}
