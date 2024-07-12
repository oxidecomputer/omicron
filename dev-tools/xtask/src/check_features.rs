// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask check-features

use anyhow::{bail, Context, Result};
use clap::Parser;
use std::{collections::HashSet, process::Command};

/// The default version of `cargo-hack` to install.
/// We use a patch-floating version to avoid breaking the build when a new
/// version is released (locally).
const FLOAT_VERSION: &str = "~0.6.28";

const CI_EXCLUDED_FEATURES: [&str; 2] = ["image-trampoline", "image-standard"];

#[derive(Parser)]
pub struct Args {
    /// Run in CI mode, with a default set of features excluded.
    #[clap(long, default_value_t = false)]
    ci: bool,
    /// Features to exclude from the check.
    #[clap(long, value_name = "FEATURES")]
    exclude_features: Option<Vec<String>>,
    /// Depth of the feature powerset to check.
    #[clap(long, value_name = "NUM")]
    depth: Option<usize>,
    /// Error format passed to `cargo hack check`.
    #[clap(long, value_name = "FMT")]
    message_format: Option<String>,
    /// Version of `cargo-hack` to install.
    #[clap(long, value_name = "VERSION")]
    install_version: Option<String>,
}

/// Run `cargo hack check`.
pub fn run_cmd(args: Args) -> Result<()> {
    // Install `cargo-hack` if the `install-version` was specified.
    if let Some(version) = args.install_version {
        install_cargo_hack(Some(version))?;
    }

    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let mut command = Command::new(&cargo);

    // Add the `hack check` subcommand.
    command.args(&["hack", "check"]);

    // Add the `--exclude-features` flag if we are running in CI mode.
    if args.ci {
        let ex = if let Some(mut features) = args.exclude_features {
            // Extend the list of features to exclude with the CI defaults.
            features.extend(
                CI_EXCLUDED_FEATURES.into_iter().map(|s| s.to_string()),
            );

            // Remove duplicates.
            let excludes = features.into_iter().collect::<HashSet<_>>();

            excludes.into_iter().collect::<Vec<_>>().join(",")
        } else {
            CI_EXCLUDED_FEATURES.join(",")
        };

        command.args(["--exclude-features", &ex]);
    } else {
        // Add "only" the `--exclude-features` flag if it was provided.
        if let Some(features) = args.exclude_features {
            command.args(["--exclude-features", &features.join(",")]);
        }
    }

    if let Some(depth) = args.depth {
        command.args(&["--depth", &depth.to_string()]);
    }

    // Pass along the `--message-format` flag if it was provided.
    if let Some(fmt) = args.message_format {
        command.args(["--message-format", &fmt]);
    }

    command
        // Make sure we check everything.
        .arg("--workspace")
        // We want to check the binaries.
        .arg("--bins")
        // We want to check the feature powerset.
        .arg("--feature-powerset")
        // We will not check the dev-dependencies, which should covered by tests.
        .arg("--no-dev-deps");

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
