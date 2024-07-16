// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask check-features

use anyhow::{bail, Result};
use camino::Utf8PathBuf;
use clap::Parser;
use std::{collections::HashSet, process::Command};

const SUPPORTED_ARCHITECTURES: [&str; 1] = ["x86_64"];
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
    /// Version of `cargo-hack` to install. By default, we download a pre-built
    /// version.
    #[clap(long, value_name = "VERSION")]
    install_version: Option<String>,
}

/// Run `cargo hack check`.
pub fn run_cmd(args: Args) -> Result<()> {
    // We cannot specify both `--ci` and `--install-version`, as the former
    // implies we are using a pre-built version.
    if args.ci && args.install_version.is_some() {
        bail!("cannot specify --ci and --install-version together");
    }

    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));

    let mut command = Command::new(&cargo);

    // Add the `hack check` subcommand.
    command.args(&["hack", "check"]);

    if args.ci {
        install_prebuilt_cargo_hack(&cargo)?;

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

        // Add the `--exclude-features` flag if we are running in CI mode.
        command.args(["--exclude-features", &ex]);
    } else {
        install_cargo_hack(&cargo, args.install_version)?;
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

    exec(command)
}

/// The supported operating systems.
enum Os {
    Illumos,
    Linux,
    Mac,
}

/// Get the current OS.
fn os_name() -> Result<Os> {
    let os = match std::env::consts::OS {
        "linux" => Os::Linux,
        "macos" => Os::Mac,
        "solaris" | "illumos" => Os::Illumos,
        other => bail!("OS not supported: {other}"),
    };
    Ok(os)
}

/// This is a workaround for the lack of a CARGO_WORKSPACE_DIR environment
/// variable, as suggested in <https://github.com/rust-lang/cargo/issues/3946#issuecomment-1433384192>.
/// A better workaround might be to set this in the `[env]` section of
/// `.cargo/config.toml`.
fn project_root() -> Utf8PathBuf {
    Utf8PathBuf::from(&concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
}

/// Get the path to the `out` directory from the project root/workspace
/// directory.
fn out_dir() -> Utf8PathBuf {
    project_root().join("out/cargo-hack")
}

/// Install `cargo-hack` if the `install-version` was specified; otherwise,
/// download a pre-built version if it's not already in our `out` directory.
fn install_cargo_hack(cargo: &str, version: Option<String>) -> Result<()> {
    if let Some(version) = version {
        let mut command = Command::new(cargo);

        eprintln!(
            "installing cargo-hack at version {} to {}",
            version,
            env!("CARGO_HOME")
        );
        command.args(&["install", "cargo-hack", "--version", &version]);
        exec(command)
    } else if !out_dir().exists() {
        install_prebuilt_cargo_hack(cargo)
    } else {
        let out_dir = out_dir();
        eprintln!("cargo-hack found in {}", out_dir);
        Ok(())
    }
}

/// Download a pre-built version of `cargo-hack` to the `out` directory via the
/// download `xtask`.
fn install_prebuilt_cargo_hack(cargo: &str) -> Result<()> {
    let mut command = Command::new(cargo);

    let out_dir = out_dir();
    eprintln!(
        "cargo-hack not found in {}, downloading a pre-built version",
        out_dir
    );

    let os = os_name()?;
    match os {
        Os::Illumos | Os::Linux | Os::Mac
            if SUPPORTED_ARCHITECTURES.contains(&std::env::consts::ARCH) =>
        {
            // Download the pre-built version of `cargo-hack` via our
            // download `xtask`.
            command.args(&["xtask", "download", "cargo-hack"]);
        }
        _ => {
            bail!(
                "cargo-hack is not pre-built for this os {} / arch {}",
                std::env::consts::OS,
                std::env::consts::ARCH
            );
        }
    }

    exec(command)
}

/// Execute the command and check the exit status.
fn exec(mut command: Command) -> Result<()> {
    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));

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
