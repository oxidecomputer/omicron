// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Workspace-related developer tools
//!
//! See <https://github.com/matklad/cargo-xtask>.

use anyhow::{Context, Result};
use cargo_metadata::Metadata;
use clap::{Parser, Subcommand};
use std::env;
use std::os::unix::process::CommandExt;
use std::process::Command;

mod check_features;
mod check_workspace_deps;
mod clippy;
mod common;
#[cfg_attr(not(target_os = "illumos"), allow(dead_code))]
mod external;
mod live_tests;
mod usdt;

#[cfg(target_os = "illumos")]
mod verify_libraries;
#[cfg(target_os = "illumos")]
mod virtual_hardware;

#[derive(Parser)]
#[command(
    name = "cargo xtask",
    bin_name = "cargo xtask",
    about = "Workspace-related developer tools"
)]
struct Args {
    #[command(subcommand)]
    cmd: Cmds,
}

#[derive(Subcommand)]
enum Cmds {
    /// Run Argon2 hash with specific parameters (quick performance check)
    Argon2(external::External),

    /// Utilities for working with certificates.
    CertDev(external::External),
    /// Utilities for working with ClickHouse.
    #[clap(alias = "clickhouse-dev")]
    ChDev(external::External),
    /// Utilities for working with CockroachDB databases.
    DbDev(external::External),

    /// Show information about Progenitor-based APIs
    LsApis(external::External),

    /// Check that all features are flagged correctly
    CheckFeatures(check_features::Args),
    /// Check that dependencies are not duplicated in any packages in the
    /// workspace
    CheckWorkspaceDeps,
    /// Run configured clippy checks
    Clippy(clippy::ClippyArgs),
    /// Download binaries, OpenAPI specs, and other out-of-repo utilities.
    Download(external::External),

    /// Create a bundle of live tests
    LiveTests(live_tests::Args),

    /// Utilities for working with MGS.
    MgsDev(external::External),
    /// Utilities for working with Omicron.
    OmicronDev(external::External),

    /// Manage OpenAPI specifications.
    ///
    /// For more information, see dev-tools/openapi-manager/README.adoc.
    Openapi(external::External),

    #[cfg(target_os = "illumos")]
    /// Build a TUF repo
    Releng(external::External),

    /// Verify we are not leaking library bindings outside of intended
    /// crates
    #[cfg(target_os = "illumos")]
    VerifyLibraries(verify_libraries::Args),
    /// Manage virtual hardware
    #[cfg(target_os = "illumos")]
    VirtualHardware(virtual_hardware::Args),

    /// (this command is only available on illumos)
    #[cfg(not(target_os = "illumos"))]
    Releng,
    /// (this command is only available on illumos)
    #[cfg(not(target_os = "illumos"))]
    VerifyLibraries,
    /// (this command is only available on illumos)
    #[cfg(not(target_os = "illumos"))]
    VirtualHardware,

    /// Print USDT probes in Omicron binaries.
    Probes {
        /// An optional filter applied to binary names.
        ///
        /// This is a simple substring match. Any binary with the filter as a
        /// substring of its name will be examined for probes.
        filter: Option<String>,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Cmds::Argon2(external) => {
            external.cargo_args(["--release"]).exec_example("argon2")
        }
        Cmds::CertDev(external) => external.exec_bin("cert-dev"),
        Cmds::ChDev(external) => external.exec_bin("ch-dev"),
        Cmds::Clippy(args) => clippy::run_cmd(args),
        Cmds::CheckFeatures(args) => check_features::run_cmd(args),
        Cmds::CheckWorkspaceDeps => check_workspace_deps::run_cmd(),
        Cmds::DbDev(external) => external.exec_bin("db-dev"),
        Cmds::Download(external) => {
            // Allow specialized environments (e.g., testbed/a4x2) that can't
            // `cargo run ...` to specify a path to `xtask-downloader` via an
            // environment variable.
            if let Ok(bin_path) = env::var("XTASK_DOWNLOADER_BIN") {
                let error = Command::new(&bin_path)
                    .args(external.trailing_args())
                    .exec();
                Err(error)
                    .with_context(|| format!("failed to exec `{bin_path}`"))
            } else {
                external.exec_bin("xtask-downloader")
            }
        }
        Cmds::LiveTests(args) => live_tests::run_cmd(args),
        Cmds::LsApis(external) => external.exec_bin("ls-apis"),
        Cmds::MgsDev(external) => external.exec_bin("mgs-dev"),
        Cmds::OmicronDev(external) => external.exec_bin("omicron-dev"),
        Cmds::Openapi(external) => external.exec_bin("omicron-dropshot-apis"),
        #[cfg(target_os = "illumos")]
        Cmds::Releng(external) => {
            external.cargo_args(["--release"]).exec_bin("omicron-releng")
        }
        #[cfg(target_os = "illumos")]
        Cmds::VerifyLibraries(args) => verify_libraries::run_cmd(args),
        #[cfg(target_os = "illumos")]
        Cmds::VirtualHardware(args) => virtual_hardware::run_cmd(args),

        #[cfg(not(target_os = "illumos"))]
        Cmds::Releng | Cmds::VerifyLibraries | Cmds::VirtualHardware => {
            anyhow::bail!("this command is only available on illumos");
        }
        Cmds::Probes { filter } => usdt::print_probes(filter),
    }
}

pub fn load_workspace() -> Result<Metadata> {
    cargo_metadata::MetadataCommand::new()
        .exec()
        .context("loading cargo metadata")
}
