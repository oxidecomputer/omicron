// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Workspace-related developer tools
//!
//! See <https://github.com/matklad/cargo-xtask>.

use anyhow::{Context, Result};
use cargo_metadata::Metadata;
use clap::{Parser, Subcommand};

mod check_workspace_deps;
mod clippy;
mod download;
#[cfg_attr(not(target_os = "illumos"), allow(dead_code))]
mod external;
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

    /// Check that dependencies are not duplicated in any packages in the
    /// workspace
    CheckWorkspaceDeps,
    /// Run configured clippy checks
    Clippy(clippy::ClippyArgs),
    /// Download binaries, OpenAPI specs, and other out-of-repo utilities.
    Download(download::DownloadArgs),

    #[cfg(target_os = "illumos")]
    /// Build a TUF repo
    Releng(external::External),

    /// Verify we are not leaking library bindings outside of intended
    /// crates
    #[cfg(target_os = "illumos")]
    VerifyLibraries,
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Cmds::Argon2(external) => {
            external.cargo_args(["--release"]).exec_example("argon2")
        }
        Cmds::Clippy(args) => clippy::run_cmd(args),
        Cmds::CheckWorkspaceDeps => check_workspace_deps::run_cmd(),
        Cmds::Download(args) => download::run_cmd(args).await,
        #[cfg(target_os = "illumos")]
        Cmds::Releng(external) => external.exec_bin("omicron-releng"),
        #[cfg(target_os = "illumos")]
        Cmds::VerifyLibraries => verify_libraries::run_cmd(),
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
