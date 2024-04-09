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
#[cfg(target_os = "illumos")]
mod verify_libraries;

#[cfg(target_os = "illumos")]
mod virtual_hardware;
#[cfg(not(target_os = "illumos"))]
#[path = "virtual_hardware_stub.rs"]
mod virtual_hardware;

#[derive(Parser)]
#[command(name = "cargo xtask", about = "Workspace-related developer tools")]
struct Args {
    #[command(subcommand)]
    cmd: Cmds,
}

#[derive(Subcommand)]
enum Cmds {
    /// Check that dependencies are not duplicated in any packages in the
    /// workspace
    CheckWorkspaceDeps,
    /// Run configured clippy checks
    Clippy(clippy::ClippyArgs),
    /// Download binaries, OpenAPI specs, and other out-of-repo utilities.
    Download(download::DownloadArgs),
    /// Verify we are not leaking library bindings outside of intended
    /// crates
    VerifyLibraries,
    /// Manage virtual hardware
    VirtualHardware(virtual_hardware::Args),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Cmds::Clippy(args) => clippy::run_cmd(args),
        Cmds::CheckWorkspaceDeps => check_workspace_deps::run_cmd(),
        Cmds::Download(args) => download::run_cmd(args).await,
        Cmds::VerifyLibraries => {
            #[cfg(target_os = "illumos")]
            return verify_libraries::run_cmd();
            #[cfg(not(target_os = "illumos"))]
            unimplemented!(
                "Library verification is only available on illumos!"
            );
        }
        Cmds::VirtualHardware(args) => virtual_hardware::run_cmd(args),
    }
}

pub fn load_workspace() -> Result<Metadata> {
    cargo_metadata::MetadataCommand::new()
        .exec()
        .context("loading cargo metadata")
}
