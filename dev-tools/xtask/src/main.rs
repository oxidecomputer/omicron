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
    /// Verify we are not leaking library bindings outside of intended
    /// crates
    VerifyLibraries(verify_libraries::Args),
    /// Manage virtual hardware
    VirtualHardware(virtual_hardware::Args),
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Cmds::Clippy(args) => clippy::run_cmd(args),
        Cmds::CheckWorkspaceDeps => check_workspace_deps::run_cmd(),
        Cmds::VerifyLibraries(args) => verify_libraries::run_cmd(args),
        Cmds::VirtualHardware(args) => virtual_hardware::run_cmd(args),
    }
}

pub fn load_workspace() -> Result<Metadata> {
    cargo_metadata::MetadataCommand::new()
        .exec()
        .context("loading cargo metadata")
}
