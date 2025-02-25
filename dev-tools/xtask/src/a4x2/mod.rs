#![cfg(target_os = "illumos")]

use anyhow::Result;
use clap::Subcommand;

mod a4x2_deploy;
mod a4x2_package;

#[derive(Subcommand)]
pub enum A4x2Cmds {
    /// Generate a tarball with omicron packaged for deployment onto a4x2
    Package(a4x2_package::A4x2PackageArgs),

    /// Run a4x2 and deploy omicron onto it, and optionally run live-tests and end to end tests
    Deploy(a4x2_deploy::A4x2DeployArgs),
}

pub fn run_cmd(args: A4x2Cmds) -> Result<()> {
    match args {
        A4x2Cmds::Package(args) => a4x2_package::run_cmd(args),
        A4x2Cmds::Deploy(args) => a4x2_deploy::run_cmd(args),
    }
}
