#![cfg(target_os = "illumos")]

use crate::common::SANITIZED_ENV_VARS;
use anyhow::Result;
use clap::Subcommand;
use std::env;
use std::ffi::OsStr;
use xshell::Cmd;

mod a4x2_deploy;
mod a4x2_package;

/// live-tests nextest bundle is named this.
const LIVE_TEST_BUNDLE_NAME: &str = "live-tests-bundle.tgz";

/// directory within the bundle tar
const LIVE_TEST_BUNDLE_DIR: &str = "live-tests-bundle";

/// script within the bundle directory
const LIVE_TEST_BUNDLE_SCRIPT: &str = "run-live-tests";

/// Top level directory in a4x2 package bundles
const A4X2_PACKAGE_DIR_PATH: &str = "a4x2-package";

/// Default location where a4x2-package places output and a4x2-deploy reads it.
const DEFAULT_A4X2_PKG_PATH: &str = "out/a4x2-package.tar.gz";

/// These environment variables are set to statically defined values for
/// commands executed by the xtask.
const STATIC_ENV_VARS: &[(&str, &str)] = &[
    // Consistent command output
    ("LANG", "C.UTF-8"),
    // Timestamps in logs outside the sleds consistent with timestamps inside
    // the sleds
    ("TZ", "UTC"),
];

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

/// Removes unwanted variables from the environment. Also sets some variables to
/// static values according to [STATIC_ENV_VARS]
fn scrub_env(cmd: Cmd<'_>) -> Cmd<'_> {
    let passthru = env::vars_os().filter(|(varname, _)| {
        // LC_* is removed because it can change command output in unexpected
        // ways.
        //
        // SANITIZED_ENV_VARS deals in variables known to cause unnecessary
        // recompilation.
        varname.to_str().map_or(true, |varname| {
            !varname.starts_with("LC_") && !SANITIZED_ENV_VARS.matches(varname)
        })
    });
    let static_vars =
        STATIC_ENV_VARS.iter().map(|(k, v)| (OsStr::new(k), OsStr::new(v)));
    cmd.env_clear().envs(passthru).envs(static_vars)
}

/// Drop-in shim for xshell::cmd!() that scrubs environment with [scrub_env]
macro_rules! cmd {
    ($($tt:tt)*) => {
        crate::a4x2::scrub_env(xshell::cmd!($($tt)*))
    }
}
pub(crate) use cmd;
