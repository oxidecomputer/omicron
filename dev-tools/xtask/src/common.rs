// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common xtask command helpers

use anyhow::{Context, Result, bail};
use std::process::Command;

/// Runs the given command, printing some basic debug information around it, and
/// failing with an error message if the command does not exit successfully
pub fn run_subcmd(mut command: Command) -> Result<()> {
    eprintln!(
        "running: {} {}",
        command.get_program().to_str().unwrap(),
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
        bail!("failed: {}", exit_status);
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) struct SanitizedEnvVars {
    // At the moment we only ban some prefixes, but we may also want to ban env
    // vars by exact name in the future.
    prefixes: &'static [&'static str],
}

impl SanitizedEnvVars {
    const fn new() -> Self {
        // Remove many of the environment variables set in
        // https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts.
        // This is done to avoid recompilation with crates like ring between
        // `cargo clippy` and `cargo xtask clippy`. (This is really a bug in
        // both ring's build script and in Cargo.)
        //
        // The current list is informed by looking at ring's build script, so
        // it's not guaranteed to be exhaustive and it may need to grow over
        // time.
        let prefixes = &["CARGO_PKG_", "CARGO_MANIFEST_", "CARGO_CFG_"];
        Self { prefixes }
    }

    pub(crate) fn matches(&self, key: &str) -> bool {
        self.prefixes.iter().any(|prefix| key.starts_with(prefix))
    }
}

pub(crate) static SANITIZED_ENV_VARS: SanitizedEnvVars =
    SanitizedEnvVars::new();
