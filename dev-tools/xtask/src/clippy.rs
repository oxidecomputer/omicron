// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask clippy

use anyhow::{bail, Context, Result};
use clap::Parser;
use std::process::Command;

#[derive(Parser)]
pub struct ClippyArgs {
    /// Automatically apply lint suggestions.
    #[clap(long)]
    fix: bool,
}

pub fn run_cmd(args: ClippyArgs) -> Result<()> {
    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let mut command = Command::new(&cargo);
    command.arg("clippy");

    if args.fix {
        command.arg("--fix");
    }

    command
        // Make sure we check everything.
        .arg("--all-targets")
        .arg("--")
        // For a list of lints, see
        // https://rust-lang.github.io/rust-clippy/master.
        //
        // We disallow warnings by default.
        .arg("--deny")
        .arg("warnings")
        // Clippy's style nits are useful, but not worth keeping in CI.  This
        // override belongs in src/lib.rs, and it is there, but that doesn't
        // reliably work due to rust-lang/rust-clippy#6610.
        .arg("--allow")
        .arg("clippy::style")
        // But continue to warn on anything in the "disallowed_" namespace.
        // (These will be turned into errors by `--deny warnings` above.)
        .arg("--warn")
        .arg("clippy::disallowed_macros")
        .arg("--warn")
        .arg("clippy::disallowed_methods")
        .arg("--warn")
        .arg("clippy::disallowed_names")
        .arg("--warn")
        .arg("clippy::disallowed_script_idents")
        .arg("--warn")
        .arg("clippy::disallowed_types")
        // Warn on some more style lints that are relatively stable and make
        // sense.
        .arg("--warn")
        .arg("clippy::iter_cloned_collect")
        .arg("--warn")
        .arg("clippy::iter_next_slice")
        .arg("--warn")
        .arg("clippy::iter_nth")
        .arg("--warn")
        .arg("clippy::iter_nth_zero")
        .arg("--warn")
        .arg("clippy::iter_skip_next")
        .arg("--warn")
        .arg("clippy::len_zero")
        .arg("--warn")
        .arg("clippy::redundant_field_names")
        .arg("--warn")
        .arg("clippy::declare_interior_mutable_const")
        // Also warn on casts, preferring explicit conversions instead.
        //
        // We'd like to warn on lossy casts in the future, but lossless casts
        // are the easiest ones to convert over.
        .arg("--warn")
        .arg("clippy::cast_lossless");

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
        bail!("clippy failed: {}", exit_status);
    }

    Ok(())
}
