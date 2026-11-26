// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault Management simulator command line binary.
//!
//! This is a thin shell around [`Simulator`]. It reads lines (from a script or
//! interactively), hands each one to the simulator, and prints the results.

use camino::Utf8PathBuf;
use clap::Parser;
use fm_sim::{Error, Simulator};
use omicron_repl_utils::{run_repl_from_file, run_repl_on_stdin};

/// fm-sim: Fault Management Simulator
#[derive(Debug, Parser)]
struct FmSim {
    /// Path to a script of commands to run. If absent, commands are read
    /// interactively from standard input.
    script: Option<Utf8PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let args = FmSim::parse();
    let mut sim = Simulator::new();

    let run_line = |line: &str| {
        sim.run(line).or_else(|error| match error {
            // For clap parse errors specifically, use clap's own `Error::print`
            // method to show them, and then return `Ok` to continue the REPL.
            Error::Parse(error) => {
                error.print()?;
                Ok(String::new())
            }
            // For all other errors, return them to `repl-utils`.
            error => Err(error.into()),
        })
    };
    match &args.script {
        Some(script) => run_repl_from_file(script, run_line),
        None => run_repl_on_stdin(run_line),
    }
}
