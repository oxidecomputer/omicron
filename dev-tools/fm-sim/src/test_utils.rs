// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers shared by the unit tests: feed a line to the parser or the
//! simulator and expect a result.

use crate::{CliCommand, Error, Simulator, parse};

/// Parses `line`, panicking with the error if it does not parse.
#[track_caller]
pub(crate) fn parse_ok(line: &str) -> CliCommand {
    match parse(line) {
        Ok(command) => command,
        Err(e) => panic!("`{line}` should parse: {e}"),
    }
}

/// Asserts that none of `lines` parse.
#[track_caller]
pub(crate) fn parse_err(lines: &[&str]) {
    for line in lines {
        if let Ok(command) = parse(line) {
            panic!("`{line}` should not parse, got {command:?}");
        }
    }
}

/// Runs `line` through the simulator and returns what it printed.
#[track_caller]
pub(crate) fn run_ok(sim: &mut Simulator, line: &str) -> String {
    match sim.run(line) {
        Ok(output) => output,
        Err(e) => panic!("`{line}` failed: {e}"),
    }
}

/// Runs `line` through the simulator and returns why it failed.
#[track_caller]
pub(crate) fn run_err(sim: &mut Simulator, line: &str) -> Error {
    sim.run(line).expect_err(line)
}
