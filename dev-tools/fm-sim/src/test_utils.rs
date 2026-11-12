// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers shared by the unit tests: feed a line to the parser and expect a
//! result.

use crate::{CliCommand, parse};

/// Parses `line`, panicking with the error if it does not parse.
pub(crate) fn parse_ok(line: &str) -> CliCommand {
    parse(line).unwrap_or_else(|e| panic!("`{line}` should parse: {e}"))
}

/// Asserts that none of `lines` parse.
pub(crate) fn parse_err(lines: &[&str]) {
    for line in lines {
        if let Ok(command) = parse(line) {
            panic!("`{line}` should not parse, got {command:?}");
        }
    }
}
