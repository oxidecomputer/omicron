// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Script-based tests for the reconfigurator CLI.
//!
//! This custom-harness test generates a test for every file in the input
//! directory starting with `cmds`.

use camino::Utf8Path;

mod common;
use common::script_with_cwd;

fn script(path: &Utf8Path) -> datatest_stable::Result<()> {
    script_with_cwd(path, None)
}

datatest_stable::harness! {
    { test = script, root = "tests/input", pattern = r"cmds.*\.txt" }
}
