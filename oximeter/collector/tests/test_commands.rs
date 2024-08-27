// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2021 Oxide Computer Company

use std::path::PathBuf;

use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::{
    assert_exit_code, path_to_executable, run_command, EXIT_USAGE,
};
use subprocess::Exec;

/// name of the "oximeter" executable
const CMD_OXIMETER: &str = env!("CARGO_BIN_EXE_oximeter");

fn path_to_oximeter() -> PathBuf {
    path_to_executable(CMD_OXIMETER)
}

#[test]
fn test_oximeter_no_args() {
    let exec = Exec::cmd(path_to_oximeter());
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE, &stderr_text);
    assert_contents("tests/output/cmd-oximeter-noargs-stdout", &stdout_text);
    assert_contents("tests/output/cmd-oximeter-noargs-stderr", &stderr_text);
}
