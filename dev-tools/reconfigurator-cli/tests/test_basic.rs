// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::redact_variable;
use omicron_test_utils::dev::test_cmds::run_command;
use omicron_test_utils::dev::test_cmds::EXIT_SUCCESS;
use std::path::PathBuf;
use subprocess::Exec;

fn path_to_cli() -> PathBuf {
    path_to_executable(env!("CARGO_BIN_EXE_reconfigurator-cli"))
}

// Run a battery of simple commands and make sure things basically seem to work.
#[test]
fn test_basic() {
    let exec = Exec::cmd(path_to_cli()).arg("tests/input/cmds.txt");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);
    let stdout_text = redact_variable(&stdout_text, &[]);
    assert_contents("tests/output/cmd-stdout", &stdout_text);
    assert_contents("tests/output/cmd-stderr", &stderr_text);
}
