// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The tests here are intended help us catch cases where we're introducing new
//! API dependencies, and particular circular API dependencies.  It's okay if
//! API dependencies change, but it's important to make sure we're not
//! introducing new barriers to online upgrade.
//!
//! This isn't (supposed to be) a test for the `ls-apis` tool itself.

use omicron_test_utils::dev::test_cmds::EXIT_SUCCESS;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;

/// name of the "ls-apis" executable
const CMD_LS_APIS: &str = env!("CARGO_BIN_EXE_ls-apis");

#[test]
fn test_api_dependencies() {
    let cmd_path = path_to_executable(CMD_LS_APIS);
    let exec = subprocess::Exec::cmd(cmd_path).arg("apis");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    println!("stderr:\n------\n{}\n-----", stderr_text);
    expectorate::assert_contents("tests/api_dependencies.out", &stdout_text);
}

#[test]
fn test_api_check() {
    let cmd_path = path_to_executable(CMD_LS_APIS);
    let exec = subprocess::Exec::cmd(cmd_path).arg("check");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    println!("stderr:\n------\n{}\n-----", stderr_text);
    expectorate::assert_contents("tests/api_check.out", &stdout_text);
}
