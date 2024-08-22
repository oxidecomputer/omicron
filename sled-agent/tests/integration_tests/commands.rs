// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for the executable commands in this repo.  Most functionality is tested
//! elsewhere, so this really just sanity checks argument parsing, bad args, and
//! the --openapi mode.

// TODO-coverage: test success cases of sled-agent

use camino::Utf8PathBuf;
use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use omicron_test_utils::dev::test_cmds::EXIT_USAGE;
use subprocess::Exec;

/// name of the "sled-agent-sim" executable
const CMD_SLED_AGENT_SIM: &str = env!("CARGO_BIN_EXE_sled-agent-sim");

fn path_to_sled_agent_sim() -> Utf8PathBuf {
    path_to_executable(CMD_SLED_AGENT_SIM)
        .try_into()
        .expect("Invalid Utf8 binary?")
}

#[test]
fn test_sled_agent_sim_no_args() {
    let exec = Exec::cmd(path_to_sled_agent_sim());
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE, &stderr_text);
    assert_contents(
        "tests/output/cmd-sled-agent-sim-noargs-stdout",
        &stdout_text,
    );
    assert_contents(
        "tests/output/cmd-sled-agent-sim-noargs-stderr",
        &stderr_text,
    );
}
/// name of the "sled-agent" executable
const CMD_SLED_AGENT: &str = env!("CARGO_BIN_EXE_sled-agent");

fn path_to_sled_agent() -> Utf8PathBuf {
    path_to_executable(CMD_SLED_AGENT).try_into().expect("Invalid Utf8 binary?")
}

#[test]
fn test_sled_agent_no_args() {
    let exec = Exec::cmd(path_to_sled_agent());
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE, &stderr_text);
    assert_contents("tests/output/cmd-sled-agent-noargs-stdout", &stdout_text);
    assert_contents("tests/output/cmd-sled-agent-noargs-stderr", &stderr_text);
}
