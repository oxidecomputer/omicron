/*!
 * Tests for the executable commands in this repo.  Most functionality is tested
 * elsewhere, so this really just sanity checks argument parsing, bad args, and
 * the --openapi mode.
 */

/*
 * TODO-coverage: test success cases of sled-agent
 */

use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use omicron_test_utils::dev::test_cmds::EXIT_USAGE;
use std::path::PathBuf;
use subprocess::Exec;

/** name of the "sled-agent" executable */
const CMD_SLED_AGENT: &str = env!("CARGO_BIN_EXE_sled-agent-sim");

fn path_to_sled_agent() -> PathBuf {
    path_to_executable(CMD_SLED_AGENT)
}

#[test]
fn test_sled_agent_no_args() {
    let exec = Exec::cmd(path_to_sled_agent());
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE);
    assert_contents("tests/output/cmd-sled-agent-noargs-stdout", &stdout_text);
    assert_contents("tests/output/cmd-sled-agent-noargs-stderr", &stderr_text);
}
