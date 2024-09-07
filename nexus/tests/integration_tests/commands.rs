// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for the executable commands in this repo.  Most functionality is tested
//! elsewhere, so this really just sanity checks argument parsing, bad args, and
//! the --openapi mode.

// TODO-coverage: test success cases of nexus

use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::error_for_enoent;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use omicron_test_utils::dev::test_cmds::temp_file_path;
use omicron_test_utils::dev::test_cmds::EXIT_FAILURE;
use omicron_test_utils::dev::test_cmds::EXIT_USAGE;
use std::fs;
use std::path::PathBuf;
use subprocess::Exec;

/// name of the "nexus" executable
const CMD_NEXUS: &str = env!("CARGO_BIN_EXE_nexus");

fn path_to_nexus() -> PathBuf {
    path_to_executable(CMD_NEXUS)
}

/// Write the requested string to a temporary file and return the path to that
/// file.
fn write_config(config: &str) -> PathBuf {
    let file_path = temp_file_path("test_commands_config");
    eprintln!("writing temp config: {}", file_path.display());
    fs::write(&file_path, config).expect("failed to write config file");
    file_path
}

// Tests

#[test]
fn test_nexus_no_args() {
    let exec = Exec::cmd(path_to_nexus());
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE, &stderr_text);
    assert_contents("tests/output/cmd-nexus-noargs-stdout", &stdout_text);
    assert_contents("tests/output/cmd-nexus-noargs-stderr", &stderr_text);
}

#[test]
fn test_nexus_bad_config() {
    let exec = Exec::cmd(path_to_nexus()).arg("nonexistent");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_FAILURE, &stderr_text);
    assert_contents("tests/output/cmd-nexus-badconfig-stdout", &stdout_text);
    let expected_err =
        format!("nexus: read \"nonexistent\": {}\n", error_for_enoent());
    assert!(&stderr_text.starts_with(&expected_err));
}

#[test]
fn test_nexus_invalid_config() {
    let config_path = write_config("");
    let exec = Exec::cmd(path_to_nexus()).arg(&config_path);
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    fs::remove_file(&config_path).expect("failed to remove temporary file");
    assert_exit_code(exit_status, EXIT_FAILURE, &stderr_text);
    assert_contents(
        "tests/output/cmd-nexus-invalidconfig-stdout",
        &stdout_text,
    );
    let expected_err = format!(
        "nexus: parse \"{}\": missing field `deployment`\n",
        config_path.display()
    );
    assert!(&stderr_text.starts_with(&expected_err));
}
