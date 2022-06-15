// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2021 Oxide Computer Company

use std::{fs, path::PathBuf};

use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::{
    assert_exit_code, path_to_executable, run_command, temp_file_path,
    EXIT_SUCCESS, EXIT_USAGE,
};
use openapiv3::OpenAPI;
use subprocess::Exec;

/// name of the "oximeter" executable
const CMD_OXIMETER: &str = env!("CARGO_BIN_EXE_oximeter");

fn path_to_oximeter() -> PathBuf {
    path_to_executable(CMD_OXIMETER)
}

/// Write the requested string to a temporary file and return the path to that
/// file.
fn write_config(config: &str) -> PathBuf {
    let file_path = temp_file_path("test_commands_config");
    eprintln!("writing temp config: {}", file_path.display());
    fs::write(&file_path, config).expect("failed to write config file");
    file_path
}

#[test]
fn test_oximeter_no_args() {
    let exec = Exec::cmd(path_to_oximeter());
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE, &stderr_text);
    assert_contents("tests/output/cmd-oximeter-noargs-stdout", &stdout_text);
    assert_contents("tests/output/cmd-oximeter-noargs-stderr", &stderr_text);
}

#[test]
fn test_oximeter_openapi() {
    // This is a little goofy: we need a config file for the program.
    // (Arguably, --openapi shouldn't require a config file, but it's
    // conceivable that the API metadata or the exposed endpoints would depend
    // on the configuration.)  We ship a config file in "examples", and we may
    // as well use it here -- it would be a bug if that one didn't work for this
    // purpose.  However, it's not clear how to reliably locate it at runtime.
    // But we do know where it is at compile time, so we load it then.
    let config = include_str!("../../collector/config.toml");
    let config_path = write_config(config);
    let exec = Exec::cmd(path_to_oximeter()).arg(&config_path).arg("--openapi");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    fs::remove_file(&config_path).expect("failed to remove temporary file");
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);
    assert_contents("tests/output/cmd-oximeter-openapi-stderr", &stderr_text);

    let spec: OpenAPI = serde_json::from_str(&stdout_text)
        .expect("stdout was not valid OpenAPI");

    // Check for lint errors.
    let errors = openapi_lint::validate(&spec);
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));

    // Confirm that the output hasn't changed. It's expected that we'll change
    // this file as the API evolves, but pay attention to the diffs to ensure
    // that the changes match your expectations.
    assert_contents("../../openapi/oximeter.json", &stdout_text);
}
