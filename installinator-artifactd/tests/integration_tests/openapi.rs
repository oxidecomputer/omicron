// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::PathBuf;

use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::{
    assert_exit_code, path_to_executable, run_command, EXIT_SUCCESS,
};
use openapiv3::OpenAPI;
use subprocess::Exec;

// name of executable
const CMD_SERVER: &str = env!("CARGO_BIN_EXE_installinator-artifactd");

fn path_to_server() -> PathBuf {
    path_to_executable(CMD_SERVER)
}

#[test]
fn test_server_openapi() {
    let exec = Exec::cmd(path_to_server()).arg("openapi");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);
    assert_contents("tests/output/cmd-server-openapi-stderr", &stderr_text);

    let spec: OpenAPI = serde_json::from_str(&stdout_text)
        .expect("stdout was not valid OpenAPI");

    // Check for lint errors.
    let errors = openapi_lint::validate(&spec);
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));

    // Confirm that the output hasn't changed. It's expected that we'll change
    // this file as the API evolves, but pay attention to the diffs to ensure
    // that the changes match your expectations.
    assert_contents("../openapi/installinator-artifactd.json", &stdout_text);
}
