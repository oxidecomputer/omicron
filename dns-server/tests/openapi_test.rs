// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use omicron_test_utils::dev::test_cmds::EXIT_SUCCESS;
use openapiv3::OpenAPI;
use subprocess::Exec;

const CMD_API_GEN: &str = env!("CARGO_BIN_EXE_apigen");

#[test]
fn test_dns_server_openapi() {
    let exec = Exec::cmd(path_to_executable(CMD_API_GEN));
    let (exit_status, stdout, stderr) = run_command(exec);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr);

    let spec: OpenAPI =
        serde_json::from_str(&stdout).expect("stdout was not valid OpenAPI");
    let errors = openapi_lint::validate(&spec);
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));

    assert_contents("../openapi/dns-server.json", &stdout);
}
