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
use omicron_test_utils::dev::test_cmds::EXIT_SUCCESS;
use omicron_test_utils::dev::test_cmds::EXIT_USAGE;
use openapiv3::OpenAPI;
use std::collections::BTreeMap;
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

#[track_caller]
fn run_command_with_arg(arg: &str) -> (String, String) {
    // This is a little goofy: we need a config file for the program.
    // (Arguably, --openapi shouldn't require a config file, but it's
    // conceivable that the API metadata or the exposed endpoints would depend
    // on the configuration.)  We ship a config file in "examples", and we may
    // as well use it here -- it would be a bug if that one didn't work for this
    // purpose.  However, it's not clear how to reliably locate it at runtime.
    // But we do know where it is at compile time, so we load it then.
    let config = include_str!("../../examples/config.toml");
    let config_path = write_config(config);
    let exec = Exec::cmd(path_to_nexus()).arg(&config_path).arg(arg);
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    fs::remove_file(&config_path).expect("failed to remove temporary file");
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    (stdout_text, stderr_text)
}

#[test]
fn test_nexus_openapi() {
    let (stdout_text, stderr_text) = run_command_with_arg("--openapi");
    assert_contents("tests/output/cmd-nexus-openapi-stderr", &stderr_text);

    // Make sure the result parses as a valid OpenAPI spec and sanity-check a
    // few fields.
    let spec: OpenAPI = serde_json::from_str(&stdout_text)
        .expect("stdout was not valid OpenAPI");
    assert_eq!(spec.openapi, "3.0.3");
    assert_eq!(spec.info.title, "Oxide Region API");
    assert_eq!(spec.info.version, "20240327.0");

    // Spot check a couple of items.
    assert!(!spec.paths.paths.is_empty());
    assert!(spec.paths.paths.get("/v1/projects").is_some());

    // Check for lint errors.
    let errors = openapi_lint::validate_external(&spec);
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));

    // Construct a string that helps us identify the organization of tags and
    // operations.
    let mut ops_by_tag =
        BTreeMap::<String, Vec<(String, String, String)>>::new();
    for (path, method, op) in spec.operations() {
        // Make sure each operation has exactly one tag. Note, we intentionally
        // do this before validating the OpenAPI output as fixing an error here
        // would necessitate refreshing the spec file again.
        assert_eq!(
            op.tags.len(),
            1,
            "operation '{}' has {} tags rather than 1",
            op.operation_id.as_ref().unwrap(),
            op.tags.len()
        );

        // Every non-hidden endpoint must have a summary
        if !op.tags.contains(&"hidden".to_string()) {
            assert!(
                op.summary.is_some(),
                "operation '{}' is missing a summary doc comment",
                op.operation_id.as_ref().unwrap()
            );
        }

        ops_by_tag
            .entry(op.tags.first().unwrap().to_string())
            .or_default()
            .push((
                op.operation_id.as_ref().unwrap().to_string(),
                method.to_string().to_uppercase(),
                path.to_string(),
            ));
    }

    let mut tags = String::new();
    for (tag, mut ops) in ops_by_tag {
        ops.sort();
        tags.push_str(&format!(r#"API operations found with tag "{}""#, tag));
        tags.push_str(&format!(
            "\n{:40} {:8} {}\n",
            "OPERATION ID", "METHOD", "URL PATH"
        ));
        for (operation_id, method, path) in ops {
            tags.push_str(&format!(
                "{:40} {:8} {}\n",
                operation_id, method, path
            ));
        }
        tags.push('\n');
    }

    // Confirm that the output hasn't changed. It's expected that we'll change
    // this file as the API evolves, but pay attention to the diffs to ensure
    // that the changes match your expectations.
    assert_contents("../openapi/nexus.json", &stdout_text);

    // When this fails, verify that operations on which you're adding,
    // renaming, or changing the tags are what you intend.
    assert_contents("tests/output/nexus_tags.txt", &tags);
}

#[test]
fn test_nexus_openapi_internal() {
    let (stdout_text, _) = run_command_with_arg("--openapi-internal");
    let spec: OpenAPI = serde_json::from_str(&stdout_text)
        .expect("stdout was not valid OpenAPI");

    // Check for lint errors.
    let errors = openapi_lint::validate(&spec);
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));

    // Confirm that the output hasn't changed. It's expected that we'll change
    // this file as the API evolves, but pay attention to the diffs to ensure
    // that the changes match your expectations.
    assert_contents("../openapi/nexus-internal.json", &stdout_text);
}
