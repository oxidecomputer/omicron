/*!
 * Tests for the executable commands in this repo.  Most functionality is tested
 * elsewhere, so this really just sanity checks argument parsing, bad args, and
 * the --openapi mode.
 */

/*
 * TODO-coverage: test success cases of nexus
 */

use expectorate::assert_contents;
use omicron_common::dev::test_cmds::assert_exit_code;
use omicron_common::dev::test_cmds::error_for_enoent;
use omicron_common::dev::test_cmds::path_to_executable;
use omicron_common::dev::test_cmds::run_command;
use omicron_common::dev::test_cmds::temp_file_path;
use omicron_common::dev::test_cmds::EXIT_FAILURE;
use omicron_common::dev::test_cmds::EXIT_SUCCESS;
use omicron_common::dev::test_cmds::EXIT_USAGE;
use openapiv3::OpenAPI;
use std::fs;
use std::path::PathBuf;
use subprocess::Exec;

/** name of the "nexus" executable */
const CMD_NEXUS: &str = env!("CARGO_BIN_EXE_nexus");

fn path_to_nexus() -> PathBuf {
    path_to_executable(CMD_NEXUS)
}

/**
 * Write the requested string to a temporary file and return the path to that
 * file.
 */
fn write_config(config: &str) -> PathBuf {
    let file_path = temp_file_path("test_commands_config");
    eprintln!("writing temp config: {}", file_path.display());
    fs::write(&file_path, config).expect("failed to write config file");
    file_path
}

/*
 * Tests
 */

#[test]
fn test_nexus_no_args() {
    let exec = Exec::cmd(path_to_nexus());
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE);
    assert_contents("tests/output/cmd-nexus-noargs-stdout", &stdout_text);
    assert_contents("tests/output/cmd-nexus-noargs-stderr", &stderr_text);
}

#[test]
fn test_nexus_bad_config() {
    let exec = Exec::cmd(path_to_nexus()).arg("nonexistent");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_FAILURE);
    assert_contents("tests/output/cmd-nexus-badconfig-stdout", &stdout_text);
    assert_eq!(
        stderr_text,
        format!("nexus: read \"nonexistent\": {}\n", error_for_enoent())
    );
}

#[test]
fn test_nexus_invalid_config() {
    let config_path = write_config("");
    let exec = Exec::cmd(path_to_nexus()).arg(&config_path);
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    fs::remove_file(&config_path).expect("failed to remove temporary file");
    assert_exit_code(exit_status, EXIT_FAILURE);
    assert_contents(
        "tests/output/cmd-nexus-invalidconfig-stdout",
        &stdout_text,
    );
    assert_eq!(
        stderr_text,
        format!(
            "nexus: parse \"{}\": missing field \
             `dropshot_external`\n",
            config_path.display()
        ),
    );
}

#[test]
fn test_nexus_openapi() {
    /*
     * This is a little goofy: we need a config file for the program.
     * (Arguably, --openapi shouldn't require a config file, but it's
     * conceivable that the API metadata or the exposed endpoints would depend
     * on the configuration.)  We ship a config file in "examples", and we may
     * as well use it here -- it would be a bug if that one didn't work for this
     * purpose.  However, it's not clear how to reliably locate it at runtime.
     * But we do know where it is at compile time, so we load it then.
     */
    let config = include_str!("../examples/config.toml");
    let config_path = write_config(config);
    let exec = Exec::cmd(path_to_nexus()).arg(&config_path).arg("--openapi");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    fs::remove_file(&config_path).expect("failed to remove temporary file");
    assert_exit_code(exit_status, EXIT_SUCCESS);
    assert_contents("tests/output/cmd-nexus-openapi-stderr", &stderr_text);

    /*
     * Make sure the result parses as a valid OpenAPI spec and sanity-check a
     * few fields.
     */
    let spec: OpenAPI = serde_json::from_str(&stdout_text)
        .expect("stdout was not valid OpenAPI");
    assert_eq!(spec.openapi, "3.0.3");
    assert_eq!(spec.info.title, "Oxide Region API");
    assert_eq!(spec.info.version, "0.0.1");

    /*
     * Spot check a couple of items.
     */
    assert!(spec.paths.len() > 0);
    assert!(spec.paths.get("/projects").is_some());

    /*
     * Check for lint errors.
     */
    let errors = openapi_lint::validate(&spec);
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));

    /*
     * Confirm that the output hasn't changed. It's expected that we'll change
     * this file as the API evolves, but pay attention to the diffs to ensure
     * that the changes match your expectations.
     */
    assert_contents("tests/output/nexus-openapi.json", &stdout_text);
}
