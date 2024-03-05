// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use camino::Utf8Path;
use expectorate::assert_contents;
use nexus_types::deployment::UnstableReconfiguratorState;
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

// To do more interesting testing, we need more interesting data than we can
// currently cons up in the REPL environment.  This test uses a file saved from
// the "madrid" rig using `omdb db reconfigurator-save`.
//
// This test may be broken if the format of this file changes (see the
// UnstableReconfiguratorState struct).  To fix it, generate a new file from a
// real system with at least one inventory collection and at least two
// blueprints.  You will also need to set EXPECTORATE=overwrite when running
// this for the first time with the new input file in order to generate the new
// output file.  Check it.
//
// If this test becomes too burdensome, we could drop it.  But for now it's
// useful to have a smoke test for the command.
#[test]
fn test_complex() {
    let input_path = Utf8Path::new("tests/input/complex.json");
    let input_file = std::fs::File::open(input_path)
        .with_context(|| format!("open {:?}", input_path))
        .unwrap();
    let input_data: UnstableReconfiguratorState =
        serde_json::from_reader(&input_file)
            .with_context(|| format!("read {:?}", input_path))
            .unwrap();
    assert!(
        input_data.collections.len() > 0,
        "input file must have at least one inventory collection"
    );
    assert!(
        input_data.blueprints.len() > 1,
        "input file must have at least two blueprints"
    );

    let collection = input_data.collections.iter().next().unwrap().id;
    let mut blueprints = input_data.blueprints.iter().rev();
    let blueprint2 = blueprints.next().unwrap().id;
    let blueprint1 = blueprints.next().unwrap().id;
    let tmpdir = camino_tempfile::tempdir().expect("failed to create tmpdir");
    let tmpfile = tmpdir.path().join("cmds.txt");

    // We construct the list of commands dynamically to avoid having to re-do it
    // when the input file has to be regenerated.
    let input_cmds = [
        "sled-list",
        "inventory-list",
        "blueprint-list",
        &format!("file-contents {}", input_path),
        &format!("load {} {}", input_path, collection),
        "sled-list",
        "inventory-list",
        "blueprint-list",
        &format!("blueprint-show {}", blueprint2),
        &format!("blueprint-diff-inventory {} {}", collection, blueprint2),
        &format!("blueprint-diff {} {}", blueprint1, blueprint2),
        &format!("blueprint-diff {} {}", blueprint2, blueprint1),
        "sled-add dde1c0e2-b10d-4621-b420-f179f7a7a00a",
        &format!("blueprint-plan {} {}", blueprint2, collection),
    ]
    .into_iter()
    .map(|s| format!("{}\n", s))
    .collect::<Vec<_>>()
    .join("\n");

    println!("will execute commands:\n{}", input_cmds);

    std::fs::write(&tmpfile, &input_cmds)
        .with_context(|| format!("write {:?}", &tmpfile))
        .unwrap();

    let exec = Exec::cmd(path_to_cli()).arg(&tmpfile);
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    // This is a much lighter form of redaction than `redact_variable()` does.
    let stdout_text = regex::Regex::new(
        r"generated blueprint .* based on parent blueprint",
    )
    .unwrap()
    .replace_all(
        &stdout_text,
        "generated blueprint REDACTED_UUID based on parent blueprint",
    )
    .to_string();

    assert_contents("tests/output/cmd-complex-stdout", &stdout_text);
}
