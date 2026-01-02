// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common code used by integration tests

use camino::Utf8Path;
use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::EXIT_SUCCESS;
use omicron_test_utils::dev::test_cmds::Redactor;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use std::path::PathBuf;
use subprocess::{Exec, ExitStatus};

fn path_to_cli() -> PathBuf {
    path_to_executable(env!("CARGO_BIN_EXE_reconfigurator-cli"))
}

fn run_cli(
    file: impl AsRef<Utf8Path>,
    args: &[&str],
    cwd: Option<&Utf8Path>,
) -> (ExitStatus, String, String) {
    let file = file.as_ref();

    // Turn the path into an absolute one, because we're going to set a custom
    // cwd for the subprocess.
    let file = file.canonicalize_utf8().expect("file canonicalized");
    eprintln!("using file: {file}");

    // Create a temporary directory for the CLI to use -- that will let it
    // read and write files in its own sandbox.
    let tmpdir;
    let cwd = match cwd {
        None => {
            tmpdir =
                camino_tempfile::tempdir().expect("failed to create tmpdir");
            tmpdir.path()
        }
        Some(c) => c,
    };
    let exec = Exec::cmd(path_to_cli()).arg(file).args(args).cwd(cwd);
    run_command(exec)
}

pub fn script_with_cwd(
    path: &Utf8Path,
    cwd: Option<&Utf8Path>,
) -> datatest_stable::Result<()> {
    let (exit_status, stdout_text, stderr_text) =
        run_cli(path, &["--seed", "reconfigurator-cli-test"], cwd);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    // Everything is deterministic, so we don't need to redact UUIDs.
    // However, it's necessary to redact paths from generated log entries.
    let stdout_text = Redactor::default()
        .uuids(false)
        .field("assembling repository in", ".*")
        .field("extracting uploaded archive to", ".*")
        .field("created directory to store extracted artifacts, path:", ".*")
        .do_redact(&stdout_text);

    // This is the file name without the extension.
    let test_name = path.file_stem().unwrap();
    let stdout_file = format!("tests/output/{test_name}-stdout");
    let stderr_file = format!("tests/output/{test_name}-stderr");

    assert_contents(&stdout_file, &stdout_text);
    assert_contents(&stderr_file, &stderr_text);

    Ok(())
}
