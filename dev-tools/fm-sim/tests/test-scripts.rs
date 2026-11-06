// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Script-based tests for `fm-sim`.

use camino::Utf8Path;
use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::{
    EXIT_SUCCESS, assert_exit_code, path_to_executable, run_command,
};
use subprocess::Exec;

fn script(path: &Utf8Path) -> datatest_stable::Result<()> {
    let fm_sim = path_to_executable(env!("CARGO_BIN_EXE_fm-sim"));
    let script = path.canonicalize_utf8()?;
    let tmpdir = camino_tempfile::tempdir()?;
    let (exit_status, stdout_text, stderr_text) =
        run_command(Exec::cmd(fm_sim).arg(script).cwd(tmpdir.path()));
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    let test_name = path.file_stem().unwrap();
    assert_contents(format!("tests/output/{test_name}-stdout"), &stdout_text);
    assert_contents(format!("tests/output/{test_name}-stderr"), &stderr_text);

    Ok(())
}

// Run every script in `tests/input` through the `fm-sim` binary and compare
// its output against the corresponding expected results in `tests/output`.
datatest_stable::harness! {
    { test = script, root = "tests/input", pattern = r"cmds.*\.txt" }
}
