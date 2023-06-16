// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::Path;

use assert_cmd::Command;

#[test]
fn test_wicket_ssh_force_command_like() {
    let tempdir = tempfile::tempdir().unwrap();

    let mut cmd = make_cmd(tempdir.path());
    cmd.env("SSH_ORIGINAL_COMMAND", "help");
    cmd.assert().success();

    let mut cmd = make_cmd(tempdir.path());
    cmd.env("SSH_ORIGINAL_COMMAND", "--help");
    cmd.args(["-c", "--help"]);
    cmd.assert().success();

    let mut cmd = make_cmd(tempdir.path());
    cmd.env("SSH_ORIGINAL_COMMAND", "upload-repo --no-upload")
        .write_stdin("upload-test");
    cmd.assert().success();
}

fn make_cmd(tempdir: &Path) -> Command {
    let mut cmd = Command::cargo_bin("wicket").unwrap();
    // Set the log path to the temp dir, because the default is to log to
    // /tmp/wicket.log (which might be owned by a different user).
    cmd.env("WICKET_LOG_PATH", tempdir.join("wicket.log"));
    cmd
}
