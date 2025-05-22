// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8Path;
use expectorate::assert_contents;
use omicron_test_utils::dev::test_cmds::EXIT_SUCCESS;
use omicron_test_utils::dev::test_cmds::Redactor;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use std::path::PathBuf;
use subprocess::Exec;
use subprocess::ExitStatus;

fn path_to_cli() -> PathBuf {
    path_to_executable(env!("CARGO_BIN_EXE_reconfigurator-cli"))
}

fn run_cli(
    file: impl AsRef<Utf8Path>,
    args: &[&str],
) -> (ExitStatus, String, String) {
    let file = file.as_ref();

    // Turn the path into an absolute one, because we're going to set a custom
    // cwd for the subprocess.
    let file = file.canonicalize_utf8().expect("file canonicalized");
    eprintln!("using file: {file}");

    // Create a temporary directory for the CLI to use -- that will let it
    // read and write files in its own sandbox.
    let tmpdir = camino_tempfile::tempdir().expect("failed to create tmpdir");
    let exec = Exec::cmd(path_to_cli()).arg(file).args(args).cwd(tmpdir.path());
    run_command(exec)
}

// Run a battery of simple commands and make sure things basically seem to work.
#[test]
fn test_basic() {
    let (exit_status, stdout_text, stderr_text) =
        run_cli("tests/input/cmds.txt", &["--seed", "test_basic"]);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    // Everything is deterministic, so we don't need to redact UUIDs.
    let stdout_text = Redactor::default().uuids(false).do_redact(&stdout_text);
    assert_contents("tests/output/cmd-stdout", &stdout_text);
    assert_contents("tests/output/cmd-stderr", &stderr_text);
}

// Run tests against a loaded example system.
#[test]
fn test_example() {
    let (exit_status, stdout_text, stderr_text) =
        run_cli("tests/input/cmds-example.txt", &["--seed", "test_example"]);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    // The example system uses a fixed seed, which means that UUIDs are
    // deterministic. Some of the test commands also use those UUIDs, and it's
    // convenient for everyone if they aren't redacted.
    let stdout_text = Redactor::default().uuids(false).do_redact(&stdout_text);
    assert_contents("tests/output/cmd-example-stdout", &stdout_text);
    assert_contents("tests/output/cmd-example-stderr", &stderr_text);
}

// Run tests to expunge an external DNS zone, plan (which should add it again),
// then expunge the newly-added zone.
#[test]
fn test_expunge_newly_added_external_dns() {
    let (exit_status, stdout_text, stderr_text) = run_cli(
        "tests/input/cmds-expunge-newly-added-external-dns.txt",
        &["--seed", "test_expunge_newly_added_external_dns"],
    );
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    // The example system uses a fixed seed, which means that UUIDs are
    // deterministic. Some of the test commands also use those UUIDs, and it's
    // convenient for everyone if they aren't redacted.
    let stdout_text = Redactor::default().uuids(false).do_redact(&stdout_text);
    assert_contents(
        "tests/output/cmd-expunge-newly-added-external-dns-stdout",
        &stdout_text,
    );
    assert_contents(
        "tests/output/cmd-expunge-newly-added-external-dns-stderr",
        &stderr_text,
    );
}

// Run tests to expunge an internal DNS zone, plan (which should add it again),
// then expunge the newly-added zone. This is mechanically similar to the above
// test about external DNS zones, but is particularly useful to ensure that DNS
// record maintenance is handled correctly for external zones as well.
#[test]
fn test_expunge_newly_added_internal_dns() {
    let (exit_status, stdout_text, stderr_text) = run_cli(
        "tests/input/cmds-expunge-newly-added-internal-dns.txt",
        &["--seed", "test_expunge_newly_added_internal_dns"],
    );
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    // The example system uses a fixed seed, which means that UUIDs are
    // deterministic. Some of the test commands also use those UUIDs, and it's
    // convenient for everyone if they aren't redacted.
    let stdout_text = Redactor::default().uuids(false).do_redact(&stdout_text);
    assert_contents(
        "tests/output/cmd-expunge-newly-added-internal-dns-stdout",
        &stdout_text,
    );
    assert_contents(
        "tests/output/cmd-expunge-newly-added-internal-dns-stderr",
        &stderr_text,
    );
}


// Run tests that exercise the ability to set zone image sources.
#[test]
fn test_set_zone_images() {
    let (exit_status, stdout_text, stderr_text) = run_cli(
        "tests/input/cmds-set-zone-images.txt",
        &["--seed", "test_set_zone_images"],
    );
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    // The example system uses a fixed seed, which means that UUIDs are
    // deterministic. Some of the test commands also use those UUIDs, and it's
    // convenient for everyone if they aren't redacted.
    let stdout_text = Redactor::default().uuids(false).do_redact(&stdout_text);
    assert_contents("tests/output/cmd-set-zone-images-stdout", &stdout_text);
    assert_contents("tests/output/cmd-set-zone-images-stderr", &stderr_text);
}

// Run tests that exercise the ability to configured MGS-managed updates.
#[test]
fn test_set_mgs_updates() {
    let (exit_status, stdout_text, stderr_text) = run_cli(
        "tests/input/cmds-set-mgs-updates.txt",
        &["--seed", "test_set_mgs_updates"],
    );
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    // The example system uses a fixed seed, which means that UUIDs are
    // deterministic. Some of the test commands also use those UUIDs, and it's
    // convenient for everyone if they aren't redacted.
    let stdout_text = Redactor::default().uuids(false).do_redact(&stdout_text);
    assert_contents("tests/output/cmd-set-mgs-updates-stdout", &stdout_text);
    assert_contents("tests/output/cmd-set-mgs-updates-stderr", &stderr_text);
}
