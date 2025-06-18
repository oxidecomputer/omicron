// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `reconfigurator-cli` tests that require custom setup
//!
//! If your test does not require custom setup, use `test-scripts.rs` instead.

use camino::Utf8PathBuf;
use clap::Parser as _;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::test_util::LogContext;

mod common;
use common::script_with_cwd;

// This test is special-cased because it requires custom setup (creating a TUF
// repo).
#[tokio::test]
async fn test_target_release() {
    let logctx = LogContext::new(
        "reconfigurator-cli-target-release",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
    );
    let tmpdir = camino_tempfile::tempdir().expect("failed to create tmpdir");
    let path = tmpdir.path().join("repo-1.0.0.zip");
    tufaceous::Args::try_parse_from([
        "tufaceous",
        "assemble",
        "../../update-common/manifests/fake.toml",
        path.as_str(),
    ])
    .expect("can't parse tufaceous args")
    .exec(&logctx.log)
    .await
    .expect("can't assemble TUF repo");

    script_with_cwd(
        &Utf8PathBuf::from("tests/input/target-release.txt"),
        Some(tmpdir.path()),
    )
    .expect("test failed");

    // Dropping the tmpdir will clean it up.
    logctx.cleanup_successful();
}
