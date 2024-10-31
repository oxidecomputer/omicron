// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::Path;

use anyhow::Result;
use assert_cmd::Command;
use camino::Utf8PathBuf;
use omicron_common::{
    api::internal::nexus::KnownArtifactKind, update::ArtifactKind,
};
use omicron_test_utils::dev::test_setup_log;
use predicates::prelude::*;
use tufaceous_lib::{Key, OmicronRepo};

#[tokio::test]
async fn test_init_and_add() -> Result<()> {
    let logctx = test_setup_log("test_init_and_add");
    let tempdir = tempfile::tempdir().unwrap();
    let key = Key::generate_ed25519()?;

    let mut cmd = make_cmd_with_repo(tempdir.path(), &key);
    cmd.args(["init", "0.0.0"]);
    cmd.assert().success();

    // Create a couple of stub files on disk.
    let nexus_path = tempdir.path().join("nexus.tar.gz");
    fs_err::write(&nexus_path, "test")?;
    let unknown_path = tempdir.path().join("my-unknown-kind.tar.gz");
    fs_err::write(&unknown_path, "unknown test")?;

    let mut cmd = make_cmd_with_repo(tempdir.path(), &key);
    cmd.args(["add", "gimlet_sp"]);
    cmd.arg(&nexus_path);
    cmd.arg("42.0.0");
    cmd.assert().success();

    // Try adding an unknown kind without --allow-unknown-kinds.
    let mut cmd = make_cmd_with_repo(tempdir.path(), &key);
    cmd.args(["add", "my_unknown_kind"]);
    cmd.arg(&nexus_path);
    cmd.arg("0.0.0");
    cmd.assert().failure().stderr(predicate::str::contains(
        "invalid value 'my_unknown_kind' for '<KIND>'",
    ));

    // Try adding one with --allow-unknown-kinds.
    let mut cmd = make_cmd_with_repo(tempdir.path(), &key);
    cmd.args(["add", "my_unknown_kind", "--allow-unknown-kinds"]);
    cmd.arg(&unknown_path);
    cmd.arg("0.1.0");
    cmd.assert().success();

    // Now read the repository and ensure the list of expected artifacts.
    let repo_path: Utf8PathBuf = tempdir.path().join("repo").try_into()?;
    let repo = OmicronRepo::load_untrusted(&logctx.log, &repo_path).await?;

    let artifacts = repo.read_artifacts().await?;
    assert_eq!(
        artifacts.artifacts.len(),
        2,
        "repo should contain exactly 2 artifacts: {artifacts:?}"
    );

    let mut artifacts_iter = artifacts.artifacts.into_iter();
    let artifact = artifacts_iter.next().unwrap();
    assert_eq!(artifact.name, "nexus", "artifact name");
    assert_eq!(artifact.version, "42.0.0".parse().unwrap(), "artifact version");
    assert_eq!(
        artifact.kind,
        ArtifactKind::from_known(KnownArtifactKind::GimletSp),
        "artifact kind"
    );
    assert_eq!(
        artifact.target, "gimlet_sp-nexus-42.0.0.tar.gz",
        "artifact target"
    );

    let artifact = artifacts_iter.next().unwrap();
    assert_eq!(artifact.name, "my-unknown-kind", "artifact name");
    assert_eq!(artifact.version, "0.1.0".parse().unwrap(), "artifact version");
    assert_eq!(
        artifact.kind,
        ArtifactKind::new("my_unknown_kind".to_owned()),
        "artifact kind"
    );
    assert_eq!(
        artifact.target, "my_unknown_kind-my-unknown-kind-0.1.0.tar.gz",
        "artifact target"
    );

    // Create an archive from the given path.
    let archive_path = tempdir.path().join("archive.zip");
    let mut cmd = make_cmd_with_repo(tempdir.path(), &key);
    cmd.arg("archive");
    cmd.arg(&archive_path);
    cmd.assert().success();

    // Extract the archive to a new directory.
    let dest_path = tempdir.path().join("dest");
    let mut cmd = make_cmd_with_repo(tempdir.path(), &key);
    cmd.arg("extract");
    cmd.arg(&archive_path);
    cmd.arg(&dest_path);

    cmd.assert().success();

    logctx.cleanup_successful();
    Ok(())
}

#[test]
fn test_assemble_fake() -> Result<()> {
    let logctx = test_setup_log("test_assemble_fake");
    let tempdir = tempfile::tempdir().unwrap();
    let key = Key::generate_ed25519()?;

    let archive_path = tempdir.path().join("archive.zip");

    let mut cmd = make_cmd(&key);
    cmd.args(["assemble", "manifests/fake.toml"]);
    cmd.arg(&archive_path);
    cmd.assert().success();

    // Extract the archive to a new directory.
    let dest_path = tempdir.path().join("dest");
    let mut cmd = make_cmd(&key);
    cmd.arg("extract");
    cmd.arg(&archive_path);
    cmd.arg(&dest_path);

    cmd.assert().success();

    logctx.cleanup_successful();
    Ok(())
}

fn make_cmd(key: &Key) -> Command {
    let mut cmd = Command::cargo_bin("tufaceous").unwrap();
    cmd.env("TUFACEOUS_KEY", key.to_string());

    cmd
}

fn make_cmd_with_repo(tempdir: &Path, key: &Key) -> Command {
    let mut cmd = make_cmd(key);
    cmd.arg("--repo");
    cmd.arg(tempdir.join("repo"));

    cmd
}
