// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::Path;

use anyhow::Result;
use assert_cmd::Command;
use camino::Utf8PathBuf;
use omicron_common::{
    api::internal::nexus::UpdateArtifactKind, update::ArtifactKind,
};
use tufaceous_lib::{Key, OmicronRepo};

#[test]
fn test_init_and_add() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let key = Key::generate_ed25519();

    let mut cmd = make_cmd(tempdir.path(), &key);
    cmd.args(["init"]);
    cmd.assert().success();

    // Create a stub nexus file on disk.
    let nexus_path = tempdir.path().join("omicron-nexus.tar.gz");
    fs_err::write(&nexus_path, "test")?;

    let mut cmd = make_cmd(tempdir.path(), &key);
    cmd.arg("add-zone");
    cmd.arg(&nexus_path);
    cmd.arg("42.0.0");
    cmd.assert().success();

    // Now read the repository and ensure that artifacts includes omicron-nexus.
    let repo_path: Utf8PathBuf = tempdir.path().join("repo").try_into()?;
    let repo = OmicronRepo::load(&repo_path)?;

    let artifacts = repo.read_artifacts()?;
    assert_eq!(
        artifacts.artifacts.len(),
        1,
        "repo should contain exactly 1 artifact: {artifacts:?}"
    );
    let artifact = artifacts.artifacts.into_iter().next().unwrap();
    assert_eq!(artifact.name, "omicron-nexus", "artifact name");
    assert_eq!(artifact.version, "42.0.0", "artifact version");
    assert_eq!(
        artifact.kind,
        ArtifactKind::Known(UpdateArtifactKind::Zone),
        "artifact kind"
    );
    assert_eq!(
        artifact.target, "omicron-nexus-42.0.0.tar.gz",
        "artifact target"
    );

    Ok(())
}

fn make_cmd(tempdir: &Path, key: &Key) -> Command {
    let mut cmd = Command::cargo_bin("tufaceous").unwrap();
    cmd.arg("--repo");
    cmd.arg(tempdir.join("repo"));
    cmd.env("TUFACEOUS_KEY", key.to_string());

    cmd
}
