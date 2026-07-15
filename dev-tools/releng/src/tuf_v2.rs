// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::error::Error as _;
use std::fmt::Write;

use anyhow::{Context, Result, ensure};
use camino::Utf8PathBuf;
use chrono::Utc;
use fs_err::tokio as fs;
use semver::Version;
use slog::Logger;
use tokio::sync::{mpsc, oneshot};
use tufaceous_artifact_v2::{OsVariant, RotSlot};
use tufaceous_v2::{RepositoryLoader, edit::RepositoryEditor};

pub(crate) async fn build_tuf_repo(
    logger: Logger,
    output_dir: Utf8PathBuf,
    editor: SharedEditor,
    threads: usize,
) -> Result<()> {
    let repo_path = output_dir.join("repo-v2.zip");
    let sha256_path = output_dir.join("repo-v2.zip.sha256.txt");

    editor
        .take_editor()
        .await?
        .finish()
        .await?
        .generate_root()
        .sign()
        .await?
        .write_zip_file(&repo_path, Utc::now())
        .await?;

    let repo = RepositoryLoader::new()
        .unsafe_blindly_trust_repo()
        .compute_archive_sha256(true)
        .load_zip_path(repo_path, &logger)
        .await?;
    let sha256 =
        repo.archive_sha256().context("archive sha256 was not calculated")?;
    fs::write(sha256_path, format!("{}\n", hex::encode(sha256))).await?;

    repo.verify_targets(threads).await?;

    let mut problems = String::new();
    for problem in repo.check_problems().await? {
        write!(&mut problems, "\n- {problem}")?;
        let mut source = problem.source();
        while let Some(s) = source {
            write!(&mut problems, ": {s}")?;
            source = s.source();
        }
    }
    ensure!(problems.is_empty(), "found compatibility problems:{problems}",);

    Ok(())
}

#[derive(Clone)]
pub(crate) struct SharedEditor(mpsc::Sender<EditorRequest>);

enum EditorRequest {
    AddInput(AddKind, Utf8PathBuf, oneshot::Sender<Result<()>>),
    TakeEditor(oneshot::Sender<RepositoryEditor<'static>>),
}

enum AddKind {
    MeasurementCorpus,
    OsImageDir(OsVariant),
    RotArchive(RotSlot),
    RotBootloaderArchive,
    SpArchive,
    ZoneImage,
}

impl SharedEditor {
    pub(crate) fn new(system_version: Version) -> Result<Self> {
        let mut editor_cell = Some(RepositoryEditor::new(system_version)?);
        let (tx, mut rx) = mpsc::channel(1);
        tokio::task::spawn(async move {
            while let Some(editor) = editor_cell.take()
                && let Some(msg) = rx.recv().await
            {
                match msg {
                    EditorRequest::AddInput(kind, path, tx) => {
                        let result = match kind {
                            AddKind::MeasurementCorpus => {
                                editor.add_measurement_corpus(path).await
                            }
                            AddKind::OsImageDir(os_variant) => {
                                editor.add_os_image_dir(os_variant, path).await
                            }
                            AddKind::RotArchive(rot_slot) => {
                                editor.add_rot_archive(rot_slot, path).await
                            }
                            AddKind::RotBootloaderArchive => {
                                editor.add_rot_bootloader_archive(path).await
                            }
                            AddKind::SpArchive => {
                                editor.add_sp_archive(path).await
                            }
                            AddKind::ZoneImage => {
                                editor.add_zone_image(path).await
                            }
                        };
                        match result {
                            Ok(editor) => {
                                editor_cell = Some(editor);
                                tx.send(Ok(())).ok();
                            }
                            Err(err) => {
                                tx.send(Err(err.into())).ok();
                            }
                        }
                    }
                    EditorRequest::TakeEditor(tx) => {
                        tx.send(editor).ok();
                    }
                }
            }
        });
        Ok(Self(tx))
    }

    async fn add(&self, kind: AddKind, path: Utf8PathBuf) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let req = EditorRequest::AddInput(kind, path, tx);
        ensure!(self.0.send(req).await.is_ok(), "shared editor task hung up");
        rx.await.context("shared editor task hung up")?
    }

    pub(crate) async fn add_measurement_corpus(
        &self,
        path: Utf8PathBuf,
    ) -> Result<()> {
        self.add(AddKind::MeasurementCorpus, path).await
    }

    pub(crate) async fn add_os_image_dir(
        &self,
        os_variant: OsVariant,
        path: Utf8PathBuf,
    ) -> Result<()> {
        self.add(AddKind::OsImageDir(os_variant), path).await
    }

    pub(crate) async fn add_rot_archive(
        &self,
        rot_slot: RotSlot,
        path: Utf8PathBuf,
    ) -> Result<()> {
        self.add(AddKind::RotArchive(rot_slot), path).await
    }

    pub(crate) async fn add_rot_bootloader_archive(
        &self,
        path: Utf8PathBuf,
    ) -> Result<()> {
        self.add(AddKind::RotBootloaderArchive, path).await
    }

    pub(crate) async fn add_sp_archive(&self, path: Utf8PathBuf) -> Result<()> {
        self.add(AddKind::SpArchive, path).await
    }

    pub(crate) async fn add_zone_image(&self, path: Utf8PathBuf) -> Result<()> {
        self.add(AddKind::ZoneImage, path).await
    }

    pub(crate) async fn take_editor(
        &self,
    ) -> Result<RepositoryEditor<'static>> {
        let (tx, rx) = oneshot::channel();
        let req = EditorRequest::TakeEditor(tx);
        ensure!(self.0.send(req).await.is_ok(), "shared editor task hung up");
        rx.await.context("shared editor task hung up")
    }
}
