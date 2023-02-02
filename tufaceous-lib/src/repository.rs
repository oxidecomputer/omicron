// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{key::Key, target::TargetWriter, AddArtifact};
use anyhow::{anyhow, bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use fs_err::{self as fs, File};
use omicron_common::update::{Artifact, ArtifactsDocument};
use std::num::NonZeroU64;
use tough::{
    editor::{signed::SignedRole, RepositoryEditor},
    schema::Root,
    ExpirationEnforcement, Repository, RepositoryLoader, TargetName,
};
use url::Url;

/// A TUF repository describing Omicron.
pub struct OmicronRepo {
    repo: Repository,
    repo_path: Utf8PathBuf,
}

impl OmicronRepo {
    /// Initializes a new repository at the given path, writing it to disk.
    pub fn initialize(
        repo_path: &Utf8Path,
        keys: Vec<Key>,
        expiry: DateTime<Utc>,
    ) -> Result<Self> {
        let root = crate::root::new_root(keys.clone(), expiry)?;
        let editor = OmicronRepoEditor::initialize(repo_path.to_owned(), root)?;

        editor
            .sign_and_finish(keys, expiry)
            .context("error signing new repository")?;

        Self::load(repo_path)
    }

    /// Loads a repository from the given path.
    ///
    /// This method enforces expirations. To load without expiration enforcement, use
    /// [`Self::load_ignore_expiration`].
    pub fn load(repo_path: &Utf8Path) -> Result<Self> {
        Self::load_impl(repo_path, ExpirationEnforcement::Safe)
    }

    /// Loads a repository from the given path, ignoring expiration.
    ///
    /// Use cases for this include:
    ///
    /// 1. When you're editing an existing repository and will re-sign it afterwards.
    /// 2. In an environment in which time isn't available.
    pub fn load_ignore_expiration(repo_path: &Utf8Path) -> Result<Self> {
        Self::load_impl(repo_path, ExpirationEnforcement::Unsafe)
    }

    fn load_impl(
        repo_path: &Utf8Path,
        exp: ExpirationEnforcement,
    ) -> Result<Self> {
        let repo_path = repo_path.canonicalize_utf8()?;

        let repo = RepositoryLoader::new(
            File::open(repo_path.join("metadata").join("1.root.json"))?,
            Url::from_file_path(repo_path.join("metadata"))
                .expect("the canonical path is not absolute?"),
            Url::from_file_path(repo_path.join("targets"))
                .expect("the canonical path is not absolute?"),
        )
        .expiration_enforcement(exp)
        .load()?;

        Ok(Self { repo, repo_path })
    }

    /// Returns a canonicalized form of the repository path.
    pub fn repo_path(&self) -> &Utf8Path {
        &self.repo_path
    }

    /// Returns the repository.
    pub fn repo(&self) -> &Repository {
        &self.repo
    }

    /// Reads the artifacts document from the repo.
    pub fn read_artifacts(&self) -> Result<ArtifactsDocument> {
        let reader = self
            .repo
            .read_target(&"artifacts.json".try_into()?)?
            .ok_or_else(|| anyhow!("artifacts.json should be present"))?;
        serde_json::from_reader(reader)
            .context("error deserializing artifacts.json")
    }

    /// Converts `self` into an `OmicronRepoEditor`, which can be used to perform
    /// modifications to the repository.
    pub fn into_editor(self) -> Result<OmicronRepoEditor> {
        OmicronRepoEditor::new(self)
    }
}

/// An [`OmicronRepo`] than can be edited.
///
/// Created by [`OmicronRepo::into_editor`].
pub struct OmicronRepoEditor {
    editor: RepositoryEditor,
    repo_path: Utf8PathBuf,
    artifacts: ArtifactsDocument,
    existing_targets: Vec<TargetName>,
}

impl OmicronRepoEditor {
    fn new(repo: OmicronRepo) -> Result<Self> {
        let artifacts = repo.read_artifacts()?;

        let existing_targets = repo
            .repo
            .targets()
            .signed
            .targets_iter()
            .map(|(name, _)| name.to_owned())
            .collect::<Vec<_>>();

        let editor = RepositoryEditor::from_repo(
            repo.repo_path
                .join("metadata")
                .join(format!("{}.root.json", repo.repo.root().signed.version)),
            repo.repo,
        )?;

        Ok(Self {
            editor,
            repo_path: repo.repo_path,
            artifacts,
            existing_targets,
        })
    }

    fn initialize(
        repo_path: Utf8PathBuf,
        root: SignedRole<Root>,
    ) -> Result<Self> {
        let metadata_dir = repo_path.join("metadata");
        let targets_dir = repo_path.join("targets");
        let root_path = metadata_dir
            .join(format!("{}.root.json", root.signed().signed.version));

        fs::create_dir_all(&metadata_dir)?;
        fs::create_dir_all(&targets_dir)?;
        fs::write(&root_path, root.buffer())?;

        let editor = RepositoryEditor::new(&root_path)?;

        Ok(Self {
            editor,
            repo_path,
            artifacts: ArtifactsDocument::default(),
            existing_targets: vec![],
        })
    }

    /// Adds a zone to the repository.
    ///
    /// If the name isn't specified, it is derived from the zone path by taking
    /// the file name and stripping the extension.
    pub fn add_artifact(&mut self, new_artifact: &AddArtifact) -> Result<()> {
        let filename = format!(
            "{}-{}.tar.gz",
            new_artifact.name(),
            new_artifact.version(),
        );

        // if we already have an artifact of this name/version/kind, replace it.
        if let Some(artifact) =
            self.artifacts.artifacts.iter_mut().find(|artifact| {
                artifact.name == new_artifact.name()
                    && artifact.version == new_artifact.version()
                    && artifact.kind == new_artifact.kind().into()
            })
        {
            self.editor.remove_target(&artifact.target.as_str().try_into()?)?;
            artifact.target = filename.clone();
        } else {
            // if we don't, make sure we're not overriding another target.
            if self.existing_targets.iter().any(|target_name| {
                target_name.raw() == filename
                    && target_name.resolved() == filename
            }) {
                bail!(
                    "a target named {} already exists in the repository",
                    filename
                );
            }
            self.artifacts.artifacts.push(Artifact {
                name: new_artifact.name().to_owned(),
                version: new_artifact.version().to_owned(),
                kind: new_artifact.kind().into(),
                target: filename.clone(),
            })
        }

        let targets_dir = self.repo_path.join("targets");

        let mut file = TargetWriter::new(&targets_dir, filename)?;
        std::io::copy(&mut File::open(new_artifact.path())?, &mut file)?;
        file.finish(&mut self.editor)?;

        Ok(())
    }

    /// Consumes self, signing the repository and writing out this repository to disk.
    pub fn sign_and_finish(
        mut self,
        keys: Vec<Key>,
        expiry: DateTime<Utc>,
    ) -> Result<()> {
        let targets_dir = self.repo_path.join("targets");

        let mut file = TargetWriter::new(&targets_dir, "artifacts.json")?;
        serde_json::to_writer_pretty(&mut file, &self.artifacts)?;
        file.finish(&mut self.editor)?;

        update_versions(&mut self.editor, expiry)?;

        let signed = self
            .editor
            .sign(&crate::key::boxed_keys(keys))
            .context("error signing keys")?;
        signed
            .write(self.repo_path.join("metadata"))
            .context("error writing repository")?;
        Ok(())
    }
}

fn update_versions(
    editor: &mut RepositoryEditor,
    expiry: DateTime<Utc>,
) -> Result<()> {
    let version = u64::try_from(Utc::now().timestamp())
        .and_then(NonZeroU64::try_from)
        .expect("bad epoch");
    editor.snapshot_version(version);
    editor.targets_version(version)?;
    editor.timestamp_version(version);
    editor.snapshot_expires(expiry);
    editor.targets_expires(expiry)?;
    editor.timestamp_expires(expiry);
    Ok(())
}
