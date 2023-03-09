// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{key::Key, target::TargetWriter, AddArtifact, ArchiveBuilder};
use anyhow::{anyhow, bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use fs_err::{self as fs, File};
use omicron_common::{
    api::external::SemverVersion,
    update::{Artifact, ArtifactsDocument},
};
use std::num::NonZeroU64;
use tough::{
    editor::{signed::SignedRole, RepositoryEditor},
    schema::{Root, Target},
    ExpirationEnforcement, Repository, RepositoryLoader, TargetName,
};
use url::Url;

/// A TUF repository describing Omicron.
pub struct OmicronRepo {
    log: slog::Logger,
    repo: Repository,
    repo_path: Utf8PathBuf,
}

impl OmicronRepo {
    /// Initializes a new repository at the given path, writing it to disk.
    pub fn initialize(
        log: &slog::Logger,
        repo_path: &Utf8Path,
        system_version: SemverVersion,
        keys: Vec<Key>,
        expiry: DateTime<Utc>,
    ) -> Result<Self> {
        let root = crate::root::new_root(keys.clone(), expiry)?;
        let editor = OmicronRepoEditor::initialize(
            repo_path.to_owned(),
            root,
            system_version,
        )?;

        editor
            .sign_and_finish(keys, expiry)
            .context("error signing new repository")?;

        Self::load(log, repo_path)
    }

    /// Loads a repository from the given path.
    ///
    /// This method enforces expirations. To load without expiration enforcement, use
    /// [`Self::load_ignore_expiration`].
    pub fn load(log: &slog::Logger, repo_path: &Utf8Path) -> Result<Self> {
        Self::load_impl(log, repo_path, ExpirationEnforcement::Safe)
    }

    /// Loads a repository from the given path, ignoring expiration.
    ///
    /// Use cases for this include:
    ///
    /// 1. When you're editing an existing repository and will re-sign it afterwards.
    /// 2. In an environment in which time isn't available.
    pub fn load_ignore_expiration(
        log: &slog::Logger,
        repo_path: &Utf8Path,
    ) -> Result<Self> {
        Self::load_impl(log, repo_path, ExpirationEnforcement::Unsafe)
    }

    fn load_impl(
        log: &slog::Logger,
        repo_path: &Utf8Path,
        exp: ExpirationEnforcement,
    ) -> Result<Self> {
        let log = log.new(slog::o!("component" => "OmicronRepo"));
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

        Ok(Self { log, repo, repo_path })
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

    /// Archives the repository to the given path as a zip file.
    ///
    /// ## Why zip and not tar?
    ///
    /// The main reason is that zip supports random access to files and tar does
    /// not.
    ///
    /// In principle it should be possible to read the repository out of a zip
    /// file from memory, but we ran into [this
    /// issue](https://github.com/awslabs/tough/pull/563) while implementing it.
    /// Once that is resolved (or we write our own TUF crate) it should be
    /// possible to do that.
    ///
    /// Regardless of this roadblock, we don't want to foreclose that option
    /// forever, so this code uses zip rather than having to deal with a
    /// migration in the future.
    pub fn archive(&self, output_path: &Utf8Path) -> Result<()> {
        let mut builder = ArchiveBuilder::new(output_path.to_owned())?;

        let metadata_dir = self.repo_path.join("metadata");

        // Gather metadata files.
        for entry in metadata_dir.read_dir_utf8().with_context(|| {
            format!("error reading entries from {metadata_dir}")
        })? {
            let entry =
                entry.context("error reading entry from {metadata_dir}")?;
            let file_name = entry.file_name();
            if file_name.ends_with(".root.json")
                || file_name == "timestamp.json"
                || file_name.ends_with(".snapshot.json")
                || file_name.ends_with(".targets.json")
            {
                // This is a valid metadata file.
                builder.write_file(
                    entry.path(),
                    &Utf8Path::new("metadata").join(file_name),
                )?;
            }
        }

        let targets_dir = self.repo_path.join("targets");

        // Gather all targets.
        for (name, target) in self.repo.targets().signed.targets_iter() {
            let target_filename = self.target_filename(target, name);
            let target_path = targets_dir.join(&target_filename);
            slog::trace!(self.log, "adding {} to archive", name.resolved());
            builder.write_file(
                &target_path,
                &Utf8Path::new("targets").join(&target_filename),
            )?;
        }

        builder.finish()?;

        Ok(())
    }

    /// Converts `self` into an `OmicronRepoEditor`, which can be used to perform
    /// modifications to the repository.
    pub fn into_editor(self) -> Result<OmicronRepoEditor> {
        OmicronRepoEditor::new(self)
    }

    /// Prepends the target digest to the name if using consistent snapshots. Returns both the
    /// digest and the filename.
    ///
    /// Adapted from tough's source.
    fn target_filename(&self, target: &Target, name: &TargetName) -> String {
        let sha256 = &target.hashes.sha256.clone().into_vec();
        if self.repo.root().signed.consistent_snapshot {
            format!("{}.{}", hex::encode(sha256), name.resolved())
        } else {
            name.resolved().to_owned()
        }
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
        system_version: SemverVersion,
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
            artifacts: ArtifactsDocument::empty(system_version),
            existing_targets: vec![],
        })
    }

    /// Adds an artifact to the repository.
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
                    && &artifact.version == new_artifact.version()
                    && artifact.kind == new_artifact.kind().clone()
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
                kind: new_artifact.kind().clone(),
                target: filename.clone(),
            })
        }

        let targets_dir = self.repo_path.join("targets");

        let mut file = TargetWriter::new(&targets_dir, filename.clone())?;
        new_artifact
            .write_to(&mut file)
            .with_context(|| format!("error writing artifact `{filename}"))?;
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
