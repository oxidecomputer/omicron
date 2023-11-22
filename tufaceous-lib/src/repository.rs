// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{key::Key, target::TargetWriter, AddArtifact, ArchiveBuilder};
use anyhow::{anyhow, bail, Context, Result};
use buf_list::BufList;
use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use fs_err::{self as fs};
use futures::TryStreamExt;
use omicron_common::{
    api::external::SemverVersion,
    update::{Artifact, ArtifactsDocument},
};
use std::{collections::BTreeSet, num::NonZeroU64};
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
    pub async fn initialize(
        log: &slog::Logger,
        repo_path: &Utf8Path,
        system_version: SemverVersion,
        keys: Vec<Key>,
        expiry: DateTime<Utc>,
    ) -> Result<Self> {
        let root = crate::root::new_root(keys.clone(), expiry).await?;
        let editor = OmicronRepoEditor::initialize(
            repo_path.to_owned(),
            root,
            system_version,
        )
        .await?;

        editor
            .sign_and_finish(keys, expiry)
            .await
            .context("error signing new repository")?;

        // In theory we "trust" the key we just used to sign this repository,
        // but the code path is equivalent to `load_untrusted`.
        Self::load_untrusted(log, repo_path).await
    }

    /// Loads a repository from the given path.
    ///
    /// This method enforces expirations. To load without expiration enforcement, use
    /// [`Self::load_untrusted_ignore_expiration`].
    pub async fn load_untrusted(
        log: &slog::Logger,
        repo_path: &Utf8Path,
    ) -> Result<Self> {
        Self::load_untrusted_impl(log, repo_path, ExpirationEnforcement::Safe)
            .await
    }

    /// Loads a repository from the given path, ignoring expiration.
    ///
    /// Use cases for this include:
    ///
    /// 1. When you're editing an existing repository and will re-sign it afterwards.
    /// 2. In an environment in which time isn't available.
    pub async fn load_untrusted_ignore_expiration(
        log: &slog::Logger,
        repo_path: &Utf8Path,
    ) -> Result<Self> {
        Self::load_untrusted_impl(log, repo_path, ExpirationEnforcement::Unsafe)
            .await
    }

    async fn load_untrusted_impl(
        log: &slog::Logger,
        repo_path: &Utf8Path,
        exp: ExpirationEnforcement,
    ) -> Result<Self> {
        let log = log.new(slog::o!("component" => "OmicronRepo"));
        let repo_path = repo_path.canonicalize_utf8()?;
        let root_json = repo_path.join("metadata").join("1.root.json");
        let root = tokio::fs::read(&root_json)
            .await
            .with_context(|| format!("error reading from {root_json}"))?;

        let repo = RepositoryLoader::new(
            &root,
            Url::from_file_path(repo_path.join("metadata"))
                .expect("the canonical path is not absolute?"),
            Url::from_file_path(repo_path.join("targets"))
                .expect("the canonical path is not absolute?"),
        )
        .expiration_enforcement(exp)
        .load()
        .await?;

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
    pub async fn read_artifacts(&self) -> Result<ArtifactsDocument> {
        let reader = self
            .repo
            .read_target(&"artifacts.json".try_into()?)
            .await?
            .ok_or_else(|| anyhow!("artifacts.json should be present"))?;
        let buf_list = reader
            .try_collect::<BufList>()
            .await
            .context("error reading from artifacts.json")?;
        serde_json::from_reader(buf_list::Cursor::new(&buf_list))
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
    pub async fn into_editor(self) -> Result<OmicronRepoEditor> {
        OmicronRepoEditor::new(self).await
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

    // Set of `TargetName::resolved()` names for every target that existed when
    // the repo was opened. We use this to ensure we don't overwrite an existing
    // target when adding new artifacts.
    existing_target_names: BTreeSet<String>,
}

impl OmicronRepoEditor {
    async fn new(repo: OmicronRepo) -> Result<Self> {
        let artifacts = repo.read_artifacts().await?;

        let existing_target_names = repo
            .repo
            .targets()
            .signed
            .targets_iter()
            .map(|(name, _)| name.resolved().to_string())
            .collect::<BTreeSet<_>>();

        let editor = RepositoryEditor::from_repo(
            repo.repo_path
                .join("metadata")
                .join(format!("{}.root.json", repo.repo.root().signed.version)),
            repo.repo,
        )
        .await?;

        Ok(Self {
            editor,
            repo_path: repo.repo_path,
            artifacts,
            existing_target_names,
        })
    }

    async fn initialize(
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

        let editor = RepositoryEditor::new(&root_path).await?;

        Ok(Self {
            editor,
            repo_path,
            artifacts: ArtifactsDocument::empty(system_version),
            existing_target_names: BTreeSet::new(),
        })
    }

    /// Adds an artifact to the repository.
    pub fn add_artifact(&mut self, new_artifact: &AddArtifact) -> Result<()> {
        let target_name = format!(
            "{}-{}-{}.tar.gz",
            new_artifact.kind(),
            new_artifact.name(),
            new_artifact.version(),
        );

        // make sure we're not overwriting an existing target (either one that
        // existed when we opened the repo, or one that's been added via this
        // method)
        if !self.existing_target_names.insert(target_name.clone()) {
            bail!(
                "a target named {target_name} already exists in the repository",
            );
        }

        self.artifacts.artifacts.push(Artifact {
            name: new_artifact.name().to_owned(),
            version: new_artifact.version().to_owned(),
            kind: new_artifact.kind().clone(),
            target: target_name.clone(),
        });

        let targets_dir = self.repo_path.join("targets");

        let mut file = TargetWriter::new(&targets_dir, target_name.clone())?;
        new_artifact.write_to(&mut file).with_context(|| {
            format!("error writing artifact `{target_name}")
        })?;
        file.finish(&mut self.editor)?;

        Ok(())
    }

    /// Consumes self, signing the repository and writing out this repository to disk.
    pub async fn sign_and_finish(
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
            .await
            .context("error signing keys")?;
        signed
            .write(self.repo_path.join("metadata"))
            .await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArtifactSource;
    use buf_list::BufList;
    use camino_tempfile::Utf8TempDir;
    use chrono::Days;
    use omicron_test_utils::dev::test_setup_log;

    #[tokio::test]
    async fn reject_artifacts_with_the_same_filename() {
        let logctx = test_setup_log("reject_artifacts_with_the_same_filename");
        let tempdir = Utf8TempDir::new().unwrap();
        let mut repo = OmicronRepo::initialize(
            &logctx.log,
            tempdir.path(),
            "0.0.0".parse().unwrap(),
            vec![Key::generate_ed25519()],
            Utc::now() + Days::new(1),
        )
        .await
        .unwrap()
        .into_editor()
        .await
        .unwrap();

        // Targets are uniquely identified by their kind/name/version triple;
        // trying to add two artifacts with identical triples should fail.
        let kind = "test-kind";
        let name = "test-artifact-name";
        let version = "1.0.0";

        repo.add_artifact(&AddArtifact::new(
            kind.parse().unwrap(),
            name.to_string(),
            version.parse().unwrap(),
            ArtifactSource::Memory(BufList::new()),
        ))
        .unwrap();

        let err = repo
            .add_artifact(&AddArtifact::new(
                kind.parse().unwrap(),
                name.to_string(),
                version.parse().unwrap(),
                ArtifactSource::Memory(BufList::new()),
            ))
            .unwrap_err()
            .to_string();

        assert!(err.contains("a target named"));
        assert!(err.contains(kind));
        assert!(err.contains(name));
        assert!(err.contains(version));
        assert!(err.contains("already exists"));

        logctx.cleanup_successful();
    }
}
