// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ExtractedArtifactDataHandle;
use super::UpdatePlan;
use super::UpdatePlanBuildOutput;
use super::UpdatePlanBuilder;
use crate::errors::RepositoryError;
use anyhow::anyhow;
use bytes::Bytes;
use camino_tempfile::Utf8TempDir;
use debug_ignore::DebugIgnore;
use dropshot::HttpError;
use futures::Stream;
use futures::TryStreamExt;
use omicron_common::api::external::TufRepoDescription;
use omicron_common::api::external::TufRepoMeta;
use omicron_common::update::ArtifactId;
use sha2::{Digest, Sha256};
use slog::Logger;
use slog::info;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use tokio::io::AsyncWriteExt;
use tough::TargetName;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactHashId;
use tufaceous_lib::ArchiveExtractor;
use tufaceous_lib::OmicronRepo;

/// A collection of artifacts along with an update plan using those artifacts.
#[derive(Debug)]
pub struct ArtifactsWithPlan {
    // A description of this repository.
    description: TufRepoDescription,

    // Map of top-level artifact IDs (present in the TUF repo) to the actual
    // artifacts we're serving (e.g., a top-level RoT artifact will map to two
    // artifact hashes: one for each of the A and B images).
    //
    // The sum of the lengths of the values of this map will match the number of
    // entries in `by_hash`.
    by_id: BTreeMap<ArtifactId, Vec<ArtifactHashId>>,

    // Map of the artifact hashes (and their associated kind) that we extracted
    // from a TUF repository to a handle to the data of that artifact.
    //
    // An example of the difference between `by_id` and `by_hash`: An uploaded
    // TUF repository will contain an artifact for the host OS (i.e.,
    // `KnownArtifactKind::Host`). On ingest, we will unpack that artifact into
    // the parts it contains: a phase 1 image (`ArtifactKind::HOST_PHASE_1`) and
    // a phase 2 image (`ArtifactKind::HOST_PHASE_2`). We will hash each of
    // those images and store them in a temporary directory. `by_id` will
    // contain a single entry mapping the original TUF repository artifact ID
    // to the two `ArtifactHashId`s extracted from that artifact, and `by_hash`
    // will contain two entries mapping each of the images to their data.
    by_hash: DebugIgnore<HashMap<ArtifactHashId, ExtractedArtifactDataHandle>>,

    // Map from Rot artifact IDs to hash of signing information. This is
    // used to select between different artifact versions in the same
    // repository
    rot_by_sign: DebugIgnore<HashMap<ArtifactId, Vec<u8>>>,
    // The plan to use to update a component within the rack.
    plan: UpdatePlan,
}

impl ArtifactsWithPlan {
    /// Creates a new `ArtifactsWithPlan` from the given stream of `Bytes`.
    ///
    /// This method reads the stream representing a TUF repo, and writes it to
    /// a temporary file. Afterwards, it builds an `ArtifactsWithPlan` from the
    /// contents of that file.
    pub async fn from_stream(
        body: impl Stream<Item = Result<Bytes, HttpError>> + Send,
        file_name: Option<String>,
        zone_mode: ControlPlaneZonesMode,
        verification_mode: VerificationMode<'_>,
        log: &Logger,
    ) -> Result<Self, RepositoryError> {
        // Create a temporary file to store the incoming archive.``
        let tempfile = tokio::task::spawn_blocking(|| {
            camino_tempfile::tempfile().map_err(RepositoryError::TempFileCreate)
        })
        .await
        .unwrap()?;
        let mut tempfile =
            tokio::io::BufWriter::new(tokio::fs::File::from_std(tempfile));

        let mut body = std::pin::pin!(body);

        // Stream the uploaded body into our tempfile.
        let mut hasher = Sha256::new();
        while let Some(bytes) = body
            .try_next()
            .await
            .map_err(RepositoryError::ReadChunkFromStream)?
        {
            hasher.update(&bytes);
            tempfile
                .write_all(&bytes)
                .await
                .map_err(RepositoryError::TempFileWrite)?;
        }

        let repo_hash = ArtifactHash(hasher.finalize().into());

        // Flush writes. We don't need to seek back to the beginning of the file
        // because extracting the repository will do its own seeking as a part of
        // unzipping this repo.
        tempfile.flush().await.map_err(RepositoryError::TempFileFlush)?;

        let tempfile = tempfile.into_inner().into_std().await;

        let artifacts_with_plan = Self::from_zip(
            io::BufReader::new(tempfile),
            file_name,
            repo_hash,
            zone_mode,
            verification_mode,
            log,
        )
        .await?;

        Ok(artifacts_with_plan)
    }

    pub async fn from_zip<T>(
        zip_data: T,
        file_name: Option<String>,
        repo_hash: ArtifactHash,
        zone_mode: ControlPlaneZonesMode,
        verification_mode: VerificationMode<'_>,
        log: &Logger,
    ) -> Result<Self, RepositoryError>
    where
        T: io::Read + io::Seek + Send + 'static,
    {
        // Create a temporary directory to hold the extracted TUF repository.
        let dir = {
            let log = log.clone();
            tokio::task::spawn_blocking(move || {
                // This is an expensive synchronous method, so run it on the
                // blocking thread pool.
                //
                // TODO: at the moment we don't restrict the size of the
                // extracted contents or its memory usage, making it
                // susceptible to zip bombs and other related attacks.
                // https://github.com/zip-rs/zip/issues/228. We need to think
                // about this at some point.
                unzip_into_tempdir(zip_data, &log)
            })
            .await
            .map_err(|join_error| {
                RepositoryError::Extract(
                    anyhow!(join_error).context("unzip_into_tempdir panicked"),
                )
            })??
        };

        // We want validly-signed zip archives to always work even if the
        // signatures are expired, even when the system time is correct. (If we
        // eventually load TUF repositories over HTTP, the system should enforce
        // signature expiration.)
        let repository = match verification_mode {
            VerificationMode::TrustStore(trusted_roots) => {
                OmicronRepo::load_ignore_expiration(
                    log,
                    dir.path(),
                    trusted_roots,
                )
                .await
                .map_err(RepositoryError::LoadRepository)?
            }
            VerificationMode::BlindlyTrustAnything => {
                OmicronRepo::load_untrusted_ignore_expiration(log, dir.path())
                    .await
                    .map_err(RepositoryError::LoadRepository)?
            }
        };

        let artifacts = repository
            .read_artifacts()
            .await
            .map_err(RepositoryError::ReadArtifactsDocument)?;

        // Create another temporary directory where we'll "permanently" (as long
        // as this plan is in use) hold extracted artifacts we need; most of
        // these are just direct copies of artifacts we just unpacked into
        // `dir`, but we'll also unpack nested artifacts like the RoT dual A/B
        // archives.
        let mut builder = UpdatePlanBuilder::new(
            artifacts.system_version.clone(),
            zone_mode,
            log,
        )?;

        // Make a pass through each artifact in the repo. For each artifact, we
        // do one of the following:
        //
        // 1. Ignore it (if it's of an artifact kind we don't understand)
        // 2. Add it directly to tempdir managed by `extracted_artifacts`; we'll
        //    keep a handle to any such file and use it later. (SP images fall
        //    into this category.)
        // 3. Unpack its contents and copy inner artifacts into the tempdir
        //    managed by `extracted_artifacts`. (RoT artifacts and OS images
        //    fall into this category: RoT artifacts themselves contain A and B
        //    hubris archives, and OS images artifacts contain separate phase1
        //    and phase2 blobs.)
        //
        // For artifacts in case 2, we should be able to move the file instead
        // of copying it, but the source paths aren't exposed by `repository`.
        // The only artifacts that fall into this category are SP images, which
        // are not very large, so fixing this is not a particularly high
        // priority - copying small SP artifacts is neglible compared to the
        // work we do to unpack host OS images.

        for artifact in &artifacts.artifacts {
            let target_name = TargetName::try_from(artifact.target.as_str())
                .map_err(|error| RepositoryError::LocateTarget {
                    target: artifact.target.clone(),
                    error: Box::new(error),
                })?;

            let artifact_hash =
                lookup_artifact_hash(&repository, artifact, &target_name)?;

            let stream = repository
                .repo()
                .read_target(&target_name)
                .await
                .map_err(|error| RepositoryError::LocateTarget {
                    target: artifact.target.clone(),
                    error: Box::new(error),
                })?
                .ok_or_else(|| {
                    RepositoryError::MissingTarget(artifact.target.clone())
                })?;

            builder
                .add_artifact(artifact.clone().into(), artifact_hash, stream)
                .await?;
        }

        // Ensure we know how to apply updates from this set of artifacts; we'll
        // remember the plan we create.
        let UpdatePlanBuildOutput {
            plan,
            by_id,
            by_hash,
            rot_by_sign,
            artifacts_meta,
        } = builder.build()?;

        let tuf_repository = repository.repo();

        let file_name = file_name.unwrap_or_else(|| {
            // Just pick a reasonable-sounding file name if we don't have one.
            format!("system-update-v{}.zip", artifacts.system_version)
        });

        let repo_meta = TufRepoMeta {
            hash: repo_hash,
            targets_role_version: tuf_repository.targets().signed.version.get(),
            valid_until: tuf_repository
                .root()
                .signed
                .expires
                .min(tuf_repository.snapshot().signed.expires)
                .min(tuf_repository.targets().signed.expires)
                .min(tuf_repository.timestamp().signed.expires),
            system_version: artifacts.system_version,
            file_name,
        };
        let description =
            TufRepoDescription { repo: repo_meta, artifacts: artifacts_meta };

        Ok(Self {
            description,
            by_id,
            by_hash: by_hash.into(),
            rot_by_sign: rot_by_sign.into(),
            plan,
        })
    }

    /// Returns the `ArtifactsDocument` corresponding to this TUF repo.
    pub fn description(&self) -> &TufRepoDescription {
        &self.description
    }

    pub fn by_id(&self) -> &BTreeMap<ArtifactId, Vec<ArtifactHashId>> {
        &self.by_id
    }

    #[cfg(test)]
    pub(super) fn by_hash(
        &self,
    ) -> &HashMap<ArtifactHashId, ExtractedArtifactDataHandle> {
        &self.by_hash
    }

    pub fn plan(&self) -> &UpdatePlan {
        &self.plan
    }

    pub fn rot_by_sign(&self) -> &HashMap<ArtifactId, Vec<u8>> {
        &self.rot_by_sign
    }

    pub fn get_by_hash(
        &self,
        id: &ArtifactHashId,
    ) -> Option<ExtractedArtifactDataHandle> {
        self.by_hash.get(id).cloned()
    }
}

fn lookup_artifact_hash(
    repository: &OmicronRepo,
    artifact: &tufaceous_artifact::Artifact,
    target_name: &TargetName,
) -> Result<ArtifactHash, RepositoryError> {
    let target_hash = repository
        .repo()
        .targets()
        .signed
        .find_target(target_name, false)
        .map_err(|error| RepositoryError::TargetHashRead {
            target: artifact.target.clone(),
            error: Box::new(error),
        })?
        .hashes
        .sha256
        .clone()
        .into_vec();
    let artifact_hash = ArtifactHash(
        target_hash.try_into().map_err(RepositoryError::TargetHashLength)?,
    );
    Ok(artifact_hash)
}

#[derive(Debug, Clone, Copy)]
pub enum ControlPlaneZonesMode {
    /// Ensure the control plane zones are combined into a single composite
    /// `ControlPlane` artifact, used by Wicket.
    Composite,
    /// Ensure the control plane zones are individual `Zone` artifacts, used
    /// by Nexus.
    Split,
}

#[derive(Debug, Clone, Copy)]
pub enum VerificationMode<'a> {
    /// Verify the uploaded repository is accepted by one of these trusted root
    /// roles.
    TrustStore(&'a [Vec<u8>]),
    /// Blindly trust the root role present in the repository (but still perform
    /// signature checks using that root role).
    BlindlyTrustAnything,
}

fn unzip_into_tempdir<T>(
    zip_data: T,
    log: &Logger,
) -> Result<Utf8TempDir, RepositoryError>
where
    T: io::Read + io::Seek,
{
    let mut extractor = ArchiveExtractor::new(zip_data)
        .map_err(RepositoryError::OpenArchive)?;

    let dir =
        camino_tempfile::tempdir().map_err(RepositoryError::TempDirCreate)?;

    info!(log, "extracting uploaded archive to {}", dir.path());

    // XXX: might be worth differentiating between server-side issues (503)
    // and issues with the uploaded archive (400).
    extractor.extract(dir.path()).map_err(RepositoryError::Extract)?;

    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context, Result};
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use clap::Parser;
    use omicron_test_utils::dev::test_setup_log;
    use std::{collections::BTreeSet, time::Duration};
    use tufaceous_artifact::{ArtifactKind, KnownArtifactKind};

    /// Test that `ArtifactsWithPlan` can extract the fake repository generated
    /// by tufaceous.
    ///
    /// See documentation for extract_nested_artifact_pair in update_plan.rs
    /// for why multi_thread is required.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_extract_fake() -> Result<()> {
        let logctx = test_setup_log("test_extract_fake");
        let temp_dir = Utf8TempDir::new()?;
        let archive_path = temp_dir.path().join("archive.zip");

        // Create the archive.
        create_fake_archive(&logctx.log, &archive_path).await?;

        // Now check that it can be read by the archive extractor.
        let plan = build_artifacts_with_plan(
            &logctx.log,
            &archive_path,
            ControlPlaneZonesMode::Composite,
        )
        .await?;
        // Check that all known artifact kinds are present in the map.
        let by_id_kinds: BTreeSet<_> =
            plan.by_id().keys().map(|id| id.kind.clone()).collect();
        let by_hash_kinds: BTreeSet<_> =
            plan.by_hash().keys().map(|id| id.kind.clone()).collect();
        let artifact_meta_kinds: BTreeSet<_> = plan
            .description
            .artifacts
            .iter()
            .map(|meta| meta.id.kind.clone())
            .collect();

        // `by_id` should contain one entry for every `KnownArtifactKind`
        // (except `Zone`), as well as `installinator_document`.
        let mut expected_kinds: BTreeSet<_> = KnownArtifactKind::iter()
            .filter(|k| !matches!(k, KnownArtifactKind::Zone))
            .map(ArtifactKind::from)
            .chain(std::iter::once(ArtifactKind::INSTALLINATOR_DOCUMENT))
            .collect();
        assert_eq!(
            expected_kinds, by_id_kinds,
            "expected kinds match by_id kinds"
        );

        // ... but `by_hash` should replace the artifacts that contain nested
        // artifacts to be expanded into their inner parts (phase1/phase2 for OS
        // images and A/B images for the RoT) during import.
        for remove in [
            KnownArtifactKind::Host,
            KnownArtifactKind::Trampoline,
            KnownArtifactKind::GimletRot,
            KnownArtifactKind::PscRot,
            KnownArtifactKind::SwitchRot,
        ] {
            assert!(expected_kinds.remove(&remove.into()));
        }
        for add in [
            ArtifactKind::HOST_PHASE_1,
            ArtifactKind::HOST_PHASE_2,
            ArtifactKind::TRAMPOLINE_PHASE_1,
            ArtifactKind::TRAMPOLINE_PHASE_2,
            ArtifactKind::GIMLET_ROT_IMAGE_A,
            ArtifactKind::GIMLET_ROT_IMAGE_B,
            ArtifactKind::PSC_ROT_IMAGE_A,
            ArtifactKind::PSC_ROT_IMAGE_B,
            ArtifactKind::SWITCH_ROT_IMAGE_A,
            ArtifactKind::SWITCH_ROT_IMAGE_B,
        ] {
            assert!(expected_kinds.insert(add));
        }
        assert_eq!(
            expected_kinds, by_hash_kinds,
            "expected kinds match by_hash kinds"
        );
        assert_eq!(
            expected_kinds, artifact_meta_kinds,
            "expected kinds match artifact_meta kinds"
        );

        // Every value present in `by_id` should also be a key in `by_hash`.
        for (id, hash_ids) in plan.by_id() {
            for hash_id in hash_ids {
                assert!(
                    plan.by_hash().contains_key(hash_id),
                    "plan.by_hash is missing an entry for \
                     {hash_id:?} (derived from {id:?})"
                );
            }
        }

        //

        logctx.cleanup_successful();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_extract_fake_split() -> Result<()> {
        let logctx = test_setup_log("test_extract_fake");
        let temp_dir = Utf8TempDir::new()?;
        let archive_path = temp_dir.path().join("archive.zip");

        // Create the archive.
        create_fake_archive(&logctx.log, &archive_path).await?;

        // Now check that it can be read by the archive extractor in split mode.
        let plan = build_artifacts_with_plan(
            &logctx.log,
            &archive_path,
            ControlPlaneZonesMode::Split,
        )
        .await?;

        // `by_id` should contain one entry for every `KnownArtifactKind`
        // (except `Zone`), as well as `installinator_document`.
        let by_id_kinds: BTreeSet<_> =
            plan.by_id().keys().map(|id| id.kind.clone()).collect();
        let expected_kinds: BTreeSet<_> = KnownArtifactKind::iter()
            .filter(|k| !matches!(k, KnownArtifactKind::ControlPlane))
            .map(ArtifactKind::from)
            .chain(std::iter::once(ArtifactKind::INSTALLINATOR_DOCUMENT))
            .collect();
        assert_eq!(
            expected_kinds, by_id_kinds,
            "expected kinds match by_id kinds"
        );

        logctx.cleanup_successful();

        Ok(())
    }

    /// Test that the archive generated by running `tufaceous assemble` twice
    /// has the same artifacts and hashes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fake_archive_idempotent() -> Result<()> {
        let logctx = test_setup_log("test_fake_archive_idempotent");
        let temp_dir = Utf8TempDir::new()?;
        let archive_path = temp_dir.path().join("archive1.zip");

        // Create the archive and build a plan from it.
        create_fake_archive(&logctx.log, &archive_path).await?;
        let mut plan1 = build_artifacts_with_plan(
            &logctx.log,
            &archive_path,
            ControlPlaneZonesMode::Composite,
        )
        .await?;

        // Add a 2 second delay to ensure that if we bake any second-based
        // timestamps in, that they end up being different from those in the
        // first archive.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let archive2_path = temp_dir.path().join("archive2.zip");
        create_fake_archive(&logctx.log, &archive2_path).await?;
        let mut plan2 = build_artifacts_with_plan(
            &logctx.log,
            &archive2_path,
            ControlPlaneZonesMode::Composite,
        )
        .await?;

        // At the moment, the repo .zip itself doesn't match because it bakes
        // in timestamps. However, the artifacts inside should match exactly.
        plan1.description.sort_artifacts();
        plan2.description.sort_artifacts();

        assert_eq!(
            plan1.description.artifacts, plan2.description.artifacts,
            "artifacts match"
        );

        logctx.cleanup_successful();

        Ok(())
    }

    async fn create_fake_archive(
        log: &slog::Logger,
        archive_path: &Utf8Path,
    ) -> Result<()> {
        let args = tufaceous::Args::try_parse_from([
            "tufaceous",
            "assemble",
            "manifests/fake.toml",
            archive_path.as_str(),
        ])
        .context("error parsing args")?;

        args.exec(log).await.context("error executing assemble command")?;

        Ok(())
    }

    async fn build_artifacts_with_plan(
        log: &slog::Logger,
        archive_path: &Utf8Path,
        zone_mode: ControlPlaneZonesMode,
    ) -> Result<ArtifactsWithPlan> {
        let zip_bytes = std::fs::File::open(&archive_path)
            .context("error opening archive.zip")?;
        // We could also compute the hash from the file here, but the repo hash
        // doesn't matter for the test.
        let repo_hash = ArtifactHash([0u8; 32]);
        let plan = ArtifactsWithPlan::from_zip(
            zip_bytes,
            None,
            repo_hash,
            zone_mode,
            VerificationMode::BlindlyTrustAnything,
            log,
        )
        .await
        .with_context(|| format!("error reading {archive_path}"))?;

        Ok(plan)
    }
}
