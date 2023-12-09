// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino_tempfile::Utf8TempDir;
use debug_ignore::DebugIgnore;
use omicron_common::update::ArtifactHash;
use omicron_common::update::ArtifactHashId;
use omicron_common::update::ArtifactId;
use slog::info;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use tough::TargetName;
use tufaceous_lib::ArchiveExtractor;
use tufaceous_lib::OmicronRepo;
use update_common::artifacts::ExtractedArtifactDataHandle;
use update_common::artifacts::UpdatePlan;
use update_common::artifacts::UpdatePlanBuilder;
use update_common::errors::RepositoryError;

/// A collection of artifacts along with an update plan using those artifacts.
#[derive(Debug)]
pub(super) struct ArtifactsWithPlan {
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

    // The plan to use to update a component within the rack.
    plan: UpdatePlan,
}

impl ArtifactsWithPlan {
    pub(super) async fn from_zip<T>(
        zip_data: T,
        log: &Logger,
    ) -> Result<Self, RepositoryError>
    where
        T: io::Read + io::Seek,
    {
        // Create a temporary directory to hold the extracted TUF repository.
        let dir = unzip_into_tempdir(zip_data, log)?;

        // Time is unavailable during initial setup, so ignore expiration. Even
        // if time were available, we might want to be able to load older
        // versions of artifacts over the technician port in an emergency.
        //
        // XXX we aren't checking against a root of trust at this point --
        // anyone can sign the repositories and this code will accept that.
        let repository =
            OmicronRepo::load_untrusted_ignore_expiration(log, dir.path())
                .await
                .map_err(RepositoryError::LoadRepository)?;

        let artifacts = repository
            .read_artifacts()
            .await
            .map_err(RepositoryError::ReadArtifactsDocument)?;

        // Create another temporary directory where we'll "permanently" (as long
        // as this plan is in use) hold extracted artifacts we need; most of
        // these are just direct copies of artifacts we just unpacked into
        // `dir`, but we'll also unpack nested artifacts like the RoT dual A/B
        // archives.
        let mut builder =
            UpdatePlanBuilder::new(artifacts.system_version, log)?;

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

        let mut by_id = BTreeMap::new();
        let mut by_hash = HashMap::new();
        for artifact in artifacts.artifacts {
            let target_name = TargetName::try_from(artifact.target.as_str())
                .map_err(|error| RepositoryError::LocateTarget {
                    target: artifact.target.clone(),
                    error: Box::new(error),
                })?;

            let target_hash = repository
                .repo()
                .targets()
                .signed
                .find_target(&target_name)
                .map_err(|error| RepositoryError::TargetHashRead {
                    target: artifact.target.clone(),
                    error,
                })?
                .hashes
                .sha256
                .clone()
                .into_vec();

            // The target hash is SHA-256, which is 32 bytes long.
            let artifact_hash = ArtifactHash(
                target_hash
                    .try_into()
                    .map_err(RepositoryError::TargetHashLength)?,
            );

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
                .add_artifact(
                    artifact.into_id(),
                    artifact_hash,
                    stream,
                    &mut by_id,
                    &mut by_hash,
                )
                .await?;
        }

        // Ensure we know how to apply updates from this set of artifacts; we'll
        // remember the plan we create.
        let artifacts = builder.build()?;

        Ok(Self { by_id, by_hash: by_hash.into(), plan: artifacts })
    }

    pub(super) fn by_id(&self) -> &BTreeMap<ArtifactId, Vec<ArtifactHashId>> {
        &self.by_id
    }

    #[cfg(test)]
    pub(super) fn by_hash(
        &self,
    ) -> &HashMap<ArtifactHashId, ExtractedArtifactDataHandle> {
        &self.by_hash
    }

    pub(super) fn plan(&self) -> &UpdatePlan {
        &self.plan
    }

    pub(super) fn get_by_hash(
        &self,
        id: &ArtifactHashId,
    ) -> Option<ExtractedArtifactDataHandle> {
        self.by_hash.get(id).cloned()
    }
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
    use camino_tempfile::Utf8TempDir;
    use clap::Parser;
    use omicron_common::{
        api::internal::nexus::KnownArtifactKind, update::ArtifactKind,
    };
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::BTreeSet;

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
        let args = tufaceous::Args::try_parse_from([
            "tufaceous",
            "assemble",
            "../tufaceous/manifests/fake.toml",
            archive_path.as_str(),
        ])
        .context("error parsing args")?;

        args.exec(&logctx.log)
            .await
            .context("error executing assemble command")?;

        // Now check that it can be read by the archive extractor.
        let zip_bytes = std::fs::File::open(&archive_path)
            .context("error opening archive.zip")?;
        let plan = ArtifactsWithPlan::from_zip(zip_bytes, &logctx.log)
            .await
            .context("error reading archive.zip")?;
        // Check that all known artifact kinds are present in the map.
        let by_id_kinds: BTreeSet<_> =
            plan.by_id().keys().map(|id| id.kind.clone()).collect();
        let by_hash_kinds: BTreeSet<_> =
            plan.by_hash().keys().map(|id| id.kind.clone()).collect();

        // `by_id` should contain one entry for every `KnownArtifactKind`...
        let mut expected_kinds: BTreeSet<_> =
            KnownArtifactKind::iter().map(ArtifactKind::from).collect();
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

        logctx.cleanup_successful();

        Ok(())
    }
}
