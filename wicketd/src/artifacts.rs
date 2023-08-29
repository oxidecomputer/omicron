// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    io, mem,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use camino::Utf8PathBuf;
use camino_tempfile::Utf8TempDir;
use debug_ignore::DebugIgnore;
use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use hyper::Body;
use installinator_artifactd::{ArtifactGetter, EventReportStatus};
use omicron_common::api::{
    external::SemverVersion, internal::nexus::KnownArtifactKind,
};
use omicron_common::update::{
    ArtifactHash, ArtifactHashId, ArtifactId, ArtifactKind,
};
use slog::{error, info, Logger};
use thiserror::Error;
use tough::TargetName;
use tufaceous_lib::{ArchiveExtractor, OmicronRepo};
use uuid::Uuid;

use crate::{
    http_entrypoints::InstallableArtifacts,
    installinator_progress::IprArtifactServer,
};

mod extracted_artifacts;
mod update_plan;

pub(crate) use self::extracted_artifacts::ExtractedArtifactDataHandle;
use self::update_plan::UpdatePlanBuilder;

// A collection of artifacts along with an update plan using those artifacts.
#[derive(Debug)]
struct ArtifactsWithPlan {
    // Map of top-level artifact IDs (present in the TUF repo) to the actual
    // artifacts we're serving (e.g., a top-level RoT artifact will map to two
    // artifact hashes: one for each of the A and B images).
    //
    // The sum of the lengths of the values of this map will match the number of
    // entries in `by_hash`.
    by_id: BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
    by_hash: DebugIgnore<HashMap<ArtifactHashId, ExtractedArtifactDataHandle>>,
    plan: UpdatePlan,
}

/// The artifact server interface for wicketd.
#[derive(Debug)]
pub(crate) struct WicketdArtifactServer {
    #[allow(dead_code)]
    log: Logger,
    store: WicketdArtifactStore,
    ipr_artifact: IprArtifactServer,
}

impl WicketdArtifactServer {
    pub(crate) fn new(
        log: &Logger,
        store: WicketdArtifactStore,
        ipr_artifact: IprArtifactServer,
    ) -> Self {
        let log = log.new(slog::o!("component" => "wicketd artifact server"));
        Self { log, store, ipr_artifact }
    }
}

#[async_trait]
impl ArtifactGetter for WicketdArtifactServer {
    async fn get_by_hash(&self, id: &ArtifactHashId) -> Option<(u64, Body)> {
        let data_handle = self.store.get_by_hash(id)?;
        let size = data_handle.file_size() as u64;
        let data_stream = match data_handle.reader_stream().await {
            Ok(stream) => stream,
            Err(err) => {
                error!(
                    self.log, "failed to open extracted archive on demand";
                    "error" => #%err,
                );
                return None;
            }
        };

        Some((size, Body::wrap_stream(data_stream)))
    }

    async fn report_progress(
        &self,
        update_id: Uuid,
        report: installinator_common::EventReport,
    ) -> Result<EventReportStatus, HttpError> {
        Ok(self.ipr_artifact.report_progress(update_id, report))
    }
}

/// The artifact store for wicketd.
///
/// This can be cheaply cloned, and is intended to be shared across the parts of artifactd that
/// upload artifacts and the parts that fetch them.
#[derive(Clone, Debug)]
pub struct WicketdArtifactStore {
    log: Logger,
    // NOTE: this is a `std::sync::Mutex` rather than a `tokio::sync::Mutex`
    // because the critical sections are extremely small.
    artifacts_with_plan: Arc<Mutex<Option<ArtifactsWithPlan>>>,
}

impl WicketdArtifactStore {
    pub(crate) fn new(log: &Logger) -> Self {
        let log = log.new(slog::o!("component" => "wicketd artifact store"));
        Self { log, artifacts_with_plan: Default::default() }
    }

    pub(crate) async fn put_repository<T>(
        &self,
        data: T,
    ) -> Result<(), HttpError>
    where
        T: io::Read + io::Seek + Send + 'static,
    {
        slog::debug!(self.log, "adding repository");

        let log = self.log.clone();
        let new_artifacts = tokio::task::spawn_blocking(move || {
            ArtifactsWithPlan::from_zip(data, &log)
                .map_err(|error| error.to_http_error())
        })
        .await
        .unwrap()?;
        self.replace(new_artifacts);

        Ok(())
    }

    pub(crate) fn system_version_and_artifact_ids(
        &self,
    ) -> Option<(SemverVersion, Vec<InstallableArtifacts>)> {
        let artifacts = self.artifacts_with_plan.lock().unwrap();
        let artifacts = artifacts.as_ref()?;
        let system_version = artifacts.plan.system_version.clone();
        let artifact_ids = artifacts
            .by_id
            .iter()
            .map(|(k, v)| InstallableArtifacts {
                artifact_id: k.clone(),
                installable: v.clone(),
            })
            .collect();
        Some((system_version, artifact_ids))
    }

    /// Obtain the current plan.
    ///
    /// Exposed for testing.
    pub fn current_plan(&self) -> Option<UpdatePlan> {
        // We expect this hashmap to be relatively small (order ~10), and
        // cloning both ArtifactIds and ExtractedArtifactDataHandles are cheap.
        self.artifacts_with_plan
            .lock()
            .unwrap()
            .as_ref()
            .map(|artifacts| artifacts.plan.clone())
    }

    // ---
    // Helper methods
    // ---

    fn get_by_hash(
        &self,
        id: &ArtifactHashId,
    ) -> Option<ExtractedArtifactDataHandle> {
        self.artifacts_with_plan.lock().unwrap().as_ref()?.get_by_hash(id)
    }

    // `pub` to allow use in integration tests.
    pub fn contains_by_hash(&self, id: &ArtifactHashId) -> bool {
        self.get_by_hash(id).is_some()
    }

    /// Replaces the artifact hash map, returning the previous map.
    fn replace(
        &self,
        new_artifacts: ArtifactsWithPlan,
    ) -> Option<ArtifactsWithPlan> {
        let mut artifacts = self.artifacts_with_plan.lock().unwrap();
        mem::replace(&mut *artifacts, Some(new_artifacts))
    }
}

impl ArtifactsWithPlan {
    fn from_zip<T>(zip_data: T, log: &Logger) -> Result<Self, RepositoryError>
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
                .map_err(RepositoryError::LoadRepository)?;

        let artifacts = repository
            .read_artifacts()
            .map_err(RepositoryError::ReadArtifactsDocument)?;

        // Create another temporary directory where we'll "permanently" (as long
        // as this plan is in use) hold extracted artifacts we need; most of
        // these are just direct copies of artifacts we just unpacked into
        // `dir`, but we'll also unpack nested artifacts like the RoT dual A/B
        // archives.
        let mut plan_builder =
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

            let reader = repository
                .repo()
                .read_target(&target_name)
                .map_err(|error| RepositoryError::LocateTarget {
                    target: artifact.target.clone(),
                    error: Box::new(error),
                })?
                .ok_or_else(|| {
                    RepositoryError::MissingTarget(artifact.target.clone())
                })?;

            plan_builder.add_artifact(
                artifact.into_id(),
                artifact_hash,
                io::BufReader::new(reader),
                &mut by_id,
                &mut by_hash,
            )?;
        }

        // Ensure we know how to apply updates from this set of artifacts; we'll
        // remember the plan we create.
        let plan = plan_builder.build()?;

        Ok(Self { by_id, by_hash: by_hash.into(), plan })
    }

    fn get_by_hash(
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

#[derive(Debug, Error)]
enum RepositoryError {
    #[error("error opening archive")]
    OpenArchive(#[source] anyhow::Error),

    #[error("error creating temporary directory")]
    TempDirCreate(#[source] std::io::Error),

    #[error("error creating temporary file in {path}")]
    TempFileCreate {
        path: Utf8PathBuf,
        #[source]
        error: std::io::Error,
    },

    #[error("error extracting repository")]
    Extract(#[source] anyhow::Error),

    #[error("error loading repository")]
    LoadRepository(#[source] anyhow::Error),

    #[error("error reading artifacts.json")]
    ReadArtifactsDocument(#[source] anyhow::Error),

    #[error("error reading target hash for `{target}` in repository")]
    TargetHashRead {
        target: String,
        #[source]
        error: tough::schema::Error,
    },

    #[error("target hash `{}` expected to be 32 bytes long, was {}", hex::encode(.0), .0.len())]
    TargetHashLength(Vec<u8>),

    #[error("error locating target `{target}` in repository")]
    LocateTarget {
        target: String,
        #[source]
        error: Box<tough::error::Error>,
    },

    #[error(
        "artifacts.json defines target `{0}` which is missing from the repo"
    )]
    MissingTarget(String),

    #[error("error copying artifact of kind `{kind}` from repository")]
    CopyExtractedArtifact {
        kind: ArtifactKind,
        #[source]
        error: anyhow::Error,
    },

    #[error("error extracting tarball for {kind} from repository")]
    TarballExtract {
        kind: KnownArtifactKind,
        #[source]
        error: anyhow::Error,
    },

    #[error("multiple artifacts found for kind `{0:?}`")]
    DuplicateArtifactKind(KnownArtifactKind),

    #[error("duplicate board found for kind `{kind:?}`: `{board}`")]
    DuplicateBoardEntry { board: String, kind: KnownArtifactKind },

    #[error("error parsing artifact {id:?} as hubris archive")]
    ParsingHubrisArchive {
        id: ArtifactId,
        #[source]
        error: Box<hubtools::Error>,
    },

    #[error("error reading hubris caboose from {id:?}")]
    ReadHubrisCaboose {
        id: ArtifactId,
        #[source]
        error: Box<hubtools::Error>,
    },

    #[error("error reading board from hubris caboose of {id:?}")]
    ReadHubrisCabooseBoard {
        id: ArtifactId,
        #[source]
        error: hubtools::CabooseError,
    },

    #[error(
        "error reading board from hubris caboose of {0:?}: non-utf8 value"
    )]
    ReadHubrisCabooseBoardUtf8(ArtifactId),

    #[error("missing artifact of kind `{0:?}`")]
    MissingArtifactKind(KnownArtifactKind),

    #[error(
        "duplicate hash entries found in artifacts.json for kind `{}`, hash `{}`", .0.kind, .0.hash
    )]
    DuplicateHashEntry(ArtifactHashId),
}

impl RepositoryError {
    fn to_http_error(&self) -> HttpError {
        let message = DisplayErrorChain::new(self).to_string();

        match self {
            // Errors we had that are unrelated to the contents of a repository
            // uploaded by a client.
            RepositoryError::TempDirCreate(_)
            | RepositoryError::TempFileCreate { .. } => {
                HttpError::for_unavail(None, message)
            }

            // Errors that are definitely caused by bad repository contents.
            RepositoryError::DuplicateArtifactKind(_)
            | RepositoryError::LocateTarget { .. }
            | RepositoryError::TargetHashLength(_)
            | RepositoryError::MissingArtifactKind(_)
            | RepositoryError::MissingTarget(_)
            | RepositoryError::DuplicateHashEntry(_)
            | RepositoryError::DuplicateBoardEntry { .. }
            | RepositoryError::ParsingHubrisArchive { .. }
            | RepositoryError::ReadHubrisCaboose { .. }
            | RepositoryError::ReadHubrisCabooseBoard { .. }
            | RepositoryError::ReadHubrisCabooseBoardUtf8(_) => {
                HttpError::for_bad_request(None, message)
            }

            // Gray area - these are _probably_ caused by bad repository
            // contents, but there might be some cases (or cases-with-cases)
            // where good contents still produce one of these errors. We'll opt
            // for sending a 4xx bad request in hopes that it was our client's
            // fault.
            RepositoryError::OpenArchive(_)
            | RepositoryError::Extract(_)
            | RepositoryError::TarballExtract { .. }
            | RepositoryError::LoadRepository(_)
            | RepositoryError::ReadArtifactsDocument(_)
            | RepositoryError::TargetHashRead { .. }
            | RepositoryError::CopyExtractedArtifact { .. } => {
                HttpError::for_bad_request(None, message)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ArtifactIdData {
    pub(crate) id: ArtifactId,
    pub(crate) data: ExtractedArtifactDataHandle,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Board(pub(crate) String);

impl Borrow<String> for Board {
    fn borrow(&self) -> &String {
        &self.0
    }
}

/// The update plan currently in effect.
///
/// Exposed for testing.
#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub(crate) system_version: SemverVersion,
    pub(crate) gimlet_sp: BTreeMap<Board, ArtifactIdData>,
    pub(crate) gimlet_rot_a: ArtifactIdData,
    pub(crate) gimlet_rot_b: ArtifactIdData,
    pub(crate) psc_sp: BTreeMap<Board, ArtifactIdData>,
    pub(crate) psc_rot_a: ArtifactIdData,
    pub(crate) psc_rot_b: ArtifactIdData,
    pub(crate) sidecar_sp: BTreeMap<Board, ArtifactIdData>,
    pub(crate) sidecar_rot_a: ArtifactIdData,
    pub(crate) sidecar_rot_b: ArtifactIdData,

    // Note: The Trampoline image is broken into phase1/phase2 as part of our
    // update plan (because they go to different destinations), but the two
    // phases will have the _same_ `ArtifactId` (i.e., the ID of the Host
    // artifact from the TUF repository.
    //
    // The same would apply to the host phase1/phase2, but we don't actually
    // need the `host_phase_2` data as part of this plan (we serve it from the
    // artifact server instead).
    pub(crate) host_phase_1: ArtifactIdData,
    pub(crate) trampoline_phase_1: ArtifactIdData,
    pub(crate) trampoline_phase_2: ArtifactIdData,

    // We need to send installinator the hash of the host_phase_2 data it should
    // fetch from us; we compute it while generating the plan.
    //
    // This is exposed for testing.
    pub host_phase_2_hash: ArtifactHash,

    // We also need to send installinator the hash of the control_plane image it
    // should fetch from us. This is already present in the TUF repository, but
    // we record it here for use by the update process.
    //
    // This is exposed for testing.
    pub control_plane_hash: ArtifactHash,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    use anyhow::{Context, Result};
    use camino_tempfile::Utf8TempDir;
    use clap::Parser;
    use omicron_test_utils::dev::test_setup_log;

    /// Test that `ArtifactsWithPlan` can extract the fake repository generated
    /// by tufaceous.
    #[test]
    fn test_extract_fake() -> Result<()> {
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

        args.exec(&logctx.log).context("error executing assemble command")?;

        // Now check that it can be read by the archive extractor.
        let zip_bytes = std::fs::File::open(&archive_path)
            .context("error opening archive.zip")?;
        let plan = ArtifactsWithPlan::from_zip(zip_bytes, &logctx.log)
            .context("error reading archive.zip")?;
        // Check that all known artifact kinds are present in the map.
        let by_id_kinds: BTreeSet<_> =
            plan.by_id.keys().map(|id| id.kind.clone()).collect();
        let by_hash_kinds: BTreeSet<_> =
            plan.by_hash.keys().map(|id| id.kind.clone()).collect();

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
        for (id, hash_ids) in &plan.by_id {
            for hash_id in hash_ids {
                assert!(
                    plan.by_hash.contains_key(hash_id),
                    "plan.by_hash is missing an entry for \
                     {hash_id:?} (derived from {id:?})"
                );
            }
        }

        logctx.cleanup_successful();

        Ok(())
    }
}
