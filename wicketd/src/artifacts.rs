// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    borrow::Borrow,
    collections::{btree_map, hash_map::Entry, BTreeMap, HashMap},
    convert::Infallible,
    io::{self, Read},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use buf_list::BufList;
use bytes::{BufMut, Bytes, BytesMut};
use debug_ignore::DebugIgnore;
use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use futures::stream;
use hubtools::RawHubrisArchive;
use hyper::Body;
use installinator_artifactd::{ArtifactGetter, EventReportStatus};
use omicron_common::api::{
    external::SemverVersion, internal::nexus::KnownArtifactKind,
};
use omicron_common::update::{
    ArtifactHash, ArtifactHashId, ArtifactId, ArtifactKind,
};
use sha2::{Digest, Sha256};
use slog::Logger;
use thiserror::Error;
use tough::TargetName;
use tufaceous_lib::{
    ArchiveExtractor, HostPhaseImages, OmicronRepo, RotArchives,
};
use uuid::Uuid;

use crate::installinator_progress::IprArtifactServer;

// A collection of artifacts along with an update plan using those artifacts.
#[derive(Debug, Default)]
struct ArtifactsWithPlan {
    // TODO: replace with BufList once it supports Read via a cursor (required
    // for host tarball extraction)
    by_id: DebugIgnore<HashMap<ArtifactId, (ArtifactHash, Bytes)>>,
    by_hash: DebugIgnore<HashMap<ArtifactHashId, Bytes>>,
    plan: Option<UpdatePlan>,
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
    async fn get(&self, id: &ArtifactId) -> Option<(u64, Body)> {
        // This is a test artifact name used by the installinator.
        if id.name == "__installinator-test" {
            // For testing, the major version is the size of the artifact.
            let size: u64 = id.version.0.major;
            let mut bytes = BytesMut::with_capacity(size as usize);
            bytes.put_bytes(0, size as usize);
            return Some((size, Body::from(bytes.freeze())));
        }

        let buf_list = self.store.get(id)?;
        let size = buf_list.num_bytes() as u64;
        // Return the list as a stream of bytes.
        Some((
            size,
            Body::wrap_stream(stream::iter(
                buf_list.into_iter().map(|bytes| Ok::<_, Infallible>(bytes)),
            )),
        ))
    }

    async fn get_by_hash(&self, id: &ArtifactHashId) -> Option<(u64, Body)> {
        let buf_list = self.store.get_by_hash(id)?;
        let size = buf_list.num_bytes() as u64;
        Some((
            size,
            Body::wrap_stream(stream::iter(
                buf_list.into_iter().map(|bytes| Ok::<_, Infallible>(bytes)),
            )),
        ))
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
    // NOTE: this is a `std::sync::Mutex` rather than a `tokio::sync::Mutex` because the critical
    // sections are extremely small.
    artifacts_with_plan: Arc<Mutex<ArtifactsWithPlan>>,
}

impl WicketdArtifactStore {
    pub(crate) fn new(log: &Logger) -> Self {
        let log = log.new(slog::o!("component" => "wicketd artifact store"));
        Self { log, artifacts_with_plan: Default::default() }
    }

    pub(crate) fn put_repository(
        &self,
        bytes: BufList,
    ) -> Result<(), HttpError> {
        slog::debug!(self.log, "adding repository"; "size" => bytes.num_bytes());

        let new_artifacts = ArtifactsWithPlan::from_zip(bytes, &self.log)
            .map_err(|error| error.to_http_error())?;
        self.replace(new_artifacts);
        Ok(())
    }

    pub(crate) fn system_version_and_artifact_ids(
        &self,
    ) -> (Option<SemverVersion>, Vec<ArtifactId>) {
        let artifacts = self.artifacts_with_plan.lock().unwrap();
        let system_version =
            artifacts.plan.as_ref().map(|p| p.system_version.clone());
        let artifact_ids = artifacts.by_id.keys().cloned().collect();
        (system_version, artifact_ids)
    }

    /// Obtain the current plan.
    ///
    /// Exposed for testing.
    pub fn current_plan(&self) -> Option<UpdatePlan> {
        // We expect this hashmap to be relatively small (order ~10), and
        // cloning both ArtifactIds and BufLists are cheap.
        self.artifacts_with_plan.lock().unwrap().plan.clone()
    }

    // ---
    // Helper methods
    // ---

    fn get(&self, id: &ArtifactId) -> Option<BufList> {
        // NOTE: cloning a `BufList` is cheap since it's just a bunch of reference count bumps.
        // Cloning it here also means we can release the lock quickly.
        self.artifacts_with_plan.lock().unwrap().get(id).map(BufList::from)
    }

    pub fn get_by_hash(&self, id: &ArtifactHashId) -> Option<BufList> {
        // NOTE: cloning a `BufList` is cheap since it's just a bunch of reference count bumps.
        // Cloning it here also means we can release the lock quickly.
        self.artifacts_with_plan
            .lock()
            .unwrap()
            .get_by_hash(id)
            .map(BufList::from)
    }

    /// Replaces the artifact hash map, returning the previous map.
    fn replace(&self, new_artifacts: ArtifactsWithPlan) -> ArtifactsWithPlan {
        let mut artifacts = self.artifacts_with_plan.lock().unwrap();
        std::mem::replace(&mut *artifacts, new_artifacts)
    }
}

impl ArtifactsWithPlan {
    fn from_zip(
        zip_bytes: BufList,
        log: &Logger,
    ) -> Result<Self, RepositoryError> {
        let mut extractor = ArchiveExtractor::from_owned_buf_list(zip_bytes)
            .map_err(RepositoryError::OpenArchive)?;

        // Create a temporary directory to hold artifacts in (we'll read them
        // into memory as we extract them).
        let dir = camino_tempfile::tempdir()
            .map_err(RepositoryError::TempDirCreate)?;

        slog::info!(log, "extracting uploaded archive to {}", dir.path());

        // XXX: might be worth differentiating between server-side issues (503)
        // and issues with the uploaded archive (400).
        extractor.extract(dir.path()).map_err(RepositoryError::Extract)?;

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

        // Read the artifact into memory.
        //
        // XXX Could also read the artifact from disk directly, keeping the
        // files around. That introduces some new failure domains (e.g. what if
        // the directory gets removed from underneath us?) but is worth
        // revisiting.
        //
        // Notes:
        //
        // 1. With files on disk it is possible to keep the file descriptor
        //    open, preventing deletes from affecting us. However, tough doesn't
        //    quite provide an interface to do that.
        // 2. Ideally we'd write the zip to disk, implement a zip transport, and
        //    just keep the zip's file descriptor open. Unfortunately, a zip
        //    transport can't currently be written in safe Rust due to
        //    https://github.com/awslabs/tough/pull/563. If that lands and/or we
        //    write our own TUF implementation, we should switch to that approach.
        let mut by_id = HashMap::new();
        let mut by_hash = HashMap::new();
        for artifact in artifacts.artifacts {
            // The artifact kind might be unknown here: possibly attempting to do an
            // update where the current version of wicketd isn't aware of a new
            // artifact kind.
            let artifact_id = artifact.id();

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
            let artifact_hash_id = ArtifactHashId {
                kind: artifact_id.kind.clone(),
                hash: artifact_hash,
            };

            let mut reader = repository
                .repo()
                .read_target(&target_name)
                .map_err(|error| RepositoryError::LocateTarget {
                    target: artifact.target.clone(),
                    error: Box::new(error),
                })?
                .ok_or_else(|| {
                    RepositoryError::MissingTarget(artifact.target.clone())
                })?;
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).map_err(|error| {
                RepositoryError::ReadTarget {
                    target: artifact.target.clone(),
                    error,
                }
            })?;

            let bytes = Bytes::from(buf);
            let num_bytes = bytes.len();

            match by_id.entry(artifact_id.clone()) {
                Entry::Occupied(_) => {
                    // We got two entries for an artifact?
                    return Err(RepositoryError::DuplicateEntry(artifact_id));
                }
                Entry::Vacant(entry) => {
                    entry.insert((artifact_hash, bytes.clone()));
                }
            }

            match by_hash.entry(artifact_hash_id.clone()) {
                Entry::Occupied(_) => {
                    // We got two entries for an artifact?
                    return Err(RepositoryError::DuplicateHashEntry(
                        artifact_hash_id,
                    ));
                }
                Entry::Vacant(entry) => {
                    entry.insert(bytes);
                }
            }

            slog::info!(
                log, "added artifact";
                "kind" => %artifact_id.kind,
                "id" => format!("{}:{}", artifact_id.name, artifact_id.version),
                "hash" => %artifact_hash,
                "length" => num_bytes,
            );
        }

        // Ensure we know how to apply updates from this set of artifacts; we'll
        // remember the plan we create.
        let plan = UpdatePlan::new(
            artifacts.system_version,
            &by_id,
            &mut by_hash,
            log,
            read_hubris_board_from_archive,
        )?;

        Ok(Self {
            by_id: by_id.into(),
            by_hash: by_hash.into(),
            plan: Some(plan),
        })
    }

    fn get(&self, id: &ArtifactId) -> Option<BufList> {
        self.by_id
            .get(id)
            .cloned()
            .map(|(_hash, bytes)| BufList::from_iter([bytes]))
    }

    fn get_by_hash(&self, id: &ArtifactHashId) -> Option<BufList> {
        self.by_hash.get(id).cloned().map(|bytes| BufList::from_iter([bytes]))
    }
}

#[derive(Debug, Error)]
enum RepositoryError {
    #[error("error opening archive")]
    OpenArchive(#[source] anyhow::Error),

    #[error("error creating temporary directory")]
    TempDirCreate(#[source] std::io::Error),

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

    #[error("error reading target `{target}` from repository")]
    ReadTarget {
        target: String,
        #[source]
        error: std::io::Error,
    },

    #[error("error extracting tarball for {kind} from repository")]
    TarballExtract {
        kind: KnownArtifactKind,
        #[source]
        error: anyhow::Error,
    },

    #[error(
        "duplicate entries found in artifacts.json for kind `{}`, `{}:{}`", .0.kind, .0.name, .0.version
    )]
    DuplicateEntry(ArtifactId),

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
            RepositoryError::TempDirCreate(_) => {
                HttpError::for_unavail(None, message)
            }

            // Errors that are definitely caused by bad repository contents.
            RepositoryError::DuplicateEntry(_)
            | RepositoryError::DuplicateArtifactKind(_)
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
            | RepositoryError::ReadTarget { .. } => {
                HttpError::for_bad_request(None, message)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ArtifactIdData {
    pub(crate) id: ArtifactId,
    pub(crate) data: DebugIgnore<Bytes>,
    pub(crate) hash: ArtifactHash,
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

impl UpdatePlan {
    // `read_hubris_board` should always be `read_hubris_board_from_archive`; we
    // take it as an argument to allow unit tests to give us invalid/fake
    // "hubris archives" `read_hubris_board_from_archive` wouldn't be able to
    // handle.
    fn new<F>(
        system_version: SemverVersion,
        by_id: &HashMap<ArtifactId, (ArtifactHash, Bytes)>,
        by_hash: &mut HashMap<ArtifactHashId, Bytes>,
        log: &Logger,
        read_hubris_board: F,
    ) -> Result<Self, RepositoryError>
    where
        F: Fn(
            ArtifactId,
            Vec<u8>,
        ) -> Result<(ArtifactId, Board), RepositoryError>,
    {
        // We expect at least one of each of these kinds to be present, but we
        // allow multiple (keyed by the Hubris archive caboose `board` value,
        // which identifies hardware revision). We could do the same for the RoT
        // images, but to date the RoT images are applicable to all revs; only
        // the SP images change from rev to rev.
        let mut gimlet_sp = BTreeMap::new();
        let mut psc_sp = BTreeMap::new();
        let mut sidecar_sp = BTreeMap::new();

        // We expect exactly one of each of these kinds to be present in the
        // snapshot. Scan the snapshot and record the first of each we find,
        // failing if we find a second.
        let mut gimlet_rot_a = None;
        let mut gimlet_rot_b = None;
        let mut psc_rot_a = None;
        let mut psc_rot_b = None;
        let mut sidecar_rot_a = None;
        let mut sidecar_rot_b = None;
        let mut host_phase_1 = None;
        let mut host_phase_2 = None;
        let mut trampoline_phase_1 = None;
        let mut trampoline_phase_2 = None;

        // We get the `ArtifactHash`s of top-level artifacts for free from the
        // TUF repo, but for artifacts we expand, we recompute hashes of the
        // inner parts ourselves.
        let compute_hash = |data: &Bytes| {
            let mut hasher = Sha256::new();
            hasher.update(data);
            ArtifactHash(hasher.finalize().into())
        };

        // Helper called for each SP image found in the loop below.
        let sp_image_found = |out: &mut BTreeMap<Board, ArtifactIdData>,
                              id,
                              hash: &ArtifactHash,
                              data: &Bytes| {
            let (id, board) = read_hubris_board(id, data.clone().into())?;

            match out.entry(board) {
                btree_map::Entry::Vacant(slot) => {
                    slot.insert(ArtifactIdData {
                        id,
                        data: DebugIgnore(data.clone()),
                        hash: *hash,
                    });
                    Ok(())
                }
                btree_map::Entry::Occupied(slot) => {
                    Err(RepositoryError::DuplicateBoardEntry {
                        board: slot.key().0.clone(),
                        // This closure is only called with well-known kinds.
                        kind: slot.get().id.kind.to_known().unwrap(),
                    })
                }
            }
        };

        // Helper called for each non-SP artifact found in the loop below.
        let artifact_found = |out: &mut Option<ArtifactIdData>,
                              id,
                              hash: Option<&ArtifactHash>,
                              data: &Bytes| {
            let hash = match hash {
                Some(hash) => *hash,
                None => compute_hash(data),
            };
            let data = DebugIgnore(data.clone());
            match out.replace(ArtifactIdData { id, hash, data }) {
                None => Ok(()),
                Some(prev) => {
                    // This closure is only called with well-known kinds.
                    let kind = prev.id.kind.to_known().unwrap();
                    Err(RepositoryError::DuplicateArtifactKind(kind))
                }
            }
        };

        for (artifact_id, (hash, data)) in by_id.iter() {
            // In generating an update plan, skip any artifact kinds that are
            // unknown to us (we wouldn't know how to incorporate them into our
            // plan).
            let Some(artifact_kind) = artifact_id.kind.to_known() else {
                continue;
            };
            let artifact_id = artifact_id.clone();

            match artifact_kind {
                KnownArtifactKind::GimletSp => {
                    sp_image_found(&mut gimlet_sp, artifact_id, hash, data)?
                }
                KnownArtifactKind::GimletRot => {
                    slog::debug!(log, "extracting gimlet rot tarball");
                    let archives = unpack_rot_artifact(artifact_kind, data)?;
                    artifact_found(
                        &mut gimlet_rot_a,
                        artifact_id.clone(),
                        None,
                        &archives.archive_a,
                    )?;
                    artifact_found(
                        &mut gimlet_rot_b,
                        artifact_id.clone(),
                        None,
                        &archives.archive_b,
                    )?;
                }
                KnownArtifactKind::PscSp => {
                    sp_image_found(&mut psc_sp, artifact_id, hash, data)?
                }
                KnownArtifactKind::PscRot => {
                    slog::debug!(log, "extracting psc rot tarball");
                    let archives = unpack_rot_artifact(artifact_kind, data)?;
                    artifact_found(
                        &mut psc_rot_a,
                        artifact_id.clone(),
                        None,
                        &archives.archive_a,
                    )?;
                    artifact_found(
                        &mut psc_rot_b,
                        artifact_id.clone(),
                        None,
                        &archives.archive_b,
                    )?;
                }
                KnownArtifactKind::SwitchSp => {
                    sp_image_found(&mut sidecar_sp, artifact_id, hash, data)?
                }
                KnownArtifactKind::SwitchRot => {
                    slog::debug!(log, "extracting switch rot tarball");
                    let archives = unpack_rot_artifact(artifact_kind, data)?;
                    artifact_found(
                        &mut sidecar_rot_a,
                        artifact_id.clone(),
                        None,
                        &archives.archive_a,
                    )?;
                    artifact_found(
                        &mut sidecar_rot_b,
                        artifact_id.clone(),
                        None,
                        &archives.archive_b,
                    )?;
                }
                KnownArtifactKind::Host => {
                    slog::debug!(log, "extracting host tarball");
                    let images = unpack_host_artifact(artifact_kind, data)?;
                    artifact_found(
                        &mut host_phase_1,
                        artifact_id.clone(),
                        None,
                        &images.phase_1,
                    )?;
                    artifact_found(
                        &mut host_phase_2,
                        artifact_id,
                        None,
                        &images.phase_2,
                    )?;
                }
                KnownArtifactKind::Trampoline => {
                    slog::debug!(log, "extracting trampoline tarball");
                    let images = unpack_host_artifact(artifact_kind, data)?;
                    artifact_found(
                        &mut trampoline_phase_1,
                        artifact_id.clone(),
                        None,
                        &images.phase_1,
                    )?;
                    artifact_found(
                        &mut trampoline_phase_2,
                        artifact_id,
                        None,
                        &images.phase_2,
                    )?;
                }
                KnownArtifactKind::ControlPlane => {
                    // Only the installinator needs this artifact.
                }
            }
        }

        // Find the TUF hash of the control plane artifact. This is a little
        // hokey: scan through `by_hash` looking for the right artifact kind.
        let control_plane_hash = by_hash
            .keys()
            .find_map(|hash_id| {
                if hash_id.kind.to_known()
                    == Some(KnownArtifactKind::ControlPlane)
                {
                    Some(hash_id.hash)
                } else {
                    None
                }
            })
            .ok_or(RepositoryError::MissingArtifactKind(
                KnownArtifactKind::ControlPlane,
            ))?;

        // Ensure we found a Host artifact.
        let host_phase_2 = host_phase_2.ok_or(
            RepositoryError::MissingArtifactKind(KnownArtifactKind::Host),
        )?;

        // Add the host phase 2 image to the set of artifacts we're willing to
        // serve by hash; that's how installinator will be requesting it.
        let host_phase_2_hash_id = ArtifactHashId {
            kind: ArtifactKind::HOST_PHASE_2,
            hash: host_phase_2.hash,
        };
        match by_hash.entry(host_phase_2_hash_id.clone()) {
            Entry::Occupied(_) => {
                // We got two entries for an artifact?
                return Err(RepositoryError::DuplicateHashEntry(
                    host_phase_2_hash_id,
                ));
            }
            Entry::Vacant(entry) => {
                slog::info!(log, "added host phase 2 artifact";
                    "kind" => %host_phase_2_hash_id.kind,
                    "hash" => %host_phase_2_hash_id.hash,
                    "length" => host_phase_2.data.len(),
                );
                entry.insert(host_phase_2.data.0.clone());
            }
        }

        // Ensure our multi-board-supporting kinds have at least one board
        // present.
        if gimlet_sp.is_empty() {
            return Err(RepositoryError::MissingArtifactKind(
                KnownArtifactKind::GimletSp,
            ));
        }
        if psc_sp.is_empty() {
            return Err(RepositoryError::MissingArtifactKind(
                KnownArtifactKind::PscSp,
            ));
        }
        if sidecar_sp.is_empty() {
            return Err(RepositoryError::MissingArtifactKind(
                KnownArtifactKind::SwitchSp,
            ));
        }

        Ok(Self {
            system_version,
            gimlet_sp,
            gimlet_rot_a: gimlet_rot_a.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::GimletRot,
                ),
            )?,
            gimlet_rot_b: gimlet_rot_b.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::GimletRot,
                ),
            )?,
            psc_sp,
            psc_rot_a: psc_rot_a.ok_or(
                RepositoryError::MissingArtifactKind(KnownArtifactKind::PscRot),
            )?,
            psc_rot_b: psc_rot_b.ok_or(
                RepositoryError::MissingArtifactKind(KnownArtifactKind::PscRot),
            )?,
            sidecar_sp,
            sidecar_rot_a: sidecar_rot_a.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::SwitchRot,
                ),
            )?,
            sidecar_rot_b: sidecar_rot_b.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::SwitchRot,
                ),
            )?,
            host_phase_1: host_phase_1.ok_or(
                RepositoryError::MissingArtifactKind(KnownArtifactKind::Host),
            )?,
            trampoline_phase_1: trampoline_phase_1.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::Trampoline,
                ),
            )?,
            trampoline_phase_2: trampoline_phase_2.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::Trampoline,
                ),
            )?,
            host_phase_2_hash: host_phase_2.hash,
            control_plane_hash,
        })
    }
}

fn unpack_host_artifact(
    kind: KnownArtifactKind,
    data: &Bytes,
) -> Result<HostPhaseImages, RepositoryError> {
    HostPhaseImages::extract(io::Cursor::new(data))
        .map_err(|error| RepositoryError::TarballExtract { kind, error })
}

fn unpack_rot_artifact(
    kind: KnownArtifactKind,
    data: &Bytes,
) -> Result<RotArchives, RepositoryError> {
    RotArchives::extract(io::Cursor::new(data))
        .map_err(|error| RepositoryError::TarballExtract { kind, error })
}

// This function takes and returns `id` to avoid an unnecessary clone; `id` will
// be present in either the Ok tuple or the error.
fn read_hubris_board_from_archive(
    id: ArtifactId,
    data: Vec<u8>,
) -> Result<(ArtifactId, Board), RepositoryError> {
    let archive = match RawHubrisArchive::from_vec(data).map_err(Box::new) {
        Ok(archive) => archive,
        Err(error) => {
            return Err(RepositoryError::ParsingHubrisArchive { id, error });
        }
    };
    let caboose = match archive.read_caboose().map_err(Box::new) {
        Ok(caboose) => caboose,
        Err(error) => {
            return Err(RepositoryError::ReadHubrisCaboose { id, error });
        }
    };
    let board = match caboose.board() {
        Ok(board) => board,
        Err(error) => {
            return Err(RepositoryError::ReadHubrisCabooseBoard { id, error });
        }
    };
    let board = match std::str::from_utf8(board) {
        Ok(s) => s,
        Err(_) => {
            return Err(RepositoryError::ReadHubrisCabooseBoardUtf8(id));
        }
    };
    Ok((id, Board(board.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    use anyhow::{Context, Result};
    use camino_tempfile::Utf8TempDir;
    use clap::Parser;
    use omicron_test_utils::dev::test_setup_log;
    use rand::{distributions::Standard, thread_rng, Rng};

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
        let zip_bytes = fs_err::read(&archive_path)?.into();
        let plan = ArtifactsWithPlan::from_zip(zip_bytes, &logctx.log)
            .context("error reading archive.zip")?;
        // Check that all known artifact kinds are present in the map.
        let by_id_kinds: BTreeSet<_> =
            plan.by_id.keys().map(|id| id.kind.clone()).collect();
        let by_hash_kinds: BTreeSet<_> =
            plan.by_hash.keys().map(|id| id.kind.clone()).collect();

        let mut expected_kinds: BTreeSet<_> =
            KnownArtifactKind::iter().map(ArtifactKind::from).collect();
        assert_eq!(
            expected_kinds, by_id_kinds,
            "expected kinds match by_id kinds"
        );

        // The by_hash map has the host phase 2 kind.
        // XXX should by_id also contain this kind?
        expected_kinds.insert(ArtifactKind::HOST_PHASE_2);
        assert_eq!(
            expected_kinds, by_hash_kinds,
            "expected kinds match by_hash kinds"
        );

        logctx.cleanup_successful();

        Ok(())
    }

    fn make_random_bytes() -> Vec<u8> {
        thread_rng().sample_iter(Standard).take(128).collect()
    }

    struct RandomHostOsImage {
        phase1: Bytes,
        phase2: Bytes,
        tarball: Bytes,
    }

    fn make_random_host_os_image() -> RandomHostOsImage {
        use tufaceous_lib::CompositeHostArchiveBuilder;

        let phase1 = make_random_bytes();
        let phase2 = make_random_bytes();

        let mut builder = CompositeHostArchiveBuilder::new(Vec::new()).unwrap();
        builder.append_phase_1(phase1.len(), phase1.as_slice()).unwrap();
        builder.append_phase_2(phase2.len(), phase2.as_slice()).unwrap();

        let tarball = builder.finish().unwrap();

        RandomHostOsImage {
            phase1: Bytes::from(phase1),
            phase2: Bytes::from(phase2),
            tarball: Bytes::from(tarball),
        }
    }

    struct RandomRotImage {
        archive_a: Bytes,
        archive_b: Bytes,
        tarball: Bytes,
    }

    fn make_random_rot_image() -> RandomRotImage {
        use tufaceous_lib::CompositeRotArchiveBuilder;

        let archive_a = make_random_bytes();
        let archive_b = make_random_bytes();

        let mut builder = CompositeRotArchiveBuilder::new(Vec::new()).unwrap();
        builder
            .append_archive_a(archive_a.len(), archive_a.as_slice())
            .unwrap();
        builder
            .append_archive_b(archive_b.len(), archive_b.as_slice())
            .unwrap();

        let tarball = builder.finish().unwrap();

        RandomRotImage {
            archive_a: Bytes::from(archive_a),
            archive_b: Bytes::from(archive_b),
            tarball: Bytes::from(tarball),
        }
    }

    #[test]
    fn test_update_plan_from_artifacts() {
        const VERSION_0: SemverVersion = SemverVersion::new(0, 0, 0);

        let mut by_id = HashMap::new();
        let mut by_hash = HashMap::new();

        // The control plane artifact can be arbitrary bytes; just populate it
        // with random data.
        {
            let kind = KnownArtifactKind::ControlPlane;
            let data = Bytes::from(make_random_bytes());
            let hash = ArtifactHash(Sha256::digest(&data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            let hash_id = ArtifactHashId { kind: kind.into(), hash };
            by_id.insert(id, (hash, data.clone()));
            by_hash.insert(hash_id, data);
        }

        // For each SP image, we'll insert two artifacts: these should end up in
        // the update plan's SP image maps keyed by their "board". Normally the
        // board is read from the archive itself via hubtools; we'll inject a
        // test function that returns the artifact ID name as the board instead.
        for (kind, boards) in [
            (KnownArtifactKind::GimletSp, ["test-gimlet-a", "test-gimlet-b"]),
            (KnownArtifactKind::PscSp, ["test-psc-a", "test-psc-b"]),
            (KnownArtifactKind::SwitchSp, ["test-switch-a", "test-switch-b"]),
        ] {
            for board in boards {
                let data = Bytes::from(make_random_bytes());
                let hash = ArtifactHash(Sha256::digest(&data).into());
                let id = ArtifactId {
                    name: board.to_string(),
                    version: VERSION_0,
                    kind: kind.into(),
                };
                println!("{kind:?}={board:?} => {id:?}");
                let hash_id = ArtifactHashId { kind: kind.into(), hash };
                by_id.insert(id, (hash, data.clone()));
                by_hash.insert(hash_id, data);
            }
        }

        // The Host, Trampoline, and RoT artifacts must be structed the way we
        // expect (i.e., .tar.gz's containing multiple inner artifacts).
        let host = make_random_host_os_image();
        let trampoline = make_random_host_os_image();

        for (kind, image) in [
            (KnownArtifactKind::Host, &host),
            (KnownArtifactKind::Trampoline, &trampoline),
        ] {
            let hash = ArtifactHash(Sha256::digest(&image.tarball).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            let hash_id = ArtifactHashId { kind: kind.into(), hash };
            by_id.insert(id, (hash, image.tarball.clone()));
            by_hash.insert(hash_id, image.tarball.clone());
        }

        let gimlet_rot = make_random_rot_image();
        let psc_rot = make_random_rot_image();
        let sidecar_rot = make_random_rot_image();

        for (kind, artifact) in [
            (KnownArtifactKind::GimletRot, &gimlet_rot),
            (KnownArtifactKind::PscRot, &psc_rot),
            (KnownArtifactKind::SwitchRot, &sidecar_rot),
        ] {
            let hash = ArtifactHash(Sha256::digest(&artifact.tarball).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            let hash_id = ArtifactHashId { kind: kind.into(), hash };
            by_id.insert(id, (hash, artifact.tarball.clone()));
            by_hash.insert(hash_id, artifact.tarball.clone());
        }

        let logctx = test_setup_log("test_update_plan_from_artifacts");

        let plan = UpdatePlan::new(
            VERSION_0,
            &by_id,
            &mut by_hash,
            &logctx.log,
            |id, _data| {
                let board = id.name.clone();
                Ok((id, Board(board)))
            },
        )
        .unwrap();

        assert_eq!(plan.gimlet_sp.len(), 2);
        assert_eq!(plan.psc_sp.len(), 2);
        assert_eq!(plan.sidecar_sp.len(), 2);

        for (id, (hash, data)) in &by_id {
            match id.kind.to_known().unwrap() {
                KnownArtifactKind::GimletSp => {
                    assert!(
                        id.name.starts_with("test-gimlet-"),
                        "unexpected id.name {:?}",
                        id.name
                    );
                    assert_eq!(
                        *plan.gimlet_sp.get(&id.name).unwrap().data,
                        data
                    );
                }
                KnownArtifactKind::ControlPlane => {
                    assert_eq!(plan.control_plane_hash, *hash);
                }
                KnownArtifactKind::PscSp => {
                    assert!(
                        id.name.starts_with("test-psc-"),
                        "unexpected id.name {:?}",
                        id.name
                    );
                    assert_eq!(*plan.psc_sp.get(&id.name).unwrap().data, data);
                }
                KnownArtifactKind::SwitchSp => {
                    assert!(
                        id.name.starts_with("test-switch-"),
                        "unexpected id.name {:?}",
                        id.name
                    );
                    assert_eq!(
                        *plan.sidecar_sp.get(&id.name).unwrap().data,
                        data
                    );
                }
                KnownArtifactKind::Host
                | KnownArtifactKind::Trampoline
                | KnownArtifactKind::GimletRot
                | KnownArtifactKind::PscRot
                | KnownArtifactKind::SwitchRot => {
                    // special; we check these below
                }
            }
        }

        // Check extracted host and trampoline data
        assert_eq!(*plan.host_phase_1.data, host.phase1);
        assert_eq!(*plan.trampoline_phase_1.data, trampoline.phase1);
        assert_eq!(*plan.trampoline_phase_2.data, trampoline.phase2);

        let hash = Sha256::digest(&host.phase2);
        assert_eq!(plan.host_phase_2_hash.0, *hash);

        // Check extracted RoT data
        assert_eq!(*plan.gimlet_rot_a.data, gimlet_rot.archive_a);
        assert_eq!(*plan.gimlet_rot_b.data, gimlet_rot.archive_b);
        assert_eq!(*plan.psc_rot_a.data, psc_rot.archive_a);
        assert_eq!(*plan.psc_rot_b.data, psc_rot.archive_b);
        assert_eq!(*plan.sidecar_rot_a.data, sidecar_rot.archive_a);
        assert_eq!(*plan.sidecar_rot_b.data, sidecar_rot.archive_b);

        logctx.cleanup_successful();
    }
}
