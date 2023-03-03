// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::{hash_map::Entry, HashMap},
    convert::Infallible,
    io::Read,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use buf_list::BufList;
use bytes::{BufMut, Bytes, BytesMut};
use camino::Utf8Path;
use debug_ignore::DebugIgnore;
use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use futures::stream;
use hyper::Body;
use installinator_artifactd::{ArtifactGetter, ProgressReportStatus};
use installinator_common::ProgressReport;
use omicron_common::api::internal::nexus::KnownArtifactKind;
use omicron_common::update::{
    ArtifactHash, ArtifactHashId, ArtifactId, ArtifactKind,
};
use sha2::{Digest, Sha256};
use slog::{warn, Logger};
use thiserror::Error;
use tough::TargetName;
use tufaceous_lib::{ArchiveExtractor, OmicronRepo};
use uuid::Uuid;

use crate::installinator_progress::IprArtifactServer;

// A collection of artifacts along with an update plan using those artifacts.
#[derive(Debug, Default)]
struct ArtifactsWithPlan {
    by_id: DebugIgnore<HashMap<ArtifactId, BufList>>,
    by_hash: DebugIgnore<HashMap<ArtifactHashId, BufList>>,
    plan: Option<UpdatePlan>,
}

/// The artifact server interface for wicketd.
#[derive(Debug)]
pub(crate) struct WicketdArtifactServer {
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
            // For testing, the version is the size of the artifact.
            let size: u64 = id
                        .version
                        .parse()
                        .map_err(|err| {
                            slog::warn!(
                                self.log,
                                "for installinator-test, version should be a u64 indicating the size but found {}: {err}",
                                id.version
                            );
                        })
                        .ok()?;
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
        report: ProgressReport,
    ) -> Result<ProgressReportStatus, HttpError> {
        Ok(self.ipr_artifact.report_progress(update_id, report).await)
    }
}

/// The artifact store for wicketd.
///
/// This can be cheaply cloned, and is intended to be shared across the parts of artifactd that
/// upload artifacts and the parts that fetch them.
#[derive(Clone, Debug)]
pub(crate) struct WicketdArtifactStore {
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

    pub(crate) fn put_repository(&self, bytes: &[u8]) -> Result<(), HttpError> {
        slog::debug!(self.log, "adding repository"; "size" => bytes.len());

        let new_artifacts = ArtifactsWithPlan::from_zip(bytes, &self.log)
            .map_err(|error| error.to_http_error())?;
        self.replace(new_artifacts);
        Ok(())
    }

    pub(crate) fn artifact_ids(&self) -> Vec<ArtifactId> {
        self.artifacts_with_plan.lock().unwrap().by_id.keys().cloned().collect()
    }

    pub(crate) fn current_plan(&self) -> Option<UpdatePlan> {
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
        self.artifacts_with_plan.lock().unwrap().get(id)
    }

    fn get_by_hash(&self, id: &ArtifactHashId) -> Option<BufList> {
        // NOTE: cloning a `BufList` is cheap since it's just a bunch of reference count bumps.
        // Cloning it here also means we can release the lock quickly.
        self.artifacts_with_plan.lock().unwrap().get_by_hash(id)
    }

    /// Replaces the artifact hash map, returning the previous map.
    fn replace(&self, new_artifacts: ArtifactsWithPlan) -> ArtifactsWithPlan {
        let mut artifacts = self.artifacts_with_plan.lock().unwrap();
        std::mem::replace(&mut *artifacts, new_artifacts)
    }
}

impl ArtifactsWithPlan {
    fn from_zip(
        zip_bytes: &[u8],
        log: &Logger,
    ) -> Result<Self, RepositoryError> {
        let mut extractor = ArchiveExtractor::from_borrowed_bytes(zip_bytes)
            .map_err(RepositoryError::OpenArchive)?;

        // Create a temporary directory to hold artifacts in (we'll read them
        // into memory as we extract them).
        let dir =
            tempfile::tempdir().map_err(RepositoryError::TempDirCreate)?;
        let temp_path = <&Utf8Path>::try_from(dir.path()).map_err(|error| {
            RepositoryError::TempDirCreate(error.into_io_error())
        })?;

        slog::info!(log, "extracting uploaded archive to {temp_path}");

        // XXX: might be worth differentiating between server-side issues (503)
        // and issues with the uploaded archive (400).
        extractor.extract(temp_path).map_err(RepositoryError::Extract)?;

        // Time is unavailable during initial setup, so ignore expiration. Even
        // if time were available, we might want to be able to load older
        // versions of artifacts over the technician port in an emergency.
        //
        // XXX we aren't checking against a root of trust at this point --
        // anyone can sign the repositories and this code will accept that.
        let repository = OmicronRepo::load_ignore_expiration(temp_path)
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
                    error,
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
                    error,
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

            let buf_list = BufList::from_iter([Bytes::from(buf)]);
            let num_bytes = buf_list.num_bytes();

            match by_id.entry(artifact_id.clone()) {
                Entry::Occupied(_) => {
                    // We got two entries for an artifact?
                    return Err(RepositoryError::DuplicateEntry(artifact_id));
                }
                Entry::Vacant(entry) => {
                    entry.insert(buf_list.clone());
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
                    entry.insert(buf_list);
                }
            }

            slog::debug!(
                log,
                "added artifact with kind {}, id {}:{}, hash {}, length {}",
                artifact_id.kind,
                artifact_id.name,
                artifact_id.version,
                artifact_hash,
                num_bytes,
            );
        }

        // Ensure we know how to apply updates from this set of artifacts; we'll
        // remember the plan we create.
        let plan = UpdatePlan::new(&by_id, &mut by_hash, log)?;

        Ok(Self {
            by_id: by_id.into(),
            by_hash: by_hash.into(),
            plan: Some(plan),
        })
    }

    fn get(&self, id: &ArtifactId) -> Option<BufList> {
        self.by_id.get(id).cloned()
    }

    fn get_by_hash(&self, id: &ArtifactHashId) -> Option<BufList> {
        self.by_hash.get(id).cloned()
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
        error: tough::error::Error,
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

    #[error(
        "duplicate entries found in artifacts.json for kind `{}`, `{}:{}`", .0.kind, .0.name, .0.version
    )]
    DuplicateEntry(ArtifactId),

    #[error("multiple artifacts found for kind `{0:?}`")]
    DuplicateArtifactKind(KnownArtifactKind),

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
            | RepositoryError::DuplicateHashEntry(_) => {
                HttpError::for_bad_request(None, message)
            }

            // Gray area - these are _probably_ caused by bad repository
            // contents, but there might be some cases (or cases-with-cases)
            // where good contents still produce one of these errors. We'll opt
            // for sending a 4xx bad request in hopes that it was our client's
            // fault.
            RepositoryError::OpenArchive(_)
            | RepositoryError::Extract(_)
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
    pub(crate) data: DebugIgnore<BufList>,
}

#[derive(Debug, Clone)]
pub(crate) struct UpdatePlan {
    pub(crate) gimlet_sp: ArtifactIdData,
    pub(crate) psc_sp: ArtifactIdData,
    pub(crate) sidecar_sp: ArtifactIdData,

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
    pub(crate) host_phase_2_hash: ArtifactHash,

    // We also need to send installinator the hash of the control_plane image it
    // should fetch from us. This is already present in the TUF repository, but
    // we record it here for use by the update process.
    pub(crate) control_plane_hash: ArtifactHash,
}

impl UpdatePlan {
    fn new(
        by_id: &HashMap<ArtifactId, BufList>,
        by_hash: &mut HashMap<ArtifactHashId, BufList>,
        log: &Logger,
    ) -> Result<Self, RepositoryError> {
        // We expect exactly one of each of these kinds to be present in the
        // snapshot. Scan the snapshot and record the first of each we find,
        // failing if we find a second.
        let mut gimlet_sp = None;
        let mut psc_sp = None;
        let mut sidecar_sp = None;
        let mut host_phase_1 = None;
        let mut host_phase_2 = None;
        let mut trampoline_phase_1 = None;
        let mut trampoline_phase_2 = None;

        let artifact_found =
            |out: &mut Option<ArtifactIdData>, id, data: &BufList| {
                let data = DebugIgnore(data.clone());
                match out.replace(ArtifactIdData { id, data }) {
                    None => Ok(()),
                    Some(prev) => {
                        // This closure is only called with well-known kinds.
                        let kind = prev.id.kind.to_known().unwrap();
                        Err(RepositoryError::DuplicateArtifactKind(kind))
                    }
                }
            };

        for (artifact_id, data) in by_id.iter() {
            // In generating an update plan, skip any artifact kinds that are
            // unknown to us (we wouldn't know how to incorporate them into our
            // plan).
            let Some(artifact_kind) = artifact_id.kind.to_known() else { continue };
            let artifact_id = artifact_id.clone();

            match artifact_kind {
                KnownArtifactKind::GimletSp => {
                    artifact_found(&mut gimlet_sp, artifact_id, data)?
                }
                KnownArtifactKind::PscSp => {
                    artifact_found(&mut psc_sp, artifact_id, data)?
                }
                KnownArtifactKind::SwitchSp => {
                    artifact_found(&mut sidecar_sp, artifact_id, data)?
                }
                KnownArtifactKind::Host => {
                    let (phase1, phase2) = unpack_host_artifact(data, log)?;
                    artifact_found(
                        &mut host_phase_1,
                        artifact_id.clone(),
                        &phase1,
                    )?;
                    artifact_found(&mut host_phase_2, artifact_id, &phase2)?;
                }
                KnownArtifactKind::Trampoline => {
                    let (phase1, phase2) = unpack_host_artifact(data, log)?;
                    artifact_found(
                        &mut trampoline_phase_1,
                        artifact_id.clone(),
                        &phase1,
                    )?;
                    artifact_found(
                        &mut trampoline_phase_2,
                        artifact_id,
                        &phase2,
                    )?;
                }
                _ => continue,
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

        // Compute the SHA-256 of the extracted host phase 2 data.
        let mut host_phase_2_hash = Sha256::new();
        for chunk in host_phase_2.data.iter() {
            host_phase_2_hash.update(chunk);
        }
        let host_phase_2_hash =
            ArtifactHash(host_phase_2_hash.finalize().into());

        // Add the host phase 2 image to the set of artifacts we're willing to
        // serve by hash; that's how installinator will be requesting it.
        let host_phase_2_hash_id = ArtifactHashId {
            kind: ArtifactKind::HOST_PHASE_2,
            hash: host_phase_2_hash,
        };
        match by_hash.entry(host_phase_2_hash_id.clone()) {
            Entry::Occupied(_) => {
                // We got two entries for an artifact?
                return Err(RepositoryError::DuplicateHashEntry(
                    host_phase_2_hash_id,
                ));
            }
            Entry::Vacant(entry) => {
                entry.insert(host_phase_2.data.0.clone());
            }
        }

        Ok(Self {
            gimlet_sp: gimlet_sp.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::GimletSp,
                ),
            )?,
            psc_sp: psc_sp.ok_or(RepositoryError::MissingArtifactKind(
                KnownArtifactKind::PscSp,
            ))?,
            sidecar_sp: sidecar_sp.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::SwitchSp,
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
            host_phase_2_hash,
            control_plane_hash,
        })
    }
}

fn unpack_host_artifact(
    data: &BufList,
    log: &Logger,
) -> Result<(BufList, BufList), RepositoryError> {
    // TODO What do host artifacts look like? Probably a tarball? We need to
    // unpack it here and find the phase1/phase2. For now, just return the
    // incoming data as phase1 and an empty buflist as phase 2.
    warn!(log, "FIXME FIXME FIXME PLACEHOLDER UNPACKING HOST ARTIFACT");
    Ok((data.clone(), BufList::new()))
}
