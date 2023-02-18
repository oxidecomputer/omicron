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
use installinator_artifactd::ArtifactGetter;
use omicron_common::update::{ArtifactHash, ArtifactHashId, ArtifactId};
use thiserror::Error;
use tough::TargetName;
use tufaceous_lib::{ArchiveExtractor, OmicronRepo};

/// The artifact store for wicketd.
///
/// This can be cheaply cloned, and is intended to be shared across the parts of artifactd that
/// upload artifacts and the parts that fetch them.
#[derive(Clone, Debug)]
pub(crate) struct WicketdArtifactStore {
    log: slog::Logger,
    // NOTE: this is a `std::sync::Mutex` rather than a `tokio::sync::Mutex` because the critical
    // sections are extremely small.
    artifacts: Arc<Mutex<DebugIgnore<TufRepoArtifacts>>>,
}

impl WicketdArtifactStore {
    pub(crate) fn new(log: &slog::Logger) -> Self {
        let log = log.new(slog::o!("component" => "wicketd artifact store"));
        Self { log, artifacts: Default::default() }
    }

    pub(crate) fn put_repository(&self, bytes: &[u8]) -> Result<(), HttpError> {
        slog::debug!(self.log, "adding repository"; "size" => bytes.len());

        let new_artifacts = TufRepoArtifacts::from_zip(&bytes, &self.log)
            .map_err(|error| error.to_http_error())?;
        self.replace(new_artifacts);
        Ok(())
    }

    // ---
    // Helper methods
    // ---

    fn get(&self, id: &ArtifactId) -> Option<BufList> {
        // NOTE: cloning a `BufList` is cheap since it's just a bunch of reference count bumps.
        // Cloning it here also means we can release the lock quickly.
        self.artifacts.lock().unwrap().get(id)
    }

    fn get_by_hash(&self, id: &ArtifactHashId) -> Option<BufList> {
        // NOTE: cloning a `BufList` is cheap since it's just a bunch of reference count bumps.
        // Cloning it here also means we can release the lock quickly.
        self.artifacts.lock().unwrap().get_by_hash(id)
    }

    /// Replaces the artifact hash map, returning the previous map.
    fn replace(&self, new_artifacts: TufRepoArtifacts) -> TufRepoArtifacts {
        let mut artifacts = self.artifacts.lock().unwrap();
        std::mem::replace(&mut **artifacts, new_artifacts)
    }
}

#[async_trait]
impl ArtifactGetter for WicketdArtifactStore {
    async fn get(&self, id: &ArtifactId) -> Option<Body> {
        // This is a test artifact name used by the installinator.
        if id.name == "__installinator-test" {
            // For testing, the version is the size of the artifact.
            let size: usize = id
                        .version
                        .parse()
                        .map_err(|err| {
                            slog::warn!(
                                self.log,
                                "for installinator-test, version should be a usize indicating the size but found {}: {err}",
                                id.version
                            );
                        })
                        .ok()?;
            let mut bytes = BytesMut::with_capacity(size);
            bytes.put_bytes(0, size);
            return Some(Body::from(bytes.freeze()));
        }

        let buf_list = self.get(id)?;
        // Return the list as a stream of bytes.
        Some(Body::wrap_stream(stream::iter(
            buf_list.into_iter().map(|bytes| Ok::<_, Infallible>(bytes)),
        )))
    }

    async fn get_by_hash(&self, id: &ArtifactHashId) -> Option<Body> {
        let buf_list = self.get_by_hash(id)?;
        Some(Body::wrap_stream(stream::iter(
            buf_list.into_iter().map(|bytes| Ok::<_, Infallible>(bytes)),
        )))
    }
}

#[derive(Debug, Default)]
struct TufRepoArtifacts {
    by_id: HashMap<ArtifactId, BufList>,
    by_hash: HashMap<ArtifactHashId, BufList>,
}

impl TufRepoArtifacts {
    fn from_zip(
        zip_bytes: &[u8],
        log: &slog::Logger,
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

        // TODO: build out update planner here

        Ok(TufRepoArtifacts { by_id, by_hash })
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

    #[error(
        "duplicate hash entries found in artifacts.json for kind `{}`, hash `{}`", .0.kind, .0.hash
    )]
    DuplicateHashEntry(ArtifactHashId),
}

impl RepositoryError {
    fn to_http_error(&self) -> HttpError {
        // TODO: add better errors than just 503
        HttpError::for_unavail(None, DisplayErrorChain::new(self).to_string())
    }
}
