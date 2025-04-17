// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Makes artifact contents available for use in updates

use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use slog::o;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use thiserror::Error;
use tokio::{io::AsyncWriteExt, sync::watch};
use tufaceous_artifact::ArtifactHash;

type RepoDepotError = repo_depot_client::Error<repo_depot_client::types::Error>;

/// Makes update artifact contents available to consumers that need it
// This implementation is currently very minimal.  It doesn't actually cache and
// doesn't avoid concurrent fetches for the same object.
pub struct ArtifactCache {
    log: slog::Logger,
    repo_depot_backends: watch::Receiver<qorb::resolver::AllBackends>,
    next: AtomicUsize,
}

impl ArtifactCache {
    pub fn new(
        log: slog::Logger,
        repo_depot_backends: watch::Receiver<qorb::resolver::AllBackends>,
    ) -> ArtifactCache {
        ArtifactCache { log, repo_depot_backends, next: AtomicUsize::new(0) }
    }

    /// Retrieve the entire contents of the artifact identified by `hash`
    ///
    /// Since this will buffer the whole artifact in memory, this should only be
    /// used for artifacts known to be relatively small.
    pub async fn artifact_contents(
        &self,
        hash: &ArtifactHash,
    ) -> Result<Vec<u8>, ArtifactCacheError> {
        let client = self.client()?;
        let writer = std::io::Cursor::new(Vec::new());
        let byte_stream = client
            .artifact_get_by_sha256(&hash.to_string())
            .await?
            .into_inner()
            .into_inner();
        let mut sha256 = Sha256::new();
        let mut nbytes = 0;
        let writer = byte_stream
            .map_err(ArtifactCacheError::Read)
            .try_fold(writer, |mut writer, chunk| {
                nbytes += chunk.len();
                sha256.update(&chunk);
                async move {
                    writer
                        .write_all(&chunk)
                        .await
                        .map_err(ArtifactCacheError::Buffer)?;
                    Ok(writer)
                }
            })
            .await?;
        let buffer = writer.into_inner();
        let digest = sha256.finalize();
        if digest.as_slice() != hash.as_ref() {
            return Err(ArtifactCacheError::HashMismatch {
                nbytes,
                found: ArtifactHash(digest.into()),
                expected: *hash,
            });
        }

        Ok(buffer)
    }

    fn client(&self) -> Result<repo_depot_client::Client, ArtifactCacheError> {
        // It's important that we drop the borrowed value before returning so
        // that we don't keep the watch channel locked.
        //
        // "next" is used to try to avoid re-using the same client every time.
        // But it's not critical that we go in any particular order.
        let idx = self.next.fetch_add(1, Ordering::SeqCst);
        let clients = self.repo_depot_backends.borrow();
        if clients.is_empty() {
            Err(ArtifactCacheError::NoClients)
        } else {
            let addresses: Vec<_> = clients.values().collect();
            let addr = addresses[idx % addresses.len()];
            let url = format!("http://{}", addr.address);
            let log = self.log.new(o!("repo_depot_url" => url.clone()));
            Ok(repo_depot_client::Client::new(&url, log))
        }
    }
}

#[derive(Debug, Error)]
pub enum ArtifactCacheError {
    #[error("no repo depot clients available")]
    NoClients,

    #[error("failed to fetch artifact")]
    Fetch(#[from] RepoDepotError),

    #[error("reading artifact")]
    Read(reqwest::Error),

    #[error("buffering data for artifact")]
    Buffer(std::io::Error),

    #[error(
        "artifact hash mismatch (read {nbytes} bytes, expected {expected}, \
         found {found})"
    )]
    HashMismatch { expected: ArtifactHash, found: ArtifactHash, nbytes: usize },
}
