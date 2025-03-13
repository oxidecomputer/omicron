// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Makes artifact contents available for use in updates

use futures::TryStreamExt;
use omicron_common::update::ArtifactHash;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

// XXX-dap want omdb-based introspection, control
// XXX-dap in an ideal world this would load everything it needs in the
// background.  Maybe this should go into a background task?

type RepoDepotError = repo_depot_client::Error<repo_depot_client::types::Error>;

pub struct ArtifactCache {
    client: repo_depot_client::Client,
    // XXX-dap actually cache
}

impl ArtifactCache {
    pub fn new(client: repo_depot_client::Client) -> ArtifactCache {
        ArtifactCache { client }
    }

    pub async fn artifact_contents(
        &self,
        hash: &ArtifactHash,
    ) -> Result<Vec<u8>, ArtifactCacheError> {
        // XXX-dap want a mechanism to avoid fetching multiple times
        // concurrently.
        let writer = std::io::Cursor::new(Vec::new());
        // XXX-dap is there a better way to do this?  This is cribbed from
        // sled-agent.
        let byte_stream = self
            .client
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
                expected: hash.clone(),
            });
        }

        Ok(buffer)
    }
}

#[derive(Debug, Error)]
pub enum ArtifactCacheError {
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
