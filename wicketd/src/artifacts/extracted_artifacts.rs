// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::RepositoryError;
use anyhow::Context;
use bytes::Bytes;
use camino::Utf8PathBuf;
use camino_tempfile::Utf8TempDir;
use omicron_common::update::ArtifactHash;
use omicron_common::update::ArtifactHashId;
use omicron_common::update::ArtifactKind;
use sha2::Digest;
use sha2::Sha256;
use slog::info;
use slog::Logger;
use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

/// Handle to the data of an extracted artifact.
///
/// This does not contain the actual data; use `reader_stream()` to get a new
/// handle to the underlying file to read it on demand.
#[derive(Debug, Clone)]
pub(crate) struct ExtractedArtifactDataHandle {
    tempdir: Arc<Utf8TempDir>,
    file_size: usize,
    hash_id: ArtifactHashId,
}

// We implement this by hand to use `Arc::ptr_eq`, because `Utf8TempDir`
// (sensibly!) does not implement `PartialEq`. We only use it it in tests.
#[cfg(test)]
impl PartialEq for ExtractedArtifactDataHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.tempdir, &other.tempdir)
            && self.file_size == other.file_size
            && self.hash_id == other.hash_id
    }
}

#[cfg(test)]
impl Eq for ExtractedArtifactDataHandle {}

impl ExtractedArtifactDataHandle {
    /// File size of this artifact in bytes.
    pub(super) fn file_size(&self) -> usize {
        self.file_size
    }

    pub(super) fn read_to_vec(&self) -> Result<Vec<u8>, RepositoryError> {
        let path = path_for_artifact(&self.tempdir, &self.hash_id);
        std::fs::read(&path).map_err(|error| {
            RepositoryError::ReadExtractedArtifact { path, error }
        })
    }

    pub(crate) fn hash(&self) -> ArtifactHash {
        self.hash_id.hash
    }

    /// Async stream to read the contents of this artifact on demand.
    ///
    /// This can fail due to I/O errors outside our control (e.g., something
    /// removed the contents of our temporary directory).
    pub(crate) async fn reader_stream(
        &self,
    ) -> anyhow::Result<ReaderStream<impl AsyncRead>> {
        let path = path_for_artifact(&self.tempdir, &self.hash_id);

        let file = tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("failed to open {path}"))?;

        Ok(ReaderStream::new(file))
    }
}

/// `ExtractedArtifacts` is a temporary wrapper around a `Utf8TempDir` for use
/// when ingesting a new TUF repository.
///
/// It provides methods to copy artifacts into the tempdir (`store` and
/// `store_and_hash`) that return `ExtractedArtifactDataHandle`. The handles
/// keep shared references to the `Utf8TempDir`, so it will not be removed until
/// all handles are dropped (e.g., when a new TUF repository is uploaded). The
/// handles can be used to on-demand read files that were copied into the temp
/// dir during ingest.
#[derive(Debug)]
pub(crate) struct ExtractedArtifacts {
    // Directory in which we store extracted artifacts. This is currently a
    // single flat directory with files named by artifact hash; we don't expect
    // more than a few dozen files total, so no need to nest directories.
    tempdir: Arc<Utf8TempDir>,
}

impl ExtractedArtifacts {
    pub(super) fn new(log: &Logger) -> Result<Self, RepositoryError> {
        let tempdir = camino_tempfile::Builder::new()
            .prefix("wicketd-update-artifacts.")
            .tempdir()
            .map_err(RepositoryError::TempDirCreate)?;
        info!(
            log, "created directory to store extracted artifacts";
            "path" => %tempdir.path(),
        );
        Ok(Self { tempdir: Arc::new(tempdir) })
    }

    /// Copy from `reader` into our temp directory, returning a handle to the
    /// extracted artifact on success.
    pub(super) fn store(
        &mut self,
        artifact_hash_id: ArtifactHashId,
        mut reader: impl Read,
    ) -> Result<ExtractedArtifactDataHandle, RepositoryError> {
        let output_path =
            self.tempdir.path().join(format!("{}", artifact_hash_id.hash));

        let mut writer = BufWriter::new(
            File::create(&output_path)
                .with_context(|| {
                    format!("failed to create temp file {output_path}")
                })
                .map_err(|error| RepositoryError::CopyExtractedArtifact {
                    kind: artifact_hash_id.kind.clone(),
                    error,
                })?,
        );

        let file_size = io::copy(&mut reader, &mut writer)
            .with_context(|| format!("failed writing to {output_path}"))
            .map_err(|error| RepositoryError::CopyExtractedArtifact {
                kind: artifact_hash_id.kind.clone(),
                error,
            })? as usize;

        writer
            .flush()
            .with_context(|| format!("failed flushing {output_path}"))
            .map_err(|error| RepositoryError::CopyExtractedArtifact {
                kind: artifact_hash_id.kind.clone(),
                error,
            })?;

        Ok(ExtractedArtifactDataHandle {
            tempdir: Arc::clone(&self.tempdir),
            file_size,
            hash_id: artifact_hash_id,
        })
    }

    /// Copy from `bytes` into our temp directory while computing its artifact
    /// hash.
    pub(super) fn store_and_hash(
        &mut self,
        kind: ArtifactKind,
        data: &Bytes,
    ) -> Result<ExtractedArtifactDataHandle, RepositoryError> {
        let hash = ArtifactHash(Sha256::digest(data).into());
        let artifact_hash_id = ArtifactHashId { kind, hash };
        self.store(artifact_hash_id, io::Cursor::new(data))
    }
}

fn path_for_artifact(
    tempdir: &Utf8TempDir,
    artifact_hash_id: &ArtifactHashId,
) -> Utf8PathBuf {
    tempdir.path().join(format!("{}", artifact_hash_id.hash))
}
