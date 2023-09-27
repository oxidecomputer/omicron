// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::error::RepositoryError;
use anyhow::Context;
use camino::Utf8PathBuf;
use camino_tempfile::NamedUtf8TempFile;
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
///
/// Note that although this type implements `Clone` and that cloning is
/// relatively cheap, it has additional implications on filesystem cleanup.
/// `ExtractedArtifactDataHandle`s point to a file in a temporary directory
/// created when a TUF repo is uploaded. That directory contains _all_
/// extracted artifacts from the TUF repo, and the directory will not be
/// cleaned up until all `ExtractedArtifactDataHandle`s that refer to files
/// inside it have been dropped. Therefore, one must be careful not to squirrel
/// away unneeded clones of `ExtractedArtifactDataHandle`s: only clone this in
/// contexts where you need the data and need the temporary directory containing
/// it to stick around.
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
/// It provides methods to copy artifacts into the tempdir (`store` and the
/// combo of `new_tempfile` + `store_tempfile`) that return
/// `ExtractedArtifactDataHandle`. The handles keep shared references to the
/// `Utf8TempDir`, so it will not be removed until all handles are dropped
/// (e.g., when a new TUF repository is uploaded). The handles can be used to
/// on-demand read files that were copied into the temp dir during ingest.
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

    fn path_for_artifact(
        &self,
        artifact_hash_id: &ArtifactHashId,
    ) -> Utf8PathBuf {
        self.tempdir.path().join(format!("{}", artifact_hash_id.hash))
    }

    /// Copy from `reader` into our temp directory, returning a handle to the
    /// extracted artifact on success.
    pub(super) fn store(
        &mut self,
        artifact_hash_id: ArtifactHashId,
        mut reader: impl Read,
    ) -> Result<ExtractedArtifactDataHandle, RepositoryError> {
        let output_path = self.path_for_artifact(&artifact_hash_id);

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

    /// Create a new temporary file inside this temporary directory.
    ///
    /// As the returned file is written to, the data will be hashed; once
    /// writing is complete, call [`ExtractedArtifacts::store_tempfile()`] to
    /// persist the temporary file into an [`ExtractedArtifactDataHandle()`].
    pub(super) fn new_tempfile(
        &self,
    ) -> Result<HashingNamedUtf8TempFile, RepositoryError> {
        let file = NamedUtf8TempFile::new_in(self.tempdir.path()).map_err(
            |error| RepositoryError::TempFileCreate {
                path: self.tempdir.path().to_owned(),
                error,
            },
        )?;
        Ok(HashingNamedUtf8TempFile {
            file: io::BufWriter::new(file),
            hasher: Sha256::new(),
            bytes_written: 0,
        })
    }

    /// Persist a temporary file that was returned by
    /// [`ExtractedArtifacts::new_tempfile()`] as an extracted artifact.
    pub(super) fn store_tempfile(
        &self,
        kind: ArtifactKind,
        file: HashingNamedUtf8TempFile,
    ) -> Result<ExtractedArtifactDataHandle, RepositoryError> {
        let HashingNamedUtf8TempFile { file, hasher, bytes_written } = file;

        // We don't need to `.flush()` explicitly: `into_inner()` does that for
        // us.
        let file = file
            .into_inner()
            .context("failed to flush temp file")
            .map_err(|error| RepositoryError::CopyExtractedArtifact {
                kind: kind.clone(),
                error,
            })?;

        let hash = ArtifactHash(hasher.finalize().into());
        let artifact_hash_id = ArtifactHashId { kind, hash };
        let output_path = self.path_for_artifact(&artifact_hash_id);
        file.persist(&output_path)
            .map_err(|error| error.error)
            .with_context(|| {
                format!("failed to persist temp file to {output_path}")
            })
            .map_err(|error| RepositoryError::CopyExtractedArtifact {
                kind: artifact_hash_id.kind.clone(),
                error,
            })?;

        Ok(ExtractedArtifactDataHandle {
            tempdir: Arc::clone(&self.tempdir),
            file_size: bytes_written,
            hash_id: artifact_hash_id,
        })
    }
}

fn path_for_artifact(
    tempdir: &Utf8TempDir,
    artifact_hash_id: &ArtifactHashId,
) -> Utf8PathBuf {
    tempdir.path().join(format!("{}", artifact_hash_id.hash))
}

// Wrapper around a `NamedUtf8TempFile` that hashes contents as they're written.
pub(super) struct HashingNamedUtf8TempFile {
    file: io::BufWriter<NamedUtf8TempFile>,
    hasher: Sha256,
    bytes_written: usize,
}

impl Write for HashingNamedUtf8TempFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.file.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.bytes_written += n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
