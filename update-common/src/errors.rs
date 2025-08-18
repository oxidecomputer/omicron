// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error types for this crate.

use camino::Utf8PathBuf;
use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use omicron_common::update::ArtifactId;
use slog::error;
use thiserror::Error;
use tufaceous_artifact::{
    ArtifactHashId, ArtifactKind, ArtifactVersion, KnownArtifactKind,
};

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("error opening archive")]
    OpenArchive(#[source] anyhow::Error),

    #[error("error creating temporary directory")]
    TempDirCreate(#[source] std::io::Error),

    #[error("error creating temporary file")]
    TempFileCreate(#[source] std::io::Error),

    #[error("error reading chunk off of input stream")]
    ReadChunkFromStream(#[source] HttpError),

    #[error("error writing to temporary file")]
    TempFileWrite(#[source] std::io::Error),

    #[error("error flushing temporary file")]
    TempFileFlush(#[source] std::io::Error),

    #[error("error creating temporary file in {path}")]
    NamedTempFileCreate {
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
        error: Box<tough::schema::Error>,
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

    #[error("error reading artifact of kind `{kind}` from repository")]
    ReadArtifact {
        kind: ArtifactKind,
        #[source]
        error: Box<tough::error::Error>,
    },

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

    #[error("multiple installinator documents found")]
    DuplicateInstallinatorDocument,

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
    #[error("error reading name from hubris caboose of {id:?}")]
    ReadHubrisCabooseName {
        id: ArtifactId,
        #[source]
        error: hubtools::CabooseError,
    },
    #[error("error reading board from hubris caboose of {id:?}")]
    ReadHubrisCabooseBoard {
        id: ArtifactId,
        #[source]
        error: hubtools::CabooseError,
    },
    #[error("error reading sign from hubris caboose of {id:?}")]
    ReadHubrisCabooseSign {
        id: ArtifactId,
        #[source]
        error: hubtools::CabooseError,
    },

    #[error("error reading board from hubris caboose of {0:?}: non-utf8 value")]
    ReadHubrisCabooseBoardUtf8(ArtifactId),

    #[error("error reading name from hubris caboose of {0:?}: non-utf8 value")]
    ReadHubrisCabooseNameUtf8(ArtifactId),

    #[error("missing sign from hubris caboose of {0:?}")]
    MissingHubrisCabooseSign(ArtifactId),

    #[error("missing artifact of kind `{0:?}`")]
    MissingArtifactKind(KnownArtifactKind),

    #[error(
        "muliple versions present for artifact of kind `{kind:?}`: {v1}, {v2}"
    )]
    MultipleVersionsPresent {
        kind: KnownArtifactKind,
        v1: ArtifactVersion,
        v2: ArtifactVersion,
    },
    #[error("Caboose mismatch between A {a:?} and B {b:?}")]
    CabooseMismatch { a: String, b: String },
    #[error(
        "muliple boards present for artifact of kind `{kind:?}`: {b1}, {b2}"
    )]
    MultipleBoardsPresent { kind: KnownArtifactKind, b1: String, b2: String },
    #[error(
        "duplicate hash entries found in artifacts.json for kind `{}`, hash `{}`", .0.kind, .0.hash
    )]
    DuplicateHashEntry(ArtifactHashId),
    #[error("error creating reader stream")]
    CreateReaderStream(#[source] anyhow::Error),
    #[error("error reading extracted archive kind {}, hash {}", .artifact.kind, .artifact.hash)]
    ReadExtractedArchive {
        artifact: ArtifactHashId,
        #[source]
        error: std::io::Error,
    },
}

impl RepositoryError {
    pub fn to_http_error(&self) -> HttpError {
        let message = DisplayErrorChain::new(self).to_string();

        match self {
            // Errors we had that are unrelated to the contents of a repository
            // uploaded by a client.
            RepositoryError::TempDirCreate(_)
            | RepositoryError::TempFileCreate(_)
            | RepositoryError::TempFileWrite(_)
            | RepositoryError::TempFileFlush(_)
            | RepositoryError::NamedTempFileCreate { .. }
            | RepositoryError::ReadExtractedArchive { .. }
            | RepositoryError::CreateReaderStream { .. } => {
                HttpError::for_unavail(None, message)
            }

            // This error is bubbled up.
            RepositoryError::ReadChunkFromStream(error) => HttpError {
                status_code: error.status_code,
                error_code: error.error_code.clone(),
                external_message: error.external_message.clone(),
                internal_message: error.internal_message.clone(),
                headers: None,
            },

            // Errors that are definitely caused by bad repository contents.
            RepositoryError::DuplicateArtifactKind(_)
            | RepositoryError::DuplicateInstallinatorDocument
            | RepositoryError::LocateTarget { .. }
            | RepositoryError::TargetHashLength(_)
            | RepositoryError::MissingArtifactKind(_)
            | RepositoryError::MissingHubrisCabooseSign(_)
            | RepositoryError::MissingTarget(_)
            | RepositoryError::DuplicateHashEntry(_)
            | RepositoryError::DuplicateBoardEntry { .. }
            | RepositoryError::ParsingHubrisArchive { .. }
            | RepositoryError::ReadHubrisCaboose { .. }
            | RepositoryError::ReadHubrisCabooseBoard { .. }
            | RepositoryError::ReadHubrisCabooseName { .. }
            | RepositoryError::ReadHubrisCabooseSign { .. }
            | RepositoryError::ReadHubrisCabooseBoardUtf8(_)
            | RepositoryError::ReadHubrisCabooseNameUtf8(_)
            | RepositoryError::MultipleVersionsPresent { .. }
            | RepositoryError::MultipleBoardsPresent { .. }
            | RepositoryError::CabooseMismatch { .. } => {
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
            | RepositoryError::ReadArtifact { .. }
            | RepositoryError::CopyExtractedArtifact { .. } => {
                HttpError::for_bad_request(None, message)
            }
        }
    }
}
