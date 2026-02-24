// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common extension traits and code shared between wicketd and Nexus.
//!
//! As a general rule, anything related to reading repositories belongs in
//! Tufaceous. This crate exists to glue Tufaceous types to code that is either
//! Omicron-specific or doesn't belong as a dependency of Tufaceous.

use std::error::Error as _;

use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use omicron_common::update::TufRepoDescription;
use semver::Version;
use tufaceous::Repository;
use tufaceous::edit::ArtifactsExt;
use tufaceous::error::Error;
use tufaceous::error::ErrorKind;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::Artifacts;

pub trait TufRepoDescriptionExt: Sized {
    fn fake(system_version: Version) -> Result<Self, tufaceous::error::Error>;
}

impl TufRepoDescriptionExt for TufRepoDescription {
    fn fake(system_version: Version) -> Result<Self, tufaceous::error::Error> {
        let artifacts_version = system_version.to_string().parse()?;
        Ok(Self {
            artifacts: Artifacts::fake(artifacts_version)?,
            system_version,
            hash: None,
            file_name: None,
        })
    }
}

pub trait RepositoryExt {
    fn to_description(&self) -> TufRepoDescription;
}

impl RepositoryExt for Repository {
    fn to_description(&self) -> TufRepoDescription {
        TufRepoDescription {
            artifacts: self.artifacts().clone(),
            system_version: self.system_version().clone(),
            hash: self.archive_sha256().copied().map(ArtifactHash),
            file_name: self
                .archive_path()
                .and_then(|path| path.file_name())
                .map(|file_name| file_name.to_owned()),
        }
    }
}

pub trait ErrorExt {
    fn to_http_error(&self) -> HttpError;
}

impl ErrorExt for Error {
    fn to_http_error(&self) -> HttpError {
        self.0.to_http_error()
    }
}

impl ErrorExt for ErrorKind {
    fn to_http_error(&self) -> HttpError {
        // `HttpError` from reading a stream is bubbled up, if possible.
        if let Some(source) = self.source()
            && let Some(error) = source.downcast_ref::<HttpError>()
        {
            return HttpError {
                status_code: error.status_code,
                error_code: error.error_code.clone(),
                external_message: error.external_message.clone(),
                internal_message: error.internal_message.clone(),
                headers: error.headers.clone(),
            };
        }

        let message = DisplayErrorChain::new(self).to_string();
        if self.is_repository_error() {
            // This error is definitely because of bad repository contents.
            HttpError::for_bad_request(None, message)
        } else {
            // This error is probably not due to bad repository contents.
            HttpError::for_unavail(None, message)
        }
    }
}
