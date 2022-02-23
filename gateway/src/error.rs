// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error handling facilities for the management gateway.

use crate::http_entrypoints::SpIdentifier;
use dropshot::HttpError;
use serde::{Deserialize, Serialize};

/// An error that can be generated within the gateway.
#[derive(Debug, Deserialize, thiserror::Error, PartialEq, Serialize)]
pub(crate) enum Error {
    /// A requested SP does not exist.
    ///
    /// This is not the same as the requested SP target being offline; this
    /// error indicates a fatal, invalid request (e.g., asing for the SP on the
    /// 17th switch when there are only two switches).
    #[error("SP {} (of type {:?}) does not exist", .0.slot, .0.typ)]
    SpDoesNotExist(SpIdentifier),

    /// The system encountered an unhandled operational error.
    #[error("internal error: {internal_message}")]
    InternalError { internal_message: String },
}

impl From<Error> for HttpError {
    fn from(err: Error) -> Self {
        match err {
            Error::SpDoesNotExist(_) => {
                // TODO is this error code okay?
                HttpError::for_bad_request(
                    Some(String::from("SpDoesNotExist")),
                    err.to_string(),
                )
            }
            Error::InternalError { internal_message } => {
                HttpError::for_internal_error(internal_message)
            }
        }
    }
}
