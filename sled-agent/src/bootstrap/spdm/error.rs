// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Wrap errors returned from the `spdm` crate and std::io::Error.

use spdm::{requester::RequesterError, responder::ResponderError};
use thiserror::Error;

/// Describes errors that arise from use of the SPDM protocol library
#[derive(Error, Debug)]
pub enum SpdmError {
    #[error("requester error: {0}")]
    Requester(RequesterError),

    #[error("responder error: {0}")]
    Responder(ResponderError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("invalid state transition: expected {expected}, got {got}")]
    InvalidState { expected: &'static str, got: &'static str },

    #[error("timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
}

impl From<RequesterError> for SpdmError {
    fn from(e: RequesterError) -> Self {
        SpdmError::Requester(e)
    }
}

impl From<ResponderError> for SpdmError {
    fn from(e: ResponderError) -> Self {
        SpdmError::Responder(e)
    }
}
