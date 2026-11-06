// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Changes in this version:
//!
//! * Add the [`SpComponentResetError::ResetSpOfLocalSwitch`] variant.

use dropshot::ErrorStatusCode;
use dropshot::HttpError;
use dropshot::HttpResponseError;
use schemars::JsonSchema;
use serde::Serialize;

use crate::v1;

/// The error type returned by the `sp_component_reset()` MGS endpoint.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, JsonSchema, thiserror::Error,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SpComponentResetError {
    /// MGS refuses to reset its own sled's SP.
    #[error("cannot reset SP of the sled hosting MGS")]
    ResetSpOfLocalSled,

    /// MGS refuses to reset its own sled's connected switch's SP.
    #[error("cannot reset SP of the switch connected to the sled hosting MGS")]
    ResetSpOfLocalSwitch,

    /// Other dropshot errors.
    #[error("{internal_message}")]
    Other {
        message: String,
        error_code: Option<String>,

        // Skip serializing these fields, as they are used for the
        // `fmt::Display` implementation and for determining the status code,
        // respectively, rather than included in the response body:
        #[serde(skip)]
        internal_message: String,
        #[serde(skip)]
        status: ErrorStatusCode,
    },
}

impl From<SpComponentResetError> for v1::update::SpComponentResetError {
    fn from(err: SpComponentResetError) -> Self {
        match err {
            SpComponentResetError::ResetSpOfLocalSled
            | SpComponentResetError::ResetSpOfLocalSwitch => {
                Self::ResetSpOfLocalSled
            }
            SpComponentResetError::Other {
                message,
                error_code,
                internal_message,
                status,
            } => Self::Other { message, error_code, internal_message, status },
        }
    }
}

impl HttpResponseError for SpComponentResetError {
    fn status_code(&self) -> ErrorStatusCode {
        match self {
            SpComponentResetError::ResetSpOfLocalSled
            | SpComponentResetError::ResetSpOfLocalSwitch => {
                ErrorStatusCode::BAD_REQUEST
            }
            SpComponentResetError::Other { status, .. } => *status,
        }
    }
}

impl From<HttpError> for SpComponentResetError {
    fn from(err: HttpError) -> Self {
        Self::Other {
            message: err.external_message,
            error_code: err.error_code,
            internal_message: err.internal_message,
            status: err.status_code,
        }
    }
}
