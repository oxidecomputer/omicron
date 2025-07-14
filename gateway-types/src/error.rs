// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::ErrorStatusCode;
use dropshot::HttpError;
use dropshot::HttpResponseError;
use schemars::JsonSchema;
use serde::Serialize;

#[derive(Debug, Serialize, JsonSchema, thiserror::Error)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ComponentFlashError {
    #[error("hash needs to be calculated")]
    HashUncalculated,
    #[error("hash needs to be recalculated")]
    HashStale,
    #[error("hash is currently being calculated; try again shortly")]
    HashInProgress,
    #[error("{internal_message}")]
    Other {
        message: String,
        error_code: Option<String>,

        // Skip serializing these fields, as they are used for the
        // `fmt::Display` implementation and for determining the status
        // code, respectively, rather than included in the response body:
        #[serde(skip)]
        internal_message: String,
        #[serde(skip)]
        status: ErrorStatusCode,
    },
}

impl HttpResponseError for ComponentFlashError {
    fn status_code(&self) -> ErrorStatusCode {
        match self {
            Self::HashUncalculated | Self::HashStale => {
                ErrorStatusCode::BAD_REQUEST
            }
            Self::HashInProgress => ErrorStatusCode::SERVICE_UNAVAILABLE,
            ComponentFlashError::Other { status, .. } => *status,
        }
    }
}

impl From<HttpError> for ComponentFlashError {
    fn from(error: HttpError) -> Self {
        Self::Other {
            message: error.external_message,
            internal_message: error.internal_message,
            status: error.status_code,
            error_code: error.error_code,
        }
    }
}
