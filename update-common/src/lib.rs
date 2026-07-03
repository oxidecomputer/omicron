// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common update types and code shared between wicketd and Nexus.

pub mod artifacts;
pub mod errors;

use std::error::Error as _;

use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use tufaceous_v2::error::Error;
use tufaceous_v2::error::ErrorKind;

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
