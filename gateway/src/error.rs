// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error handling facilities for the management gateway.

use std::borrow::Borrow;

use dropshot::HttpError;
use gateway_sp_comms::error::Error as SpCommsError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invalid page token ({0})")]
    InvalidPageToken(InvalidPageToken),
    #[error(transparent)]
    CommunicationsError(#[from] SpCommsError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidPageToken {
    #[error("no such ID")]
    NoSuchId,
    #[error("invalid value for last seen item")]
    InvalidLastSeenItem,
}

impl From<Error> for HttpError {
    fn from(err: Error) -> Self {
        match err {
            Error::InvalidPageToken(_) => HttpError::for_bad_request(
                Some("InvalidPageToken".to_string()),
                err.to_string(),
            ),
            Error::CommunicationsError(err) => http_err_from_comms_err(err),
        }
    }
}

pub(crate) fn http_err_from_comms_err<E>(err: E) -> HttpError
where
    E: Borrow<SpCommsError>,
{
    let err = err.borrow();
    match err {
        SpCommsError::SpDoesNotExist(_) => HttpError::for_bad_request(
            Some("InvalidSp".to_string()),
            err.to_string(),
        ),
        SpCommsError::SpAddressUnknown(_)
        | SpCommsError::Timeout
        | SpCommsError::SpCommunicationFailed(_) => {
            HttpError::for_internal_error(err.to_string())
        }
    }
}
