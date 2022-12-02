// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error handling facilities for the management gateway.

use std::borrow::Borrow;

use dropshot::HttpError;
use gateway_messages::SpError;
use gateway_sp_comms::error::Error as SpCommsError;
use gateway_sp_comms::error::SpCommunicationError;
use gateway_sp_comms::error::UpdateError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("websocket connection failure: {0}")]
    BadWebsocketConnection(&'static str),
    #[error(transparent)]
    CommunicationsError(#[from] SpCommsError),
}

impl From<Error> for HttpError {
    fn from(err: Error) -> Self {
        match err {
            Error::CommunicationsError(err) => http_err_from_comms_err(err),
            Error::BadWebsocketConnection(_) => HttpError::for_bad_request(
                Some("BadWebsocketConnection".to_string()),
                err.to_string(),
            ),
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
        SpCommsError::SpCommunicationFailed(SpCommunicationError::SpError(
            SpError::SerialConsoleAlreadyAttached,
        )) => HttpError::for_bad_request(
            Some("SerialConsoleAttached".to_string()),
            err.to_string(),
        ),
        SpCommsError::SpCommunicationFailed(SpCommunicationError::SpError(
            SpError::RequestUnsupportedForSp,
        )) => HttpError::for_bad_request(
            Some("RequestUnsupportedForSp".to_string()),
            err.to_string(),
        ),
        SpCommsError::SpCommunicationFailed(SpCommunicationError::SpError(
            SpError::RequestUnsupportedForComponent,
        )) => HttpError::for_bad_request(
            Some("RequestUnsupportedForComponent".to_string()),
            err.to_string(),
        ),
        SpCommsError::SpCommunicationFailed(SpCommunicationError::SpError(
            SpError::InvalidSlotForComponent,
        )) => HttpError::for_bad_request(
            Some("InvalidSlotForComponent".to_string()),
            err.to_string(),
        ),
        SpCommsError::UpdateFailed(UpdateError::ImageTooLarge) => {
            HttpError::for_bad_request(
                Some("ImageTooLarge".to_string()),
                err.to_string(),
            )
        }
        SpCommsError::UpdateFailed(UpdateError::Communication(
            SpCommunicationError::SpError(SpError::UpdateSlotBusy),
        )) => HttpError::for_unavail(
            Some("UpdateSlotBusy".to_string()),
            err.to_string(),
        ),
        SpCommsError::UpdateFailed(UpdateError::Communication(
            SpCommunicationError::SpError(SpError::UpdateInProgress { .. }),
        )) => HttpError::for_unavail(
            Some("UpdateInProgress".to_string()),
            err.to_string(),
        ),
        SpCommsError::DiscoveryNotYetComplete => HttpError::for_unavail(
            Some("DiscoveryNotYetComplete".to_string()),
            err.to_string(),
        ),
        SpCommsError::SpAddressUnknown(_)
        | SpCommsError::DiscoveryFailed { .. }
        | SpCommsError::Timeout { .. }
        | SpCommsError::LocalIgnitionControllerAddressUnknown
        | SpCommsError::SpCommunicationFailed(_)
        | SpCommsError::UpdateFailed(_) => {
            HttpError::for_internal_error(err.to_string())
        }
    }
}
