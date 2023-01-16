// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error handling facilities for the management gateway.

use crate::management_switch::SpIdentifier;
use dropshot::HttpError;
use gateway_messages::SpError;
use gateway_sp_comms::error::CommunicationError;
use gateway_sp_comms::error::UpdateError;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid configuration file: {}", .reasons.join(", "))]
    InvalidConfig { reasons: Vec<String> },
}

#[derive(Debug, Error)]
pub enum SpCommsError {
    #[error("discovery process not yet complete")]
    DiscoveryNotYetComplete,
    #[error("location discovery failed: {reason}")]
    DiscoveryFailed { reason: String },
    #[error("nonexistent SP (type {:?}, slot {})", .0.typ, .0.slot)]
    SpDoesNotExist(SpIdentifier),
    #[error(
        "unknown socket address for SP (type {:?}, slot {})",
        .0.typ,
        .0.slot,
    )]
    SpAddressUnknown(SpIdentifier),
    #[error(
        "timeout ({timeout:?}) elapsed communicating with {sp:?} on port {port}"
    )]
    Timeout { timeout: Duration, port: usize, sp: Option<SpIdentifier> },
    #[error("error communicating with SP: {0}")]
    SpCommunicationFailed(#[from] CommunicationError),
    #[error("updating SP failed: {0}")]
    UpdateFailed(#[from] UpdateError),
}

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
            Error::CommunicationsError(err) => err.into(),
            Error::BadWebsocketConnection(_) => HttpError::for_bad_request(
                Some("BadWebsocketConnection".to_string()),
                err.to_string(),
            ),
        }
    }
}

impl From<SpCommsError> for HttpError {
    fn from(err: SpCommsError) -> Self {
        match err {
            SpCommsError::SpDoesNotExist(_) => HttpError::for_bad_request(
                Some("InvalidSp".to_string()),
                err.to_string(),
            ),
            SpCommsError::SpCommunicationFailed(
                CommunicationError::SpError(
                    SpError::SerialConsoleAlreadyAttached,
                ),
            ) => HttpError::for_bad_request(
                Some("SerialConsoleAttached".to_string()),
                err.to_string(),
            ),
            SpCommsError::SpCommunicationFailed(
                CommunicationError::SpError(SpError::RequestUnsupportedForSp),
            ) => HttpError::for_bad_request(
                Some("RequestUnsupportedForSp".to_string()),
                err.to_string(),
            ),
            SpCommsError::SpCommunicationFailed(
                CommunicationError::SpError(
                    SpError::RequestUnsupportedForComponent,
                ),
            ) => HttpError::for_bad_request(
                Some("RequestUnsupportedForComponent".to_string()),
                err.to_string(),
            ),
            SpCommsError::SpCommunicationFailed(
                CommunicationError::SpError(SpError::InvalidSlotForComponent),
            ) => HttpError::for_bad_request(
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
                CommunicationError::SpError(SpError::UpdateSlotBusy),
            )) => HttpError::for_unavail(
                Some("UpdateSlotBusy".to_string()),
                err.to_string(),
            ),
            SpCommsError::UpdateFailed(UpdateError::Communication(
                CommunicationError::SpError(SpError::UpdateInProgress {
                    ..
                }),
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
            | SpCommsError::SpCommunicationFailed(_)
            | SpCommsError::UpdateFailed(_) => {
                HttpError::for_internal_error(err.to_string())
            }
        }
    }
}
