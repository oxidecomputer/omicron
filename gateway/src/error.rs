// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error handling facilities for the management gateway.

use crate::management_switch::SpIdentifier;
use anyhow::anyhow;
use dropshot::HttpError;
use gateway_messages::SpError;
pub use gateway_sp_comms::error::CommunicationError;
use gateway_sp_comms::error::UpdateError;
use gateway_sp_comms::BindError;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("invalid configuration file: {}", .reasons.join(", "))]
    InvalidConfig { reasons: Vec<String> },

    #[error(transparent)]
    BindError(#[from] BindError),
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

impl From<SpCommsError> for HttpError {
    fn from(err: SpCommsError) -> Self {
        match err {
            SpCommsError::SpDoesNotExist(_) => HttpError::for_bad_request(
                Some("InvalidSp".to_string()),
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::SpCommunicationFailed(
                CommunicationError::SpError(
                    SpError::SerialConsoleAlreadyAttached,
                ),
            ) => HttpError::for_bad_request(
                Some("SerialConsoleAttached".to_string()),
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::SpCommunicationFailed(
                CommunicationError::SpError(SpError::RequestUnsupportedForSp),
            ) => HttpError::for_bad_request(
                Some("RequestUnsupportedForSp".to_string()),
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::SpCommunicationFailed(
                CommunicationError::SpError(
                    SpError::RequestUnsupportedForComponent,
                ),
            ) => HttpError::for_bad_request(
                Some("RequestUnsupportedForComponent".to_string()),
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::SpCommunicationFailed(
                CommunicationError::SpError(SpError::InvalidSlotForComponent),
            ) => HttpError::for_bad_request(
                Some("InvalidSlotForComponent".to_string()),
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::UpdateFailed(UpdateError::ImageTooLarge) => {
                HttpError::for_bad_request(
                    Some("ImageTooLarge".to_string()),
                    format!("{:#}", anyhow!(err)),
                )
            }
            SpCommsError::UpdateFailed(UpdateError::Communication(
                CommunicationError::SpError(SpError::UpdateSlotBusy),
            )) => http_err_with_message(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "UpdateSlotBusy",
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::UpdateFailed(UpdateError::Communication(
                CommunicationError::SpError(SpError::UpdateInProgress {
                    ..
                }),
            )) => http_err_with_message(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "UpdateInProgress",
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::DiscoveryNotYetComplete => http_err_with_message(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "DiscoveryNotYetComplete",
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::SpAddressUnknown(_) => http_err_with_message(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "SpAddressUnknown",
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::DiscoveryFailed { .. } => http_err_with_message(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "DiscoveryFailed ",
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::Timeout { .. } => http_err_with_message(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "Timeout ",
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::SpCommunicationFailed(_) => http_err_with_message(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "SpCommunicationFailed",
                format!("{:#}", anyhow!(err)),
            ),
            SpCommsError::UpdateFailed(_) => http_err_with_message(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "UpdateFailed",
                format!("{:#}", anyhow!(err)),
            ),
        }
    }
}

// Helper function to return an `HttpError` with the same internal and external
// message. MGS is an "internal" service - even when we return a 500-level
// status code, we want to give our caller some information about what is going
// wrong (e.g., we timed out waiting for an SP).
pub(crate) fn http_err_with_message(
    status_code: http::StatusCode,
    error_code: &str,
    message: String,
) -> HttpError {
    HttpError {
        status_code,
        error_code: Some(error_code.to_string()),
        external_message: message.clone(),
        internal_message: message,
    }
}
