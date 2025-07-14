// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error handling facilities for the management gateway.

use crate::management_switch::SpIdentifier;
use dropshot::HttpError;
use gateway_messages::HfError;
use gateway_messages::SpError;
use gateway_sp_comms::BindError;
pub use gateway_sp_comms::error::CommunicationError;
use gateway_sp_comms::error::UpdateError;
use gateway_types::error::ComponentFlashError;
use slog_error_chain::InlineErrorChain;
use slog_error_chain::SlogInlineError;
use std::error::Error;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error, SlogInlineError)]
pub enum StartupError {
    #[error("invalid configuration file: {}", .reasons.join(", "))]
    InvalidConfig { reasons: Vec<String> },

    #[error(transparent)]
    BindError(#[from] BindError),
}

#[derive(Debug, Error, SlogInlineError)]
pub enum SpCommsError {
    #[error(transparent)]
    Discovery(#[from] SpLookupError),
    #[error("unknown socket address for SP {0:?}")]
    SpAddressUnknown(SpIdentifier),
    #[error(
        "timeout ({timeout:?}) elapsed communicating with {sp:?} on port {port}"
    )]
    Timeout { timeout: Duration, port: usize, sp: Option<SpIdentifier> },
    #[error("error communicating with SP {sp:?}")]
    SpCommunicationFailed {
        sp: SpIdentifier,
        #[source]
        err: CommunicationError,
    },
    #[error("updating SP {sp:?} failed")]
    UpdateFailed {
        sp: SpIdentifier,
        #[source]
        err: UpdateError,
    },
}

#[derive(Debug, Error, SlogInlineError)]
pub enum EreportError {
    #[error("error communicating with SP {sp:?}")]
    SpCommunicationFailed {
        sp: SpIdentifier,
        #[source]
        err: CommunicationError,
    },
    #[error("error reading ereports from SP {sp:?}")]
    Ereport {
        sp: SpIdentifier,
        #[source]
        err: gateway_sp_comms::error::EreportError,
    },
}

/// Errors returned by attempts to look up a SP in the management switch's
/// discovery map.
#[derive(Debug, Error, SlogInlineError)]
pub enum SpLookupError {
    #[error("discovery process not yet complete")]
    DiscoveryNotYetComplete,
    #[error("location discovery failed: {reason}")]
    DiscoveryFailed { reason: String },
    #[error("nonexistent SP {0:?}")]
    SpDoesNotExist(SpIdentifier),
}

impl From<SpCommsError> for HttpError {
    fn from(error: SpCommsError) -> Self {
        match error {
            SpCommsError::Discovery(err) => HttpError::from(err),
            SpCommsError::SpCommunicationFailed {
                err:
                    CommunicationError::SpError(
                        SpError::SerialConsoleAlreadyAttached,
                    ),
                ..
            } => HttpError::for_bad_request(
                Some("SerialConsoleAttached".to_string()),
                InlineErrorChain::new(&error).to_string(),
            ),
            ref e @ SpCommsError::SpCommunicationFailed { ref err, .. } => {
                comms_error_to_http(&e, err)
            }
            SpCommsError::UpdateFailed {
                err: UpdateError::ImageTooLarge,
                ..
            } => HttpError::for_bad_request(
                Some("ImageTooLarge".to_string()),
                InlineErrorChain::new(&error).to_string(),
            ),
            SpCommsError::UpdateFailed {
                err:
                    UpdateError::Communication(CommunicationError::SpError(
                        SpError::UpdateSlotBusy,
                    )),
                ..
            } => http_err_with_message(
                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                "UpdateSlotBusy",
                InlineErrorChain::new(&error).to_string(),
            ),
            SpCommsError::UpdateFailed {
                err:
                    UpdateError::Communication(CommunicationError::SpError(
                        SpError::UpdateInProgress { .. },
                    )),
                ..
            } => http_err_with_message(
                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                "UpdateInProgress",
                InlineErrorChain::new(&error).to_string(),
            ),
            SpCommsError::SpAddressUnknown(_) => http_err_with_message(
                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                "SpAddressUnknown",
                InlineErrorChain::new(&error).to_string(),
            ),
            SpCommsError::Timeout { .. } => http_err_with_message(
                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                "Timeout ",
                InlineErrorChain::new(&error).to_string(),
            ),
            SpCommsError::UpdateFailed { .. } => http_err_with_message(
                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                "UpdateFailed",
                InlineErrorChain::new(&error).to_string(),
            ),
        }
    }
}

impl From<EreportError> for HttpError {
    fn from(error: EreportError) -> HttpError {
        match error {
            ref e @ EreportError::SpCommunicationFailed { ref err, .. } => {
                comms_error_to_http(&e, err)
            }
            ref e @ EreportError::Ereport { .. } => http_err_with_message(
                dropshot::ErrorStatusCode::INTERNAL_SERVER_ERROR,
                "EreportError",
                InlineErrorChain::new(&e).to_string(),
            ),
        }
    }
}

fn comms_error_to_http(
    error: &dyn Error,
    kind: &CommunicationError,
) -> HttpError {
    match kind {
        CommunicationError::SpError(SpError::SerialConsoleAlreadyAttached) => {
            HttpError::for_bad_request(
                Some("SerialConsoleAttached".to_string()),
                InlineErrorChain::new(&error).to_string(),
            )
        }
        CommunicationError::SpError(SpError::RequestUnsupportedForSp) => {
            HttpError::for_bad_request(
                Some("RequestUnsupportedForSp".to_string()),
                InlineErrorChain::new(&error).to_string(),
            )
        }
        CommunicationError::SpError(
            SpError::RequestUnsupportedForComponent,
        ) => HttpError::for_bad_request(
            Some("RequestUnsupportedForComponent".to_string()),
            InlineErrorChain::new(&error).to_string(),
        ),
        CommunicationError::SpError(SpError::InvalidSlotForComponent) => {
            HttpError::for_bad_request(
                Some("InvalidSlotForComponent".to_string()),
                InlineErrorChain::new(&error).to_string(),
            )
        }
        _ => http_err_with_message(
            dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
            "SpCommunicationFailed",
            InlineErrorChain::new(&error).to_string(),
        ),
    }
}

impl From<SpLookupError> for HttpError {
    fn from(error: SpLookupError) -> Self {
        match error {
            SpLookupError::SpDoesNotExist(_) => HttpError::for_bad_request(
                Some("InvalidSp".to_string()),
                InlineErrorChain::new(&error).to_string(),
            ),
            SpLookupError::DiscoveryNotYetComplete => http_err_with_message(
                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                "DiscoveryNotYetComplete",
                InlineErrorChain::new(&error).to_string(),
            ),
            SpLookupError::DiscoveryFailed { .. } => http_err_with_message(
                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                "DiscoveryFailed ",
                InlineErrorChain::new(&error).to_string(),
            ),
        }
    }
}

// Helper function to return an `HttpError` with the same internal and external
// message. MGS is an "internal" service - even when we return a 500-level
// status code, we want to give our caller some information about what is going
// wrong (e.g., we timed out waiting for an SP).
pub(crate) fn http_err_with_message(
    status_code: dropshot::ErrorStatusCode,
    error_code: &str,
    message: String,
) -> HttpError {
    HttpError {
        status_code,
        error_code: Some(error_code.to_string()),
        external_message: message.clone(),
        internal_message: message,
        headers: None,
    }
}

/// Convert a `CommunicationError` with a given `sp` into a
/// `ComponentFlashError`.
pub(crate) fn map_component_flash_error(
    sp: SpIdentifier,
    err: CommunicationError,
) -> ComponentFlashError {
    if let CommunicationError::SpError(err) = &err {
        if let SpError::Hf(err) = err {
            match err {
                HfError::HashUncalculated => {
                    return ComponentFlashError::HashUncalculated;
                }
                HfError::RecalculateHash => {
                    return ComponentFlashError::HashStale;
                }
                HfError::HashInProgress => {
                    return ComponentFlashError::HashInProgress;
                }
                HfError::NotMuxedToSp
                | HfError::BadAddress
                | HfError::QspiTimeout
                | HfError::QspiTransferError => (),
            }
        }
    }
    ComponentFlashError::from(HttpError::from(
        SpCommsError::SpCommunicationFailed { sp, err },
    ))
}
