// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error handling facilities for the Oxide control plane
//!
//! For HTTP-level error handling, see Dropshot.

use crate::api::external::Name;
use crate::api::external::ResourceType;
use dropshot::HttpError;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;
use uuid::Uuid;

/// An error that can be generated within a control plane component
///
/// These may be generated while handling a client request or as part of
/// background operation.  When generated as part of an HTTP request, an
/// `Error` will be converted into an HTTP error as one of the last steps in
/// processing the request.  This allows most of the system to remain agnostic to
/// the transport with which the system communicates with clients.
///
/// General best practices for error design apply here.  Where possible, we want
/// to reuse existing variants rather than inventing new ones to distinguish
/// cases that no programmatic consumer needs to distinguish.
#[derive(Clone, Debug, Deserialize, thiserror::Error, PartialEq, Serialize)]
pub enum Error {
    /// An object needed as part of this operation was not found.
    #[error("Object (of type {lookup_type:?}) not found: {type_name}")]
    ObjectNotFound { type_name: ResourceType, lookup_type: LookupType },
    /// An object already exists with the specified name or identifier.
    #[error("Object (of type {type_name:?}) already exists: {object_name}")]
    ObjectAlreadyExists { type_name: ResourceType, object_name: String },
    /// The request was well-formed, but the operation cannot be completed given
    /// the current state of the system.
    #[error("Invalid Request: {message}")]
    InvalidRequest { message: String },
    /// Authentication credentials were required but either missing or invalid.
    /// The HTTP status code is called "Unauthorized", but it's more accurate to
    /// call it "Unauthenticated".
    #[error("Missing or invalid credentials")]
    Unauthenticated { internal_message: String },
    /// The specified input field is not valid.
    #[error("Invalid Value: {label}, {message}")]
    InvalidValue { label: String, message: String },
    /// The request is not authorized to perform the requested operation.
    #[error("Forbidden")]
    Forbidden,

    /// The system encountered an unhandled operational error.
    #[error("Internal Error: {internal_message}")]
    InternalError { internal_message: String },
    /// The system (or part of it) is unavailable.
    #[error("Service Unavailable: {internal_message}")]
    ServiceUnavailable { internal_message: String },
    /// Method Not Allowed
    #[error("Method Not Allowed: {internal_message}")]
    MethodNotAllowed { internal_message: String },

    #[error("Type version mismatch! {internal_message}")]
    TypeVersionMismatch { internal_message: String },
}

/// Indicates how an object was looked up (for an `ObjectNotFound` error)
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum LookupType {
    /// a specific name was requested
    ByName(String),
    /// a specific id was requested
    ById(Uuid),
    /// a session token was requested
    BySessionToken(String),
    /// a specific id was requested with some composite type
    /// (caller summarizes it)
    ByCompositeId(String),
}

impl LookupType {
    /// Returns an ObjectNotFound error appropriate for the case where this
    /// lookup failed
    pub fn into_not_found(self, type_name: ResourceType) -> Error {
        Error::ObjectNotFound { type_name, lookup_type: self }
    }
}

impl From<&str> for LookupType {
    fn from(name: &str) -> Self {
        LookupType::ByName(name.to_owned())
    }
}

impl From<&Name> for LookupType {
    fn from(name: &Name) -> Self {
        LookupType::from(name.as_str())
    }
}

impl From<Uuid> for LookupType {
    fn from(uuid: Uuid) -> Self {
        LookupType::ById(uuid)
    }
}

impl Error {
    /// Returns whether the error is likely transient and could reasonably be
    /// retried
    pub fn retryable(&self) -> bool {
        match self {
            Error::ServiceUnavailable { .. } => true,

            Error::ObjectNotFound { .. }
            | Error::ObjectAlreadyExists { .. }
            | Error::Unauthenticated { .. }
            | Error::InvalidRequest { .. }
            | Error::InvalidValue { .. }
            | Error::Forbidden
            | Error::MethodNotAllowed { .. }
            | Error::InternalError { .. }
            | Error::TypeVersionMismatch { .. } => false,
        }
    }

    /// Generates an [`Error::ObjectNotFound`] error for a lookup by object
    /// name.
    pub fn not_found_by_name(type_name: ResourceType, name: &Name) -> Error {
        LookupType::from(name).into_not_found(type_name)
    }

    /// Generates an [`Error::ObjectNotFound`] error for a lookup by object id.
    pub fn not_found_by_id(type_name: ResourceType, id: &Uuid) -> Error {
        LookupType::ById(*id).into_not_found(type_name)
    }

    /// Generates an [`Error::InternalError`] error with the specific message
    ///
    /// InternalError should be used for operational conditions that should not
    /// happen but that we cannot reasonably handle at runtime (e.g.,
    /// deserializing a value from the database, or finding two records for
    /// something that is supposed to be unique).
    pub fn internal_error(internal_message: &str) -> Error {
        Error::InternalError { internal_message: internal_message.to_owned() }
    }

    /// Generates an [`Error::InvalidRequest`] error with the specific message
    ///
    /// This should be used for failures due possibly to invalid client input
    /// or malformed requests.
    pub fn invalid_request(message: &str) -> Error {
        Error::InvalidRequest { message: message.to_owned() }
    }

    /// Generates an [`Error::ServiceUnavailable`] error with the specific
    /// message
    ///
    /// This should be used for transient failures where the caller might be
    /// expected to retry.  Logic errors or other problems indicating that a
    /// retry would not work should probably be an InternalError (if it's a
    /// server problem) or InvalidRequest (if it's a client problem) instead.
    pub fn unavail(message: &str) -> Error {
        Error::ServiceUnavailable { internal_message: message.to_owned() }
    }

    /// Generates an [`Error::TypeVersionMismatch`] with a specific message.
    ///
    /// TypeVersionMismatch errors are a specific type of error arising from differences
    /// between types in different versions of Nexus. For example, a param
    /// struct enum and db struct enum may have the same variants for one
    /// version of Nexus, but different variants when one of those Nexus servers
    /// is upgraded. Deserializing from the database in the new Nexus would be
    /// ok, but in the old Nexus would otherwise be a 500.
    pub fn type_version_mismatch(message: &str) -> Error {
        Error::TypeVersionMismatch { internal_message: message.to_owned() }
    }

    /// Given an [`Error`] with an internal message, return the same error with
    /// `context` prepended to it to provide more context
    ///
    /// If the error has no internal message, then it is returned unchanged.
    pub fn internal_context<C>(self, context: C) -> Error
    where
        C: Display + Send + Sync + 'static,
    {
        match self {
            Error::ObjectNotFound { .. }
            | Error::ObjectAlreadyExists { .. }
            | Error::InvalidRequest { .. }
            | Error::InvalidValue { .. }
            | Error::Forbidden => self,
            Error::Unauthenticated { internal_message } => {
                Error::Unauthenticated {
                    internal_message: format!(
                        "{}: {}",
                        context, internal_message
                    ),
                }
            }
            Error::InternalError { internal_message } => Error::InternalError {
                internal_message: format!("{}: {}", context, internal_message),
            },
            Error::ServiceUnavailable { internal_message } => {
                Error::ServiceUnavailable {
                    internal_message: format!(
                        "{}: {}",
                        context, internal_message
                    ),
                }
            }
            Error::MethodNotAllowed { internal_message } => {
                Error::MethodNotAllowed {
                    internal_message: format!(
                        "{}: {}",
                        context, internal_message
                    ),
                }
            }
            Error::TypeVersionMismatch { internal_message } => {
                Error::TypeVersionMismatch {
                    internal_message: format!(
                        "{}: {}",
                        context, internal_message
                    ),
                }
            }
        }
    }
}

impl From<Error> for HttpError {
    /// Converts an `Error` error into an `HttpError`.  This defines how
    /// errors that are represented internally using `Error` are ultimately
    /// exposed to clients over HTTP.
    fn from(error: Error) -> HttpError {
        match error {
            Error::ObjectNotFound { type_name: t, lookup_type: lt } => {
                // TODO-cleanup is there a better way to express this?
                let (lookup_field, lookup_value) = match lt {
                    LookupType::ByName(name) => ("name", name),
                    LookupType::ById(id) => ("id", id.to_string()),
                    LookupType::ByCompositeId(label) => ("id", label),
                    LookupType::BySessionToken(token) => {
                        ("session token", token)
                    }
                };
                let message = format!(
                    "not found: {} with {} \"{}\"",
                    t, lookup_field, lookup_value
                );
                HttpError::for_client_error(
                    Some(String::from("ObjectNotFound")),
                    http::StatusCode::NOT_FOUND,
                    message,
                )
            }

            Error::ObjectAlreadyExists { type_name: t, object_name: n } => {
                let message = format!("already exists: {} \"{}\"", t, n);
                HttpError::for_bad_request(
                    Some(String::from("ObjectAlreadyExists")),
                    message,
                )
            }

            Error::Unauthenticated { internal_message } => HttpError {
                status_code: http::StatusCode::UNAUTHORIZED,
                // TODO-polish We may want to rethink this error code.  This is
                // what HTTP calls it, but it's confusing.
                error_code: Some(String::from("Unauthorized")),
                external_message: String::from(
                    "credentials missing or invalid",
                ),
                internal_message,
            },

            Error::InvalidRequest { message } => HttpError::for_bad_request(
                Some(String::from("InvalidRequest")),
                message,
            ),

            Error::InvalidValue { label, message } => {
                let message =
                    format!("unsupported value for \"{}\": {}", label, message);
                HttpError::for_bad_request(
                    Some(String::from("InvalidValue")),
                    message,
                )
            }

            // TODO: RFC-7231 requires that 405s generate an Accept header to describe
            // what methods are available in the response
            Error::MethodNotAllowed { internal_message } => {
                HttpError::for_client_error(
                    Some(String::from("MethodNotAllowed")),
                    http::StatusCode::METHOD_NOT_ALLOWED,
                    internal_message,
                )
            }

            Error::Forbidden => HttpError::for_client_error(
                Some(String::from("Forbidden")),
                http::StatusCode::FORBIDDEN,
                String::from("Forbidden"),
            ),

            Error::InternalError { internal_message } => {
                HttpError::for_internal_error(internal_message)
            }

            Error::ServiceUnavailable { internal_message } => {
                HttpError::for_unavail(
                    Some(String::from("ServiceNotAvailable")),
                    internal_message,
                )
            }

            Error::TypeVersionMismatch { internal_message } => {
                HttpError::for_internal_error(internal_message)
            }
        }
    }
}

pub trait ClientError: std::fmt::Debug {
    fn message(&self) -> String;
}

// TODO-security this `From` may give us a shortcut in situtations where we
// want more robust consideration of errors. For example, while some errors
// from other services may directly result in errors that percolate to the
// external client, others may require, for example, retries with an alternate
// service instance or additional interpretation to sanitize the output error.
// This should be removed to avoid leaking data.
impl<T: ClientError> From<progenitor::progenitor_client::Error<T>> for Error {
    fn from(e: progenitor::progenitor_client::Error<T>) -> Self {
        match e {
            // This error indicates that the inputs were not valid for this API
            // call. It's reflective of either a client-side programming error.
            progenitor::progenitor_client::Error::InvalidRequest(msg) => {
                Error::internal_error(&format!("InvalidRequest: {}", msg))
            }

            // This error indicates a problem with the request to the remote
            // service that did not result in an HTTP response code, but rather
            // pertained to local (i.e. client-side) encoding or network
            // communication.
            progenitor::progenitor_client::Error::CommunicationError(ee) => {
                Error::internal_error(&format!("CommunicationError: {}", ee))
            }

            // This error represents an expected error from the remote service.
            progenitor::progenitor_client::Error::ErrorResponse(rv) => {
                let message = rv.message();

                match rv.status() {
                    http::StatusCode::SERVICE_UNAVAILABLE => {
                        Error::unavail(&message)
                    }
                    status if status.is_client_error() => {
                        Error::invalid_request(&message)
                    }
                    _ => Error::internal_error(&message),
                }
            }

            // This error indicates that the body returned by the client didn't
            // match what was documented in the OpenAPI description for the
            // service. This could only happen for us in the case of a severe
            // logic/encoding bug in the remote service or due to a failure of
            // our version constraints (i.e. that the call was to a newer
            // service with an incompatible response).
            progenitor::progenitor_client::Error::InvalidResponsePayload(
                ee,
            ) => Error::internal_error(&format!(
                "InvalidResponsePayload: {}",
                ee,
            )),

            // This error indicates that the client generated a response code
            // that was not described in the OpenAPI description for the
            // service; this could be a success or failure response, but either
            // way it indicates a logic or version error as above.
            progenitor::progenitor_client::Error::UnexpectedResponse(r) => {
                Error::internal_error(&format!(
                    "UnexpectedResponse: status code {}",
                    r.status(),
                ))
            }
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::internal_error(&e.to_string())
    }
}

/// Like [`assert!`], except that instead of panicking, this function returns an
/// `Err(Error::InternalError)` with an appropriate message if the given
/// condition is not true.
#[macro_export]
macro_rules! bail_unless {
    ($cond:expr $(,)?) => {
        bail_unless!($cond, "failed runtime check: {:?}", stringify!($cond))
    };
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            Err($crate::api::external::Error::internal_error(&format!(
                $($arg)*)))?;
        }
    };
}

/// Implements a pattern similar to [`anyhow::Context`] for providing extra
/// context for internal error messages
///
/// Unlike `anyhow::Context`, this does not add a new Error to the cause chain.
/// It replaces the given Error with one that has the modified
/// `internal_message`.
///
/// If the given `Error` variant does not have an `internal_message`, then this
/// currently returns an equivalent Error to what was given, without prepending
/// anything to anything.  Future work could add internal context to all
/// variants.
///
/// ## Example
///
/// ```
/// use omicron_common::api::external::Error;
/// use omicron_common::api::external::InternalContext;
///
/// let error: Result<(), Error> = Err(Error::internal_error("boom"));
/// assert_eq!(
///     error.internal_context("uh-oh").unwrap_err().to_string(),
///     "Internal Error: uh-oh: boom"
/// );
/// ```
pub trait InternalContext<T> {
    fn internal_context<C>(self, s: C) -> Result<T, Error>
    where
        C: Display + Send + Sync + 'static;

    fn with_internal_context<C, F>(self, f: F) -> Result<T, Error>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C;
}

impl<T> InternalContext<T> for Result<T, Error> {
    fn internal_context<C>(self, context: C) -> Result<T, Error>
    where
        C: Display + Send + Sync + 'static,
    {
        self.map_err(|error| error.internal_context(context))
    }

    fn with_internal_context<C, F>(self, make_context: F) -> Result<T, Error>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        self.map_err(|error| error.internal_context(make_context()))
    }
}

#[cfg(test)]
mod test {
    use super::Error;
    use super::InternalContext;

    #[test]
    fn test_bail_unless() {
        #![allow(clippy::eq_op)]
        // Success cases
        let no_bail = || -> Result<(), Error> {
            bail_unless!(1 + 1 == 2, "wrong answer: {}", 3);
            Ok(())
        };
        let no_bail_label_args = || -> Result<(), Error> {
            bail_unless!(1 + 1 == 2, "wrong answer: {}", 3);
            Ok(())
        };
        assert_eq!(Ok(()), no_bail());
        assert_eq!(Ok(()), no_bail_label_args());

        // Failure cases
        let do_bail = || {
            bail_unless!(1 + 1 == 3);
            Ok(())
        };
        let do_bail_label = || {
            bail_unless!(1 + 1 == 3, "uh-oh");
            Ok(())
        };
        let do_bail_label_args = || {
            bail_unless!(1 + 1 == 3, "wrong answer: {}", 3);
            Ok(())
        };

        let checks = [
            (do_bail(), "failed runtime check: \"1 + 1 == 3\""),
            (do_bail_label(), "uh-oh"),
            (do_bail_label_args(), "wrong answer: 3"),
        ];

        for (result, expected_message) in &checks {
            let error = result.as_ref().unwrap_err();
            if let Error::InternalError { internal_message } = error {
                assert_eq!(*expected_message, internal_message);
            } else {
                panic!("got something other than an InternalError");
            }
        }
    }

    #[test]
    fn test_context() {
        // test `internal_context()` and (separately) `InternalError` variant
        let error: Result<(), Error> = Err(Error::internal_error("boom"));
        match error.internal_context("uh-oh") {
            Err(Error::InternalError { internal_message }) => {
                assert_eq!(internal_message, "uh-oh: boom");
            }
            _ => panic!("returned wrong type"),
        };

        // test `with_internal_context()` and (separately) `ServiceUnavailable`
        // variant
        let error: Result<(), Error> = Err(Error::unavail("boom"));
        match error.with_internal_context(|| format!("uh-oh (#{:2})", 2)) {
            Err(Error::ServiceUnavailable { internal_message }) => {
                assert_eq!(internal_message, "uh-oh (# 2): boom");
            }
            _ => panic!("returned wrong type"),
        };

        // test using a variant that doesn't have an internal error
        let error: Result<(), Error> = Err(Error::Forbidden);
        assert!(matches!(error.internal_context("foo"), Err(Error::Forbidden)));
    }
}
