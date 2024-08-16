// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Error handling facilities for the Oxide control plane
//!
//! For HTTP-level error handling, see Dropshot.

use crate::api::external::Name;
use crate::api::external::ResourceType;
use dropshot::HttpError;
use omicron_uuid_kinds::GenericUuid;
use serde::Deserialize;
use serde::Serialize;
use slog_error_chain::SlogInlineError;
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
#[derive(
    Clone,
    Debug,
    Deserialize,
    thiserror::Error,
    PartialEq,
    Serialize,
    SlogInlineError,
)]
pub enum Error {
    /// An object needed as part of this operation was not found.
    #[error("Object (of type {lookup_type:?}) not found: {type_name}")]
    ObjectNotFound { type_name: ResourceType, lookup_type: LookupType },
    /// An object already exists with the specified name or identifier.
    #[error("Object (of type {type_name:?}) already exists: {object_name}")]
    ObjectAlreadyExists { type_name: ResourceType, object_name: String },
    /// The request was well-formed, but the operation cannot be completed given
    /// the current state of the system.
    #[error("Invalid Request: {}", .message.display_internal())]
    InvalidRequest { message: MessagePair },
    /// Authentication credentials were required but either missing or invalid.
    /// The HTTP status code is called "Unauthorized", but it's more accurate to
    /// call it "Unauthenticated".
    #[error("Missing or invalid credentials")]
    Unauthenticated { internal_message: String },
    /// The specified input field is not valid.
    #[error("Invalid Value: {label}, {}", .message.display_internal())]
    InvalidValue { label: String, message: MessagePair },
    /// The request is not authorized to perform the requested operation.
    #[error("Forbidden")]
    Forbidden,

    /// The system encountered an unhandled operational error.
    #[error("Internal Error: {internal_message}")]
    InternalError { internal_message: String },
    /// The system (or part of it) is unavailable.
    #[error("Service Unavailable: {internal_message}")]
    ServiceUnavailable { internal_message: String },

    /// There is insufficient capacity to perform the requested operation.
    ///
    /// This variant is translated to 507 Insufficient Storage, and it carries
    /// both an external and an internal message. The external message is
    /// intended for operator consumption and is intended to not leak any
    /// implementation details.
    #[error("Insufficient Capacity: {}", .message.display_internal())]
    InsufficientCapacity { message: MessagePair },

    #[error("Type version mismatch! {internal_message}")]
    TypeVersionMismatch { internal_message: String },

    #[error("Conflict: {}", .message.display_internal())]
    Conflict { message: MessagePair },

    /// A generic 404 response. If there is an applicable ResourceType, use
    /// ObjectNotFound instead.
    #[error("Not found: {}", .message.display_internal())]
    NotFound { message: MessagePair },

    /// Access to the target resource is no longer available, and this condition
    /// is likely to be permanent.
    #[error("Gone")]
    Gone,
}

/// Represents an error message which has an external component, along with
/// some internal context possibly attached to it.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct MessagePair {
    external_message: String,
    internal_context: String,
}

impl MessagePair {
    pub fn new(external_message: String) -> Self {
        Self { external_message, internal_context: String::new() }
    }

    pub fn new_full(
        external_message: String,
        internal_context: String,
    ) -> Self {
        Self { external_message, internal_context }
    }

    pub fn external_message(&self) -> &str {
        &self.external_message
    }

    pub fn internal_context(&self) -> &str {
        &self.internal_context
    }

    fn with_internal_context<C>(self, context: C) -> Self
    where
        C: Display + Send + Sync + 'static,
    {
        let internal_context = if self.internal_context.is_empty() {
            context.to_string()
        } else {
            format!("{}: {}", context, self.internal_context)
        };
        Self { external_message: self.external_message, internal_context }
    }

    pub fn into_internal_external(self) -> (String, String) {
        let internal = self.display_internal().to_string();
        (internal, self.external_message)
    }

    // Do not implement `fmt::Display` for this enum because we don't want users to
    // accidentally display the internal message to the client. Instead, use a
    // private formatter.
    fn display_internal(&self) -> MessagePairDisplayInternal<'_> {
        MessagePairDisplayInternal(self)
    }
}

struct MessagePairDisplayInternal<'a>(&'a MessagePair);

impl<'a> Display for MessagePairDisplayInternal<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.external_message)?;
        if !self.0.internal_context.is_empty() {
            write!(f, " (with internal context: {})", self.0.internal_context)?;
        }
        Ok(())
    }
}

/// Indicates how an object was looked up (for an `ObjectNotFound` error)
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum LookupType {
    /// a specific name was requested
    ByName(String),
    /// a specific id was requested
    ById(Uuid),
    /// a specific id was requested with some composite type
    /// (caller summarizes it)
    ByCompositeId(String),
    /// object selected by criteria that would be confusing to call an ID
    ByOther(String),
}

impl LookupType {
    /// Constructs a `ById` lookup type from a typed or untyped UUID.
    pub fn by_id<T: GenericUuid>(id: T) -> Self {
        LookupType::ById(id.into_untyped_uuid())
    }

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
            | Error::InsufficientCapacity { .. }
            | Error::InternalError { .. }
            | Error::TypeVersionMismatch { .. }
            | Error::NotFound { .. }
            | Error::Conflict { .. }
            | Error::Gone => false,
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
    pub fn invalid_request(message: impl Into<String>) -> Error {
        Error::InvalidRequest { message: MessagePair::new(message.into()) }
    }

    /// Generates an [`Error::InvalidValue`] error with the specific label and
    /// message.
    pub fn invalid_value(
        label: impl Into<String>,
        message: impl Into<String>,
    ) -> Error {
        Error::InvalidValue {
            label: label.into(),
            message: MessagePair::new(message.into()),
        }
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

    /// Generates an [`Error::InsufficientCapacity`] error with external and
    /// and internal messages.
    ///
    /// This should be used for failures where there is insufficient capacity,
    /// and where the caller must either take action or wait until capacity is
    /// freed.
    ///
    /// In the future, we may want to provide more help here: e.g. a link to a
    /// status or support page.
    pub fn insufficient_capacity(
        external_message: impl Into<String>,
        internal_message: impl Into<String>,
    ) -> Error {
        Error::InsufficientCapacity {
            message: MessagePair::new_full(
                external_message.into(),
                internal_message.into(),
            ),
        }
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

    /// Generates an [`Error::Conflict`] with a specific message.
    ///
    /// This is used in cases where a request cannot proceed because the target
    /// resource is currently in a state that's incompatible with that request,
    /// but where the request might succeed if it is retried or modified and
    /// retried. The internal message should provide more information about the
    /// source of the conflict and possible actions the caller can take to
    /// resolve it (if any).
    pub fn conflict(message: impl Into<String>) -> Error {
        Error::Conflict { message: MessagePair::new(message.into()) }
    }

    /// Generates an [`Error::NotFound`] with a specific message.
    ///
    /// This is used in cases where a generic 404 is required. For cases where
    /// there is a ResourceType, use a function that produces
    /// [`Error::ObjectNotFound`] instead.
    pub fn non_resourcetype_not_found(message: impl Into<String>) -> Error {
        Error::NotFound { message: MessagePair::new(message.into()) }
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
            | Error::Forbidden
            | Error::Gone => self,
            Error::InvalidRequest { message } => Error::InvalidRequest {
                message: message.with_internal_context(context),
            },
            Error::InvalidValue { label, message } => Error::InvalidValue {
                label,
                message: message.with_internal_context(context),
            },
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
            Error::InsufficientCapacity { message } => {
                Error::InsufficientCapacity {
                    message: message.with_internal_context(context),
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
            Error::Conflict { message } => Error::Conflict {
                message: message.with_internal_context(context),
            },
            Error::NotFound { message } => Error::NotFound {
                message: message.with_internal_context(context),
            },
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
                let message = match lt {
                    LookupType::ByName(name) => {
                        format!("{} with name \"{}\"", t, name)
                    }
                    LookupType::ById(id) => {
                        format!("{} with id \"{}\"", t, id)
                    }
                    LookupType::ByCompositeId(label) => {
                        format!("{} with id \"{}\"", t, label)
                    }
                    LookupType::ByOther(msg) => msg,
                };
                HttpError::for_client_error(
                    Some(String::from("ObjectNotFound")),
                    http::StatusCode::NOT_FOUND,
                    format!("not found: {}", message),
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

            Error::InvalidRequest { message } => {
                let (internal_message, external_message) =
                    message.into_internal_external();
                HttpError {
                    status_code: http::StatusCode::BAD_REQUEST,
                    error_code: Some(String::from("InvalidRequest")),
                    external_message,
                    internal_message,
                }
            }

            Error::InvalidValue { label, message } => {
                let (internal_message, external_message) =
                    message.into_internal_external();
                HttpError {
                    status_code: http::StatusCode::BAD_REQUEST,
                    error_code: Some(String::from("InvalidValue")),
                    external_message: format!(
                        "unsupported value for \"{}\": {}",
                        label, external_message
                    ),
                    internal_message,
                }
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

            Error::InsufficientCapacity { message } => {
                let (internal_message, external_message) =
                    message.into_internal_external();
                // Need to construct an `HttpError` explicitly to present both
                // an internal and an external message.
                HttpError {
                    status_code: http::StatusCode::INSUFFICIENT_STORAGE,
                    error_code: Some(String::from("InsufficientCapacity")),
                    external_message: format!(
                        "Insufficient capacity: {}",
                        external_message
                    ),
                    internal_message,
                }
            }

            Error::TypeVersionMismatch { internal_message } => {
                HttpError::for_internal_error(internal_message)
            }

            Error::Conflict { message } => {
                let (internal_message, external_message) =
                    message.into_internal_external();
                HttpError {
                    status_code: http::StatusCode::CONFLICT,
                    error_code: Some(String::from("Conflict")),
                    external_message,
                    internal_message,
                }
            }

            Error::NotFound { message } => {
                let (internal_message, external_message) =
                    message.into_internal_external();
                HttpError {
                    status_code: http::StatusCode::NOT_FOUND,
                    error_code: Some(String::from("Not Found")),
                    external_message,
                    internal_message,
                }
            }

            Error::Gone => HttpError::for_client_error(
                Some(String::from("Gone")),
                http::StatusCode::GONE,
                String::from("Gone"),
            ),
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
            // For most error variants, we delegate to the display impl for the
            // Progenitor error type, but we pick apart an error response more
            // carefully.
            progenitor::progenitor_client::Error::InvalidRequest(_)
            | progenitor::progenitor_client::Error::CommunicationError(_)
            | progenitor::progenitor_client::Error::InvalidResponsePayload(
                ..,
            )
            | progenitor::progenitor_client::Error::UnexpectedResponse(_)
            | progenitor::progenitor_client::Error::InvalidUpgrade(_)
            | progenitor::progenitor_client::Error::ResponseBodyError(_)
            | progenitor::progenitor_client::Error::PreHookError(_) => {
                Error::internal_error(&e.to_string())
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
