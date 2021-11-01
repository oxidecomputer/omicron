/*!
 * Error handling facilities for the Oxide control plane
 *
 * For HTTP-level error handling, see Dropshot.
 */

use crate::api::external::Name;
use crate::api::external::ResourceType;
use dropshot::HttpError;
use dropshot::HttpErrorResponseBody;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/**
 * An error that can be generated within a control plane component
 *
 * These may be generated while handling a client request or as part of
 * background operation.  When generated as part of an HTTP request, an
 * `Error` will be converted into an HTTP error as one of the last steps in
 * processing the request.  This allows most of the system to remain agnostic to
 * the transport with which the system communicates with clients.
 *
 * General best practices for error design apply here.  Where possible, we want
 * to reuse existing variants rather than inventing new ones to distinguish
 * cases that no programmatic consumer needs to distinguish.
 */
#[derive(Debug, Deserialize, thiserror::Error, PartialEq, Serialize)]
pub enum Error {
    /** An object needed as part of this operation was not found. */
    #[error("Object (of type {lookup_type:?}) not found: {type_name}")]
    ObjectNotFound { type_name: ResourceType, lookup_type: LookupType },
    /** An object already exists with the specified name or identifier. */
    #[error("Object (of type {type_name:?}) already exists: {object_name}")]
    ObjectAlreadyExists { type_name: ResourceType, object_name: String },
    /**
     * The request was well-formed, but the operation cannot be completed given
     * the current state of the system.
     */
    #[error("Invalid Request: {message}")]
    InvalidRequest { message: String },
    /** The specified input field is not valid. */
    #[error("Invalid Value: {label}, {message}")]
    InvalidValue { label: String, message: String },
    /** The system encountered an unhandled operational error. */
    #[error("Internal Error: {internal_message}")]
    InternalError { internal_message: String },
    /** The system (or part of it) is unavailable. */
    #[error("Service Unavailable: {message}")]
    ServiceUnavailable { message: String },
}

/** Indicates how an object was looked up (for an `ObjectNotFound` error) */
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub enum LookupType {
    /** a specific name was requested */
    ByName(String),
    /** a specific id was requested */
    ById(Uuid),
    /** some other lookup type was used */
    Other(String),
}

impl Error {
    /**
     * Returns whether the error is likely transient and could reasonably be
     * retried
     */
    pub fn retryable(&self) -> bool {
        match self {
            Error::ServiceUnavailable { .. } => true,

            Error::ObjectNotFound { .. }
            | Error::ObjectAlreadyExists { .. }
            | Error::InvalidRequest { .. }
            | Error::InvalidValue { .. }
            | Error::InternalError { .. } => false,
        }
    }

    /**
     * Generates an [`Error::ObjectNotFound`] error for a lookup by object
     * name.
     */
    pub fn not_found_by_name(type_name: ResourceType, name: &Name) -> Error {
        Error::ObjectNotFound {
            type_name,
            lookup_type: LookupType::ByName(name.as_str().to_owned()),
        }
    }

    /**
     * Generates an [`Error::ObjectNotFound`] error for a lookup by object id.
     */
    pub fn not_found_by_id(type_name: ResourceType, id: &Uuid) -> Error {
        Error::ObjectNotFound { type_name, lookup_type: LookupType::ById(*id) }
    }

    /**
     * Generates an [`Error::ObjectNotFound`] error for some other kind of
     * lookup.
     */
    pub fn not_found_other(type_name: ResourceType, message: String) -> Error {
        Error::ObjectNotFound {
            type_name,
            lookup_type: LookupType::Other(message),
        }
    }

    /**
     * Generates an [`Error::InternalError`] error with the specific message
     *
     * InternalError should be used for operational conditions that should not
     * happen but that we cannot reasonably handle at runtime (e.g.,
     * deserializing a value from the database, or finding two records for
     * something that is supposed to be unique).
     */
    pub fn internal_error(internal_message: &str) -> Error {
        Error::InternalError { internal_message: internal_message.to_owned() }
    }

    /**
     * Generates an [`Error::ServiceUnavailable`] error with the specific
     * message
     *
     * This should be used for transient failures where the caller might be
     * expected to retry.  Logic errors or other problems indicating that a
     * retry would not work should probably be an InternalError (if it's a
     * server problem) or InvalidRequest (if it's a client problem) instead.
     */
    pub fn unavail(message: &str) -> Error {
        Error::ServiceUnavailable { message: message.to_owned() }
    }

    /**
     * Given an error returned in an HTTP response, reconstitute an `Error`
     * that describes that error.  This is intended for use when returning an
     * error from one control plane service to another while preserving
     * information about the error.  If the error is of an unknown kind or
     * doesn't match the expected form, an internal error will be returned.
     */
    pub fn from_response(
        error_message_base: String,
        error_response: HttpErrorResponseBody,
    ) -> Error {
        /*
         * We currently only handle the simple case of an InvalidRequest because
         * that's the only case that we currently use.  If we want to preserve
         * others of these (e.g., ObjectNotFound), we will probably need to
         * include more information in the HttpErrorResponseBody.
         */
        match error_response.error_code.as_deref() {
            Some("InvalidRequest") => {
                Error::InvalidRequest { message: error_response.message }
            }
            _ => Error::InternalError {
                internal_message: format!(
                    "{}: unknown error from dependency: {:?}",
                    error_message_base, error_response
                ),
            },
        }
    }
}

impl From<Error> for HttpError {
    /**
     * Converts an `Error` error into an `HttpError`.  This defines how
     * errors that are represented internally using `Error` are ultimately
     * exposed to clients over HTTP.
     */
    fn from(error: Error) -> HttpError {
        match error {
            Error::ObjectNotFound { type_name: t, lookup_type: lt } => {
                if let LookupType::Other(message) = lt {
                    HttpError::for_client_error(
                        Some(String::from("ObjectNotFound")),
                        http::StatusCode::NOT_FOUND,
                        message,
                    )
                } else {
                    /* TODO-cleanup is there a better way to express this? */
                    let (lookup_field, lookup_value) = match lt {
                        LookupType::ByName(name) => ("name", name),
                        LookupType::ById(id) => ("id", id.to_string()),
                        LookupType::Other(_) => panic!("unhandled other"),
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
            }

            Error::ObjectAlreadyExists { type_name: t, object_name: n } => {
                let message = format!("already exists: {} \"{}\"", t, n);
                HttpError::for_bad_request(
                    Some(String::from("ObjectAlreadyExists")),
                    message,
                )
            }

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

            Error::InternalError { internal_message } => {
                HttpError::for_internal_error(internal_message)
            }

            Error::ServiceUnavailable { message } => HttpError::for_unavail(
                Some(String::from("ServiceNotAvailable")),
                message,
            ),
        }
    }
}

/**
 * Like [`assert!`], except that instead of panicking, this function returns an
 * `Err(Error::InternalError)` with an appropriate message if the given
 * condition is not true.
 */
#[macro_export]
macro_rules! bail_unless {
    ($cond:expr $(,)?) => {
        bail_unless!($cond, "failed runtime check: {:?}", stringify!($cond))
    };
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            return Err($crate::api::external::Error::internal_error(&format!(
                $($arg)*)))
        }
    };
}

#[cfg(test)]
mod test {
    use super::Error;

    #[test]
    fn test_bail_unless() {
        /* Success cases */
        let no_bail = || {
            bail_unless!(1 + 1 == 2, "wrong answer: {}", 3);
            Ok(())
        };
        let no_bail_label_args = || {
            bail_unless!(1 + 1 == 2, "wrong answer: {}", 3);
            Ok(())
        };
        assert_eq!(Ok(()), no_bail());
        assert_eq!(Ok(()), no_bail_label_args());

        /* Failure cases */
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
            if let Error::InternalError { message } = error {
                assert_eq!(*expected_message, message);
            } else {
                panic!("got something other than an InternalError");
            }
        }
    }
}
