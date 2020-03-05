/*!
 * API error handling facilities
 *
 * Error handling in the Oxide API: a straw man
 * --------------------------------------------
 *
 * Our approach for managing errors within the API server balances several
 * goals:
 *
 * * Every incoming HTTP request should conclude with a response, which is
 *   either successful (200-level or 300-level status code) or a failure
 *   (400-level for client errors, 500-level for server errors).
 * * There are several different sources of errors within the API server:
 *     * The HTTP layer of the server may generate an error.  In this case, it
 *       may be just as easy to generate the appropriate HTTP response (with a
 *       400-level or 500-level status code) as it would be to generate an Error
 *       object of some kind.
 *     * An HTTP-agnostic layer of the API server code base may generate an
 *       error.  It would be nice (but not essential) if the model and backend
 *       layers did not need to know about HTTP-specific things like status
 *       codes, particularly since they may not map straightforwardly.  For
 *       example, a NotFound error from the model may not result in a 404 out
 *       the API -- it might just mean that something in the model layer needs
 *       to create an object before using it.
 *     * A library that's not part of the API server code base may generate an
 *       error.  This would include standard library interfaces returning
 *       `std::io::Error` and Hyper returning `hyper::Error`, for examples.
 * * We'd like to take advantage of Rust's built-in error handling control flow
 *   tools, like Results and the '?' operator.
 *
 * To achive this, we first define `ApiHttpError`, which provides a status code,
 * error code (via an Enum), external message (for sending in the response),
 * optional metadata, and an internal message (for the log file or other
 * instrumentation).  The HTTP layers of the request-handling stack may use this
 * struct directly.  **The set of possible error codes here is part of the
 * OpenAPI contract, as is the schema for any metadata.**  By the time an error
 * bubbles up to the top of the request handling stack, it must be an
 * ApiHttpError.
 *
 * For the HTTP-agnostic layers of the API server (i.e., the model and the
 * backend), we'll use a separate enum `ApiError` representing their errors.
 * We'll provide a `From` implementation that converts these errors into
 * ApiHttpErrors.  One could imagine separate enums for separate layers, but the
 * added complexity doesn't buy us much.  We could also consider merging this
 * with `ApiHttpError`, but it seems useful to keep these components separate
 * from the HTTP implementation (i.e., they shouldn't have to know about status
 * codes, and it may not be trivial to map status codes from these errors).
 */

use crate::api_http_util;
use crate::api_model;
use serde::Deserialize;
use serde::Serialize;
use serde_json::error::Error as SerdeError;

/**
 * ApiHttpError represents an error generated as part of handling an API
 * request.  When these bubble up to the top of the request handling stack
 * (which is most of the time that they're generated), these are turned into an
 * HTTP response, which includes:
 *
 *   * a status code, which is likely either 400-level (indicating a client
 *     error, like bad input) or 500-level (indicating a server error).
 *   * a structured (JSON) body, which includes:
 *       * a string error code, which identifies the underlying error condition
 *         so that clients can potentially make programmatic decisions based on
 *         the error type
 *       * a string error message, which is the human-readable summary of the
 *         issue, intended to make sense for API users (i.e., not API server
 *         developers)
 *       * optionally: additional metadata describing the issue.  For a
 *         validation error, this could include information about which
 *         parameter was invalid and why.  This should conform to a schema
 *         associated with the error code.
 *
 * It's easy to go overboard with the error codes and metadata.  Generally, we
 * should avoid creating specific codes and metadata unless there's a good
 * reason for a client to care.
 *
 * Besides that, ApiHttpErrors also have an internal error message, which may
 * differ from the error message that gets reported to users.  For example, if
 * the request fails because an internal database is unreachable, the client may
 * just see "internal error", while the server log would include more details
 * like "failed to acquire connection to database at 10.1.2.3".
 */
#[derive(Debug)]
pub struct ApiHttpError {
    /*
     * TODO-polish add string error code and coverage in the test suite
     * TODO-polish add cause chain for a complete log message?
     */
    /** HTTP status code for this error */
    pub status_code: http::StatusCode,
    /** Error message to be sent to API client for this error */
    pub external_message: String,
    /** Error message recorded in the log for this error */
    pub internal_message: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiHttpErrorResponseBody {
    pub message: String
}

impl From<SerdeError> for ApiHttpError {
    fn from(error: SerdeError)
        -> Self
    {
        /*
         * TODO-polish it would really be much better to annotate this with
         * context about what we were parsing.
         */
        ApiHttpError::for_bad_request(format!("invalid input: {}", error))
    }
}

impl From<hyper::error::Error> for ApiHttpError {
    fn from(error: hyper::error::Error)
        -> Self
    {
        /*
         * TODO-correctness dig deeper into the various cases to make sure this
         * is a valid way to represent it.
         */
        ApiHttpError::for_bad_request(format!(
            "error processing request: {}", error))
    }
}

impl From<http::Error> for ApiHttpError {
    fn from(error: http::Error)
        -> Self
    {
        /*
         * TODO-correctness dig deeper into the various cases to make sure this
         * is a valid way to represent it.
         */
        ApiHttpError::for_bad_request(format!(
            "error processing request: {}", error))
    }
}

impl From<ApiError> for ApiHttpError {
    fn from(error: ApiError)
        -> ApiHttpError
    {
        match error {
            ApiError::ObjectNotFound { type_name: t, object_name: n } => {
                let message = format!("not found: {} \"{}\"", t, n);
                ApiHttpError::for_client_error(
                    http::StatusCode::NOT_FOUND, message)
            },

            ApiError::ObjectAlreadyExists { type_name: t, object_name: n } => {
                let message = format!("already exists: {} \"{}\"", t, n);
                ApiHttpError::for_bad_request(message)
            },
        }
    }
}

impl ApiHttpError {
    pub fn for_bad_request(message: String)
        -> Self
    {
        ApiHttpError::for_client_error(http::StatusCode::BAD_REQUEST, message)
    }

    pub fn for_status(code: http::StatusCode)
        -> Self
    {
        /* TODO-polish This should probably be our own message. */
        let message = code.canonical_reason().unwrap().to_string();
        ApiHttpError::for_client_error(code, message)
    }

    pub fn for_client_error(code: http::StatusCode, message: String)
        -> Self
    {
        assert!(code.is_client_error());
        ApiHttpError {
            status_code: code,
            internal_message: message.clone(),
            external_message: message.clone(),
        }
    }

    pub fn for_internal_error(
        message_internal: String
    ) -> Self
    {
        let code = http::StatusCode::INTERNAL_SERVER_ERROR;
        ApiHttpError {
            status_code: code,
            external_message: code.canonical_reason().unwrap().to_string(),
            internal_message: message_internal.clone(),
        }
    }

    pub fn into_response(self)
        -> hyper::Response<hyper::Body>
    {
        /*
         * TODO-hardening: consider handling the operational errors that the
         * Serde serialization fails or the response construction fails.  In
         * those cases, we should probably try to report this as a serious
         * problem (e.g., to the log) and send back a 500-level response.  (Of
         * course, that could fail in the same way, but it's less likely because
         * there's only one possible set of input and we can test it.  We'll
         * probably have to use unwrap() there and make sure we've tested that
         * code at least once!)
         */
        hyper::Response::builder()
            .status(self.status_code)
            .header(
                http::header::CONTENT_TYPE,
                api_http_util::CONTENT_TYPE_JSON)
            .body(serde_json::to_string_pretty(&ApiHttpErrorResponseBody {
                message: self.external_message
            }).unwrap().into()).unwrap()
    }
}

/**
 * ApiError represents errors that can be generated within the API server.  See
 * the module-level documentation for details.
 */
pub enum ApiError {
    ObjectNotFound {
        type_name: api_model::ApiResourceType,
        object_name: String,
    },
    ObjectAlreadyExists {
        type_name: api_model::ApiResourceType,
        object_name: String,
    },
}
