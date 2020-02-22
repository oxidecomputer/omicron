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
 * ** The HTTP layer of the server may generate an error.  In this case, it
 *    may be just as easy to generate the appropriate HTTP response (with a
 *    400-level or 500-level status code) as it would be to generate an Error
 *    object of some kind.
 * ** An HTTP-agnostic layer of the API server code base may generate an error.
 *    It would be nice (but not essential) if the model and backend layers did
 *    not need to know about HTTP-specific things like status codes,
 *    particularly since they may not map straightforwardly.  For example, a
 *    NotFound error from the model may not result in a 404 out the API -- it
 *    might just mean that something in the model layer needs to create an
 *    object before using it.
 * ** A library that's not part of the API server code base may generate an
 *    error.  This would include standard library interfaces returning
 *    `std::io::Error` and Hyper returning `hyper::Error`, for examples.
 * * We'd like to take advantage of Rust's built-in error handling control flow
 *   tools, like Results and the '?' operator.
 *
 * We adopt the following approach:
 *
 * * We define `ApiError`, which provides a status code, error code (via an
 *   Enum), external message (for sending in the response), optional metadata,
 *   and an internal message (for the log file or other instrumentation).  The
 *   HTTP layers of the request-handling stack may use this struct directly.
 *   **The set of possible error codes here is part of the OpenAPI contract, as
 *   is the schema for any metadata.**  By the time an error bubbles up to the
 *   top of the request handling stack, it must be an ApiError.  
 * * For the HTTP-agnostic layers of the API server (i.e., the model and the
 *   backend), we'll use a separate enum representing their errors.  We'll
 *   provide a `From` implementation that converts these errors into ApiErrors.
 *   One could imagine separate enums for separate layers, but the added
 *   complexity doesn't buy us much.  We could also consider merging this with
 *   `ApiError`, but it seems useful to keep these components separate from
 *   the HTTP implementation (i.e., they shouldn't have to know about status
 *   codes, and it may not be trivial to map status codes from these errors).
 *
 * TODO working here
 */

use serde_json::error::Error as SerdeError;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result;

/**
 * ApiError represents an error generated as part of handling an API request.
 * When these bubble up to the top of the request handling stack (which is most
 * of the time that they're generated), these need to be turned into an HTTP
 * response, which includes:
 *
 *   * a status code, which is likely either 400-level (indicating a client
 *     error, like bad input) or 500-level (indicating a server error).
 *   * a structured (JSON) body, which includes:
 *   ** a string error code, which identifies the underlying error condition so
 *      that clients can potentially make programmatic decisions based on the
 *      error type
 *   ** a string error message, which is the human-readable summary of the
 *      issue, intended to make sense for API users (i.e., not API server
 *      developers)
 *   ** optionally: additional metadata describing the issue.  For a validation
 *      error, this could include information about which parameter was invalid
 *      and why.  This should conform to a schema associated with the error
 *      code.
 *
 * It's easy to go overboard with the error codes and metadata.  Generally, we
 * should avoid creating specific codes and metadata unless there's a good
 * reason for a client to care.
 *
 * Besides that, ApiErrors also have an internal error message, which may differ
 * from the error message that gets reported to users.  For example, if the
 * request fails because an internal database is unreachable, the client may
 * just see "internal error", while the server log would include more details
 * like "failed to acquire connection to database at 10.1.2.3".
 */
#[derive(Debug)]
pub struct ApiError {
}
impl std::error::Error for ApiError {
}

impl Display for ApiError {
    fn fmt(&self, _f: &mut Formatter) -> Result {
        // XXX What is this used for?  Should this emit JSON?
        // (We have to implement it in order to implement the
        // std::error::Error trait.)
        Ok(())
    }
}

impl From<SerdeError> for ApiError {
    fn from(_error: SerdeError)
        -> Self
    {
        // XXX
        ApiError {}
    }
}

impl From<hyper::error::Error> for ApiError {
    fn from(_error: hyper::error::Error)
        -> Self
    {
        // XXX
        ApiError {}
    }
}

impl From<http::Error> for ApiError {
    fn from(_error: http::Error)
        -> Self
    {
        // XXX
        ApiError {}
    }
}

impl ApiError {
    pub fn into_generic_error(self) -> Box<dyn std::error::Error + Send + Sync> {
        // XXX
        unimplemented!("convert API error to generic error");
    }
}
