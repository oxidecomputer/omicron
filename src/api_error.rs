/*!
 * API-specific error handling facilities.  See dropshot/error.rs for details.
 */

use crate::api_model;
use dropshot::HttpError;

/**
 * ApiError represents errors that can be generated within the API server.  See
 * the module-level documentation for details.
 */
#[derive(Debug)]
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

impl From<ApiError> for HttpError {
    fn from(error: ApiError) -> HttpError {
        match error {
            ApiError::ObjectNotFound {
                type_name: t,
                object_name: n,
            } => {
                let message = format!("not found: {} \"{}\"", t, n);
                HttpError::for_client_error(
                    http::StatusCode::NOT_FOUND,
                    message,
                )
            }

            ApiError::ObjectAlreadyExists {
                type_name: t,
                object_name: n,
            } => {
                let message = format!("already exists: {} \"{}\"", t, n);
                HttpError::for_bad_request(message)
            }
        }
    }
}
