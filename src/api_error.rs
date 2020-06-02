/*!
 * API-specific error handling facilities.  See dropshot/error.rs for details.
 */

use crate::api_model::ApiName;
use crate::api_model::ApiResourceType;
use dropshot::HttpError;
use dropshot::HttpErrorResponseBody;
use uuid::Uuid;

/**
 * ApiError represents errors that can be generated within the API server.
 * These are HTTP-agnostic.  See the module-level documentation for details.
 */
#[derive(Debug, PartialEq)]
pub enum ApiError {
    ObjectNotFound { type_name: ApiResourceType, lookup_type: LookupType },
    ObjectAlreadyExists { type_name: ApiResourceType, object_name: String },
    InvalidRequest { message: String },
    InvalidValue { label: String, message: String },
    InternalError { message: String },
    ServiceUnavailable { message: String },
}

#[derive(Debug, PartialEq)]
pub enum LookupType {
    ByName(String),
    ById(Uuid),
    Other(String),
}

impl ApiError {
    pub fn not_found_by_name(
        type_name: ApiResourceType,
        name: &ApiName,
    ) -> ApiError {
        ApiError::ObjectNotFound {
            type_name: type_name,
            lookup_type: LookupType::ByName(String::from(name.clone())),
        }
    }

    pub fn not_found_by_id(type_name: ApiResourceType, id: &Uuid) -> ApiError {
        ApiError::ObjectNotFound {
            type_name: type_name,
            lookup_type: LookupType::ById(id.clone()),
        }
    }

    pub fn not_found_other(
        type_name: ApiResourceType,
        message: String,
    ) -> ApiError {
        ApiError::ObjectNotFound {
            type_name: type_name,
            lookup_type: LookupType::Other(message),
        }
    }

    /**
     * Given an error returned in an HTTP response, reconstitute a corresponding
     * ApiError.  This is intended for use when returning an error from one
     * control plane service to another while preserving information about the
     * error.  If the error is of an unknown kind or doesn't match the expected
     * form, an internal error will be returned.
     */
    pub fn from_response(
        error_message_base: String,
        error_response: HttpErrorResponseBody,
    ) -> ApiError {
        /*
         * We currently only handle the simple case of an InvalidRequest because
         * that's the only case that we currently use.  If we want to preserve
         * others of these (e.g., ObjectNotFound), we will probably need to
         * include more information in the HttpErrorResponseBody.
         */
        match error_response.error_code.as_ref().map(|s| s.as_str()) {
            Some("InvalidRequest") => ApiError::InvalidRequest {
                message: error_response.message,
            },
            _ => ApiError::InternalError {
                message: format!(
                    "{}: unknown error from dependency: {:?}",
                    error_message_base, error_response
                ),
            },
        }
    }
}

impl From<ApiError> for HttpError {
    fn from(error: ApiError) -> HttpError {
        match error {
            ApiError::ObjectNotFound {
                type_name: t,
                lookup_type: lt,
            } => {
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

            ApiError::ObjectAlreadyExists {
                type_name: t,
                object_name: n,
            } => {
                let message = format!("already exists: {} \"{}\"", t, n);
                HttpError::for_bad_request(
                    Some(String::from("ObjectAlreadyExists")),
                    message,
                )
            }

            ApiError::InvalidRequest {
                message,
            } => HttpError::for_bad_request(
                Some(String::from("InvalidRequest")),
                message,
            ),

            ApiError::InvalidValue {
                label,
                message,
            } => {
                let message =
                    format!("unsupported value for \"{}\": {}", label, message);
                HttpError::for_bad_request(
                    Some(String::from("InvalidValue")),
                    message,
                )
            }

            ApiError::InternalError {
                message,
            } => HttpError::for_internal_error(message),

            ApiError::ServiceUnavailable {
                message,
            } => HttpError::for_unavail(
                Some(String::from("ServiceNotAvailable")),
                message,
            ),
        }
    }
}
