/*!
 * API-specific error handling facilities.  See dropshot/error.rs for details.
 */

use crate::api_model::ApiName;
use crate::api_model::ApiResourceType;
use dropshot::HttpError;
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
    ResourceNotAvailable { message: String },
    DependencyError { message: String },
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
                HttpError::for_bad_request(message)
            }

            ApiError::InvalidRequest {
                message,
            } => HttpError::for_bad_request(message),

            ApiError::InvalidValue {
                label,
                message,
            } => {
                let message =
                    format!("unsupported value for \"{}\": {}", label, message);
                HttpError::for_bad_request(message)
            }

            ApiError::ResourceNotAvailable {
                message,
            } => HttpError::for_unavail(message),
        }
    }
}
