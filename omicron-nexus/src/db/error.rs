//! Error handling and conversions.

use async_bb8_diesel::{ConnectionError, PoolError};
use diesel::result::DatabaseErrorInformation;
use diesel::result::DatabaseErrorKind as DieselErrorKind;
use diesel::result::Error as DieselError;
use omicron_common::api::external::{
    Error as PublicError, LookupType, ResourceType,
};

/// Converts a Diesel pool error to an external error.
pub fn public_error_from_diesel_pool(
    error: PoolError,
    resource_type: ResourceType,
    lookup_type: LookupType,
) -> PublicError {
    match error {
        PoolError::Connection(error) => match error {
            ConnectionError::Checkout(error) => {
                PublicError::ServiceUnavailable {
                    message: format!(
                        "Failed to access connection pool: {}",
                        error
                    ),
                }
            }
            ConnectionError::Query(error) => {
                public_error_from_diesel(error, resource_type, lookup_type)
            }
        },
        PoolError::Timeout => {
            PublicError::unavail("Timeout accessing connection pool")
        }
    }
}

/// Converts a Diesel pool error to an external error,
/// when requested as part of a creation operation
pub fn public_error_from_diesel_pool_create(
    error: PoolError,
    resource_type: ResourceType,
    object_name: &str,
) -> PublicError {
    match error {
        PoolError::Connection(error) => match error {
            ConnectionError::Checkout(error) => {
                PublicError::ServiceUnavailable {
                    message: format!(
                        "Failed to access connection pool: {}",
                        error
                    ),
                }
            }
            ConnectionError::Query(error) => public_error_from_diesel_create(
                error,
                resource_type,
                object_name,
            ),
        },
        PoolError::Timeout => {
            PublicError::unavail("Timeout accessing connection pool")
        }
    }
}

/// Converts a Diesel error to an external error.
pub fn public_error_from_diesel(
    error: DieselError,
    resource_type: ResourceType,
    lookup_type: LookupType,
) -> PublicError {
    match error {
        DieselError::NotFound => PublicError::ObjectNotFound {
            type_name: resource_type,
            lookup_type,
        },
        DieselError::DatabaseError(kind, info) => {
            PublicError::internal_error(&format_database_error(kind, &*info))
        }
        error => PublicError::internal_error(&format!(
            "Unknown diesel error: {:?}",
            error
        )),
    }
}

pub fn format_database_error(
    kind: DieselErrorKind,
    info: &dyn DatabaseErrorInformation,
) -> String {
    let mut rv =
        format!("database error (kind = {:?}): {}\n", kind, info.message());
    if let Some(details) = info.details() {
        rv.push_str(&format!("DETAILS: {}\n", details));
    }
    if let Some(hint) = info.hint() {
        rv.push_str(&format!("HINT: {}\n", hint));
    }
    if let Some(table_name) = info.table_name() {
        rv.push_str(&format!("TABLE NAME: {}\n", table_name));
    }
    if let Some(column_name) = info.column_name() {
        rv.push_str(&format!("COLUMN NAME: {}\n", column_name));
    }
    if let Some(constraint_name) = info.constraint_name() {
        rv.push_str(&format!("CONSTRAINT NAME: {}\n", constraint_name));
    }
    if let Some(statement_position) = info.statement_position() {
        rv.push_str(&format!("STATEMENT POSITION: {}\n", statement_position));
    }
    rv
}

/// Converts a Diesel error to an external error, when requested as
/// part of a creation operation.
pub fn public_error_from_diesel_create(
    error: DieselError,
    resource_type: ResourceType,
    object_name: &str,
) -> PublicError {
    match error {
        DieselError::DatabaseError(kind, _info) => match kind {
            DieselErrorKind::UniqueViolation => {
                PublicError::ObjectAlreadyExists {
                    type_name: resource_type,
                    object_name: object_name.to_string(),
                }
            }
            _ => PublicError::unavail(
                format!("Database error: {:?}", kind).as_str(),
            ),
        },
        _ => PublicError::internal_error("Unknown diesel error"),
    }
}
