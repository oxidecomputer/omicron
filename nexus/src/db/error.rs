//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

//! Error handling and conversions.

use async_bb8_diesel::{ConnectionError, PoolError, PoolResult};
use diesel::result::DatabaseErrorInformation;
use diesel::result::DatabaseErrorKind as DieselErrorKind;
use diesel::result::Error as DieselError;
use omicron_common::api::external::{
    Error as PublicError, LookupType, ResourceType,
};

/// Summarizes details provided with a database error.
fn format_database_error(
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

/// Like [`diesel::result::OptionalExtension<T>::optional`]. This turns Ok(v)
/// into Ok(Some(v)), Err("NotFound") into Ok(None), and leave all other values
/// unchanged.
pub fn diesel_pool_result_optional<T>(
    result: PoolResult<T>,
) -> PoolResult<Option<T>> {
    match result {
        Ok(v) => Ok(Some(v)),
        Err(PoolError::Connection(ConnectionError::Query(
            DieselError::NotFound,
        ))) => Ok(None),
        Err(e) => Err(e),
    }
}

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
                    internal_message: format!(
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
                    internal_message: format!(
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

/// Converts a Diesel error to an external error, when requested as
/// part of a creation operation.
pub fn public_error_from_diesel_create(
    error: DieselError,
    resource_type: ResourceType,
    object_name: &str,
) -> PublicError {
    match error {
        DieselError::DatabaseError(kind, info) => match kind {
            DieselErrorKind::UniqueViolation => {
                PublicError::ObjectAlreadyExists {
                    type_name: resource_type,
                    object_name: object_name.to_string(),
                }
            }
            _ => PublicError::internal_error(&format_database_error(
                kind, &*info,
            )),
        },
        _ => PublicError::internal_error(&format!(
            "Unknown diesel error: {:?}",
            error
        )),
    }
}
