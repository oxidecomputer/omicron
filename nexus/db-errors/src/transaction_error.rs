// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::result::{
    DatabaseErrorInformation, DatabaseErrorKind as DieselErrorKind,
    Error as DieselError,
};
use nexus_auth::authz;
use omicron_common::api::external::{
    Error as PublicError, LookupType, ResourceType,
};

use crate::OptionalError;

/// Wrapper around an error which may be returned from a Diesel transaction.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError<T> {
    /// The customizable error type.
    ///
    /// This error should be used for all non-Diesel transaction failures.
    #[error("Custom transaction error: {0}")]
    CustomError(T),

    /// The Diesel error type.
    ///
    /// This error covers failure due to accessing the DB pool or errors
    /// propagated from the DB itself.
    #[error("Database error: {0}")]
    Database(#[from] DieselError),
}

pub fn retryable(error: &DieselError) -> bool {
    match error {
        DieselError::DatabaseError(kind, boxed_error_information) => match kind
        {
            DieselErrorKind::SerializationFailure => boxed_error_information
                .message()
                .starts_with("restart transaction"),
            _ => false,
        },
        _ => false,
    }
}

/// Identifies if the error is retryable or not.
pub enum MaybeRetryable<T> {
    /// The error isn't retryable.
    NotRetryable(T),
    /// The error is retryable.
    Retryable(DieselError),
}

impl<T> TransactionError<T> {
    /// Identifies that the error could be returned from a Diesel transaction.
    ///
    /// Allows callers to propagate arbitrary errors out of transaction contexts
    /// without losing information that might be valuable to the calling context,
    /// such as "does this particular error indicate that the entire transaction
    /// should retry?".
    pub fn retryable(self) -> MaybeRetryable<Self> {
        use MaybeRetryable::*;

        match self {
            TransactionError::Database(err) if retryable(&err) => {
                Retryable(err)
            }
            _ => NotRetryable(self),
        }
    }
}

impl<T: std::fmt::Debug> TransactionError<T> {
    /// Converts a TransactionError into a diesel error.
    ///
    /// The following pattern is used frequently in retryable transactions:
    ///
    /// - Create an `OptionalError<TransactionError<T>>`
    /// - Execute a series of database operations, which return the
    /// `TransactionError<T>` type
    /// - If the underlying operations return a retryable error from diesel,
    /// propagate that out.
    /// - Otherwise, set the OptionalError to a value of T, and rollback the transaction.
    ///
    /// This function assists with that conversion.
    pub fn into_diesel(
        self,
        err: &OptionalError<TransactionError<T>>,
    ) -> DieselError {
        match self.retryable() {
            MaybeRetryable::NotRetryable(txn_error) => err.bail(txn_error),
            MaybeRetryable::Retryable(diesel_error) => diesel_error,
        }
    }
}

impl From<PublicError> for TransactionError<PublicError> {
    fn from(err: PublicError) -> Self {
        TransactionError::CustomError(err)
    }
}

impl From<TransactionError<PublicError>> for PublicError {
    fn from(err: TransactionError<PublicError>) -> Self {
        match err {
            TransactionError::CustomError(err) => err,
            TransactionError::Database(err) => {
                public_error_from_diesel(err, ErrorHandler::Server)
            }
        }
    }
}

/// Summarizes details provided with a database error.
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

/// Allows the caller to handle user-facing errors, and provide additional
/// context which may be used to populate more informative errors.
///
/// Note that all operations may return server-level errors for a variety
/// of reasons, including being unable to contact the database, I/O errors,
/// etc.
pub enum ErrorHandler<'a> {
    /// The operation expected to fetch, update, or delete exactly one resource
    /// identified by the [`authz::ApiResource`]. If that row is not found, an
    /// appropriate "Not Found" error will be returned.
    NotFoundByResource(&'a dyn authz::ApiResource),
    /// The operation was attempting to lookup or update a resource.
    /// If that row is not found, an appropriate "Not Found" error will be
    /// returned.
    ///
    /// NOTE: If you already have an [`authz::ApiResource`] object, you should
    /// use the [`ErrorHandler::NotFoundByResource`] variant instead.
    /// Eventually, the only uses of this function should be in the DataStore
    /// functions that actually look up a record for the first time.
    NotFoundByLookup(ResourceType, LookupType),
    /// The operation was attempting to create a resource with a name.
    /// If a resource already exists with that name, an "Object Already Exists"
    /// error will be returned.
    Conflict(ResourceType, &'a str),
    /// The operation does not expect any user errors.
    /// Without additional context, all errors from this variant are translated
    /// to an "Internal Server Error".
    Server,
}

/// Converts a Diesel connection error to a public-facing error.
///
/// [`ErrorHandler`] may be used to add additional handlers for the error
/// being returned.
pub fn public_error_from_diesel(
    error: DieselError,
    handler: ErrorHandler<'_>,
) -> PublicError {
    match handler {
        ErrorHandler::NotFoundByResource(resource) => {
            public_error_from_diesel_lookup(
                error,
                resource.resource_type(),
                resource.lookup_type(),
            )
        }
        ErrorHandler::NotFoundByLookup(resource_type, lookup_type) => {
            public_error_from_diesel_lookup(error, resource_type, &lookup_type)
        }
        ErrorHandler::Conflict(resource_type, object_name) => {
            public_error_from_diesel_create(error, resource_type, object_name)
        }
        ErrorHandler::Server => PublicError::internal_error(&format!(
            "unexpected database error: {:#}",
            error
        )),
    }
}

/// Converts a Diesel error to an external error, handling "NotFound" using
/// `make_not_found_error`.
pub fn public_error_from_diesel_lookup(
    error: DieselError,
    resource_type: ResourceType,
    lookup_type: &LookupType,
) -> PublicError {
    match error {
        DieselError::NotFound => {
            lookup_type.clone().into_not_found(resource_type)
        }
        DieselError::DatabaseError(kind, info) => {
            PublicError::internal_error(&format_database_error(kind, &*info))
        }
        error => {
            let context =
                format!("accessing {:?} {:?}", resource_type, lookup_type);
            PublicError::internal_error(&format!(
                "Unknown diesel error {}: {:#}",
                context, error
            ))
        }
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
            "Unknown diesel error creating {:?} called {:?}: {:#}",
            resource_type, object_name, error
        )),
    }
}
