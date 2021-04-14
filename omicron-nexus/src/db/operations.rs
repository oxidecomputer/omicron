/*!
 * Low-level facilities that work directly with tokio_postgres to run queries
 * and extract values
 */

use omicron_common::db::sql_error_generic;
use omicron_common::db::DbError;
use omicron_common::error::ApiError;
use std::convert::TryFrom;
use tokio_postgres::types::ToSql;

/**
 * Wrapper around [`tokio_postgres::Client::query`] that produces errors
 * that include the SQL.
 */
/*
 * TODO-debugging It would be really, really valuable to include the
 * query parameters here as well.  However, the only thing we can do with
 * ToSql is to serialize it to a PostgreSQL type (as a string of bytes).
 * We could require that these impl some trait that also provides Debug, and
 * then we could use that.  Or we could find a way to parse the resulting
 * PostgreSQL value and print _that_ out as a String somehow?
 */
pub async fn sql_query(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<tokio_postgres::Row>, DbError> {
    client
        .query(sql, params)
        .await
        .map_err(|e| DbError::SqlError { sql: sql.to_owned(), source: e })
}

/**
 * Like [`sql_query()`], but produces an error unless exactly one row is
 * returned.
 */
pub async fn sql_query_always_one(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<tokio_postgres::Row, DbError> {
    sql_query(client, sql, params).await.and_then(|mut rows| match rows.len() {
        1 => Ok(rows.pop().unwrap()),
        nrows_found => Err(DbError::BadRowCount {
            sql: sql.to_owned(),
            nrows_found: u64::try_from(nrows_found).unwrap(),
        }),
    })
}

/**
 * Like [`sql_query()`], but produces an error based on the row count:
 *
 * * the result of `mkzerror()` if there are no rows returned.  This is
 *   expected to be a suitable [`ApiError::ObjectNotFound`] error.
 * * a generic InternalError if more than one row is returned.  This is
 *   expected to be impossible for this query.  Otherwise, the caller should use
 *   [`sql_query()`].
 */
/*
 * TODO-debugging can we include the SQL in the ApiError
 */
pub async fn sql_query_maybe_one(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
    mkzerror: impl Fn() -> ApiError,
) -> Result<tokio_postgres::Row, ApiError> {
    sql_query(client, sql, params).await.map_err(sql_error_generic).and_then(
        |mut rows| match rows.len() {
            1 => Ok(rows.pop().unwrap()),
            0 => Err(mkzerror()),
            nrows_found => Err(sql_error_generic(DbError::BadRowCount {
                sql: sql.to_owned(),
                nrows_found: u64::try_from(nrows_found).unwrap(),
            })),
        },
    )
}

/**
 * Wrapper around [`tokio_postgres::Client::execute`] that produces errors
 * that include the SQL.
 */
/* TODO-debugging See sql_query(). */
pub async fn sql_execute(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, DbError> {
    client
        .execute(sql, params)
        .await
        .map_err(|e| DbError::SqlError { sql: sql.to_owned(), source: e })
}

/**
 * Like [`sql_execute()`], but produces an error based on the row count:
 *
 * * the result of `mkzerror()` if there are no rows returned.  This is
 *   expected to be a suitable [`ApiError::ObjectNotFound`] error.
 * * a generic InternalError if more than one row is returned.  This is
 *   expected to be impossible for this query.  Otherwise, the caller should use
 *   [`sql_query()`].
 */
/* TODO-debugging can we include the SQL in the ApiError */
pub async fn sql_execute_maybe_one(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
    mkzerror: impl Fn() -> ApiError,
) -> Result<(), ApiError> {
    sql_execute(client, sql, params).await.map_err(sql_error_generic).and_then(
        |nrows| match nrows {
            1 => Ok(()),
            0 => Err(mkzerror()),
            nrows_found => Err(sql_error_generic(DbError::BadRowCount {
                sql: sql.to_owned(),
                nrows_found,
            })),
        },
    )
}
