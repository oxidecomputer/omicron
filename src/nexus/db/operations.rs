/*!
 * Low-level facilities that work directly with tokio_postgres to run queries
 * and extract values
 */

use crate::api_error::ApiError;
use std::convert::TryFrom;
use std::fmt;
use thiserror::Error;
use tokio_postgres::types::FromSql;
use tokio_postgres::types::ToSql;

/*
 * TODO-debug It would be nice to have a backtrace in these errors.  thiserror
 * is capable of including one automatically if there's a member called
 * "backtrace" with the type "Backtrace", but it's not clear how that would get
 * populated aside from maybe a thiserror-generated `From` impl.  It looks like
 * one has to do that explicitly using std::backtrace::Backtrace::capture(),
 * which is a nightly-only API.  So we'll defer that for now.
 */
#[derive(Error, Debug)]
pub enum DbError {
    #[error("executing {sql:?}: {source:#}")]
    SqlError { sql: String, source: tokio_postgres::Error },

    #[error("extracting column {column_name:?} from row: {source:#}")]
    DeserializeError { column_name: String, source: tokio_postgres::Error },

    #[error("executing {sql:?}: expected one row, but found {nrows_found}")]
    BadRowCount { sql: String, nrows_found: u64 },
}

impl DbError {
    pub fn db_source(&self) -> Option<&tokio_postgres::Error> {
        match self {
            DbError::SqlError { ref source, .. } => Some(source),
            DbError::DeserializeError { ref source, .. } => Some(source),
            DbError::BadRowCount { .. } => None,
        }
    }
}

/** Given an arbitrary [`DbError`], produce an [`ApiError`] for the problem. */
/*
 * This could potentially be an `impl From<DbError> for ApiError`.  However,
 * there are multiple different ways to do this transformation, depending on
 * what the caller is doing.  See `sql_error_on_create()`.  If we impl'd `From`
 * here, it would be easy to use this version instead of a more appropriate one
 * without even realizing it.
 *
 * TODO-design It's possible we could better represent the underlying condition
 * in the `DbError` so that this isn't a problem.  But that might just push the
 * problem around: the nice thing about DbError today is that lower-level
 * functions like `sql_query` can produce one without knowing what the caller is
 * doing.  The caller gets to decide whether to dig deeper into the underlying
 * error (by calling `sql_error_on_create()` or `sql_error_generic()`).  Now, we
 * could have such callers produce a new variant of `DbError` for these other
 * conversions, and then we could handle them all in the a `From` impl here.
 * But it would still have the problem of being easy to misuse.  Right now, you
 * at least have to explicitly opt into a conversion.
 */
pub fn sql_error_generic(e: DbError) -> ApiError {
    let extra = match e.db_source().and_then(|s| s.code()) {
        Some(code) => format!(" (code {})", code.code()),
        None => String::new(),
    };

    /*
     * TODO-debuggability it would be nice to preserve the DbError here
     * so that the SQL is visible in the log.
     */
    ApiError::internal_error(&format!(
        "unexpected database error{}: {:#}",
        extra, e
    ))
}

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

/**
 * Extract a named field from a row.
 */
pub fn sql_row_value<'a, I, T>(
    row: &'a tokio_postgres::Row,
    idx: I,
) -> Result<T, ApiError>
where
    I: tokio_postgres::row::RowIndex + fmt::Display,
    T: FromSql<'a>,
{
    let column_name = idx.to_string();
    row.try_get(idx).map_err(|source| {
        sql_error_generic(DbError::DeserializeError { column_name, source })
    })
}
