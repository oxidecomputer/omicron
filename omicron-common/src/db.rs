/*!
 * Very basic database facilities
 *
 * Where possible, database stuff should go into omicron_nexus::db.  The stuff
 * here is used by the model conversions, which have to live in this crate.
 */

use crate::api::external::Error;
use std::fmt;
use thiserror::Error;
use tokio_postgres::types::FromSql;

/**
 * Extract a named field from a row.
 */
pub fn sql_row_value<'a, I, T>(
    row: &'a tokio_postgres::Row,
    idx: I,
) -> Result<T, Error>
where
    I: tokio_postgres::row::RowIndex + fmt::Display,
    T: FromSql<'a>,
{
    let column_name = idx.to_string();
    row.try_get(idx).map_err(|source| {
        sql_error_generic(DbError::DeserializeError { column_name, source })
    })
}

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

/** Given an arbitrary [`DbError`], produce an [`enum@Error`] for the problem. */
/*
 * This could potentially be an `impl From<DbError> for Error`.  However,
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
pub fn sql_error_generic(e: DbError) -> Error {
    use tokio_postgres::error::SqlState;

    /*
     * Construct a message that includes the error code if there was one.
     */
    let maybe_code = e.db_source().and_then(|s| s.code());
    let extra = match maybe_code {
        Some(code) => format!(" (code {})", code.code()),
        None => String::new(),
    };
    let message = format!("unexpected database error{}: {:#}", extra, e);

    /*
     * Determine whether the problem was transient or not and construct an
     * appropriate Error.
     * TODO-cleanup Is there a better supported way to determine if the problem
     * was transient?
     * TODO-resilience need to audit the list below to see what other ones we've
     * missed.
     */
    let cons = if let Some(sqlstate) = maybe_code {
        if sqlstate == &SqlState::CONNECTION_EXCEPTION
            || sqlstate == &SqlState::CONNECTION_DOES_NOT_EXIST
        {
            Error::unavail
        } else {
            Error::internal_error
        }
    } else {
        /*
         * TODO-robustness Some errors without a SqlState may be transient, too
         * (e.g., io::Error), while others might not be (e.g., TLS handshake
         * fail).  The "kind" inside the tokio_postgres error would tell us
         * this, but we don't have any way to get that.
         */
        Error::internal_error
    };

    /*
     * TODO-debuggability it would be nice to preserve the DbError here
     * so that the SQL is visible in the log.
     */
    cons(&message)
}
