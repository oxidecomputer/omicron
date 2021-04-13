/*!
 * Facilities for executing complete SQL queries
 */

use crate::api_error::ApiError;
use crate::api_model::ApiResourceType;
use crate::api_model::DataPageParams;
use crate::api_model::ListResult;
use crate::api_model::LookupResult;
use futures::StreamExt;
use std::convert::TryFrom;
use std::future::Future;

use super::operations::sql_error_generic;
use super::operations::sql_execute;
use super::operations::sql_query;
use super::operations::sql_query_always_one;
use super::operations::sql_query_maybe_one;
use super::operations::sql_row_value;
use super::operations::DbError;
use super::sql;
use super::sql::LookupKey;
use super::sql::SqlString;
use super::sql::SqlValueSet;
use super::sql::Table;

/// Fetch parts of a row using the specified lookup
pub async fn sql_fetch_row_raw<'a, L, T>(
    client: &tokio_postgres::Client,
    scope_key: L::ScopeKey,
    item_key: &'a L::ItemKey,
    columns: &[&'static str],
) -> LookupResult<tokio_postgres::Row>
where
    L: LookupKey<'a>,
    T: Table,
{
    let mut lookup_cond_sql = SqlString::new();
    L::where_select_rows(scope_key, item_key, &mut lookup_cond_sql);

    let sql = format!(
        "SELECT {} FROM {} WHERE ({}) AND ({}) LIMIT 2",
        columns.join(", "),
        T::TABLE_NAME,
        T::LIVE_CONDITIONS,
        &lookup_cond_sql.sql_fragment(),
    );
    let query_params = lookup_cond_sql.sql_params();
    let mkzerror = move || L::where_select_error::<T>(scope_key, item_key);
    sql_query_maybe_one(client, &sql, query_params, mkzerror).await
}

/// Fetch an entire row using the specified lookup
pub async fn sql_fetch_row_by<'a, L, T>(
    client: &tokio_postgres::Client,
    scope_key: L::ScopeKey,
    item_key: &'a L::ItemKey,
) -> LookupResult<T::ModelType>
where
    L: LookupKey<'a>,
    T: Table,
{
    let row =
        sql_fetch_row_raw::<L, T>(client, scope_key, item_key, T::ALL_COLUMNS)
            .await?;
    T::ModelType::try_from(&row)
}

/// Fetch a page of rows from a table using the specified lookup
pub async fn sql_fetch_page_from_table<'a, L, T>(
    client: &'a tokio_postgres::Client,
    scope_key: L::ScopeKey,
    pagparams: &'a DataPageParams<'a, L::ItemKey>,
) -> ListResult<T::ModelType>
where
    L: LookupKey<'a>,
    L::ScopeKey: 'a,
    T: Table,
{
    sql_fetch_page_by::<L, T, T::ModelType>(
        client,
        scope_key,
        pagparams,
        T::ALL_COLUMNS,
    )
    .await
}

/// Like `fetch_page_from_table`, but the caller can specify which columns
/// to select and how to interpret the row
/*
 * The explicit desugaring of "async fn" here is due to rust-lang/rust#63033.
 * TODO-cleanup review the lifetimes and bounds on fetch_page_by()
 */
pub fn sql_fetch_page_by<'a, L, T, R>(
    client: &'a tokio_postgres::Client,
    scope_key: L::ScopeKey,
    pagparams: &'a DataPageParams<'a, L::ItemKey>,
    columns: &'static [&'static str],
) -> impl Future<Output = ListResult<R>> + 'a
where
    L: LookupKey<'a>,
    T: Table,
    R: for<'d> TryFrom<&'d tokio_postgres::Row, Error = ApiError>
        + Send
        + 'static,
{
    async move {
        let mut page_cond_sql = SqlString::new();
        L::where_select_page(scope_key, pagparams, &mut page_cond_sql);
        let limit = i64::from(pagparams.limit.get());
        let limit_clause =
            format!("LIMIT {}", page_cond_sql.next_param(&limit));
        page_cond_sql.push_str(limit_clause.as_str());

        let sql = format!(
            "SELECT {} FROM {} WHERE ({}) {}",
            columns.join(", "),
            T::TABLE_NAME,
            T::LIVE_CONDITIONS,
            &page_cond_sql.sql_fragment(),
        );
        let query_params = page_cond_sql.sql_params();
        let rows = sql_query(client, sql.as_str(), query_params)
            .await
            .map_err(sql_error_generic)?;
        let list =
            rows.iter().map(R::try_from).collect::<Vec<Result<R, ApiError>>>();
        Ok(futures::stream::iter(list).boxed())
    }
}

// XXX document, refactor, etc.
pub struct UpdatePrecond {
    pub found_state: tokio_postgres::Row,
    pub updated: bool,
}

/// Conditionally update a row
// TODO-doc more details needed here
// TODO-coverage -- and check the SQL by hand
// XXX TODO-log log for both cases here
/*
 * We want to update the instance row to indicate it's being deleted
 * only if it's in a state where that's possible.  While complicated,
 * the SQL below allows us to atomically distinguish the cases we're
 * interested in: successful update, failure because the row was in the
 * wrong state, failure because the row doesn't exist.
 */
pub async fn sql_update_precond<'a, 'b, T, L>(
    client: &'b tokio_postgres::Client,
    scope_key: L::ScopeKey,
    item_key: &'a L::ItemKey,
    precond_columns: &'b [&'static str],
    update_values: &SqlValueSet,
    precond_sql: SqlString<'a>,
) -> Result<UpdatePrecond, ApiError>
where
    T: Table,
    L: LookupKey<'a>,
{
    /*
     * The first CTE will be a SELECT that finds the row we're going to
     * update and includes the identifying columns plus any extra columns
     * that the caller asked for.  (These are usually the columns associated
     * with the preconditions.)  For example:
     *
     *     SELECT  id, state FROM Instance WHERE id = $1
     *             ^^^  ^^^                      ^^
     *              |    |
     *              |    +---- "extra" columns (precondition columns)
     *              |
     *              +--------- "identifying" columns (can be several)
     *                         (scope key  + item key columns)
     *
     * In our example, the update will be conditional on the existing value
     * of "state".
     */
    let identifying_columns = {
        let mut columns = L::SCOPE_KEY_COLUMN_NAMES.to_vec();
        columns.push(L::ITEM_KEY_COLUMN_NAME);
        columns
    };

    let mut selected_columns = identifying_columns.clone();
    selected_columns.extend_from_slice(precond_columns);
    for column in &selected_columns {
        assert!(sql::valid_cockroachdb_identifier(*column));
    }
    assert!(sql::valid_cockroachdb_identifier(T::TABLE_NAME));

    /*
     * Construct the SQL string for the "SELECT" CTE.  We use
     * SqlString::new_with_params() to construct a new SqlString whose first
     * parameter will be after the last parameter from the caller-provided
     * "precond_sql".  This is important, since we'll be sending this all
     * over as one query, including both the SQL we're generating here and
     * what the caller has constructed, and the latter already has the
     * caller's parameter numbers baked into it.
     */
    let (mut lookup_cond_sql, precond_sql_str) =
        SqlString::new_with_params(precond_sql);
    L::where_select_rows(scope_key, item_key, &mut lookup_cond_sql);
    let select_sql_str = format!(
        "SELECT {} FROM {} WHERE ({}) AND ({}) LIMIT 2",
        selected_columns.join(", "),
        T::TABLE_NAME,
        T::LIVE_CONDITIONS,
        lookup_cond_sql.sql_fragment(),
    );

    /*
     * The second CTE will be an UPDATE that _may_ update the row that we
     * found with the SELECT.  In our example, it would look like this:
     *
     *        UPDATE Instance
     *   +-->     SET state = "running"
     *   |        WHERE id = $1 AND state = "starting"
     *   |              ^^^^^^^     ^^^^^^^^^^^^^^^^^
     *   |                 |          +--- caller-provided precondition SQL
     *   |                 |               (may include parameters!)
     *   |                 |
     *   |                 +--- Same as "SELECT" clause above
     *   |
     *   +------ "SET" clause constructed from the caller-provided
     *           "update_values", a SqlValueSet
     *
     * The WHERE clause looks just like the above SELECT's WHERE clause,
     * plus the caller's preconditions.  As before, we'll use
     * SqlString::new_with_params() to start a new SqlString with parameters
     * that start after the ones we've already assembled so far.
     */
    let (mut update_set_sql, lookup_cond_str) =
        SqlString::new_with_params(lookup_cond_sql);
    update_values.to_update_sql(&mut update_set_sql);
    let update_sql_str = format!(
        "UPDATE {} SET {} WHERE ({}) AND ({}) AND ({}) LIMIT 2 RETURNING {}",
        T::TABLE_NAME,
        update_set_sql.sql_fragment(),
        T::LIVE_CONDITIONS,
        lookup_cond_str.as_str(),
        precond_sql_str,
        selected_columns.join(", "),
    );

    /*
     * Put it all together.  The result will look like this:
     *
     *    WITH found_rows   AS (  /* the above select part */ )
     *         updated_rows AS (  /* the above update part */ )
     *    SELECT
     *         found.id    AS found_id,     <--- identifying columns
     *         found.state AS found_state,  <--- initial values for "extra"
     *                                           columns
     *         updated.id  AS updated_id,   <--- identifying columns
     *         updated.state AS updated_state,
     *    FROM
     *        found_rows found          <-- result of the "SELECT" CTE
     *    FULL OUTER JOIN
     *        updated_rows updated      <-- result of the "UPDATE" CTE
     *    ON
     *        found.id = updated.id     <-- identifying columns
     *
     * Now, both the SELECT and UPDATE have "LIMIT 2", which means the FULL
     * OUTER JOIN cannot produce very many rows.  In practice, we expect the
     * SELECT and UPDATE to find at most one row.  The FULL OUTER JOIN just
     * allows us to identify state inconsistency (e.g., multiple rows with
     * the same id values).
     *
     * There will be no new parameters in the SQL we're generating now.  We
     * will need to provide the parameters from the various subparts above.
     */
    let select_columns = identifying_columns
        .iter()
        .chain(precond_columns.iter())
        .map(|name: &&'static str| {
            vec![
                format!("found.{} AS found_{}", *name, *name),
                format!("updated.{} AS updated_{}", *name, *name),
            ]
        })
        .flatten()
        .collect::<Vec<String>>();
    let join_parts = identifying_columns
        .iter()
        .map(|name: &&'static str| {
            format!("(found.{} = updated.{})", *name, *name)
        })
        .collect::<Vec<String>>();
    let sql = format!(
        "WITH found_rows   AS ({}), \
                  updated_rows AS ({}) \
             SELECT {} FROM \
                 found_rows found \
             FULL OUTER JOIN \
                 updated_rows updated \
             ON \
                 {}",
        select_sql_str,
        update_sql_str,
        select_columns.join(", "),
        join_parts.join(" AND "),
    );

    /*
     * XXX TODO update these docs
     * There are only three expected cases here:
     *
     * (1) The Instance does not exist, which is true iff there were zero
     *     returned rows.
     *
     * (2) There was exactly one Instance, and we updated it.  This is true
     *     iff there is one row with a non-null "deleted_id".
     *
     * (3) There was exactly one Instance, but we did not update it because
     *     it was not in a valid state for this update.  This is true iff
     *     there is one row with a null "deleted_id".
     *
     * A lot of other things are operationally conceivable (i.e., more than
     * one returned row), but they should not happen.  We treat these as
     * internal errors.
     */
    // XXX Is this the right place to produce the NotFound error?  Might
    // the caller have already done a name-to-id translation?
    let mkzerror = || L::where_select_error::<T>(scope_key, item_key);
    let row = sql_query_maybe_one(
        &client,
        sql.as_str(),
        update_set_sql.sql_params(),
        mkzerror,
    )
    .await?;
    /*
     * We successfully updated the row iff the "updated_*" columns for the
     * identifying columns are non-NULL.  We pick the lookup_key column,
     * purely out of convenience.
     */
    let updated_key = format!("updated_{}", L::ITEM_KEY_COLUMN_NAME);
    let check_updated_key: Option<L::ItemKey> =
        sql_row_value(&row, updated_key.as_str())?;
    Ok(UpdatePrecond { found_state: row, updated: check_updated_key.is_some() })
}

/**
 * Given a [`DbError`] while creating an instance of type `rtype`, produce an
 * appropriate [`ApiError`].
 */
fn sql_error_on_create(
    rtype: ApiResourceType,
    unique_value: &str,
    e: DbError,
) -> ApiError {
    if let Some(code) = e.db_source().and_then(|s| s.code()) {
        if *code == tokio_postgres::error::SqlState::UNIQUE_VIOLATION {
            /*
             * TODO-debuggability it would be nice to preserve the DbError here
             * so that the SQL is visible in the log.
             */
            return ApiError::ObjectAlreadyExists {
                type_name: rtype,
                object_name: unique_value.to_owned(),
            };
        }
    }

    sql_error_generic(e)
}

/**
 * Using database connection `client`, insert a row into table `T` having values
 * `values`.
 */
/* XXX TODO-doc document better */
/*
 * This is not as statically type-safe an API as you might think by looking at
 * it.  There's nothing that ensures that the types of the values correspond to
 * the right columns.  It's worth noting, however, that even if we statically
 * checked this, we would only be checking that the values correspond with some
 * Rust representation of the database schema that we've built into this
 * program.  That does not eliminate the runtime possibility that the types do
 * not, in fact, match the types in the database.
 *
 * The use of `'static` lifetimes here is just to make it harder to accidentally
 * insert untrusted input here.  Using the `async fn` syntax here runs afoul of
 * rust-lang/rust#63033.  So we desugar the `async` explicitly.
 */
pub async fn sql_insert_unique<T>(
    client: &tokio_postgres::Client,
    values: &SqlValueSet,
    unique_value: &str,
) -> Result<T::ModelType, ApiError>
where
    T: Table,
{
    let mut sql = SqlString::new();
    let param_names = values
        .values()
        .iter()
        .map(|value| sql.next_param(*value))
        .collect::<Vec<String>>();
    let column_names = values.names().iter().cloned().collect::<Vec<&str>>();

    // XXX Could assert that the specified columns are a subset of allowed ones?
    sql.push_str(
        format!(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING {}",
            T::TABLE_NAME,
            column_names.join(", "),
            param_names.join(", "),
            T::ALL_COLUMNS.join(", "),
        )
        .as_str(),
    );

    let row =
        sql_query_always_one(client, sql.sql_fragment(), sql.sql_params())
            .await
            .map_err(|e| {
                sql_error_on_create(T::RESOURCE_TYPE, unique_value, e)
            })?;

    T::ModelType::try_from(&row)
}

/*
 * The use of `'static` lifetimes here is just to make it harder to accidentally
 * insert untrusted input here.  Using the `async fn` syntax here runs afoul of
 * rust-lang/rust#63033.  So we desugar the `async` explicitly.
 */
/* XXX TODO-doc and commonize with above */
pub fn sql_insert_unique_idempotent<'a, T>(
    client: &'a tokio_postgres::Client,
    values: &'a SqlValueSet,
    unique_value: &'a str,
    ignore_conflicts_on: &'static str,
) -> impl Future<Output = Result<(), ApiError>> + 'a
where
    T: Table,
{
    let mut sql = SqlString::new();
    let param_names = values
        .values()
        .iter()
        .map(|value| sql.next_param(*value))
        .collect::<Vec<String>>();
    let column_names = values.names().iter().cloned().collect::<Vec<&str>>();

    sql.push_str(
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
            T::TABLE_NAME,
            column_names.join(", "),
            param_names.join(", "),
            ignore_conflicts_on,
        )
        .as_str(),
    );

    async move {
        sql_execute(client, sql.sql_fragment(), sql.sql_params())
            .await
            .map_err(|e| sql_error_on_create(T::RESOURCE_TYPE, unique_value, e))
            .map(|_| ())
    }
}

/* XXX TODO-doc + disclaimer about async desugaring */
/*
 * XXX working here -- want to replace callers of sql_insert_unique_idempotent
 * with this one to commonize just a bit more
 */
pub fn sql_insert_unique_idempotent_and_fetch<'a, T, L>(
    client: &'a tokio_postgres::Client,
    values: &'a SqlValueSet,
    unique_value: &'a str,
    ignore_conflicts_on: &'static str,
    scope_key: L::ScopeKey,
    item_key: &'a L::ItemKey,
) -> impl Future<Output = LookupResult<T::ModelType>> + 'a
where
    T: Table,
    L: LookupKey<'a>,
{
    async move {
        sql_insert_unique_idempotent::<T>(
            client,
            values,
            unique_value,
            ignore_conflicts_on,
        )
        .await?;

        /*
         * If we get here, then we successfully inserted the record.  It would
         * be a bug if something else were to remove that record before we have
         * a chance to fetch it.  (That's not generally true -- just something
         * that must be true for this function to be correct, and is true in the
         * cases of our callers.)
         */
        sql_fetch_row_by::<L, T>(client, scope_key, item_key)
            .await
            .map_err(sql_error_not_missing)
    }
}

/// Verifies that the given error is _not_ an `ApiObject::ObjectNotFound`.
/// If it is, then it's converted to an `ApiError::InternalError` instead,
/// on the assumption that an `ObjectNotFound` in this context is neither
/// expected nor appropriate for the caller.
fn sql_error_not_missing(e: ApiError) -> ApiError {
    if matches!(e, ApiError::ObjectNotFound { .. }) {
        ApiError::internal_error(&format!("unexpected ObjectNotFound: {:#}", e))
    } else {
        e
    }
}
