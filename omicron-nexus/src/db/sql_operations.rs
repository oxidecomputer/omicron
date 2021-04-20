/*!
 * Facilities for executing complete SQL queries
 */

use futures::StreamExt;
use omicron_common::db::sql_error_generic;
use omicron_common::db::sql_row_value;
use omicron_common::db::DbError;
use omicron_common::error::ApiError;
use omicron_common::model::ApiResourceType;
use omicron_common::model::DataPageParams;
use omicron_common::model::ListResult;
use omicron_common::model::LookupResult;
use std::convert::TryFrom;
use std::num::NonZeroU32;

use super::operations::sql_execute;
use super::operations::sql_query;
use super::operations::sql_query_always_one;
use super::operations::sql_query_maybe_one;
use super::sql;
use super::sql::LookupKey;
use super::sql::ResourceTable;
use super::sql::SqlString;
use super::sql::SqlValueSet;
use super::sql::Table;

/// Fetch parts of a row using the specified lookup
pub async fn sql_fetch_row_raw<'a, L, R>(
    client: &tokio_postgres::Client,
    scope_key: L::ScopeKey,
    item_key: &'a L::ItemKey,
    columns: &[&'static str],
) -> LookupResult<tokio_postgres::Row>
where
    L: LookupKey<'a, R>,
    R: ResourceTable,
{
    let mut lookup_cond_sql = SqlString::new();
    L::where_select_rows(scope_key, item_key, &mut lookup_cond_sql);

    let sql = format!(
        "SELECT {} FROM {} WHERE ({}) AND ({}) LIMIT 2",
        columns.join(", "),
        R::TABLE_NAME,
        R::LIVE_CONDITIONS,
        &lookup_cond_sql.sql_fragment(),
    );
    let query_params = lookup_cond_sql.sql_params();
    let mkzerror = move || L::where_select_error(scope_key, item_key);
    sql_query_maybe_one(client, &sql, query_params, mkzerror).await
}

/// Fetch an entire row using the specified lookup
pub async fn sql_fetch_row_by<'a, L, R>(
    client: &tokio_postgres::Client,
    scope_key: L::ScopeKey,
    item_key: &'a L::ItemKey,
) -> LookupResult<R::ModelType>
where
    L: LookupKey<'a, R>,
    R: ResourceTable,
{
    let row =
        sql_fetch_row_raw::<L, R>(client, scope_key, item_key, R::ALL_COLUMNS)
            .await?;
    R::ModelType::try_from(&row)
}

/// Fetch a page of rows from a table using the specified lookup
pub async fn sql_fetch_page_from_table<'a, L, T>(
    client: &'a tokio_postgres::Client,
    scope_key: L::ScopeKey,
    pagparams: &'a DataPageParams<'a, L::ItemKey>,
) -> ListResult<T::ModelType>
where
    L: LookupKey<'a, T>,
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
pub async fn sql_fetch_page_by<'a, L, T, R>(
    client: &'a tokio_postgres::Client,
    scope_key: L::ScopeKey,
    pagparams: &'a DataPageParams<'a, L::ItemKey>,
    columns: &'static [&'static str],
) -> ListResult<R>
where
    L: LookupKey<'a, T>,
    T: Table,
    R: for<'d> TryFrom<&'d tokio_postgres::Row, Error = ApiError>
        + Send
        + 'static,
{
    let rows =
        sql_fetch_page_raw::<L, T>(client, scope_key, pagparams, columns)
            .await?;
    let list =
        rows.iter().map(R::try_from).collect::<Vec<Result<R, ApiError>>>();
    Ok(futures::stream::iter(list).boxed())
}

// XXX TODO-doc
async fn sql_fetch_page_raw<'a, L, T>(
    client: &'a tokio_postgres::Client,
    scope_key: L::ScopeKey,
    pagparams: &'a DataPageParams<'a, L::ItemKey>,
    columns: &'static [&'static str],
) -> Result<Vec<tokio_postgres::Row>, ApiError>
where
    L: LookupKey<'a, T>,
    T: Table,
{
    let mut page_cond_sql = SqlString::new();
    L::where_select_page(scope_key, pagparams, &mut page_cond_sql);
    let limit = i64::from(pagparams.limit.get());
    let limit_clause = format!("LIMIT {}", page_cond_sql.next_param(&limit));
    page_cond_sql.push_str(limit_clause.as_str());

    let sql = format!(
        "SELECT {} FROM {} WHERE ({}) {}",
        columns.join(", "),
        T::TABLE_NAME,
        T::LIVE_CONDITIONS,
        &page_cond_sql.sql_fragment(),
    );
    let query_params = page_cond_sql.sql_params();
    sql_query(client, sql.as_str(), query_params)
        .await
        .map_err(sql_error_generic)
}

// XXX TODO-doc
// XXX If we buy into Streams here, we may be able to make a bunch of this
// simpler: the two primitives that we probably want are:
// (1) given a sql_query(), return the rows as a Stream
// (2) given a bunch of the above Streams, concatenate them.  This is like
//     StreamExt::chain()...except we want to do the second query after the
//     first one is exhausted.
// That said, this wouldn't buy us a whole lot?  In (1), we already get back a
// Vec of rows, so we're already buffering.
//pub async fn sql_pagination_stream<'a, L, T, R>(
//    client: &'a tokio_postgres::Client,
//    scope_key: L::ScopeKey,
//    pagparams: &'a Data
pub async fn sql_paginate<'a, L, T, R>(
    client: &'a tokio_postgres::Client,
    scope_key: L::ScopeKey,
    columns: &'static [&'static str],
) -> Result<Vec<R>, ApiError>
where
    L: LookupKey<'a, T>,
    T: Table,
    R: for<'d> TryFrom<&'d tokio_postgres::Row, Error = ApiError>
        + Send
        + 'static,
{
    let mut results = Vec::new();
    // XXX initial pagparams
    let mut pagparams = DataPageParams {
        marker: None,
        direction: dropshot::PaginationOrder::Ascending,
        limit: NonZeroU32::new(100).unwrap(),
    };
    let mut last_row_marker = None;
    loop {
        //
        // TODO-error It would be nice to have context here about what we were
        // doing.
        //
        pagparams.marker = last_row_marker.as_ref();
        let rows =
            sql_fetch_page_raw::<L, T>(client, scope_key, &pagparams, columns)
                .await?;
        if rows.len() == 0 {
            return Ok(results);
        }

        /*
         * Extract the marker column and prepare pagination parameters for the
         * next query.
         */
        let last_row = rows.last().unwrap();
        last_row_marker = Some(sql_row_value::<_, L::ItemKey>(
            last_row,
            L::ITEM_KEY_COLUMN_NAME,
        )?);

        /*
         * Now transform the rows we got and append them to the list we're
         * accumulating.
         */
        results.append(
            &mut rows
                .iter()
                .map(R::try_from)
                .collect::<Result<Vec<R>, ApiError>>()?,
        );
    }
}

///
/// Describes a successful result of [`sql_update_precond`]
///
pub struct UpdatePrecond {
    /// Information about the requested row.  This includes a number of columns
    /// "found_$c" and "updated_$c" where "$c" is a column from the table.
    /// These columns will include any columns required to find the row in the
    /// first place (typically the "id") and any columns requested by the caller
    /// in `precond_columns`.
    pub found_state: tokio_postgres::Row,

    /// If true, the row was updated (the preconditions held).  If not, the
    /// preconditions were not true.
    pub updated: bool,
}

///
/// Conditionally update a row in table `T`
///
/// The row is identified using lookup `L` and the `scope_key` and `item_key`.
/// The values to be updated are provided by `update_values`.  The preconditions
/// are specified by the SQL fragment `precond_sql`.
///
/// Besides the usual database errors, there are a few common cases here:
///
/// * The specified row was not found.  In this case, a suitable
///   [`ApiError::ObjectNotFound`] error is returned.
///
/// * The specified row was found and updated.  In this case, an
///   [`UpdatePrecond`] is returned with `updated = true`.
///
/// * The specified row was found, but it was not updated because the
///   preconditions did not hold.  In this case, an [`UpdatePrecond`] is
///   returned with `updated = false`.
///
/// On success (which includes the case where the row was not updated because
/// the precondition failed), the returned [`UpdatePrecond`] includes
/// information about the row that was found.
///
/// ## Returns
///
/// An [`UpdatePrecond`] that describes whether the row was updated and what
/// state was found, if any.
///
// TODO-coverage
// TODO-log log for both cases here
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
    L: LookupKey<'a, T>,
{
    /*
     * We want to update a row only if it meets some preconditions.  One
     * traditional way to do that is to begin a transaction, SELECT [the row]
     * FOR UPDATE, examine it here in the client, then decide whether to UPDATE
     * it.  The SELECT FOR UPDATE locks the row in the database until the
     * transaction commits or aborts.  This causes other clients that want to
     * modify the row to block.  This works okay, but it means locks are held
     * and clients blocked for an entire round-trip to the database, plus
     * processing time here.
     *
     * Another approach is to simply UPDATE the row and include the
     * preconditions directly in the WHERE clause.  This keeps database lock
     * hold time to a minimum (no round trip with locks held).  However, all you
     * know after that is whether the row was updated.  If it wasn't, you don't
     * know if it's because the row wasn't present or it didn't meet the
     * preconditions.
     *
     * One could begin a transaction, perform the UPDATE with preconditions in
     * the WHERE clause, then SELECT the row, then COMMIT.  If we could convince
     * ourselves that the UPDATE and SELECT see the same snapshot of the data,
     * this would be an improvement -- provided that we can send all four
     * statements (BEGIN, UPDATE, SELECT, COMMIT) as a single batch and still
     * see the results of the UPDATE and SELECT.  It's not clear if this is
     * possible using the PostgreSQL wire protocol, but it doesn't seem to be
     * exposed from the tokio_postgres client.  If these aren't sent in one
     * batch, you have the same problem as above: locks held for an entire
     * round-trip.
     *
     * While somewhat complicated at first glance, the SQL below allows us to
     * correctly perform the update, minimize lock hold time, and still
     * distinguish the cases we're interested in: successful update, failure
     * because the row was in the wrong state, failure because the row doesn't
     * exist
     *
     * The query starts with a CTE that SELECTs the row we're going to update.
     * This includes the identifying columns plus any extra columns that the
     * caller asked for.  (These are usually the columns associated with the
     * preconditions.)  For example:
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
     * The second CTE will be an UPDATE that conditionally updates the row that
     * we found with the SELECT.  In our example, it would look like this:
     *
     *        UPDATE Instance
     *   +-->     SET state = "running"
     *   |        WHERE id = $1 AND state = "starting"
     *   |              ^^^^^^^     ^^^^^^^^^^^^^^^^^
     *   |                 |          |
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
     * There are only three expected results of this query:
     *
     * (1) The row that we tried to update does not exist, which is true iff
     *     there were zero returned rows.  In this case, our `mkzerror` function
     *     will be used to generate an ObjectNotFound error.
     *
     * (2) We found exactly one row and updated it.  This is true iff there is
     *     one row with a non-null "updated_id" (where "id" is actually any of
     *     the identifying columns above).
     *
     * (3) There was exactly one Instance, but we did not update it because
     *     it failed the precondition.  This is true iff there is one row and
     *     its "updated_id" is null.  (Again, "id" here can be any of the
     *     identifying columns.)
     *
     * A lot of other things are operationally conceivable (like more than one
     * returned row), but they should not be possible.  We treat these as
     * internal errors.
     */
    /*
     * TODO-correctness Is this a case where the caller may have already done a
     * name-to-id translation?  If so, it might be confusing if we produce a
     * NotFound based on the id, since that's not what the end user actually
     * provided.  Maybe the caller should translate a NotFound-by-id to a
     * NotFound-by-name if they get this error.  It sounds kind of cheesy but
     * only the caller knows this translation has been done.
     */
    let mkzerror = || L::where_select_error(scope_key, item_key);
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
 * appropriate [`ApiError`]
 *
 * This looks for the specific case of a uniqueness constraint violation and
 * reports a suitable [`ApiError::ObjectAlreadyExists`] for it.  Otherwise, we
 * fall back to [`sql_error_generic`].
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
 * Insert a row into table `T` and return the resulting row
 *
 * This function expects any conflict error is on the name.  The caller should
 * provide this in `unique_value` and it will be returned with an
 * [`ApiError::ObjectAlreadyExists`] error.
 */
pub async fn sql_insert_unique<R>(
    client: &tokio_postgres::Client,
    values: &SqlValueSet,
    unique_value: &str,
) -> Result<R::ModelType, ApiError>
where
    R: ResourceTable,
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
            "INSERT INTO {} ({}) VALUES ({}) RETURNING {}",
            R::TABLE_NAME,
            column_names.join(", "),
            param_names.join(", "),
            R::ALL_COLUMNS.join(", "),
        )
        .as_str(),
    );

    let row =
        sql_query_always_one(client, sql.sql_fragment(), sql.sql_params())
            .await
            .map_err(|e| {
                sql_error_on_create(R::RESOURCE_TYPE, unique_value, e)
            })?;

    R::ModelType::try_from(&row)
}

///
/// Insert a database record, not necessarily idempotently
///
/// This is used when the caller wants to explicitly handle the case of a
/// conflict or else expects that a conflict will never happen and wants to
/// treat it as an unhandleable operational error.
///
pub async fn sql_insert<T>(
    client: &tokio_postgres::Client,
    values: &SqlValueSet,
) -> Result<(), ApiError>
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
            "INSERT INTO {} ({}) VALUES ({})",
            T::TABLE_NAME,
            column_names.join(", "),
            param_names.join(", "),
        )
        .as_str(),
    );

    sql_execute(client, sql.sql_fragment(), sql.sql_params())
        .await
        .map(|_| ())
        .map_err(sql_error_generic)
}

///
/// Idempotently insert a database record.  This is intended for cases where the
/// record may conflict with an existing record with the same id, which should
/// be ignored, or with an existing record with the same name, which should
/// produce an [`ApiError::ObjectAlreadyExists`] error.
///
/// This function is intended for use (indirectly) by sagas that want to create
/// a record.  Most sagas also want the contents of the record they inserted.
/// For that, see [`sql_insert_unique_idempotent_and_fetch`].
///
/// The contents of the row are given by the key-value pairs in `values`.
///
/// The name of the column on which conflicts should be ignored is given by
/// `ignore_conflicts_on`.  Typically, `ignore_conflicts_on` would be `"id"`.
/// It must be a `&'static str` to make it hard to accidentally provide
/// untrusted input here.
///
/// In the event of a conflict on name, `unique_value` should contain the name
/// that the caller is trying to insert.  This is provided in the returned
/// [`ApiError::ObjectAlreadyExists`] error.
///
pub async fn sql_insert_unique_idempotent<'a, R>(
    client: &'a tokio_postgres::Client,
    values: &'a SqlValueSet,
    unique_value: &'a str,
    ignore_conflicts_on: &'static str,
) -> Result<(), ApiError>
where
    R: ResourceTable,
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
            R::TABLE_NAME,
            column_names.join(", "),
            param_names.join(", "),
            ignore_conflicts_on,
        )
        .as_str(),
    );

    sql_execute(client, sql.sql_fragment(), sql.sql_params())
        .await
        .map_err(|e| sql_error_on_create(R::RESOURCE_TYPE, unique_value, e))
        .map(|_| ())
}

///
/// Idempotently insert a database record and fetch it back
///
/// This function is intended for use (indirectly) by sagas that want to create a
/// record.  It's essentially [`sql_insert_unique_idempotent`] followed by
/// [`sql_fetch_row_by`].  This first attempts to insert the specified record.
/// If the record is successfully inserted, the record is fetched again (using
/// `scope_key` and `item_key`) and returned.  If the insert fails because a
/// record exists with the same id (whatever column is identified by
/// `ignore_conflicts_on`), no changes are made and the existing record is
/// returned.
///
/// The insert + fetch sequence is not atomic.  It's conceivable that another
/// client could modify the record in between.  The caller is responsible for
/// ensuring that doesn't happen or does not matter for their purposes.  In
/// practice, the surrounding saga framework should ensure that this doesn't
/// happen.
///
//
// It's worth describing this process in more detail from the perspective of our
// caller.  We'll consider the case of a saga that creates a new Instance.
// TODO This might belong in docs for the saga instead.
//
//
// EASY CASE
//
// If we do a simple INSERT, and that succeeds -- great.  That's the common case
// and it's easy.
//
//
// ERROR: CONFLICT ON INSTANCE ID
//
// What if we get a conflict error on the instance id?  Since the id is unique
// to this saga, we must assume that means that we're being invoked a second
// time after a previous one successfully updated the database.  That means one
// of two things: (1) a previous database update succeeded, but something went
// wrong (e.g., the SEC crashed) before the saga could persistently record that
// fact; or (2) a second instance of the action is concurrently running with
// this one and already completed its database update.  ((2) is only possible
// if, after evaluating use cases like this, we decide that it's okay to _allow_
// two instances of the same action to run concurrently in the case of a
// partition among SECs or the like.  We would only do that if it's clear that
// the action implementations can always handle this.)
//
// We opt to handle this by ignoring the conflict and always fetching the
// corresponding row in the table.  In the common case, we'll find our own row.
// In the conflict case, we'll find the pre-existing row.  In that latter case,
// the caller will verify that it looks the way we expect it to look (same
// "name", parameters, generation number, etc.).  It should always match what we
// would have inserted.  Having verified that, we can simply act as though we
// inserted it ourselves.  In both cases (1) and (2) above, this action will
// complete successfully and the saga can proceed.  In (2), the SEC may have to
// deal with the fact that the same action completed twice, but it doesn't have
// to _do_ anything about it (i.e., invoke an undo action).  That said, if a
// second SEC did decide to unwind the saga and undo this action, things get
// more complicated.  It would be nice if the framework guaranteed that (2)
// wasn't the case.
//
//
// ERROR: CONFLICT ON INSTANCE ID, MISSING ROW
//
// What if we get a conflict, fetch the corresponding row, and find none?  We
// should make this impossible in a correct system.  The only two things that
// might plausibly do this are (A) the undo action for this action, and (B) an
// actual instance delete operation.  We can disallow (B) by not allowing
// anything to delete an instance whose create saga never finished (indicated by
// the state in the Instance row).  Is there any implementation of sagas that
// would allow us to wind up in case (A)?  Again, if SECs went split-brain
// because one of them got to this point in the saga, hit the easy case, then
// became partitioned from the rest of the world, and then a second SEC picked
// up the saga and reran this action, and then the first saga encountered some
// _other_ failure that triggered a saga unwind, resulting in the undo action
// for this step completing, and then the second SEC winds up in this state.
// One way to avoid this would be to have the undo action, rather than deleting
// the instance record, marking the row with a state indicating it is dead (or
// maybe never even alive).  Something needs to clean these records up, but
// that's needed anyway for any sort of deleted record.
//
//
// COMBINING THE TWO QUERIES
//
// Since the case where we successfully insert the record is expected to be so
// common, we could use a RETURNING clause and only do a subsequent SELECT on
// conflict.  This would bifurcate the code paths in a way that means we'll
// almost never wind up testing the conflict path.  We could add this
// optimization if/when we find that the extra query is a problem.
//
// Relatedly, we could put the INSERT and SELECT queries into a transaction.
// This isn't necessary for our purposes.
//
//
// PROPOSED APPROACH
//
// Putting this all together, we arrive at the approach here:
//
//   ACTION:
//   - INSERT INTO Instance ... ON CONFLICT (id) DO NOTHING.
//   - SELECT * from Instance WHERE id = ...
//
//   UNDO ACTION:
//   - UPDATE Instance SET state = deleted AND time_deleted = ... WHERE
//     id = ...;
//     (this will update 0 or 1 row, and it doesn't matter to us which)
//
// There are two more considerations:
//
// Does this work?  Recall that sagas say that the effect of the sequence:
//
//   start action (1)
//   start + finish action (2)
//   start + finish undo action
//   finish action (1)
//
// must be the same as if action (1) had never happened.  This is true of the
// proposed approach.  That is, no matter what parts of the "action (1)" happen
// before or after the undo action finishes, the net result on the _database_
// will be as if "action (1)" had never occurred.
//
//
// ERROR: CONFLICT ON NAME
//
// All of the above describes what happens when there's a conflict on "id",
// which is unique to this saga.  It's also possible that there would be a
// conflict on the unique (project_id, name) index.  In that case, we just want
// to fail right away -- the whole saga is going to fail with a user error.
//
pub async fn sql_insert_unique_idempotent_and_fetch<'a, R, L>(
    client: &'a tokio_postgres::Client,
    values: &'a SqlValueSet,
    unique_value: &'a str,
    ignore_conflicts_on: &'static str,
    scope_key: L::ScopeKey,
    item_key: &'a L::ItemKey,
) -> LookupResult<R::ModelType>
where
    R: ResourceTable,
    L: LookupKey<'a, R>,
{
    sql_insert_unique_idempotent::<R>(
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
    sql_fetch_row_by::<L, R>(client, scope_key, item_key)
        .await
        .map_err(sql_error_not_missing)
}

///
/// Verifies that the given error is _not_ an `ApiObject::ObjectNotFound`.
/// If it is, then it's converted to an `ApiError::InternalError` instead,
/// on the assumption that an `ObjectNotFound` in this context is neither
/// expected nor appropriate for the caller.
///
fn sql_error_not_missing(e: ApiError) -> ApiError {
    match e {
        e @ ApiError::ObjectNotFound { .. } => ApiError::internal_error(
            &format!("unexpected ObjectNotFound: {:#}", e),
        ),
        e => e,
    }
}
