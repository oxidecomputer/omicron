/*!
 * Facilities for executing complete SQL queries
 */

use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::db::sql_error_generic;
use omicron_common::db::sql_row_value;
use std::convert::TryFrom;
use std::num::NonZeroU32;

use super::operations::sql_execute;
use super::operations::sql_query;
use super::operations::sql_query_maybe_one;
use super::sql;
use super::sql::LookupKey;
use super::sql::SqlString;
use super::sql::SqlValueSet;
use super::sql::Table;

/// Fetches a page of rows from a table `T` using the specified lookup `L`.  The
/// page is identified by `pagparams`, which contains a marker value, a scan
/// direction, and a limit of rows to fetch.  The caller can provide additional
/// constraints in SQL using `extra_cond_sql`.
async fn sql_fetch_page_raw<'a, 'b, L, T>(
    client: &'a tokio_postgres::Client,
    scope_key: L::ScopeKey,
    pagparams: &DataPageParams<'b, L::ItemKey>,
    columns: &'static [&'static str],
    extra_cond_sql: SqlString<'b>,
) -> Result<Vec<tokio_postgres::Row>, Error>
where
    L: LookupKey<'a, T>,
    T: Table,
    'a: 'b,
{
    let (mut page_select_sql, extra_fragment) =
        SqlString::new_with_params(extra_cond_sql);
    L::where_select_page(scope_key, pagparams, &mut page_select_sql);
    let limit = i64::from(pagparams.limit.get());
    let limit_clause = format!("LIMIT {}", page_select_sql.next_param(&limit));
    page_select_sql.push_str(limit_clause.as_str());

    let sql = format!(
        "SELECT {} FROM {} WHERE ({}) AND ({}) {}",
        columns.join(", "),
        T::TABLE_NAME,
        extra_fragment,
        T::LIVE_CONDITIONS,
        &page_select_sql.sql_fragment(),
    );
    let query_params = page_select_sql.sql_params();
    sql_query(client, sql.as_str(), query_params)
        .await
        .map_err(sql_error_generic)
}

/// Fetch _all_ rows from table `T` that match SQL conditions `extra_cond`.
/// This function makes paginated requests using lookup `L`.  Rows are returned
/// as a Vec of type `R`.
pub async fn sql_paginate<'a, L, T, R>(
    client: &'a tokio_postgres::Client,
    scope_key: L::ScopeKey,
    columns: &'static [&'static str],
    extra_cond: SqlString<'a>,
) -> Result<Vec<R>, Error>
where
    L: LookupKey<'a, T>,
    T: Table,
    R: for<'b> TryFrom<&'b tokio_postgres::Row, Error = Error> + Send + 'static,
{
    let mut results = Vec::new();
    let mut last_row_marker = None;
    let direction = dropshot::PaginationOrder::Ascending;
    let limit = NonZeroU32::new(100).unwrap();
    loop {
        /* TODO-error more context here about what we were doing */
        let pagparams = DataPageParams {
            marker: last_row_marker.as_ref(),
            direction,
            limit,
        };
        let rows = sql_fetch_page_raw::<L, T>(
            client,
            scope_key,
            &pagparams,
            columns,
            extra_cond.clone(),
        )
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
                .collect::<Result<Vec<R>, Error>>()?,
        );
    }
}

/// Describes a successful result of [`sql_update_precond`]
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
///   [`Error::ObjectNotFound`] error is returned.
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
) -> Result<UpdatePrecond, Error>
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

/// Insert a database record, not necessarily idempotently
///
/// This is used when the caller wants to explicitly handle the case of a
/// conflict or else expects that a conflict will never happen and wants to
/// treat it as an unhandleable operational error.
pub async fn sql_insert<T>(
    client: &tokio_postgres::Client,
    values: &SqlValueSet,
) -> Result<(), Error>
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
