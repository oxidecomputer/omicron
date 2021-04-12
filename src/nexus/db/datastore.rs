/*!
 * Primary control plane interface for database read and write operations
 */

/*
 * XXX review all queries for use of indexes (may need "time_deleted IS
 * NOT NULL" conditions)
 *
 * Figure out how to automate this.  It's tricky, but maybe the way to think
 * about this is that we have Tables, and for each Table there's a fixed set of
 * WHERE clauses, each of which includes a string SQL fragment and a set of
 * parameters.  This way, we can easily identify the relatively small set of
 * unique WHERE clauses, but in a way that can also be inserted into UPDATE,
 * DELETE, SELECT, or CTEs using these, etc.
 *
 * Inventory of queries not done so far:
 * o update Project (possibly including conflict on unique name)
 * o create Instance/Disk (INSERT ON CONFLICT DO NOTHING, SELECT, check state)
 * o project delete (currently UPDATE, but probably needs similar CTE treatment)
 *
 * This probably breaks down into:
 * o INSERT (new trait similar to what's used for pagination, plus surrounding
 *   interfaces for doing this ON CONFLICT DO NOTHING and check state)
 * o update-based-on-precondition using CTE
 * o update-and-handle-conflict-error (same as previous case?)
 */

use super::Pool;
use crate::api_error::ApiError;
use crate::api_model::ApiDisk;
use crate::api_model::ApiDiskAttachment;
use crate::api_model::ApiDiskCreateParams;
use crate::api_model::ApiDiskRuntimeState;
use crate::api_model::ApiDiskState;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiInstanceRuntimeState;
use crate::api_model::ApiInstanceState;
use crate::api_model::ApiName;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_model::ApiResourceType;
use crate::api_model::CreateResult;
use crate::api_model::DataPageParams;
use crate::api_model::DeleteResult;
use crate::api_model::ListResult;
use crate::api_model::LookupResult;
use crate::api_model::UpdateResult;
use crate::bail_unless;
use chrono::DateTime;
use chrono::Utc;
use futures::StreamExt;
use std::convert::TryFrom;
use std::future::Future;
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use super::operations::sql_error_generic;
use super::operations::sql_execute;
use super::operations::sql_execute_maybe_one;
use super::operations::sql_query;
use super::operations::sql_query_always_one;
use super::operations::sql_query_maybe_one;
use super::operations::sql_row_value;
use super::operations::DbError;
use super::schema::Disk;
use super::schema::Instance;
use super::schema::LookupByAttachedInstance;
use super::schema::LookupByUniqueId;
use super::schema::LookupByUniqueName;
use super::schema::LookupByUniqueNameInProject;
use super::schema::Project;
use super::sql;
use super::sql::LookupKey;
use super::sql::SqlSerialize;
use super::sql::SqlString;
use super::sql::SqlValueSet;
use super::sql::Table;

// XXX document, refactor, etc.
pub struct UpdatePrecond {
    pub found_state: tokio_postgres::Row,
    pub updated: bool,
}

pub struct DataStore {
    pool: Arc<Pool>,
}

impl DataStore {
    pub fn new(pool: Arc<Pool>) -> Self {
        DataStore { pool }
    }

    /// Fetch parts of a row using the specified lookup
    async fn fetch_row_raw<'a, L, T>(
        &self,
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
    async fn fetch_row_by<'a, L, T>(
        &self,
        client: &tokio_postgres::Client,
        scope_key: L::ScopeKey,
        item_key: &'a L::ItemKey,
    ) -> LookupResult<T::ModelType>
    where
        L: LookupKey<'a>,
        T: Table,
    {
        let row = self
            .fetch_row_raw::<L, T>(client, scope_key, item_key, T::ALL_COLUMNS)
            .await?;
        T::ModelType::try_from(&row)
    }

    /// Fetch a page of rows from a table using the specified lookup
    async fn fetch_page_from_table<'a, L, T>(
        &self,
        client: &'a tokio_postgres::Client,
        scope_key: L::ScopeKey,
        pagparams: &'a DataPageParams<'a, L::ItemKey>,
    ) -> ListResult<T::ModelType>
    where
        L: LookupKey<'a>,
        L::ScopeKey: 'a,
        T: Table,
    {
        self.fetch_page_by::<L, T, T::ModelType>(
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
     * The explicit desugaring of "async fn" here is due to
     * rust-lang/rust#63033.
     * TODO-cleanup review the lifetimes and bounds on fetch_page_by()
     */
    fn fetch_page_by<'a, L, T, R>(
        &self,
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
            let list = rows
                .iter()
                .map(R::try_from)
                .collect::<Vec<Result<R, ApiError>>>();
            Ok(futures::stream::iter(list).boxed())
        }
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
    pub async fn update_precond<'a, 'b, T, L>(
        &self,
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
        Ok(UpdatePrecond {
            found_state: row,
            updated: check_updated_key.is_some(),
        })
    }

    /// Create a project
    pub async fn project_create_with_id(
        &self,
        new_id: &Uuid,
        new_project: &ApiProjectCreateParams,
    ) -> CreateResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        let mut values = SqlValueSet::new();
        values.set("id", new_id);
        values.set("time_created", &now);
        values.set("time_modified", &now);
        values.set("time_deleted", &(None as Option<DateTime<Utc>>));
        new_project.sql_serialize(&mut values);
        sql_insert_unique::<Project>(
            &client,
            &values,
            &new_project.identity.name.as_str(),
        )
        .await
    }

    /// Fetch metadata for a project
    pub async fn project_fetch(
        &self,
        project_name: &ApiName,
    ) -> LookupResult<ApiProject> {
        let client = self.pool.acquire().await?;
        self.fetch_row_by::<LookupByUniqueName, Project>(
            &client,
            (),
            project_name,
        )
        .await
    }

    /// Delete a project
    /*
     * TODO-correctness This needs to check whether there are any resources that
     * depend on the Project (Disks, Instances).  We can do this with a
     * generation counter that gets bumped when these resources are created.
     */
    pub async fn project_delete(&self, project_name: &ApiName) -> DeleteResult {
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        sql_execute_maybe_one(
            &client,
            format!(
                "UPDATE {} SET time_deleted = $1 WHERE \
                    time_deleted IS NULL AND name = $2 LIMIT 2 \
                    RETURNING {}",
                Project::TABLE_NAME,
                Project::ALL_COLUMNS.join(", ")
            )
            .as_str(),
            &[&now, &project_name],
            || {
                ApiError::not_found_by_name(
                    ApiResourceType::Project,
                    project_name,
                )
            },
        )
        .await
    }

    /// Look up the id for a project based on its name
    pub async fn project_lookup_id_by_name(
        &self,
        name: &ApiName,
    ) -> Result<Uuid, ApiError> {
        let client = self.pool.acquire().await?;
        let row = self
            .fetch_row_raw::<LookupByUniqueName, Project>(
                &client,
                (),
                name,
                &["id"],
            )
            .await?;
        sql_row_value(&row, "id")
    }

    /// List a page of projects by id
    pub async fn projects_list_by_id(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<ApiProject> {
        let client = self.pool.acquire().await?;
        self.fetch_page_from_table::<LookupByUniqueId, Project>(
            &client,
            (),
            pagparams,
        )
        .await
    }

    /// List a page of projects by name
    pub async fn projects_list_by_name(
        &self,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiProject> {
        let client = self.pool.acquire().await?;
        self.fetch_page_by::<LookupByUniqueName, Project, <Project as Table>::ModelType>(
            &client,
            (),
            pagparams,
            Project::ALL_COLUMNS,
        )
        .await
    }

    /// Updates a project by name (clobbering update -- no etag)
    pub async fn project_update(
        &self,
        project_name: &ApiName,
        update_params: &ApiProjectUpdateParams,
    ) -> UpdateResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();

        let mut sql = format!(
            "UPDATE {} SET time_modified = $1 ",
            Project::TABLE_NAME
        );
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&now];

        if let Some(new_name) = &update_params.identity.name {
            sql.push_str(&format!(", name = ${} ", params.len() + 1));
            params.push(new_name);
        }

        if let Some(new_description) = &update_params.identity.description {
            sql.push_str(&format!(", description = ${} ", params.len() + 1));
            params.push(new_description);
        }

        sql.push_str(&format!(
            " WHERE name = ${} AND time_deleted IS NULL LIMIT 2 RETURNING {}",
            params.len() + 1,
            Project::ALL_COLUMNS.join(", ")
        ));
        params.push(project_name);

        let row = sql_query_maybe_one(&client, sql.as_str(), &params, || {
            ApiError::not_found_by_name(ApiResourceType::Project, project_name)
        })
        .await?;
        Ok(ApiProject::try_from(&row)?)
    }

    /*
     * Instances
     */

    pub async fn project_create_instance(
        &self,
        instance_id: &Uuid,
        project_id: &Uuid,
        params: &ApiInstanceCreateParams,
        runtime_initial: &ApiInstanceRuntimeState,
    ) -> CreateResult<ApiInstance> {
        //
        // XXX Idempotently insert a record for this instance.  The following
        // discussion describes our caller, not technically this function, but
        // we probably want to define this function such that it makes sense for
        // the caller.
        //
        // If we do a simple INSERT, and that succeeds -- great.  That's the
        // common case and it's easy
        //
        // What if we get a conflict error on the instance id?  Since the id is
        // unique to this saga, we must assume that means that we're being
        // invoked a second time after a previous one successfully updated the
        // database.  That means one of two things: (1) a previous database
        // update succeeded, but something went wrong (e.g., the SEC crashed)
        // before the saga could persistently record that fact; or (2) a second
        // instance of the action is concurrently running with this one and
        // already completed its database update.  ((2) is only possible if,
        // after evaluating use cases like this, we decide that it's okay to
        // _allow_ two instances of the same action to run concurrently in the
        // case of a partition among SECs or the like.  We would only do that if
        // it's clear that the action implementations can always handle this.)
        //
        // How do we want to handle this?  One option would be to ignore the
        // conflict and then always fetch the corresponding row in the table.
        // In the common case, we'll find our own row.  In the conflict case,
        // we'll find the pre-existing row.  In that latter case, we should
        // certainly verify that it looks the way we expect it to look (same
        // "name", parameters, generation number, etc.), and it always should.
        // Having verified that, we can simply act as though we inserted it
        // ourselves.  In both cases (1) and (2) above, this action will
        // complete successfully and the saga can proceed.  In (2), the SEC may
        // have to deal with the fact that the same action completed twice, but
        // it doesn't have to _do_ anything about it (i.e., invoke an undo
        // action).  That said, if a second SEC did decide to unwind the saga
        // and undo this action, things get more complicated.  It sure would be
        // nice if the framework could guarantee that (2) wasn't the case.
        //
        // What if we get a conflict, fetch the corresponding row, and find
        // none?  What would ever delete an Instance row?  The obvious
        // candidates would be (A) the undo action for this action, and (B) an
        // actual instance delete operation.  Presumably we can disallow (B) by
        // not allowing anything to delete an instance whose create saga never
        // finished (indicated by some state in the Instance row).  Is there any
        // implementation of sagas that would allow us to wind up in case (A)?
        // Again, if SECs went split-brain because one of them got to this point
        // in the saga, hit the easy case, then became partitioned from the rest
        // of the world, and then a second SEC picked up the saga and reran this
        // action, and then the first saga encountered some _other_ failure that
        // triggered a saga unwind, resulting in the undo action for this step
        // completing, and then the second SEC winds up in this state.  One way
        // to avoid this would be to have the undo action, rather than deleting
        // the instance record, marking the row with a state indicating it is
        // dead (or maybe never even alive).  Something would need to clean
        // these up, but something will be needed anyway to clean up deleted
        // records.
        //
        // Finally, there's one optimization we can apply here: since the common
        // case is so common, we could use the RETURNING clause as long as it's
        // present, and only do a subsequent SELECT on conflict.  This does
        // bifurcate the code paths in a way that means we'll almost never wind
        // up testing the conflict path.  Maybe we can add this optimization
        // if/when we find that the write + read database accesses here are
        // really a problem.
        //
        // This leaves us with the following proposed approach:
        //
        // ACTION:
        // - INSERT INTO Instance ... ON CONFLICT (id) DO NOTHING.
        // - SELECT * from Instance WHERE id = ...
        //
        // UNDO ACTION:
        // - UPDATE Instance SET state = deleted AND time_deleted = ... WHERE
        //   id = ...;
        //   (this will update 0 or 1 row, and it doesn't matter to us which)
        //
        // We're not done!  There are two more considerations:
        //
        // (1) Sagas say that the effect of the sequence:
        //
        //       start action (1)
        //       start + finish action (2)
        //       start + finish undo action
        //       finish action (1)
        //
        //     must be the same as if action (1) had never happened.  This is
        //     true of the proposed approach.  That is, no matter what parts of
        //     the "action (1)" happen before or after the undo action finishes,
        //     the net result on the _database_ will be as if "action (1)" had
        //     never occurred.
        //
        // (2) All of the above describes what happens when there's a conflict
        //     on "id", which is unique to this saga.  It's also possible that
        //     there would be a conflict on the unique (project_id, name) index.
        //     In that case, we just want to fail right away -- the whole saga
        //     is going to fail with a user error.
        //
        // TODO-design It seems fair to say that this behavior is sufficiently
        // caller-specific that it does not belong in the datastore.  But what
        // does?
        //

        //
        // XXX To-be-determined:
        //
        // Should the two SQL statements in the ACTION happen in one
        // transaction?
        //
        // Is it going to be a problem that there are two possible conflicts
        // here?  CockroachDB only supports one "arbiter" -- but I think that
        // means _for a given constraint_, there can be only one thing imposing
        // it.  We have two different constraints here.
        //
        let client = self.pool.acquire().await?;
        let now = runtime_initial.time_updated;
        let mut values = SqlValueSet::new();
        values.set("id", instance_id);
        values.set("time_created", &now);
        values.set("time_modified", &now);
        values.set("time_deleted", &(None as Option<DateTime<Utc>>));
        values.set("project_id", project_id);
        params.sql_serialize(&mut values);
        runtime_initial.sql_serialize(&mut values);
        sql_insert_unique_idempotent::<Instance>(
            &client,
            &mut values,
            params.identity.name.as_str(),
            "id",
        )
        .await?;

        /*
         * If we get here, then we successfully inserted the record.  It would
         * be a bug if something else were to remove that record before we have
         * a chance to fetch it.
         */
        let instance = self
            .fetch_row_by::<LookupByUniqueId, Instance>(
                &client,
                (),
                instance_id,
            )
            .await
            .map_err(|e| {
                /* XXX helper function for this */
                if !matches!(e, ApiError::ObjectNotFound { .. }) {
                    ApiError::internal_error(&format!(
                        "failed to find saga-created record \
                        after creating it: {:#}",
                        e
                    ))
                } else {
                    e
                }
            })?;

        bail_unless!(
            instance.runtime.run_state == ApiInstanceState::Creating,
            "newly-created Instance has unexpected state: {:?}",
            instance.runtime.run_state
        );
        bail_unless!(
            instance.runtime.gen == runtime_initial.gen,
            "newly-created Instance has unexpected generation: {:?}",
            instance.runtime.gen
        );
        Ok(instance)
    }

    pub async fn project_list_instances(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiInstance> {
        let client = self.pool.acquire().await?;
        self.fetch_page_by::<LookupByUniqueNameInProject, Instance, <Instance as Table>::ModelType>(
            &client,
            (project_id,),
            pagparams,
            Instance::ALL_COLUMNS,
        )
        .await
    }

    pub async fn instance_fetch(
        &self,
        instance_id: &Uuid,
    ) -> LookupResult<ApiInstance> {
        let client = self.pool.acquire().await?;
        self.fetch_row_by::<LookupByUniqueId, Instance>(
            &client,
            (),
            instance_id,
        )
        .await
    }

    pub async fn instance_fetch_by_name(
        &self,
        project_id: &Uuid,
        instance_name: &ApiName,
    ) -> LookupResult<ApiInstance> {
        let client = self.pool.acquire().await?;
        self.fetch_row_by::<LookupByUniqueNameInProject, Instance>(
            &client,
            (project_id,),
            instance_name,
        )
        .await
    }

    /*
     * TODO-design It's tempting to return the updated state of the Instance
     * here because it's convenient for consumers and by using a RETURNING
     * clause, we could ensure that the "update" and "fetch" are atomic.
     * But in the unusual case that we _don't_ update the row because our
     * update is older than the one in the database, we would have to fetch
     * the current state explicitly.  For now, we'll just require consumers
     * to explicitly fetch the state if they want that.
     */
    pub async fn instance_update_runtime(
        &self,
        instance_id: &Uuid,
        new_runtime: &ApiInstanceRuntimeState,
    ) -> Result<bool, ApiError> {
        let client = self.pool.acquire().await?;

        let mut values = SqlValueSet::new();
        new_runtime.sql_serialize(&mut values);

        let mut cond_sql = SqlString::new();
        let param = cond_sql.next_param(&new_runtime.gen);
        cond_sql.push_str(&format!("state_generation < {}", param));

        let update = self
            .update_precond::<Instance, LookupByUniqueId>(
                &client,
                (),
                instance_id,
                &["state_generation"],
                &values,
                cond_sql,
            )
            .await?;
        let row = &update.found_state;
        let found_id: Uuid = sql_row_value(&row, "found_id")?;
        bail_unless!(found_id == *instance_id);
        Ok(update.updated)
    }

    pub async fn project_delete_instance(
        &self,
        instance_id: &Uuid,
    ) -> DeleteResult {
        /*
         * This is subject to change, but for now we're going to say that an
         * instance must be "stopped" or "failed" in order to delete it.  The
         * delete operation sets "time_deleted" (just like with other objects)
         * and also sets the state to "destroyed".  By virtue of being
         * "stopped", we assume there are no dependencies on this instance
         * (e.g., disk attachments).  If that changes, we'll want to check for
         * such dependencies here.
         */
        let client = self.pool.acquire().await?;
        let now = Utc::now();

        let mut values = SqlValueSet::new();
        values.set("instance_state", &ApiInstanceState::Destroyed);
        values.set("time_deleted", &now);

        let mut cond_sql = SqlString::new();
        let p1 = cond_sql.next_param(&ApiInstanceState::Stopped);
        let p2 = cond_sql.next_param(&ApiInstanceState::Failed);
        cond_sql.push_str(&format!("instance_state in ({}, {})", p1, p2));

        let update = self
            .update_precond::<Instance, LookupByUniqueId>(
                &client,
                (),
                instance_id,
                &["instance_state", "time_deleted"],
                &values,
                cond_sql,
            )
            .await?;

        let row = &update.found_state;
        let found_id: Uuid = sql_row_value(&row, "found_id")?;
        let instance_state: ApiInstanceState =
            sql_row_value(&row, "found_instance_state")?;
        bail_unless!(found_id == *instance_id);

        if update.updated {
            Ok(())
        } else {
            Err(ApiError::InvalidRequest {
                message: format!(
                    "instance cannot be deleted in state \"{}\"",
                    instance_state
                ),
            })
        }
    }

    /*
     * Disks
     */

    /**
     * List disks associated with a given instance.
     */
    pub async fn instance_list_disks(
        &self,
        instance_id: &Uuid,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiDiskAttachment> {
        let client = self.pool.acquire().await?;
        self.fetch_page_by::<LookupByAttachedInstance, Disk, ApiDiskAttachment>(
            &client,
            (instance_id,),
            pagparams,
            &["id", "name", "disk_state", "attach_instance_id"],
        )
        .await
    }

    pub async fn project_create_disk(
        &self,
        disk_id: &Uuid,
        project_id: &Uuid,
        params: &ApiDiskCreateParams,
        runtime_initial: &ApiDiskRuntimeState,
    ) -> CreateResult<ApiDisk> {
        /* See project_create_instance() */
        /* XXX commonize with project_create_instance() */
        let client = self.pool.acquire().await?;
        let now = runtime_initial.time_updated;
        let mut values = SqlValueSet::new();
        values.set("id", disk_id);
        values.set("time_created", &now);
        values.set("time_modified", &now);
        values.set("time_deleted", &(None as Option<DateTime<Utc>>));
        values.set("project_id", project_id);
        params.sql_serialize(&mut values);
        runtime_initial.sql_serialize(&mut values);
        sql_insert_unique_idempotent::<Disk>(
            &client,
            &mut values,
            params.identity.name.as_str(),
            "id",
        )
        .await?;

        /*
         * If we get here, then we successfully inserted the record.  It would
         * be a bug if something else were to remove that record before we have
         * a chance to fetch it.
         * XXX Still want to commonize with project_create_instance
         */
        let disk = self
            .fetch_row_by::<LookupByUniqueId, Disk>(&client, (), disk_id)
            .await
            .map_err(|e| {
                /* XXX helper function for this */
                if !matches!(e, ApiError::ObjectNotFound { .. }) {
                    ApiError::internal_error(&format!(
                        "failed to find saga-created record \
                        after creating it: {:#}",
                        e
                    ))
                } else {
                    e
                }
            })?;

        bail_unless!(
            disk.runtime.disk_state == ApiDiskState::Creating,
            "newly-created Disk has unexpected state: {:?}",
            disk.runtime.disk_state
        );
        bail_unless!(
            disk.runtime.gen == runtime_initial.gen,
            "newly-created Disk has unexpected generation: {:?}",
            disk.runtime.gen
        );
        Ok(disk)
    }

    pub async fn project_list_disks(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiDisk> {
        let client = self.pool.acquire().await?;
        self.fetch_page_by::<LookupByUniqueNameInProject, Disk, <Disk as Table>::ModelType>(
            &client,
            (project_id,),
            pagparams,
            Disk::ALL_COLUMNS,
        )
        .await
    }

    pub async fn disk_update_runtime(
        &self,
        disk_id: &Uuid,
        new_runtime: &ApiDiskRuntimeState,
    ) -> Result<bool, ApiError> {
        let client = self.pool.acquire().await?;

        let mut values = SqlValueSet::new();
        new_runtime.sql_serialize(&mut values);

        let mut cond_sql = SqlString::new();
        let param = cond_sql.next_param(&new_runtime.gen);
        cond_sql.push_str(&format!("state_generation < {}", param));

        let update = self
            .update_precond::<Disk, LookupByUniqueId>(
                &client,
                (),
                disk_id,
                &["state_generation"],
                &values,
                cond_sql,
            )
            .await?;
        let row = &update.found_state;
        let found_id: Uuid = sql_row_value(&row, "found_id")?;
        bail_unless!(found_id == *disk_id);
        Ok(update.updated)
    }

    pub async fn disk_fetch(&self, disk_id: &Uuid) -> LookupResult<ApiDisk> {
        let client = self.pool.acquire().await?;
        self.fetch_row_by::<LookupByUniqueId, Disk>(&client, (), disk_id).await
    }

    pub async fn disk_fetch_by_name(
        &self,
        project_id: &Uuid,
        disk_name: &ApiName,
    ) -> LookupResult<ApiDisk> {
        let client = self.pool.acquire().await?;
        self.fetch_row_by::<LookupByUniqueNameInProject, Disk>(
            &client,
            (project_id,),
            disk_name,
        )
        .await
    }

    pub async fn project_delete_disk(&self, disk_id: &Uuid) -> DeleteResult {
        let client = self.pool.acquire().await?;
        let now = Utc::now();

        let mut values = SqlValueSet::new();
        values.set("time_deleted", &now);
        ApiDiskState::Destroyed.sql_serialize(&mut values);

        let mut cond_sql = SqlString::new();
        let disk_state_detached = ApiDiskState::Detached.to_string();
        let p1 = cond_sql.next_param(&disk_state_detached);
        let disk_state_faulted = ApiDiskState::Faulted.to_string();
        let p2 = cond_sql.next_param(&disk_state_faulted);
        cond_sql.push_str(&format!("disk_state in ({}, {})", p1, p2));

        let update = self
            .update_precond::<Disk, LookupByUniqueId>(
                &client,
                (),
                disk_id,
                &["disk_state", "attach_instance_id", "time_deleted"],
                &values,
                cond_sql,
            )
            .await?;

        let row = &update.found_state;
        let found_id: Uuid = sql_row_value(&row, "found_id")?;
        bail_unless!(found_id == *disk_id);

        // TODO-cleanup It would be nice to use
        // ApiDiskState::try_from(&tokio_postgres::Row), but the column names
        // are different here.
        let disk_state_str: &str = sql_row_value(&row, "found_disk_state")?;
        let attach_instance_id: Option<Uuid> =
            sql_row_value(&row, "found_attach_instance_id")?;
        let found_disk_state =
            ApiDiskState::try_from((disk_state_str, attach_instance_id))
                .map_err(|e| ApiError::internal_error(&e))?;

        if update.updated {
            Ok(())
        } else {
            Err(ApiError::InvalidRequest {
                message: format!(
                    "disk cannot be deleted in state \"{}\"",
                    found_disk_state
                ),
            })
        }
    }
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
async fn sql_insert_unique<T>(
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
fn sql_insert_unique_idempotent<'a, T>(
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
