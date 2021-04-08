/*!
 * Primary control plane interface for database read and write operations
 */

/*
 * XXX review all queries for use of indexes (may need "time_deleted IS
 * NOT NULL" conditions)
 *
 * XXX document the pattern here: things that can be expressed in a single SQL
 * column impl ToSql/FromSql directly.  Things that don't impl TryFrom<&Row>.
 * These definitions probably ought to go in a file of their own:
 * db/conversions.rs?
 */

use super::Pool;
use crate::api_error::ApiError;
use crate::api_model::ApiByteCount;
use crate::api_model::ApiDisk;
use crate::api_model::ApiDiskAttachment;
use crate::api_model::ApiDiskCreateParams;
use crate::api_model::ApiDiskRuntimeState;
use crate::api_model::ApiDiskState;
use crate::api_model::ApiGeneration;
use crate::api_model::ApiIdentityMetadata;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceCpuCount;
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
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::types::FromSql;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

/*
 * TODO-debug It would be nice to have a backtrace in these errors.  thiserror
 * is capable of including one automatically if there's a member called
 * "backtrace" with the type "Backtrace", but it's not clear how that would get
 * populated aside from maybe a thiserror-generated `From` impl.  It looks like
 * one has to do that explicitly using std::backtrace::Backtrace::capture(),
 * which is a nightly-only API.  So we'll defer that for now.
 */
#[derive(Error, Debug)]
enum DbError {
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

pub struct DataStore {
    pool: Arc<Pool>,
}

impl DataStore {
    pub fn new(pool: Arc<Pool>) -> Self {
        DataStore { pool }
    }

    /// Create a project
    pub async fn project_create_with_id(
        &self,
        new_id: &Uuid,
        new_project: &ApiProjectCreateParams,
    ) -> CreateResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        sql_insert_unique(
            &client,
            Project,
            Project::ALL_COLUMNS,
            &new_project.identity.name.as_str(),
            "",
            &[
                new_id,
                &new_project.identity.name,
                &new_project.identity.description,
                &now,
                &now,
                &(None as Option<DateTime<Utc>>),
            ],
        )
        .await
    }

    /// Fetch metadata for a project
    pub async fn project_fetch(
        &self,
        project_name: &ApiName,
    ) -> LookupResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let row = sql_query_maybe_one(
            &client,
            format!(
                "SELECT {} FROM {} WHERE time_deleted IS NULL AND \
                        name = $1 LIMIT 2",
                Project::ALL_COLUMNS.join(", "),
                Project::TABLE_NAME
            )
            .as_str(),
            &[&project_name],
            || {
                ApiError::not_found_by_name(
                    ApiResourceType::Project,
                    project_name,
                )
            },
        )
        .await?;
        Ok(ApiProject::try_from(&row)?)
    }

    /// Delete a project
    pub async fn project_delete(&self, project_name: &ApiName) -> DeleteResult {
        /*
         * XXX TODO-correctness This needs to check whether the project has any
         * resources in it.  One way to do that would be to define a certain
         * kind of generation number, maybe called a "resource-creation
         * generation number" ("rcgen").  Every resource creation bumps the
         * "rgen" of the parent project.  This operation fetches the current
         * rgen of the project, then checks for various kinds of resources, then
         * does the following UPDATE that sets time_deleted _conditional_ on the
         * rgen being the same.  If this updates 0 rows, either the project is
         * gone already or a new resource has been created.  (We'll want to make
         * sure that we report the correct error!)  We'll want to think more
         * carefully about this scheme, and maybe alternatives.  (Another idea
         * would be to have resource creation and deletion update a regular
         * counter.  But isn't that the same as denormalizing this piece of
         * information?)
         *
         * Can we do all this in one big query, maybe with a few CTEs?  (e.g.,
         * something like:
         *
         * WITH project AS
         *     (select id from Project where time_deleted IS NULL and name =
         *     $1),
         *     project_instances as (select id from Instance where time_deleted
         *     IS NULL and project_id = project.id LIMIT 1),
         *     project_disks as (select id from Disk where time_deleted IS NULL
         *     and project_id = project.id LIMIT 1),
         *
         *     UPDATE Project set time_deleted = $1 WHERE time_deleted IS NULL
         *         AND id = project.id AND project_instances ...
         *
         * I'm not sure how to finish that SQL, and moreover, I'm not sure it
         * solves the problem.  You can still create a new instance after
         * listing the instances.  So I guess you still need the "rcgen".
         * Still, you could potentially combine these to do it all in one go:
         *
         * WITH project AS
         *     (select id,rcgen from Project where time_deleted IS NULL and
         *     name = $1),
         *     project_instances as (select id from Instance where time_deleted
         *     IS NULL and project_id = project.id LIMIT 1),
         *     project_disks as (select id from Disk where time_deleted IS NULL
         *     and project_id = project.id LIMIT 1),
         *
         *     UPDATE Project set time_deleted = $1 WHERE time_deleted IS NULL
         *         AND id = project.id AND rcgen = project.rcgen AND
         *         project_instances ...
         */
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

        let row = sql_query_maybe_one(
            &client,
            format!(
                "SELECT id FROM {} WHERE name = $1 AND \
                        time_deleted IS NULL LIMIT 2",
                Project::TABLE_NAME
            )
            .as_str(),
            &[&name],
            || ApiError::not_found_by_name(ApiResourceType::Project, name),
        )
        .await?;
        sql_row_value(&row, 0)
    }

    /// List a page of projects by id
    pub async fn projects_list_by_id(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let rows = sql_pagination(
            &client,
            Project,
            Project::ALL_COLUMNS,
            "time_deleted IS NULL",
            &[],
            "id",
            pagparams,
        )
        .await?;
        let list = rows
            .iter()
            .map(|row| ApiProject::try_from(row))
            .collect::<Vec<Result<ApiProject, ApiError>>>();
        Ok(futures::stream::iter(list).boxed())
    }

    /// List a page of projects by name
    pub async fn projects_list_by_name(
        &self,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiProject> {
        let client = self.pool.acquire().await?;
        let rows = sql_pagination(
            &client,
            Project,
            Project::ALL_COLUMNS,
            "time_deleted IS NULL",
            &[],
            "name",
            pagparams,
        )
        .await?;
        let list = rows
            .iter()
            .map(|row| ApiProject::try_from(row))
            .collect::<Vec<Result<ApiProject, ApiError>>>();
        Ok(futures::stream::iter(list).boxed())
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
            "UPDATE {} SET time_metadata_updated = $1 ",
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
        let now = Utc::now();
        sql_insert_unique(
            &client,
            Instance,
            Instance::ALL_COLUMNS,
            params.identity.name.as_str(),
            "ON CONFLICT (id) DO NOTHING",
            &[
                instance_id,
                &params.identity.name,
                &params.identity.description,
                &now,
                &now,
                &(None as Option<DateTime<Utc>>),
                project_id,
                &runtime_initial.run_state.to_string(),
                &now,
                &runtime_initial.gen,
                &runtime_initial.sled_uuid,
                &params.ncpus,
                &params.memory,
                &params.hostname,
            ],
        )
        .await?;

        /*
         * If we get here, then we successfully inserted the record.  It would
         * be a bug if something else were to remove that record before we have
         * a chance to fetch it.  As a result, sql_query_always_one() is correct
         * here, even though it looks like this query could legitimately return
         * 0 rows.
         */
        let row = sql_query_always_one(
            &client,
            format!(
                "SELECT {} FROM {} WHERE id = $1",
                Instance::ALL_COLUMNS.join(", "),
                Instance::TABLE_NAME,
            )
            .as_str(),
            &[instance_id],
        )
        .await
        .map_err(|e| {
            sql_error_on_create(
                ApiResourceType::Instance,
                &params.identity.name.as_str(),
                e,
            )
        })?;

        let instance = ApiInstance::try_from(&row)?;
        if instance.runtime.run_state != ApiInstanceState::Creating
            || instance.runtime.gen != runtime_initial.gen
        {
            Err(ApiError::internal_error(&format!(
                "creating instance: found existing instance with \
                        unexpected state ({:?}) or generation ({})",
                instance.runtime.run_state, instance.runtime.gen,
            )))
        } else {
            Ok(instance)
        }
    }

    pub async fn project_list_instances(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiInstance> {
        let client = self.pool.acquire().await?;
        let rows = sql_pagination(
            &client,
            Instance,
            Instance::ALL_COLUMNS,
            "time_deleted IS NULL AND project_id = $1",
            &[project_id],
            "name",
            pagparams,
        )
        .await?;
        let list = rows
            .iter()
            .map(|row| ApiInstance::try_from(row))
            .collect::<Vec<Result<ApiInstance, ApiError>>>();
        Ok(futures::stream::iter(list).boxed())
    }

    pub async fn instance_fetch(
        &self,
        instance_id: &Uuid,
    ) -> LookupResult<ApiInstance> {
        let client = self.pool.acquire().await?;
        let row = sql_query_maybe_one(
            &client,
            format!(
                "SELECT {} FROM {} WHERE time_deleted IS NULL AND \
                        id = $1 LIMIT 2",
                Instance::ALL_COLUMNS.join(", "),
                Instance::TABLE_NAME
            )
            .as_str(),
            &[&instance_id],
            || {
                ApiError::not_found_by_id(
                    ApiResourceType::Instance,
                    instance_id,
                )
            },
        )
        .await?;
        Ok(ApiInstance::try_from(&row)?)
    }

    pub async fn instance_fetch_by_name(
        &self,
        project_id: &Uuid,
        instance_name: &ApiName,
    ) -> LookupResult<ApiInstance> {
        let client = self.pool.acquire().await?;
        let row = sql_query_maybe_one(
            &client,
            format!(
                "SELECT {} FROM {} WHERE time_deleted IS NULL AND \
                        project_id = $1 AND name = $2 LIMIT 2",
                Instance::ALL_COLUMNS.join(", "),
                Instance::TABLE_NAME
            )
            .as_str(),
            &[project_id, instance_name],
            || {
                ApiError::not_found_by_name(
                    ApiResourceType::Instance,
                    instance_name,
                )
            },
        )
        .await?;
        Ok(ApiInstance::try_from(&row)?)
    }

    pub async fn instance_update_runtime(
        &self,
        instance_id: &Uuid,
        new_runtime: &ApiInstanceRuntimeState,
    ) -> Result<bool, ApiError> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();

        /*
         * TODO-design It's tempting to return the updated state of the Instance
         * here because it's convenient for consumers and by using a RETURNING
         * clause, we could ensure that the "update" and "fetch" are atomic.
         * But in the unusual case that we _don't_ update the row because our
         * update is older than the one in the database, we would have to fetch
         * the current state explicitly.  For now, we'll just require consumers
         * to explicitly fetch the state if they want that.
         */
        // XXX Definitely needs to be commonized.  See instance delete and disk
        // runtime update.
        let row = sql_query_maybe_one(
            &client,
            format!(
                "WITH found_rows AS \
                (SELECT id, state_generation FROM {} WHERE id = $5), \
                updated_rows AS \
                (UPDATE {} SET \
                        instance_state = $1, \
                        state_generation = $2, \
                        active_server_id = $3, \
                        time_state_updated = $4 \
                    WHERE id = $5 AND state_generation < $2 \
                    LIMIT 2 RETURNING id) \
                SELECT r.id AS found_id, \
                    r.state_generation as initial_generation, \
                    u.id AS updated_id FROM found_rows r \
                    FULL OUTER JOIN updated_rows u ON r.id = u.id",
                Instance::TABLE_NAME,
                Instance::TABLE_NAME,
            )
            .as_str(),
            &[
                &new_runtime.run_state.to_string(),
                &new_runtime.gen,
                &new_runtime.sled_uuid,
                &now,
                instance_id,
            ],
            || {
                ApiError::not_found_by_id(
                    ApiResourceType::Instance,
                    instance_id,
                )
            },
        )
        .await?;

        let found_id: Uuid = sql_row_value(&row, "found_id")?;
        let previous_gen: ApiGeneration =
            sql_row_value(&row, "initial_generation")?;
        let updated_id: Option<Uuid> = sql_row_value(&row, "updated_id")?;
        bail_unless!(found_id == *instance_id);

        if let Some(uid) = updated_id {
            bail_unless!(uid == *instance_id);
            Ok(true)
        } else {
            // XXX log
            Ok(false)
        }
    }

    pub async fn project_delete_instance(
        &self,
        instance_id: &Uuid,
    ) -> DeleteResult {
        /*
         * This is subject to change, but for now we're going to say that an
         * instance must be "stopped" or "failed" in order to delete it.  The
         * delete operation sets "time_deleted" (just like with other objects)
         * and also sets the state to "destroyed".
         *
         * We want to update the instance row to indicate it's being deleted
         * only if it's in a state where that's possible.  While complicated,
         * the SQL below allows us to atomically distinguish the cases we're
         * interested in: successful update, failure because the row was in the
         * wrong state, failure because the row doesn't exist.
         *
         * By virtue of being "stopped", we assume there are no dependencies on
         * this instance (e.g., disk attachments).  If that changes, we'll want
         * to check for such dependencies here.
         *
         * XXX What's the right abstraction for this?
         */
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        let sql = "WITH \
            matching_rows AS \
            (SELECT id, instance_state FROM Instance WHERE time_deleted \
            IS NULL AND id = $1 LIMIT 2),
            \
            deleted_ids AS
            (UPDATE Instance \
            SET time_deleted = $2, instance_state = 'destroyed' WHERE \
            time_deleted IS NULL AND id = $1 AND \
            instance_state IN ('stopped', 'failed') LIMIT 2 RETURNING id)
            \
            SELECT \
                i.id AS found_id, \
                i.instance_state AS previous_state, \
                d.id as deleted_id \
                FROM matching_rows i \
                FULL OUTER JOIN deleted_ids d ON i.id = d.id";

        /*
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
        let row =
            sql_query_maybe_one(&client, sql, &[instance_id, &now], || {
                ApiError::not_found_by_id(
                    ApiResourceType::Instance,
                    instance_id,
                )
            })
            .await?;

        let found_id: Uuid = sql_row_value(&row, "found_id")?;
        let previous_state: ApiInstanceState =
            sql_row_value(&row, "previous_state")?;
        let deleted_id: Option<Uuid> = sql_row_value(&row, "deleted_id")?;
        // XXX error context -- as I look at this, would this (and some of
        // the other cases) be simpler if the return value of query()
        // included the SQL and a way to generate errors that referenced the
        // SQL?
        bail_unless!(found_id == *instance_id);
        if let Some(did) = deleted_id {
            bail_unless!(did == *instance_id);
            Ok(())
        } else {
            Err(ApiError::InvalidRequest {
                message: format!(
                    "instance cannot be deleted in state \"{}\"",
                    previous_state
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
        let rows = sql_pagination(
            &client,
            Disk,
            &["id", "name", "disk_state", "attach_instance_id"],
            "time_deleted IS NULL AND attach_instance_id IS NOT NULL AND \
            disk_state IN ('attaching', 'attached', 'detaching') AND \
            attach_instance_id = $1",
            &[instance_id],
            "name",
            pagparams,
        )
        .await?;
        let list = rows
            .iter()
            .map(|row| ApiDiskAttachment::try_from(row))
            .collect::<Vec<Result<ApiDiskAttachment, ApiError>>>();
        Ok(futures::stream::iter(list).boxed())
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
        let now = Utc::now();
        sql_insert_unique(
            &client,
            Disk,
            Disk::ALL_COLUMNS,
            params.identity.name.as_str(),
            "ON CONFLICT (id) DO NOTHING",
            &[
                disk_id,
                &params.identity.name,
                &params.identity.description,
                &now,
                &now,
                &(None as Option<DateTime<Utc>>),
                project_id,
                &runtime_initial.disk_state.to_string(),
                &now,
                &runtime_initial.gen,
                &runtime_initial.disk_state.attached_instance_id(),
                &params.size,
                &params.snapshot_id,
            ],
        )
        .await?;

        /*
         * If we get here, then we successfully inserted the record.  It would
         * be a bug if something else were to remove that record before we have
         * a chance to fetch it.  As a result, sql_query_always_one() is correct
         * here, even though it looks like this query could legitimately return
         * 0 rows.
         */
        let row = sql_query_always_one(
            &client,
            format!(
                "SELECT {} FROM {} WHERE id = $1",
                Disk::ALL_COLUMNS.join(", "),
                Disk::TABLE_NAME,
            )
            .as_str(),
            &[disk_id],
        )
        .await
        .map_err(|e| {
            sql_error_on_create(
                ApiResourceType::Disk,
                &params.identity.name.as_str(),
                e,
            )
        })?;

        let disk = ApiDisk::try_from(&row)?;
        if disk.runtime.disk_state != ApiDiskState::Creating
            || disk.runtime.gen != runtime_initial.gen
        {
            Err(ApiError::internal_error(&format!(
                "creating disk: found existing disk with \
                        unexpected state ({:?}) or generation ({})",
                disk.runtime.disk_state, disk.runtime.gen,
            )))
        } else {
            Ok(disk)
        }
    }

    /* XXX commonize with instance version */
    pub async fn project_list_disks(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiDisk> {
        let client = self.pool.acquire().await?;
        let rows = sql_pagination(
            &client,
            Disk,
            Disk::ALL_COLUMNS,
            "time_deleted IS NULL AND project_id = $1",
            &[project_id],
            "name",
            pagparams,
        )
        .await?;
        let list = rows
            .iter()
            .map(|row| ApiDisk::try_from(row))
            .collect::<Vec<Result<ApiDisk, ApiError>>>();
        Ok(futures::stream::iter(list).boxed())
    }

    pub async fn disk_update_runtime(
        &self,
        disk_id: &Uuid,
        new_runtime: &ApiDiskRuntimeState,
    ) -> Result<(), ApiError> {
        /* XXX See and commonize with instance_update_runtime() */
        let client = self.pool.acquire().await?;
        let now = Utc::now();

        sql_execute_maybe_one(
            &client,
            format!(
                "UPDATE {} SET \
                        disk_state = $1, \
                        state_generation = $2, \
                        attach_instance_id = $3, \
                        time_state_updated = $4 \
                    WHERE id = $5 AND state_generation < $2 LIMIT 2",
                Disk::TABLE_NAME,
            )
            .as_str(),
            &[
                &new_runtime.disk_state.to_string(),
                &new_runtime.gen,
                &new_runtime.disk_state.attached_instance_id(),
                &now,
                disk_id,
            ],
            || ApiError::not_found_by_id(ApiResourceType::Disk, disk_id),
        )
        .await
    }

    /* XXX commonize with instance version */
    pub async fn disk_fetch(&self, disk_id: &Uuid) -> LookupResult<ApiDisk> {
        let client = self.pool.acquire().await?;
        let row = sql_query_maybe_one(
            &client,
            format!(
                "SELECT {} FROM {} WHERE time_deleted IS NULL AND \
                        id = $1 LIMIT 2",
                Disk::ALL_COLUMNS.join(", "),
                Disk::TABLE_NAME
            )
            .as_str(),
            &[&disk_id],
            || ApiError::not_found_by_id(ApiResourceType::Disk, disk_id),
        )
        .await?;
        Ok(ApiDisk::try_from(&row)?)
    }

    /* XXX commonize with instance version */
    pub async fn disk_fetch_by_name(
        &self,
        project_id: &Uuid,
        disk_name: &ApiName,
    ) -> LookupResult<ApiDisk> {
        let client = self.pool.acquire().await?;
        let row = sql_query_maybe_one(
            &client,
            format!(
                "SELECT {} FROM {} WHERE time_deleted IS NULL AND \
                        project_id = $1 AND name = $2 LIMIT 2",
                Disk::ALL_COLUMNS.join(", "),
                Disk::TABLE_NAME
            )
            .as_str(),
            &[project_id, disk_name],
            || ApiError::not_found_by_name(ApiResourceType::Disk, disk_name),
        )
        .await?;
        Ok(ApiDisk::try_from(&row)?)
    }

    /* XXX commonize with instance version */
    pub async fn project_delete_disk(&self, disk_id: &Uuid) -> DeleteResult {
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        let sql = "WITH \
            matching_rows AS \
            (SELECT id, disk_state, attach_instance_id FROM Disk WHERE \
            time_deleted IS NULL AND id = $1 LIMIT 2),
            \
            deleted_ids AS
            (UPDATE Disk \
            SET time_deleted = $2, disk_state = 'destroyed' WHERE \
            time_deleted IS NULL AND id = $1 AND \
            disk_state IN ('detached', 'faulted') LIMIT 2 RETURNING id)
            \
            SELECT \
                i.id AS found_id, \
                i.disk_state AS previous_state, \
                i.attach_instance_id AS attach_instance_id, \
                d.id as deleted_id \
                FROM matching_rows i \
                FULL OUTER JOIN deleted_ids d ON i.id = d.id";
        // XXX Is this the right place to produce the NotFound error?  Might
        // the caller have already done a name-to-id translation?

        let row = sql_query_maybe_one(&client, sql, &[disk_id, &now], || {
            ApiError::not_found_by_id(ApiResourceType::Disk, disk_id)
        })
        .await?;

        let found_id: Uuid = sql_row_value(&row, "found_id")?;
        let previous_state_str: &str = sql_row_value(&row, "previous_state")?;
        let attached_instance_id: Option<Uuid> =
            sql_row_value(&row, "attach_instance_id")?;
        let previous_state: ApiDiskState =
            ApiDiskState::try_from((previous_state_str, attached_instance_id))
                .map_err(|e| ApiError::internal_error(&e))?;
        let deleted_id: Option<Uuid> = sql_row_value(&row, "deleted_id")?;
        bail_unless!(found_id == *disk_id);

        if let Some(did) = deleted_id {
            bail_unless!(did == *disk_id);
            Ok(())
        } else {
            Err(ApiError::InvalidRequest {
                message: format!(
                    "disk cannot be deleted in state \"{}\"",
                    previous_state
                ),
            })
        }
    }
}

/* XXX Should this be From<>?  May want more context */
fn sql_error_generic(e: DbError) -> ApiError {
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

/*
 * XXX building SQL like this sucks (obviously).  The 'static str here is just
 * to make it less likely we accidentally put a user-provided string here if
 * this sicence experiment escapes the lab.
 * As below, the explicit desugaring of "async fn" here is due to
 * rust-lang/rust#63033.
 */
fn sql_pagination<'a, T: Table, K: ToSql + Send + Sync>(
    client: &'a tokio_postgres::Client,
    _table: T, // TODO-cleanup
    columns: &'a [&'static str],
    base_where: &'static str,
    base_params: &'a [&'a (dyn ToSql + Sync)],
    column_name: &'static str,
    pagparams: &'a DataPageParams<'a, K>,
) -> impl Future<Output = Result<Vec<tokio_postgres::Row>, ApiError>> + 'a {
    let (operator, order) = match pagparams.direction {
        dropshot::PaginationOrder::Ascending => (">", "ASC"),
        dropshot::PaginationOrder::Descending => ("<", "DESC"),
    };

    let base_sql = format!(
        "SELECT {} FROM {} WHERE {} ",
        columns.join(", "),
        T::TABLE_NAME,
        base_where
    );
    let limit = i64::from(pagparams.limit.get());
    async move {
        let query_result = if let Some(marker_value) = &pagparams.marker {
            let mut params = base_params.to_vec();
            params.push(&marker_value);
            params.push(&limit);
            let sql = format!(
                "{} AND {} {} ${} ORDER BY {} {} LIMIT ${}",
                base_sql,
                column_name,
                operator,
                params.len() - 1,
                column_name,
                order,
                params.len(),
            );
            sql_query(&client, sql.as_str(), &params.as_slice()).await
        } else {
            let mut params = base_params.to_vec();
            params.push(&limit);
            let sql = format!(
                "{} ORDER BY {} {} LIMIT ${}",
                base_sql,
                column_name,
                order,
                params.len(),
            );
            sql_query(client, sql.as_str(), &params.as_slice()).await
        };

        query_result.map_err(sql_error_generic)
    }
}

/*
 * TODO-coverage tests for these FromSql and ToSql implementations
 */

/**
 * Defines impls for ToSql and FromSql for a type T such that &T: Into<D> and
 * T: TryFrom<D>.  These impls delegate to the "D" impls of these traits.  This
 * is useful because we define a bunch of numeric and string newtypes that are
 * implemented in the database as i64 and string, respectively.
 */
macro_rules! impl_sql_wrapping {
    ($T:ident, $D:ty) => {
        impl ToSql for $T {
            fn to_sql(
                &self,
                ty: &tokio_postgres::types::Type,
                out: &mut tokio_postgres::types::private::BytesMut,
            ) -> Result<
                tokio_postgres::types::IsNull,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                <$D>::from(self).to_sql(ty, out)
            }

            fn accepts(ty: &tokio_postgres::types::Type) -> bool {
                <$D as ToSql>::accepts(ty)
            }

            tokio_postgres::types::to_sql_checked!();
        }

        impl<'a> FromSql<'a> for $T {
            fn from_sql(
                ty: &tokio_postgres::types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
                let value: $D = <$D as FromSql>::from_sql(ty, raw)?;
                $T::try_from(value).map_err(|e| e.into())
            }

            fn accepts(ty: &tokio_postgres::types::Type) -> bool {
                <$D as FromSql>::accepts(ty)
            }
        }
    };
}

impl_sql_wrapping!(ApiByteCount, i64);
impl_sql_wrapping!(ApiGeneration, i64);
impl_sql_wrapping!(ApiInstanceCpuCount, i64);
impl_sql_wrapping!(ApiInstanceState, &str);
impl_sql_wrapping!(ApiName, &str);

/**
 * Wrapper around [`tokio_postgres::Client::query`] that produces errors
 * that include the SQL.
 */
/*
 * XXX TODO-debugging It would be really, really valuable to include the
 * query parameters here as well.  However, the only thing we can do with
 * ToSql is to serialize it to a PostgreSQL type (as a string of bytes).
 * We could require that these impl some trait that also provides Debug, and
 * then we could use that.  Or we could find a way to parse the resulting
 * PostgreSQL value and print _that_ out as a String somehow?
 */
async fn sql_query(
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
async fn sql_query_always_one(
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
 * XXX?
 */
/* XXX TODO-debugging can we include the SQL in the ApiError */
async fn sql_query_maybe_one(
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
/* XXX TODO-debugging See sql_query(). */
async fn sql_execute(
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
 * XXX?
 */
/* XXX TODO-debugging can we include the SQL in the ApiError */
async fn sql_execute_maybe_one(
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
fn sql_row_value<'a, I, T>(
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

/**
 * Using database connection `client`, insert a row into table `table_name`
 * having values `values` for the respective columns named `table_fields`.
 */
/*
 * This is not as statically type-safe an API as you might think by looking at
 * it.  There's nothing that ensures that the types of the values correspond to
 * the right columns.  It's worth noting, however, that even if we statically
 * checked this, we would only be checking that the values correspond with some
 * Rust representation of the database schema that we've built into this
 * program.  That does not eliminate the runtime possibility that the types do
 * not, in fact, match the types in the database.
 *
 * The use of `'static` lifetimes here is a cheesy sanity check to catch SQL
 * injection.  (This is not a _good_ way to avoid SQL injection.  This is
 * intended as a last-ditch sanity check in case this code survives longer than
 * expected.)  Using the `async fn` syntax here runs afoul of
 * rust-lang/rust#63033.  So we desugar the `async` explicitly.
 */
fn sql_insert_unique<'a, T>(
    client: &'a tokio_postgres::Client,
    _table: T, // TODO-cleanup
    table_columns: &'a [&'static str],
    unique_value: &'a str,
    conflict_sql: &'static str,
    values: &'a [&'a (dyn ToSql + Sync)],
) -> impl Future<Output = Result<T::ApiModelType, ApiError>> + 'a
where
    T: Table,
{
    assert_eq!(table_columns.len(), values.len());
    // XXX Could assert that the specified columns are a subset of allowed ones
    let table_field_str = table_columns.join(", ");
    let all_columns_str = T::ALL_COLUMNS.join(", ");
    let values_str = (1..=values.len())
        .map(|i| format!("${}", i))
        .collect::<Vec<String>>()
        .as_slice()
        .join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) {} RETURNING {}",
        T::TABLE_NAME,
        table_field_str,
        values_str,
        conflict_sql,
        all_columns_str
    );
    async move {
        let row =
            sql_query_always_one(client, sql.as_str(), values).await.map_err(
                |e| sql_error_on_create(T::RESOURCE_TYPE, unique_value, e),
            )?;

        T::ApiModelType::try_from(&row)
    }
}

/// Load an [`ApiIdentityMetadata`] from a row of any table that contains the
/// usual identity fields: "id", "name", "description, "time_created", and
/// "time_metadata_updated".
impl TryFrom<&tokio_postgres::Row> for ApiIdentityMetadata {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        // XXX What to do with non-NULL time_deleted?
        Ok(ApiIdentityMetadata {
            id: sql_row_value(value, "id")?,
            name: sql_row_value(value, "name")?,
            description: sql_row_value(value, "description")?,
            time_created: sql_row_value(value, "time_created")?,
            // XXX is it time_updated or time_metadata_updated
            time_modified: sql_row_value(value, "time_metadata_updated")?,
        })
    }
}

/*
 * We want to find a better way to abstract this.  Diesel provides a compelling
 * model in terms of using it, but it also seems fairly heavyweight, and this
 * fetch-or-insert all-fields-of-an-object likely _isn't_ our most common use
 * case, even though we do it a lot for basic CRUD.
 */
trait Table {
    type ApiModelType: for<'a> TryFrom<
        &'a tokio_postgres::Row,
        Error = ApiError,
    >;
    const RESOURCE_TYPE: ApiResourceType;
    const TABLE_NAME: &'static str;
    const ALL_COLUMNS: &'static [&'static str];
}

struct Project;
impl Table for Project {
    type ApiModelType = ApiProject;
    const RESOURCE_TYPE: ApiResourceType = ApiResourceType::Project;
    const TABLE_NAME: &'static str = "Project";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_metadata_updated",
        "time_deleted",
    ];
}

/// Load an [`ApiProject`] from a whole row of the "Project" table.
impl TryFrom<&tokio_postgres::Row> for ApiProject {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiProject { identity: ApiIdentityMetadata::try_from(value)? })
    }
}

struct Instance;
impl Table for Instance {
    type ApiModelType = ApiInstance;
    const RESOURCE_TYPE: ApiResourceType = ApiResourceType::Instance;
    const TABLE_NAME: &'static str = "Instance";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_metadata_updated",
        "time_deleted",
        "project_id",
        "instance_state",
        "time_state_updated",
        "state_generation",
        "active_server_id",
        "ncpus",
        "memory",
        "hostname",
    ];
}

/// Load an [`ApiInstance`] from a whole row of the "Instance" table.
impl TryFrom<&tokio_postgres::Row> for ApiInstance {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiInstance {
            identity: ApiIdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            ncpus: sql_row_value(value, "ncpus")?,
            memory: sql_row_value(value, "memory")?,
            hostname: sql_row_value(value, "hostname")?,
            runtime: ApiInstanceRuntimeState::try_from(value)?,
            boot_disk_size: ApiByteCount::from(0u32), // XXX
        })
    }
}

/// Load an [`ApiInstanceRuntimeState`] from a row of the "Instance" table,
/// using the "instance_state", "active_server_id", "state_generation", and
/// "time_state_updated" columns.
impl TryFrom<&tokio_postgres::Row> for ApiInstanceRuntimeState {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiInstanceRuntimeState {
            run_state: sql_row_value(value, "instance_state")?,
            reboot_in_progress: false, // XXX
            sled_uuid: sql_row_value(value, "active_server_id")?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}

struct Disk;
impl Table for Disk {
    type ApiModelType = ApiDisk;
    const RESOURCE_TYPE: ApiResourceType = ApiResourceType::Disk;
    const TABLE_NAME: &'static str = "Disk";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_metadata_updated",
        "time_deleted",
        "project_id",
        "disk_state",
        "time_state_updated",
        "state_generation",
        "attach_instance_id",
        "size_bytes",
        "origin_snapshot",
    ];
}

/// Load an [`ApiDisk`] from a row of the "Disk" table.
impl TryFrom<&tokio_postgres::Row> for ApiDisk {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiDisk {
            identity: ApiIdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            create_snapshot_id: sql_row_value(value, "origin_snapshot")?,
            size: sql_row_value(value, "size_bytes")?,
            runtime: ApiDiskRuntimeState::try_from(value)?,
        })
    }
}

/// Load an [`ApiDiskAttachment`] from a database row containing those columns
/// of the Disk table that describe the attachment: "id", "name", "disk_state",
/// "attach_instance_id"
impl TryFrom<&tokio_postgres::Row> for ApiDiskAttachment {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiDiskAttachment {
            instance_id: sql_row_value(value, "attach_instance_id")?,
            disk_id: sql_row_value(value, "id")?,
            disk_name: sql_row_value(value, "name")?,
            disk_state: ApiDiskState::try_from(value)?,
        })
    }
}

/// Load an [`ApiDiskState`] from a row from the Disk table, using the columns
/// "disk_state" and "attach_instance_id".
impl TryFrom<&tokio_postgres::Row> for ApiDiskState {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let disk_state_str: &str = sql_row_value(value, "disk_state")?;
        let instance_uuid: Option<Uuid> =
            sql_row_value(value, "attach_instance_id")?;
        ApiDiskState::try_from((disk_state_str, instance_uuid))
            .map_err(|e| ApiError::internal_error(&e))
    }
}

/// Load an [`ApiDiskRuntimeState`'] from a row from the Disk table, using the
/// columns needed for [`ApiDiskState`], plus "state_generation" and
/// "time_state_updated".
impl TryFrom<&tokio_postgres::Row> for ApiDiskRuntimeState {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiDiskRuntimeState {
            disk_state: ApiDiskState::try_from(value)?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}
