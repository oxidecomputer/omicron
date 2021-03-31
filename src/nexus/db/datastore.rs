/*!
 * Primary control plane interface for database read and write operations
 */

/*
 * XXX review all queries for use of indexes (may need "time_deleted IS
 * NOT NULL" conditions)
 */

use super::Pool;
use crate::api_error::ApiError;
use crate::api_model::ApiByteCount;
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
use crate::api_model::CreateResult2;
use crate::api_model::DataPageParams;
use crate::api_model::DeleteResult;
use crate::api_model::ListResult;
use crate::api_model::ListResult2;
use crate::api_model::LookupResult2;
use crate::api_model::UpdateResult2;
use chrono::DateTime;
use chrono::Utc;
use futures::StreamExt;
use std::convert::TryFrom;
use std::future::Future;
use std::sync::Arc;
use tokio_postgres::row::Row;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

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
    ) -> CreateResult2<ApiProject> {
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
    ) -> LookupResult2<ApiProject> {
        let client = self.pool.acquire().await?;
        let rows = client
            .query(
                format!(
                    "SELECT {} FROM {} WHERE time_deleted IS NULL AND \
                        name = $1 LIMIT 2",
                    Project::ALL_COLUMNS.join(", "),
                    Project::TABLE_NAME
                )
                .as_str(),
                &[&project_name],
            )
            .await
            .map_err(sql_error)?;
        match rows.len() {
            1 => Ok(ApiProject::try_from(&rows[0])?),
            0 => Err(ApiError::not_found_by_name(
                ApiResourceType::Project,
                project_name,
            )),
            len => Err(ApiError::internal_error(&format!(
                "expected at most one row from database query, but found {}",
                len
            ))),
        }
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
        let nrows = client
            .execute(
                format!(
                    "UPDATE {} SET time_deleted = $1 WHERE \
                    time_deleted IS NULL AND name = $2 LIMIT 2 \
                    RETURNING {}",
                    Project::TABLE_NAME,
                    Project::ALL_COLUMNS.join(", ")
                )
                .as_str(),
                &[&now, &project_name],
            )
            .await
            .map_err(sql_error)?;
        /* TODO-log log the returned row(s) */
        match nrows {
            1 => Ok(()),
            0 => Err(ApiError::not_found_by_name(
                ApiResourceType::Project,
                project_name,
            )),
            len => Err(ApiError::internal_error(&format!(
                "expected at most one row updated, found {}",
                len
            ))),
        }
    }

    /// Look up the id for a project based on its name
    pub async fn project_lookup_id_by_name(
        &self,
        name: &ApiName,
    ) -> Result<Uuid, ApiError> {
        let client = self.pool.acquire().await?;

        let rows = client
            .query(
                format!(
                    "SELECT id FROM {} WHERE name = $1 AND \
                        time_deleted IS NULL LIMIT 2",
                    Project::TABLE_NAME
                )
                .as_str(),
                &[&name],
            )
            .await
            .map_err(sql_error)?;
        match rows.len() {
            1 => Ok(rows[0].try_get(0).map_err(sql_error)?),
            0 => {
                Err(ApiError::not_found_by_name(ApiResourceType::Project, name))
            }
            /*
             * TODO-design This should really include a bunch more information,
             * like the SQL and the type of object we were expecting.
             */
            _ => Err(ApiError::internal_error(&format!(
                "expected 1 row from database query, but found {}",
                rows.len()
            ))),
        }
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
            .map(|row| ApiProject::try_from(row).map(Arc::new))
            .collect::<Vec<Result<Arc<ApiProject>, ApiError>>>();
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
            .map(|row| ApiProject::try_from(row).map(Arc::new))
            .collect::<Vec<Result<Arc<ApiProject>, ApiError>>>();
        Ok(futures::stream::iter(list).boxed())
    }

    /// Updates a project by name (clobbering update -- no etag)
    pub async fn project_update(
        &self,
        project_name: &ApiName,
        update_params: &ApiProjectUpdateParams,
    ) -> UpdateResult2<ApiProject> {
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

        let rows = client.query(sql.as_str(), &params).await.map_err(|e| {
            sql_error_create(Project::RESOURCE_TYPE, project_name.as_str(), e)
        })?;
        match rows.len() {
            0 => Err(ApiError::not_found_by_name(
                ApiResourceType::Project,
                project_name,
            )),
            1 => Ok(ApiProject::try_from(&rows[0])?),
            len => Err(ApiError::internal_error(&format!(
                "expected 1 row from UPDATE query, but found {}",
                len
            ))),
        }
    }

    pub async fn project_create_instance(
        &self,
        instance_id: &Uuid,
        project_id: &Uuid,
        params: &ApiInstanceCreateParams,
        runtime_initial: &ApiInstanceRuntimeState,
    ) -> CreateResult2<ApiInstance> {
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
                // XXX should use try_from
                &(runtime_initial.gen as i64),
                &runtime_initial.sled_uuid,
                &(params.ncpus.0 as i64),
                // XXX unwrap
                &(i64::try_from(params.memory.to_whole_mebibytes()).unwrap()),
                &params.hostname,
            ],
        )
        .await?;

        let rows = client
            .query(
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
                sql_error_create(
                    ApiResourceType::Instance,
                    &params.identity.name.as_str(),
                    e,
                )
            })?;

        match rows.len() {
            1 => {
                let instance = ApiInstance::try_from(&rows[0])?;
                if instance.runtime.run_state != ApiInstanceState::Creating
                    || instance.runtime.gen != 1
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
            len => Err(ApiError::internal_error(&format!(
                "creating instance: expected one instance, found {}",
                len
            ))),
        }
    }

    pub async fn project_list_instances(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult2<ApiInstance> {
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
    ) -> LookupResult2<ApiInstance> {
        let client = self.pool.acquire().await?;
        let rows = client
            .query(
                format!(
                    "SELECT {} FROM {} WHERE time_deleted IS NULL AND \
                        id = $1 LIMIT 2",
                    Instance::ALL_COLUMNS.join(", "),
                    Instance::TABLE_NAME
                )
                .as_str(),
                &[&instance_id],
            )
            .await
            .map_err(sql_error)?;
        match rows.len() {
            1 => Ok(ApiInstance::try_from(&rows[0])?),
            0 => Err(ApiError::not_found_by_id(
                ApiResourceType::Instance,
                instance_id,
            )),
            len => Err(ApiError::internal_error(&format!(
                "expected at most one row from database query, but found {}",
                len
            ))),
        }
    }

    pub async fn instance_fetch_by_name(
        &self,
        project_id: &Uuid,
        instance_name: &ApiName,
    ) -> LookupResult2<ApiInstance> {
        let client = self.pool.acquire().await?;
        let rows = client
            .query(
                format!(
                    "SELECT {} FROM {} WHERE time_deleted IS NULL AND \
                        project_id = $1 AND name = $2 LIMIT 2",
                    Instance::ALL_COLUMNS.join(", "),
                    Instance::TABLE_NAME
                )
                .as_str(),
                &[project_id, instance_name],
            )
            .await
            .map_err(sql_error)?;
        match rows.len() {
            1 => Ok(ApiInstance::try_from(&rows[0])?),
            0 => Err(ApiError::not_found_by_name(
                ApiResourceType::Instance,
                instance_name,
            )),
            len => Err(ApiError::internal_error(&format!(
                "expected at most one row from database query, but found {}",
                len
            ))),
        }
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
        let nupdated = client
            .execute(
                format!(
                    "UPDATE {} SET \
                        instance_state = $1, \
                        state_generation = $2, \
                        active_server_id = $3, \
                        time_state_updated = $4 \
                    WHERE id = $5 AND state_generation < $2 LIMIT 2",
                    Instance::TABLE_NAME,
                )
                .as_str(),
                &[
                    &new_runtime.run_state.to_string(),
                    &(new_runtime.gen as i64),
                    &new_runtime.sled_uuid,
                    &now,
                    instance_id,
                ],
            )
            .await
            .map_err(sql_error)?;

        if nupdated > 1 {
            return Err(ApiError::internal_error(&format!(
                "unexpected number of rows updated by UPDATE query: {}",
                nupdated
            )));
        }

        Ok(nupdated == 1)
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
        let rows =
            client.query(sql, &[instance_id, &now]).await.map_err(sql_error)?;

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
        if rows.is_empty() {
            // XXX Is this the right place to produce this error?  Might the
            // caller have already done a name-to-id translation?
            return Err(ApiError::not_found_by_id(
                ApiResourceType::Instance,
                instance_id,
            ));
        }

        if rows.len() > 1 {
            // XXX error context
            return Err(ApiError::internal_error(
                "unexpectedly got more than one row for conditional update",
            ));
        }

        let found_id: Uuid = rows[0].try_get("found_id").map_err(sql_error)?;
        let previous_state_str: &str =
            rows[0].try_get("previous_state").map_err(sql_error)?;
        let previous_state: ApiInstanceState = previous_state_str.parse()?;
        let deleted_id: Option<Uuid> =
            rows[0].try_get("deleted_id").map_err(sql_error)?;
        // TODO-cleanup would a sql_assert! macro help here?
        if found_id != *instance_id {
            // XXX error context
            return Err(ApiError::internal_error(&format!(
                "unexpected instance found (expected id {}, found {})",
                instance_id, found_id,
            )));
        }

        if let Some(did) = deleted_id {
            if did != *instance_id {
                Err(ApiError::internal_error(&format!(
                    "unexpected instance deleted (expected id {}, found {})",
                    instance_id, did,
                )))
            } else {
                Ok(())
            }
        } else {
            Err(ApiError::InvalidRequest {
                message: format!(
                    "instance cannot be deleted in state \"{}\"",
                    previous_state
                ),
            })
        }
    }
}

/* XXX Should this be From<>?  May want more context */
fn sql_error(e: tokio_postgres::Error) -> ApiError {
    match e.code() {
        None => ApiError::InternalError {
            message: format!("unexpected database error: {}", e.to_string()),
        },
        Some(code) => ApiError::InternalError {
            message: format!(
                "unexpected database error (code {}): {}",
                code.code(),
                e.to_string()
            ),
        },
    }
}

fn sql_error_create(
    rtype: ApiResourceType,
    unique_value: &str,
    e: tokio_postgres::Error,
) -> ApiError {
    if let Some(code) = e.code() {
        if *code == tokio_postgres::error::SqlState::UNIQUE_VIOLATION {
            return ApiError::ObjectAlreadyExists {
                type_name: rtype,
                object_name: unique_value.to_owned(),
            };
        }
    }

    sql_error(e)
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
) -> impl Future<Output = Result<Vec<Row>, ApiError>> + 'a {
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
            client.query(sql.as_str(), &params.as_slice()).await
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
            client.query(sql.as_str(), &params.as_slice()).await
        };

        query_result.map_err(sql_error)
    }
}

impl ToSql for ApiName {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<
        tokio_postgres::types::IsNull,
        Box<dyn std::error::Error + Sync + Send>,
    >
    where
        Self: Sized,
    {
        self.as_str().to_sql(ty, out)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool
    where
        Self: Sized,
    {
        <&str as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<
        tokio_postgres::types::IsNull,
        Box<dyn std::error::Error + Sync + Send>,
    > {
        self.as_str().to_sql_checked(ty, out)
    }
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
        let rows = client
            .query(sql.as_str(), values)
            .await
            .map_err(|e| sql_error_create(T::RESOURCE_TYPE, unique_value, e))?;
        let row = match rows.len() {
            1 => &rows[0],
            len => {
                return Err(ApiError::internal_error(&format!(
                    "expected 1 row from INSERT query, but found {}",
                    len
                )))
            }
        };

        Ok(T::ApiModelType::try_from(row)?)
    }
}

/*
 * We want to find a better way to abstract this.  Diesel provides a compelling
 * model in terms of using it, but it also seems fairly heavyweight, and this
 * fetch-or-insert all-fields-of-an-object likely _isn't_ our most common use
 * case, even though we do it a lot for basic CRUD.
 */
trait Table {
    type ApiModelType: for<'a> TryFrom<&'a Row, Error = ApiError>;
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

impl TryFrom<&tokio_postgres::Row> for ApiProject {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        // XXX really need some kind of context for these errors
        let name_str: &str = value.try_get("name").map_err(sql_error)?;
        let name = ApiName::try_from(name_str).map_err(|e| {
            ApiError::internal_error(&format!(
                "database project.name {:?}: {}",
                name_str, e
            ))
        })?;
        // XXX What to do with non-NULL time_deleted?
        Ok(ApiProject {
            generation: 1, // XXX
            identity: ApiIdentityMetadata {
                id: value.try_get("id").map_err(sql_error)?,
                name,
                description: value.try_get("description").map_err(sql_error)?,
                time_created: value
                    .try_get("time_created")
                    .map_err(sql_error)?,
                // XXX is it time_updated or time_metadata_updated
                time_modified: value
                    .try_get("time_metadata_updated")
                    .map_err(sql_error)?,
            },
        })
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
        "memory_mib",
        "hostname",
    ];
}

impl TryFrom<&tokio_postgres::Row> for ApiInstance {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        // XXX really need some kind of context for these errors
        let name_str: &str = value.try_get("name").map_err(sql_error)?;
        let name = ApiName::try_from(name_str).map_err(|e| {
            ApiError::internal_error(&format!(
                "database instance.name {:?}: {}",
                name_str, e
            ))
        })?;

        let run_state_str: &str =
            value.try_get("instance_state").map_err(sql_error)?;
        let run_state: ApiInstanceState = run_state_str.parse()?;
        let time_updated: chrono::DateTime<chrono::Utc> =
            value.try_get("time_state_updated").map_err(sql_error)?;
        let gen: i64 = value.try_get("state_generation").map_err(sql_error)?;
        let sled_uuid: Uuid =
            value.try_get("active_server_id").map_err(sql_error)?;
        let memory_mib: i64 = value.try_get("memory_mib").map_err(sql_error)?;
        let ncpus: i64 = value.try_get("ncpus").map_err(sql_error)?;

        // XXX What to do with non-NULL time_deleted?
        Ok(ApiInstance {
            identity: ApiIdentityMetadata {
                id: value.try_get("id").map_err(sql_error)?,
                name,
                description: value.try_get("description").map_err(sql_error)?,
                time_created: value
                    .try_get("time_created")
                    .map_err(sql_error)?,
                // XXX is it time_updated or time_metadata_updated
                time_modified: value
                    .try_get("time_metadata_updated")
                    .map_err(sql_error)?,
            },
            project_id: value.try_get("project_id").map_err(sql_error)?,
            ncpus: ApiInstanceCpuCount(ncpus as u16),
            memory: ApiByteCount::from_mebibytes(memory_mib as u64),
            hostname: value.try_get("hostname").map_err(sql_error)?,
            runtime: ApiInstanceRuntimeState {
                run_state,
                reboot_in_progress: false, // XXX
                sled_uuid,
                gen: gen as u64,
                time_updated,
            },
            boot_disk_size: ApiByteCount::from_bytes(0), // XXX
        })
    }
}
