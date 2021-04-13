/*!
 * Primary control plane interface for database read and write operations
 */

/*
 * TODO-scalability review all queries for use of indexes (may need
 * "time_deleted IS NOT NULL" conditions) Figure out how to automate this.
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
use chrono::Utc;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use super::operations::sql_execute_maybe_one;
use super::operations::sql_query_maybe_one;
use super::operations::sql_row_value;
use super::schema::Disk;
use super::schema::Instance;
use super::schema::LookupByAttachedInstance;
use super::schema::LookupByUniqueId;
use super::schema::LookupByUniqueName;
use super::schema::LookupByUniqueNameInProject;
use super::schema::Project;
use super::sql::SqlSerialize;
use super::sql::SqlString;
use super::sql::SqlValueSet;
use super::sql::Table;
use super::sql_operations::sql_fetch_page_by;
use super::sql_operations::sql_fetch_page_from_table;
use super::sql_operations::sql_fetch_row_by;
use super::sql_operations::sql_fetch_row_raw;
use super::sql_operations::sql_insert_unique;
use super::sql_operations::sql_insert_unique_idempotent_and_fetch;
use super::sql_operations::sql_update_precond;

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
        let mut values = SqlValueSet::new();
        values.set("id", new_id);
        values.set("time_created", &now);
        values.set("time_modified", &now);
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
        sql_fetch_row_by::<LookupByUniqueName, Project>(
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
        let row = sql_fetch_row_raw::<LookupByUniqueName, Project>(
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
        sql_fetch_page_from_table::<LookupByUniqueId, Project>(
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
        sql_fetch_page_by::<
            LookupByUniqueName,
            Project,
            <Project as Table>::ModelType,
        >(&client, (), pagparams, Project::ALL_COLUMNS)
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

        let mut sql =
            format!("UPDATE {} SET time_modified = $1 ", Project::TABLE_NAME);
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
        values.set("project_id", project_id);
        params.sql_serialize(&mut values);
        runtime_initial.sql_serialize(&mut values);
        let instance = sql_insert_unique_idempotent_and_fetch::<
            Instance,
            LookupByUniqueId,
        >(
            &client,
            &values,
            params.identity.name.as_str(),
            "id",
            (),
            instance_id,
        )
        .await?;

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
        sql_fetch_page_by::<
            LookupByUniqueNameInProject,
            Instance,
            <Instance as Table>::ModelType,
        >(&client, (project_id,), pagparams, Instance::ALL_COLUMNS)
        .await
    }

    pub async fn instance_fetch(
        &self,
        instance_id: &Uuid,
    ) -> LookupResult<ApiInstance> {
        let client = self.pool.acquire().await?;
        sql_fetch_row_by::<LookupByUniqueId, Instance>(&client, (), instance_id)
            .await
    }

    pub async fn instance_fetch_by_name(
        &self,
        project_id: &Uuid,
        instance_name: &ApiName,
    ) -> LookupResult<ApiInstance> {
        let client = self.pool.acquire().await?;
        sql_fetch_row_by::<LookupByUniqueNameInProject, Instance>(
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

        let update = sql_update_precond::<Instance, LookupByUniqueId>(
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

        let update = sql_update_precond::<Instance, LookupByUniqueId>(
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
        sql_fetch_page_by::<LookupByAttachedInstance, Disk, ApiDiskAttachment>(
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
        /*
         * See project_create_instance() for a discussion of how this function
         * works.  The pattern here is nearly identical.
         */
        let client = self.pool.acquire().await?;
        let now = runtime_initial.time_updated;
        let mut values = SqlValueSet::new();
        values.set("id", disk_id);
        values.set("time_created", &now);
        values.set("time_modified", &now);
        values.set("project_id", project_id);
        params.sql_serialize(&mut values);
        runtime_initial.sql_serialize(&mut values);

        let disk =
            sql_insert_unique_idempotent_and_fetch::<Disk, LookupByUniqueId>(
                &client,
                &mut values,
                params.identity.name.as_str(),
                "id",
                (),
                disk_id,
            )
            .await?;

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
        sql_fetch_page_by::<
            LookupByUniqueNameInProject,
            Disk,
            <Disk as Table>::ModelType,
        >(&client, (project_id,), pagparams, Disk::ALL_COLUMNS)
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

        let update = sql_update_precond::<Disk, LookupByUniqueId>(
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
        sql_fetch_row_by::<LookupByUniqueId, Disk>(&client, (), disk_id).await
    }

    pub async fn disk_fetch_by_name(
        &self,
        project_id: &Uuid,
        disk_name: &ApiName,
    ) -> LookupResult<ApiDisk> {
        let client = self.pool.acquire().await?;
        sql_fetch_row_by::<LookupByUniqueNameInProject, Disk>(
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

        let update = sql_update_precond::<Disk, LookupByUniqueId>(
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
