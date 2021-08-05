/*!
 * Primary control plane interface for database read and write operations
 */

/*
 * TODO-scalability review all queries for use of indexes (may need
 * "time_deleted IS NOT NULL" conditions) Figure out how to automate this.
 *
 * TODO-design Better support for joins?
 * The interfaces here often require that to do anything with an object, a
 * caller must first look up the id and then do operations with the id.  For
 * example, the caller of project_list_disks() always looks up the project to
 * get the project_id, then lists disks having that project_id.  It's possible
 * to implement this instead with a JOIN in the database so that we do it with
 * one database round-trip.  We could use CTEs similar to what we do with
 * conditional updates to distinguish the case where the project didn't exist
 * vs. there were no disks in it.  This seems likely to be a fair bit more
 * complicated to do safely and generally compared to what we have now.
 */

use super::Pool;
use chrono::Utc;
use omicron_common::api;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::ListResult;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::Name;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::internal::nexus::OximeterAssignment;
use omicron_common::api::internal::nexus::OximeterInfo;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::bail_unless;
use omicron_common::db::sql_row_value;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use super::operations::sql_execute_maybe_one;
use super::operations::sql_query_maybe_one;
use super::schema;
use super::schema::Disk;
use super::schema::Instance;
use super::schema::LookupByAttachedInstance;
use super::schema::LookupByUniqueId;
use super::schema::LookupByUniqueName;
use super::schema::LookupByUniqueNameInProject;
use super::schema::Project;
use super::schema::Vpc;
use super::sql::SqlSerialize;
use super::sql::SqlString;
use super::sql::SqlValueSet;
use super::sql::Table;
use super::sql_operations::sql_fetch_page_by;
use super::sql_operations::sql_fetch_page_from_table;
use super::sql_operations::sql_fetch_row_by;
use super::sql_operations::sql_fetch_row_raw;
use super::sql_operations::sql_insert;
use super::sql_operations::sql_insert_unique;
use super::sql_operations::sql_insert_unique_idempotent_and_fetch;
use super::sql_operations::sql_update_precond;
use crate::db;

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
        new_project: &api::external::ProjectCreateParams,
    ) -> CreateResult<api::internal::nexus::Project> {
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
        project_name: &Name,
    ) -> LookupResult<api::internal::nexus::Project> {
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
    pub async fn project_delete(&self, project_name: &Name) -> DeleteResult {
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
            || Error::not_found_by_name(ResourceType::Project, project_name),
        )
        .await
    }

    /// Look up the id for a project based on its name
    pub async fn project_lookup_id_by_name(
        &self,
        name: &Name,
    ) -> Result<Uuid, Error> {
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
    ) -> ListResult<api::internal::nexus::Project> {
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
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResult<api::internal::nexus::Project> {
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
        project_name: &Name,
        update_params: &api::external::ProjectUpdateParams,
    ) -> UpdateResult<api::internal::nexus::Project> {
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
            Error::not_found_by_name(ResourceType::Project, project_name)
        })
        .await?;
        Ok(api::internal::nexus::Project::try_from(&row)?)
    }

    /*
     * Instances
     */

    ///
    /// Idempotently insert a database record for an Instance
    ///
    /// This is intended to be used by a saga action.  When we say this is
    /// idempotent, we mean that if this function succeeds and the caller
    /// invokes it again with the same instance id, project id, creation
    /// parameters, and initial runtime, then this operation will succeed and
    /// return the current object in the database.  Because this is intended for
    /// use by sagas, we do assume that if the record exists, it should still be
    /// in the "Creating" state.  If it's in any other state, this function will
    /// return with an error on the assumption that we don't really know what's
    /// happened or how to proceed.
    ///
    /// ## Errors
    ///
    /// In addition to the usual database errors (e.g., no connections
    /// available), this function can fail if there is already a different
    /// instance (having a different id) with the same name in the same project.
    ///
    /*
     * TODO-design Given that this is really oriented towards the saga
     * interface, one wonders if it's even worth having an abstraction here, or
     * if sagas shouldn't directly work with the database here (i.e., just do
     * what this function does under the hood).
     */
    pub async fn project_create_instance(
        &self,
        instance_id: &Uuid,
        project_id: &Uuid,
        params: &api::external::InstanceCreateParams,
        runtime_initial: &api::internal::nexus::InstanceRuntimeState,
    ) -> CreateResult<api::internal::nexus::Instance> {
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
            instance.runtime.run_state
                == api::external::InstanceState::Creating,
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
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResult<api::internal::nexus::Instance> {
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
    ) -> LookupResult<api::internal::nexus::Instance> {
        let client = self.pool.acquire().await?;
        sql_fetch_row_by::<LookupByUniqueId, Instance>(&client, (), instance_id)
            .await
    }

    pub async fn instance_fetch_by_name(
        &self,
        project_id: &Uuid,
        instance_name: &Name,
    ) -> LookupResult<api::internal::nexus::Instance> {
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
        new_runtime: &api::internal::nexus::InstanceRuntimeState,
    ) -> Result<bool, Error> {
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
        api::external::InstanceState::Destroyed.sql_serialize(&mut values);
        values.set("time_deleted", &now);

        let mut cond_sql = SqlString::new();

        let stopped = api::external::InstanceState::Stopped.to_string();
        let p1 = cond_sql.next_param(&stopped);
        let failed = api::external::InstanceState::Failed.to_string();
        let p2 = cond_sql.next_param(&failed);
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
        let variant: &str = sql_row_value(&row, "found_instance_state")?;
        let instance_state = api::external::InstanceState::try_from(variant)
            .map_err(|e| Error::internal_error(&e))?;
        bail_unless!(found_id == *instance_id);

        if update.updated {
            Ok(())
        } else {
            Err(Error::InvalidRequest {
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
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResult<api::external::DiskAttachment> {
        let client = self.pool.acquire().await?;
        sql_fetch_page_by::<
            LookupByAttachedInstance,
            Disk,
            api::external::DiskAttachment,
        >(
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
        params: &api::external::DiskCreateParams,
        runtime_initial: &api::internal::nexus::DiskRuntimeState,
    ) -> CreateResult<api::internal::nexus::Disk> {
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

        let disk: api::internal::nexus::Disk =
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
            disk.runtime.disk_state == api::external::DiskState::Creating,
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
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResult<api::internal::nexus::Disk> {
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
        new_runtime: &api::internal::nexus::DiskRuntimeState,
    ) -> Result<bool, Error> {
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

    pub async fn disk_fetch(
        &self,
        disk_id: &Uuid,
    ) -> LookupResult<api::internal::nexus::Disk> {
        let client = self.pool.acquire().await?;
        sql_fetch_row_by::<LookupByUniqueId, Disk>(&client, (), disk_id).await
    }

    pub async fn disk_fetch_by_name(
        &self,
        project_id: &Uuid,
        disk_name: &Name,
    ) -> LookupResult<api::internal::nexus::Disk> {
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
        api::external::DiskState::Destroyed.sql_serialize(&mut values);

        let mut cond_sql = SqlString::new();
        let disk_state_detached =
            api::external::DiskState::Detached.to_string();
        let p1 = cond_sql.next_param(&disk_state_detached);
        let disk_state_faulted = api::external::DiskState::Faulted.to_string();
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
        // api::external::DiskState::try_from(&tokio_postgres::Row), but the column names
        // are different here.
        let disk_state_str: &str = sql_row_value(&row, "found_disk_state")?;
        let attach_instance_id: Option<Uuid> =
            sql_row_value(&row, "found_attach_instance_id")?;
        let found_disk_state = api::external::DiskState::try_from((
            disk_state_str,
            attach_instance_id,
        ))
        .map_err(|e| Error::internal_error(&e))?;

        if update.updated {
            Ok(())
        } else {
            Err(Error::InvalidRequest {
                message: format!(
                    "disk cannot be deleted in state \"{}\"",
                    found_disk_state
                ),
            })
        }
    }

    // Create a record for a new Oximeter instance
    pub async fn oximeter_create(
        &self,
        info: &OximeterInfo,
    ) -> Result<(), Error> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        let mut values = SqlValueSet::new();
        values.set("time_created", &now);
        values.set("time_modified", &now);
        info.sql_serialize(&mut values);
        sql_insert::<schema::Oximeter>(&client, &values).await
    }

    // Create a record for a new producer endpoint
    pub async fn producer_endpoint_create(
        &self,
        producer: &ProducerEndpoint,
    ) -> Result<(), Error> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        let mut values = SqlValueSet::new();
        values.set("time_created", &now);
        values.set("time_modified", &now);
        producer.sql_serialize(&mut values);
        sql_insert::<schema::MetricProducer>(&client, &values).await
    }

    // Create a record of an assignment of a producer to a collector
    pub async fn oximeter_assignment_create(
        &self,
        oximeter_id: Uuid,
        producer_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        let mut values = SqlValueSet::new();
        values.set("time_created", &now);
        let reg = OximeterAssignment { oximeter_id, producer_id };
        reg.sql_serialize(&mut values);
        sql_insert::<schema::OximeterAssignment>(&client, &values).await
    }

    /*
     * Saga management
     */

    pub async fn saga_create(
        &self,
        saga: &db::saga_types::Saga,
    ) -> Result<(), Error> {
        let client = self.pool.acquire().await?;
        let mut values = SqlValueSet::new();
        saga.sql_serialize(&mut values);
        sql_insert::<schema::Saga>(&client, &values).await
    }

    pub async fn saga_create_event(
        &self,
        event: &db::saga_types::SagaNodeEvent,
    ) -> Result<(), Error> {
        let client = self.pool.acquire().await?;
        let mut values = SqlValueSet::new();
        event.sql_serialize(&mut values);
        // TODO-robustness This INSERT ought to be conditional on this SEC still
        // owning this saga.
        sql_insert::<schema::SagaNodeEvent>(&client, &values).await
    }

    pub async fn saga_update_state(
        &self,
        saga_id: steno::SagaId,
        new_state: steno::SagaCachedState,
        current_sec: db::saga_types::SecId,
        current_adopt_generation: Generation,
    ) -> Result<(), Error> {
        let client = self.pool.acquire().await?;
        let mut values = SqlValueSet::new();
        values.set("saga_state", &new_state.to_string());
        let mut precond_sql = SqlString::new();
        let p1 = precond_sql.next_param(&current_sec);
        let p2 = precond_sql.next_param(&current_adopt_generation);
        precond_sql.push_str(&format!(
            "current_sec = {} AND adopt_generation = {}",
            p1, p2
        ));
        let update = sql_update_precond::<
            schema::Saga,
            schema::LookupGenericByUniqueId,
        >(
            &client,
            (),
            &saga_id.0,
            &["current_sec", "adopt_generation", "saga_state"],
            &values,
            precond_sql,
        )
        .await?;
        let row = &update.found_state;
        let found_sec: Option<&str> = sql_row_value(row, "found_current_sec")
            .unwrap_or(Some("(unknown)"));
        let found_gen =
            sql_row_value::<_, Generation>(row, "found_adopt_generation")
                .map(|i| i.to_string())
                .unwrap_or_else(|_| "(unknown)".to_owned());
        let found_saga_state =
            sql_row_value::<_, String>(row, "found_saga_state")
                .unwrap_or_else(|_| "(unknown)".to_owned());
        bail_unless!(update.updated,
            "failed to update saga {:?} with state {:?}: preconditions not met: \
            expected current_sec = {:?}, adopt_generation = {:?}, \
            but found current_sec = {:?}, adopt_generation = {:?}, state = {:?}", 
            saga_id,
            new_state,
            current_sec,
            current_adopt_generation,
            found_sec,
            found_gen,
            found_saga_state,
        );
        Ok(())
    }

    pub async fn project_list_vpcs(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResult<api::external::Vpc> {
        let client = self.pool.acquire().await?;
        sql_fetch_page_by::<
            LookupByUniqueNameInProject,
            Vpc,
            <Vpc as Table>::ModelType,
        >(&client, (project_id,), pagparams, Vpc::ALL_COLUMNS)
        .await
    }

    pub async fn project_create_vpc(
        &self,
        vpc_id: &Uuid,
        project_id: &Uuid,
        params: &api::external::VpcCreateParams,
    ) -> Result<api::external::Vpc, Error> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        let mut values = SqlValueSet::new();
        values.set("id", vpc_id);
        values.set("time_created", &now);
        values.set("time_modified", &now);
        values.set("project_id", project_id);
        params.sql_serialize(&mut values);

        sql_insert_unique_idempotent_and_fetch::<Vpc, LookupByUniqueId>(
            &client,
            &values,
            params.identity.name.as_str(),
            "id",
            (),
            &vpc_id,
        )
        .await
    }

    pub async fn project_update_vpc(
        &self,
        project_id: &Uuid,
        vpc_name: &Name,
        params: &api::external::VpcUpdateParams,
    ) -> Result<(), Error> {
        let client = self.pool.acquire().await?;
        let now = Utc::now();

        let mut values = SqlValueSet::new();
        values.set("time_modified", &now);

        if let Some(new_name) = &params.identity.name {
            values.set("name", new_name);
        }

        if let Some(new_description) = &params.identity.description {
            values.set("description", new_description);
        }

        if let Some(dns_name) = &params.dns_name {
            values.set("dns_name", dns_name);
        }

        // dummy condition because sql_update_precond breaks otherwise
        // TODO-cleanup: write sql_update that takes no preconditions?
        let mut cond_sql = SqlString::new();
        cond_sql.push_str("true");

        sql_update_precond::<Vpc, LookupByUniqueNameInProject>(
            &client,
            (project_id,),
            vpc_name,
            &[],
            &values,
            cond_sql,
        )
        .await?;

        // TODO-correctness figure out how to get sql_update_precond to return
        // the whole row
        Ok(())
    }

    pub async fn vpc_fetch_by_name(
        &self,
        project_id: &Uuid,
        vpc_name: &Name,
    ) -> LookupResult<api::external::Vpc> {
        let client = self.pool.acquire().await?;
        sql_fetch_row_by::<LookupByUniqueNameInProject, Vpc>(
            &client,
            (project_id,),
            vpc_name,
        )
        .await
    }

    pub async fn project_delete_vpc(&self, vpc_id: &Uuid) -> DeleteResult {
        let client = self.pool.acquire().await?;
        let now = Utc::now();
        sql_execute_maybe_one(
            &client,
            format!(
                "UPDATE {} SET time_deleted = $1 WHERE \
                    time_deleted IS NULL AND id = $2",
                Vpc::TABLE_NAME,
            )
            .as_str(),
            &[&now, &vpc_id],
            || Error::not_found_by_id(ResourceType::Vpc, vpc_id),
        )
        .await
    }
}
