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
use async_bb8_diesel::{AsyncRunQueryDsl, DieselConnectionManager};
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl};
use omicron_common::api;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::Name;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::bail_unless;
use std::sync::Arc;
use uuid::Uuid;

use crate::db;
use crate::db::pagination::paginated;
use crate::db::update_and_check::{UpdateAndCheck, UpdateStatus};

pub struct DataStore {
    pool: Arc<Pool>,
}

impl DataStore {
    pub fn new(pool: Arc<Pool>) -> Self {
        DataStore { pool }
    }

    fn pool(
        &self,
    ) -> &bb8::Pool<DieselConnectionManager<diesel::PgConnection>> {
        self.pool.pool()
    }

    /// Store a new sled in the database.
    pub async fn sled_create(
        &self,
        sled: db::model::Sled,
    ) -> CreateResult<db::model::Sled> {
        use db::diesel_schema::sled::dsl;

        // TODO: What's the right behavior on conflict here?
        //
        // Currently Sled Agent UUIDs come from a config
        // (omicron-sled-agent/src/server.rs), but longer-term, will they be
        // deterministic?
        //
        // I'm wondering about a case where:
        // - Sled Agent comes online, reports itself to Nexus
        // - Sled bounces, and reboots
        // - Sled Agent comes online (again), and reports itself to Nexus. Will
        // Nexus be able to reliably determine the identity of the Sled Agent is
        // the same as it was on the first registration?
        //
        // This current implementation relies on the determinism of these
        // UUIDs.
        let id = sled.id().to_string();
        diesel::insert_into(dsl::sled)
            .values(sled)
            .on_conflict(dsl::id)
            .do_nothing()
            .get_result_async(self.pool())
            .await
            .map_err(|e| Error::from_diesel_create(e, ResourceType::Sled, &id))
    }

    pub async fn sled_list(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Sled> {
        use db::diesel_schema::sled::dsl;
        paginated(dsl::sled, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .load_async::<db::model::Sled>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Sled,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn sled_fetch(&self, id: Uuid) -> LookupResult<db::model::Sled> {
        use db::diesel_schema::sled::dsl;
        dsl::sled
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(id))
            .first_async::<db::model::Sled>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(e, ResourceType::Sled, LookupType::ById(id))
            })
    }

    /// Create a project
    pub async fn project_create(
        &self,
        project: db::model::Project,
    ) -> CreateResult<db::model::Project> {
        use db::diesel_schema::project::dsl;

        let name = project.name().to_string();
        diesel::insert_into(dsl::project)
            .values(project)
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel_create(
                    e,
                    ResourceType::Project,
                    name.as_str(),
                )
            })
    }

    /// Lookup a project by name.
    pub async fn project_fetch(
        &self,
        name: &Name,
    ) -> LookupResult<db::model::Project> {
        use db::diesel_schema::project::dsl;
        dsl::project
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name.clone()))
            .first_async::<db::model::Project>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Project,
                    LookupType::ByName(name.as_str().to_owned()),
                )
            })
    }

    /// Delete a project
    /*
     * TODO-correctness This needs to check whether there are any resources that
     * depend on the Project (Disks, Instances).  We can do this with a
     * generation counter that gets bumped when these resources are created.
     */
    pub async fn project_delete(&self, name: &Name) -> DeleteResult {
        use db::diesel_schema::project::dsl;
        let now = Utc::now();
        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name.clone()))
            .set(dsl::time_deleted.eq(now))
            .get_result_async::<db::model::Project>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Project,
                    LookupType::ByName(name.as_str().to_owned()),
                )
            })?;
        Ok(())
    }

    /// Look up the id for a project based on its name
    pub async fn project_lookup_id_by_name(
        &self,
        name: &Name,
    ) -> Result<Uuid, Error> {
        use db::diesel_schema::project::dsl;
        dsl::project
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name.clone()))
            .select(dsl::id)
            .get_result_async::<Uuid>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Project,
                    LookupType::ByName(name.as_str().to_owned()),
                )
            })
    }

    pub async fn projects_list_by_id(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Project> {
        use db::diesel_schema::project::dsl;
        paginated(dsl::project, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .load_async::<db::model::Project>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Project,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn projects_list_by_name(
        &self,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Project> {
        use db::diesel_schema::project::dsl;
        paginated(dsl::project, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .load_async::<db::model::Project>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Project,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    /// Updates a project by name (clobbering update -- no etag)
    pub async fn project_update(
        &self,
        name: &Name,
        update_params: &api::external::ProjectUpdateParams,
    ) -> UpdateResult<db::model::Project> {
        use db::diesel_schema::project::dsl;
        let updates: db::model::ProjectUpdate = update_params.clone().into();

        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name.clone()))
            .set(updates)
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Project,
                    LookupType::ByName(name.as_str().to_owned()),
                )
            })
    }

    /*
     * Instances
     */

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
        runtime_initial: &db::model::InstanceRuntimeState,
    ) -> CreateResult<db::model::Instance> {
        use db::diesel_schema::instance::dsl;

        let instance = db::model::Instance::new(
            *instance_id,
            *project_id,
            params,
            runtime_initial.clone(),
        );
        let name = instance.name.clone();
        let instance: db::model::Instance = diesel::insert_into(dsl::instance)
            .values(instance)
            .on_conflict(dsl::id)
            .do_nothing()
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel_create(
                    e,
                    ResourceType::Instance,
                    name.as_str(),
                )
            })?;

        bail_unless!(
            instance.state.state() == &api::external::InstanceState::Creating,
            "newly-created Instance has unexpected state: {:?}",
            instance.state
        );
        bail_unless!(
            instance.state_generation == runtime_initial.gen,
            "newly-created Instance has unexpected generation: {:?}",
            instance.state_generation
        );
        Ok(instance)
    }

    pub async fn project_list_instances(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Instance> {
        use db::diesel_schema::instance::dsl;

        paginated(dsl::instance, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .load_async::<db::model::Instance>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Instance,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn instance_fetch(
        &self,
        instance_id: &Uuid,
    ) -> LookupResult<db::model::Instance> {
        use db::diesel_schema::instance::dsl;

        dsl::instance
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*instance_id))
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Instance,
                    LookupType::ById(*instance_id),
                )
            })
    }

    pub async fn instance_fetch_by_name(
        &self,
        project_id: &Uuid,
        instance_name: &Name,
    ) -> LookupResult<db::model::Instance> {
        use db::diesel_schema::instance::dsl;

        dsl::instance
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .filter(dsl::name.eq(instance_name.clone()))
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Instance,
                    LookupType::ByName(instance_name.as_str().to_owned()),
                )
            })
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
        new_runtime: &db::model::InstanceRuntimeState,
    ) -> Result<bool, Error> {
        use db::diesel_schema::instance::dsl;

        let updated = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*instance_id))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime.clone())
            .check_if_exists(*instance_id)
            .execute_and_check::<db::model::Instance>(self.pool())
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Instance,
                    LookupType::ById(*instance_id),
                )
            })?;

        Ok(updated)
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
        use api::external::InstanceState as ApiInstanceState;
        use db::diesel_schema::instance::dsl;
        use db::model::InstanceState as DbInstanceState;

        let now = Utc::now();

        let destroyed = DbInstanceState::new(ApiInstanceState::Destroyed);
        let stopped = DbInstanceState::new(ApiInstanceState::Stopped);
        let failed = DbInstanceState::new(ApiInstanceState::Failed);

        diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*instance_id))
            .filter(dsl::state.eq_any(vec![stopped, failed]))
            .set((dsl::state.eq(destroyed), dsl::time_deleted.eq(now)))
            .get_result_async::<db::model::Instance>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Instance,
                    LookupType::ById(*instance_id),
                )
            })?;
        Ok(())
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
    ) -> ListResultVec<db::model::DiskAttachment> {
        use db::diesel_schema::disk::dsl;

        paginated(dsl::disk, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::attach_instance_id.eq(*instance_id))
            .load_async::<db::model::Disk>(self.pool())
            .await
            .map(|disks| {
                disks
                    .into_iter()
                    // Unwrap safety: filtered by instance_id in query.
                    .map(|disk| disk.attachment().unwrap())
                    .collect()
            })
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Disk,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn project_create_disk(
        &self,
        disk_id: &Uuid,
        project_id: &Uuid,
        params: &api::external::DiskCreateParams,
        runtime_initial: &db::model::DiskRuntimeState,
    ) -> CreateResult<db::model::Disk> {
        use db::diesel_schema::disk::dsl;

        let disk = db::model::Disk::new(
            *disk_id,
            *project_id,
            params.clone(),
            runtime_initial.clone(),
        );
        let name = disk.name.clone();
        let disk: db::model::Disk = diesel::insert_into(dsl::disk)
            .values(disk)
            .on_conflict(dsl::id)
            .do_nothing()
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel_create(e, ResourceType::Disk, name.as_str())
            })?;

        let runtime = disk.runtime();
        bail_unless!(
            runtime.state().state() == &api::external::DiskState::Creating,
            "newly-created Disk has unexpected state: {:?}",
            runtime.disk_state
        );
        bail_unless!(
            runtime.gen == runtime_initial.gen,
            "newly-created Disk has unexpected generation: {:?}",
            runtime.gen
        );
        Ok(disk)
    }

    pub async fn project_list_disks(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Disk> {
        use db::diesel_schema::disk::dsl;

        paginated(dsl::disk, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .load_async::<db::model::Disk>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Disk,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn disk_update_runtime(
        &self,
        disk_id: &Uuid,
        new_runtime: &db::model::DiskRuntimeState,
    ) -> Result<bool, Error> {
        use db::diesel_schema::disk::dsl;

        let updated = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*disk_id))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime.clone())
            .check_if_exists(*disk_id)
            .execute_and_check::<db::model::Disk>(self.pool())
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Disk,
                    LookupType::ById(*disk_id),
                )
            })?;

        Ok(updated)
    }

    pub async fn disk_fetch(
        &self,
        disk_id: &Uuid,
    ) -> LookupResult<db::model::Disk> {
        use db::diesel_schema::disk::dsl;

        dsl::disk
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*disk_id))
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Disk,
                    LookupType::ById(*disk_id),
                )
            })
    }

    pub async fn disk_fetch_by_name(
        &self,
        project_id: &Uuid,
        disk_name: &Name,
    ) -> LookupResult<db::model::Disk> {
        use db::diesel_schema::disk::dsl;

        dsl::disk
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .filter(dsl::name.eq(disk_name.clone()))
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Disk,
                    LookupType::ByName(disk_name.as_str().to_owned()),
                )
            })
    }

    pub async fn project_delete_disk(&self, disk_id: &Uuid) -> DeleteResult {
        use db::diesel_schema::disk::dsl;
        let now = Utc::now();

        let destroyed = api::external::DiskState::Destroyed.label();
        let detached = api::external::DiskState::Detached.label();
        let faulted = api::external::DiskState::Faulted.label();

        let result = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*disk_id))
            .filter(dsl::disk_state.eq_any(vec![detached, faulted]))
            .set((dsl::disk_state.eq(destroyed), dsl::time_deleted.eq(now)))
            .check_if_exists(*disk_id)
            .execute_and_check::<db::model::Disk>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Disk,
                    LookupType::ById(*disk_id),
                )
            })?;

        match result.status {
            UpdateStatus::Updated => Ok(()),
            UpdateStatus::NotUpdatedButExists => Err(Error::InvalidRequest {
                message: format!(
                    "disk cannot be deleted in state \"{}\"",
                    result.found.disk_state
                ),
            }),
        }
    }

    // Create a record for a new Oximeter instance
    pub async fn oximeter_create(
        &self,
        info: &db::model::OximeterInfo,
    ) -> Result<(), Error> {
        use db::diesel_schema::oximeter::dsl;

        diesel::insert_into(dsl::oximeter)
            .values(*info)
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel_create(
                    e,
                    ResourceType::Oximeter,
                    "Oximeter Info",
                )
            })?;
        Ok(())
    }

    // Create a record for a new producer endpoint
    pub async fn producer_endpoint_create(
        &self,
        producer: &db::model::ProducerEndpoint,
    ) -> Result<(), Error> {
        use db::diesel_schema::metricproducer::dsl;

        diesel::insert_into(dsl::metricproducer)
            .values(producer.clone())
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel_create(
                    e,
                    ResourceType::Oximeter,
                    "Producer Endpoint",
                )
            })?;
        Ok(())
    }

    // Create a record of an assignment of a producer to a collector
    pub async fn oximeter_assignment_create(
        &self,
        oximeter_id: Uuid,
        producer_id: Uuid,
    ) -> Result<(), Error> {
        use db::diesel_schema::oximeterassignment::dsl;

        let assignment =
            db::model::OximeterAssignment::new(oximeter_id, producer_id);
        diesel::insert_into(dsl::oximeterassignment)
            .values(assignment)
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel_create(
                    e,
                    ResourceType::Oximeter,
                    "Oximeter Assignment",
                )
            })?;
        Ok(())
    }

    /*
     * Saga management
     */

    pub async fn saga_create(
        &self,
        saga: &db::saga_types::Saga,
    ) -> Result<(), Error> {
        use db::diesel_schema::saga::dsl;

        let name = saga.template_name.clone();
        diesel::insert_into(dsl::saga)
            .values(saga.clone())
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel_create(e, ResourceType::SagaDbg, &name)
            })?;
        Ok(())
    }

    pub async fn saga_create_event(
        &self,
        event: &db::saga_types::SagaNodeEvent,
    ) -> Result<(), Error> {
        use db::diesel_schema::saganodeevent::dsl;

        // TODO-robustness This INSERT ought to be conditional on this SEC still
        // owning this saga.
        diesel::insert_into(dsl::saganodeevent)
            .values(event.clone())
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel_create(
                    e,
                    ResourceType::SagaDbg,
                    "Saga Event",
                )
            })?;
        Ok(())
    }

    pub async fn saga_update_state(
        &self,
        saga_id: steno::SagaId,
        new_state: steno::SagaCachedState,
        current_sec: db::saga_types::SecId,
        current_adopt_generation: Generation,
    ) -> Result<(), Error> {
        use db::diesel_schema::saga::dsl;

        let saga_id: db::saga_types::SagaId = saga_id.into();
        let result = diesel::update(dsl::saga)
            .filter(dsl::id.eq(saga_id))
            .filter(dsl::current_sec.eq(current_sec))
            .filter(dsl::adopt_generation.eq(current_adopt_generation))
            .set(dsl::saga_state.eq(new_state.to_string()))
            .check_if_exists(saga_id)
            .execute_and_check::<db::saga_types::Saga>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::SagaDbg,
                    LookupType::ById(saga_id.0.into()),
                )
            })?;

        match result.status {
            UpdateStatus::Updated => Ok(()),
            UpdateStatus::NotUpdatedButExists => Err(Error::InvalidRequest {
                message: format!(
                    "failed to update saga {:?} with state {:?}: preconditions not met: \
                    expected current_sec = {:?}, adopt_generation = {:?}, \
                    but found current_sec = {:?}, adopt_generation = {:?}, state = {:?}",
                    saga_id,
                    new_state,
                    current_sec,
                    current_adopt_generation,
                    result.found.current_sec,
                    result.found.adopt_generation,
                    result.found.saga_state,
                )
            }),
        }
    }

    pub async fn project_list_vpcs(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Vpc> {
        use db::diesel_schema::vpc::dsl;

        paginated(dsl::vpc, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .load_async::<db::model::Vpc>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Vpc,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn project_create_vpc(
        &self,
        vpc_id: &Uuid,
        project_id: &Uuid,
        params: &api::external::VpcCreateParams,
    ) -> Result<db::model::Vpc, Error> {
        use db::diesel_schema::vpc::dsl;

        let vpc = db::model::Vpc::new(*vpc_id, *project_id, params.clone());
        let name = vpc.name.clone();
        let vpc: db::model::Vpc = diesel::insert_into(dsl::vpc)
            .values(vpc)
            .on_conflict(dsl::id)
            .do_nothing()
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel_create(e, ResourceType::Vpc, name.as_str())
            })?;
        Ok(vpc)
    }

    pub async fn project_update_vpc(
        &self,
        vpc_id: &Uuid,
        params: &api::external::VpcUpdateParams,
    ) -> Result<(), Error> {
        use db::diesel_schema::vpc::dsl;
        let updates: db::model::VpcUpdate = params.clone().into();

        diesel::update(dsl::vpc)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*vpc_id))
            .set(updates)
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Vpc,
                    LookupType::ById(*vpc_id),
                )
            })?;
        Ok(())
    }

    pub async fn vpc_fetch_by_name(
        &self,
        project_id: &Uuid,
        vpc_name: &Name,
    ) -> LookupResult<db::model::Vpc> {
        use db::diesel_schema::vpc::dsl;

        dsl::vpc
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .filter(dsl::name.eq(vpc_name.clone()))
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Vpc,
                    LookupType::ByName(vpc_name.as_str().to_owned()),
                )
            })
    }

    pub async fn project_delete_vpc(&self, vpc_id: &Uuid) -> DeleteResult {
        use db::diesel_schema::vpc::dsl;

        let now = Utc::now();
        diesel::update(dsl::vpc)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*vpc_id))
            .set(dsl::time_deleted.eq(now))
            .get_result_async::<db::model::Vpc>(self.pool())
            .await
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Vpc,
                    LookupType::ById(*vpc_id),
                )
            })?;
        Ok(())
    }
}
