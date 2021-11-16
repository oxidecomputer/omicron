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

use super::collection_insert::{
    AsyncInsertError, DatastoreCollection, SyncInsertError,
};
use super::error::diesel_pool_result_optional;
use super::identity::{Asset, Resource};
use super::Pool;
use crate::authz;
use crate::context::OpContext;
use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl, ConnectionManager};
use chrono::Utc;
use diesel::prelude::*;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use omicron_common::api;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::bail_unless;
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

use crate::db::{
    self,
    error::{
        public_error_from_diesel_pool, public_error_from_diesel_pool_create,
    },
    model::{
        ConsoleSession, Disk, DiskAttachment, DiskRuntimeState, Generation,
        Instance, InstanceRuntimeState, Name, Organization, OrganizationUpdate,
        OximeterInfo, ProducerEndpoint, Project, ProjectUpdate, RouterRoute,
        RouterRouteUpdate, Sled, Vpc, VpcFirewallRule, VpcRouter,
        VpcRouterUpdate, VpcSubnet, VpcSubnetUpdate, VpcUpdate,
    },
    pagination::paginated,
    update_and_check::{UpdateAndCheck, UpdateStatus},
};

pub struct DataStore {
    pool: Arc<Pool>,
}

impl DataStore {
    pub fn new(pool: Arc<Pool>) -> Self {
        DataStore { pool }
    }

    // TODO-security This should be deprecated in favor of pool_authorized(),
    // which gives us the chance to do a minimal security check before hitting
    // the database.  Eventually, this function should only be used for doing
    // authentication in the first place (since we can't do an authz check in
    // that case).
    fn pool(&self) -> &bb8::Pool<ConnectionManager<diesel::PgConnection>> {
        self.pool.pool()
    }

    fn pool_authorized(
        &self,
        opctx: &OpContext,
    ) -> Result<&bb8::Pool<ConnectionManager<diesel::PgConnection>>, Error>
    {
        opctx.authorize(authz::Action::Query, authz::DATABASE)?;
        Ok(self.pool.pool())
    }

    /// Stores a new sled in the database.
    pub async fn sled_upsert(&self, sled: Sled) -> CreateResult<Sled> {
        use db::schema::sled::dsl;
        diesel::insert_into(dsl::sled)
            .values(sled.clone())
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::ip.eq(sled.ip),
                dsl::port.eq(sled.port),
            ))
            .returning(Sled::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::Sled,
                    &sled.id().to_string(),
                )
            })
    }

    pub async fn sled_list(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Sled> {
        use db::schema::sled::dsl;
        paginated(dsl::sled, dsl::id, pagparams)
            .select(Sled::as_select())
            .load_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Sled,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn sled_fetch(&self, id: Uuid) -> LookupResult<Sled> {
        use db::schema::sled::dsl;
        dsl::sled
            .filter(dsl::id.eq(id))
            .select(Sled::as_select())
            .first_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Sled,
                    LookupType::ById(id),
                )
            })
    }

    /// Create a organization
    pub async fn organization_create(
        &self,
        opctx: &OpContext,
        organization: Organization,
    ) -> CreateResult<Organization> {
        use db::schema::organization::dsl;

        opctx.authorize(authz::Action::CreateOrganization, authz::FLEET)?;

        let name = organization.name().as_str().to_string();
        diesel::insert_into(dsl::organization)
            .values(organization)
            .returning(Organization::as_returning())
            .get_result_async(self.pool_authorized(opctx)?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::Organization,
                    name.as_str(),
                )
            })
    }

    /// Lookup a organization by name.
    pub async fn organization_fetch(
        &self,
        name: &Name,
    ) -> LookupResult<Organization> {
        use db::schema::organization::dsl;
        dsl::organization
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name.clone()))
            .select(Organization::as_select())
            .first_async::<Organization>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Organization,
                    LookupType::ByName(name.as_str().to_owned()),
                )
            })
    }

    /// Delete a organization
    pub async fn organization_delete(&self, name: &Name) -> DeleteResult {
        use db::schema::organization::dsl;
        use db::schema::project;

        let (id, rcgen) = dsl::organization
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name.clone()))
            .select((dsl::id, dsl::rcgen))
            .get_result_async::<(Uuid, Generation)>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Organization,
                    LookupType::ByName(name.as_str().to_owned()),
                )
            })?;

        // Make sure there are no projects present within this organization.
        let project_found = diesel_pool_result_optional(
            project::dsl::project
                .filter(project::dsl::organization_id.eq(id))
                .filter(project::dsl::time_deleted.is_null())
                .select(project::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool())
                .await,
        )
        .map_err(|e| {
            public_error_from_diesel_pool(
                e,
                ResourceType::Project,
                LookupType::Other("by organization_id".to_string()),
            )
        })?;
        if project_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "organization to be deleted contains a project"
                    .to_string(),
            });
        }

        let now = Utc::now();
        let updated_rows = diesel::update(dsl::organization)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(id))
            .filter(dsl::rcgen.eq(rcgen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Organization,
                    LookupType::ById(id),
                )
            })?;

        if updated_rows == 0 {
            return Err(Error::InvalidRequest {
                message: "deletion failed due to concurrent modification"
                    .to_string(),
            });
        }
        Ok(())
    }

    /// Look up the id for a organization based on its name
    pub async fn organization_lookup_id_by_name(
        &self,
        name: &Name,
    ) -> Result<Uuid, Error> {
        use db::schema::organization::dsl;
        dsl::organization
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name.clone()))
            .select(dsl::id)
            .get_result_async::<Uuid>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Organization,
                    LookupType::ByName(name.as_str().to_owned()),
                )
            })
    }

    pub async fn organizations_list_by_id(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Organization> {
        use db::schema::organization::dsl;
        paginated(dsl::organization, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(Organization::as_select())
            .load_async::<Organization>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Organization,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn organizations_list_by_name(
        &self,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Organization> {
        use db::schema::organization::dsl;
        paginated(dsl::organization, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(Organization::as_select())
            .load_async::<Organization>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Organization,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    /// Updates a organization by name (clobbering update -- no etag)
    pub async fn organization_update(
        &self,
        name: &Name,
        updates: OrganizationUpdate,
    ) -> UpdateResult<Organization> {
        use db::schema::organization::dsl;

        diesel::update(dsl::organization)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name.clone()))
            .set(updates)
            .returning(Organization::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Organization,
                    LookupType::ByName(name.as_str().to_owned()),
                )
            })
    }

    /// Create a project
    pub async fn project_create(
        &self,
        project: Project,
    ) -> CreateResult<Project> {
        use db::schema::project::dsl;

        let name = project.name().as_str().to_string();
        let organization_id = project.organization_id;
        Organization::insert_resource(
            organization_id,
            diesel::insert_into(dsl::project).values(project),
        )
        .insert_and_get_result_async(self.pool())
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Organization,
                lookup_type: LookupType::ById(organization_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::Project,
                    &name,
                )
            }
        })
    }

    /// Lookup a project by name.
    pub async fn project_fetch(
        &self,
        organization_id: &Uuid,
        name: &Name,
    ) -> LookupResult<Project> {
        use db::schema::project::dsl;
        dsl::project
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::organization_id.eq(*organization_id))
            .filter(dsl::name.eq(name.clone()))
            .select(Project::as_select())
            .first_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
    pub async fn project_delete(
        &self,
        organization_id: &Uuid,
        name: &Name,
    ) -> DeleteResult {
        use db::schema::project::dsl;
        let now = Utc::now();
        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::organization_id.eq(*organization_id))
            .filter(dsl::name.eq(name.clone()))
            .set(dsl::time_deleted.eq(now))
            .returning(Project::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
        organization_id: &Uuid,
        name: &Name,
    ) -> Result<Uuid, Error> {
        use db::schema::project::dsl;
        dsl::project
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::organization_id.eq(*organization_id))
            .filter(dsl::name.eq(name.clone()))
            .select(dsl::id)
            .get_result_async::<Uuid>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Project,
                    LookupType::ByName(name.as_str().to_owned()),
                )
            })
    }

    pub async fn projects_list_by_id(
        &self,
        organization_id: &Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Project> {
        use db::schema::project::dsl;
        paginated(dsl::project, dsl::id, pagparams)
            .filter(dsl::organization_id.eq(*organization_id))
            .filter(dsl::time_deleted.is_null())
            .select(Project::as_select())
            .load_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Project,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn projects_list_by_name(
        &self,
        organization_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Project> {
        use db::schema::project::dsl;

        paginated(dsl::project, dsl::name, &pagparams)
            .filter(dsl::organization_id.eq(*organization_id))
            .filter(dsl::time_deleted.is_null())
            .select(Project::as_select())
            .load_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Project,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    /// Updates a project by name (clobbering update -- no etag)
    pub async fn project_update(
        &self,
        organization_id: &Uuid,
        name: &Name,
        updates: ProjectUpdate,
    ) -> UpdateResult<Project> {
        use db::schema::project::dsl;

        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::organization_id.eq(*organization_id))
            .filter(dsl::name.eq(name.clone()))
            .set(updates)
            .returning(Project::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
        instance: Instance,
    ) -> CreateResult<Instance> {
        use db::schema::instance::dsl;

        let gen = instance.runtime().gen;
        let name = instance.name().clone();
        let instance: Instance = diesel::insert_into(dsl::instance)
            .values(instance)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(Instance::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::Instance,
                    name.as_str(),
                )
            })?;

        bail_unless!(
            instance.runtime().state.state()
                == &api::external::InstanceState::Creating,
            "newly-created Instance has unexpected state: {:?}",
            instance.runtime().state
        );
        bail_unless!(
            instance.runtime().gen == gen,
            "newly-created Instance has unexpected generation: {:?}",
            instance.runtime().gen
        );
        Ok(instance)
    }

    pub async fn project_list_instances(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Instance> {
        use db::schema::instance::dsl;

        paginated(dsl::instance, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .select(Instance::as_select())
            .load_async::<Instance>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Instance,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn instance_fetch(
        &self,
        instance_id: &Uuid,
    ) -> LookupResult<Instance> {
        use db::schema::instance::dsl;

        dsl::instance
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*instance_id))
            .select(Instance::as_select())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
    ) -> LookupResult<Instance> {
        use db::schema::instance::dsl;

        dsl::instance
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .filter(dsl::name.eq(instance_name.clone()))
            .select(Instance::as_select())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
        new_runtime: &InstanceRuntimeState,
    ) -> Result<bool, Error> {
        use db::schema::instance::dsl;

        let updated = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*instance_id))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime.clone())
            .check_if_exists::<Instance>(*instance_id)
            .execute_and_check(self.pool())
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel_pool(
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
        use db::model::InstanceState as DbInstanceState;
        use db::schema::instance::dsl;

        let now = Utc::now();

        let destroyed = DbInstanceState::new(ApiInstanceState::Destroyed);
        let stopped = DbInstanceState::new(ApiInstanceState::Stopped);
        let failed = DbInstanceState::new(ApiInstanceState::Failed);

        let result = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*instance_id))
            .filter(dsl::state.eq_any(vec![stopped, failed]))
            .set((dsl::state.eq(destroyed), dsl::time_deleted.eq(now)))
            .check_if_exists::<Instance>(*instance_id)
            .execute_and_check(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Instance,
                    LookupType::ById(*instance_id),
                )
            })?;
        match result.status {
            UpdateStatus::Updated => Ok(()),
            UpdateStatus::NotUpdatedButExists => {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "instance cannot be deleted in state \"{}\"",
                        result.found.runtime_state.state.state()
                    ),
                });
            }
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
    ) -> ListResultVec<DiskAttachment> {
        use db::schema::disk::dsl;

        paginated(dsl::disk, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::attach_instance_id.eq(*instance_id))
            .select(Disk::as_select())
            .load_async::<Disk>(self.pool())
            .await
            .map(|disks| {
                disks
                    .into_iter()
                    // Unwrap safety: filtered by instance_id in query.
                    .map(|disk| disk.attachment().unwrap())
                    .collect()
            })
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Disk,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn project_create_disk(&self, disk: Disk) -> CreateResult<Disk> {
        use db::schema::disk::dsl;

        let gen = disk.runtime().gen;
        let name = disk.name().clone();
        let disk: Disk = diesel::insert_into(dsl::disk)
            .values(disk)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(Disk::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::Disk,
                    name.as_str(),
                )
            })?;

        let runtime = disk.runtime();
        bail_unless!(
            runtime.state().state() == &api::external::DiskState::Creating,
            "newly-created Disk has unexpected state: {:?}",
            runtime.disk_state
        );
        bail_unless!(
            runtime.gen == gen,
            "newly-created Disk has unexpected generation: {:?}",
            runtime.gen
        );
        Ok(disk)
    }

    pub async fn project_list_disks(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Disk> {
        use db::schema::disk::dsl;

        paginated(dsl::disk, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .select(Disk::as_select())
            .load_async::<Disk>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Disk,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn disk_update_runtime(
        &self,
        disk_id: &Uuid,
        new_runtime: &DiskRuntimeState,
    ) -> Result<bool, Error> {
        use db::schema::disk::dsl;

        let updated = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*disk_id))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime.clone())
            .check_if_exists::<Disk>(*disk_id)
            .execute_and_check(self.pool())
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Disk,
                    LookupType::ById(*disk_id),
                )
            })?;

        Ok(updated)
    }

    pub async fn disk_fetch(&self, disk_id: &Uuid) -> LookupResult<Disk> {
        use db::schema::disk::dsl;

        dsl::disk
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*disk_id))
            .select(Disk::as_select())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
    ) -> LookupResult<Disk> {
        use db::schema::disk::dsl;

        dsl::disk
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .filter(dsl::name.eq(disk_name.clone()))
            .select(Disk::as_select())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Disk,
                    LookupType::ByName(disk_name.as_str().to_owned()),
                )
            })
    }

    pub async fn project_delete_disk(&self, disk_id: &Uuid) -> DeleteResult {
        use db::schema::disk::dsl;
        let now = Utc::now();

        let destroyed = api::external::DiskState::Destroyed.label();
        let detached = api::external::DiskState::Detached.label();
        let faulted = api::external::DiskState::Faulted.label();

        let result = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*disk_id))
            .filter(dsl::disk_state.eq_any(vec![detached, faulted]))
            .set((dsl::disk_state.eq(destroyed), dsl::time_deleted.eq(now)))
            .check_if_exists::<Disk>(*disk_id)
            .execute_and_check(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
                    result.found.runtime_state.disk_state
                ),
            }),
        }
    }

    // Create a record for a new Oximeter instance
    pub async fn oximeter_create(
        &self,
        info: &OximeterInfo,
    ) -> Result<(), Error> {
        use db::schema::oximeter::dsl;

        // If we get a conflict on the Oximeter ID, this means that collector instance was
        // previously registered, and it's re-registering due to something like a service restart.
        // In this case, we update the time modified and the service address, rather than
        // propagating a constraint violation to the caller.
        diesel::insert_into(dsl::oximeter)
            .values(*info)
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::ip.eq(info.ip),
                dsl::port.eq(info.port),
            ))
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::Oximeter,
                    "Oximeter Info",
                )
            })?;
        Ok(())
    }

    // Fetch a record for an Oximeter instance, by its ID.
    pub async fn oximeter_fetch(
        &self,
        id: Uuid,
    ) -> Result<OximeterInfo, Error> {
        use db::schema::oximeter::dsl;
        dsl::oximeter
            .filter(dsl::id.eq(id))
            .first_async::<OximeterInfo>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Oximeter,
                    LookupType::ById(id),
                )
            })
    }

    // List the oximeter collector instances
    pub async fn oximeter_list(
        &self,
        page_params: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<OximeterInfo> {
        use db::schema::oximeter::dsl;
        paginated(dsl::oximeter, dsl::id, page_params)
            .load_async::<OximeterInfo>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Oximeter,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    // Create a record for a new producer endpoint
    pub async fn producer_endpoint_create(
        &self,
        producer: &ProducerEndpoint,
    ) -> Result<(), Error> {
        use db::schema::metric_producer::dsl;

        // TODO: see https://github.com/oxidecomputer/omicron/issues/323
        diesel::insert_into(dsl::metric_producer)
            .values(producer.clone())
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::ip.eq(producer.ip),
                dsl::port.eq(producer.port),
                dsl::interval.eq(producer.interval),
                dsl::base_route.eq(producer.base_route.clone()),
            ))
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::MetricProducer,
                    "Producer Endpoint",
                )
            })?;
        Ok(())
    }

    // List the producer endpoint records by the oximeter instance to which they're assigned.
    pub async fn producers_list_by_oximeter_id(
        &self,
        oximeter_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<ProducerEndpoint> {
        use db::schema::metric_producer::dsl;
        paginated(dsl::metric_producer, dsl::id, &pagparams)
            .filter(dsl::oximeter_id.eq(oximeter_id))
            .order_by((dsl::oximeter_id, dsl::id))
            .select(ProducerEndpoint::as_select())
            .load_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::MetricProducer,
                    "By Oximeter ID",
                )
            })
    }

    // Sagas

    pub async fn saga_create(
        &self,
        saga: &db::saga_types::Saga,
    ) -> Result<(), Error> {
        use db::schema::saga::dsl;

        let name = saga.template_name.clone();
        diesel::insert_into(dsl::saga)
            .values(saga.clone())
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::SagaDbg,
                    &name,
                )
            })?;
        Ok(())
    }

    pub async fn saga_create_event(
        &self,
        event: &db::saga_types::SagaNodeEvent,
    ) -> Result<(), Error> {
        use db::schema::saga_node_event::dsl;

        // TODO-robustness This INSERT ought to be conditional on this SEC still
        // owning this saga.
        diesel::insert_into(dsl::saga_node_event)
            .values(event.clone())
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
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
        use db::schema::saga::dsl;

        let saga_id: db::saga_types::SagaId = saga_id.into();
        let result = diesel::update(dsl::saga)
            .filter(dsl::id.eq(saga_id))
            .filter(dsl::current_sec.eq(current_sec))
            .filter(dsl::adopt_generation.eq(current_adopt_generation))
            .set(dsl::saga_state.eq(new_state.to_string()))
            .check_if_exists::<db::saga_types::Saga>(saga_id)
            .execute_and_check(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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

    pub async fn saga_list_unfinished_by_id(
        &self,
        sec_id: &db::SecId,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::saga_types::Saga> {
        use db::schema::saga::dsl;
        paginated(dsl::saga, dsl::id, &pagparams)
            .filter(
                dsl::saga_state.ne(steno::SagaCachedState::Done.to_string()),
            )
            .filter(dsl::current_sec.eq(*sec_id))
            .load_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::SagaDbg,
                    LookupType::ById(sec_id.0),
                )
            })
    }

    pub async fn saga_node_event_list_by_id(
        &self,
        id: db::saga_types::SagaId,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<steno::SagaNodeEvent> {
        use db::schema::saga_node_event::dsl;
        paginated(dsl::saga_node_event, dsl::saga_id, &pagparams)
            .filter(dsl::saga_id.eq(id))
            .load_async::<db::saga_types::SagaNodeEvent>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::SagaDbg,
                    LookupType::ById(id.0 .0),
                )
            })?
            .into_iter()
            .map(|db_event| steno::SagaNodeEvent::try_from(db_event))
            .collect::<Result<_, Error>>()
    }

    // VPCs

    pub async fn project_list_vpcs(
        &self,
        project_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Vpc> {
        use db::schema::vpc::dsl;

        paginated(dsl::vpc, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .select(Vpc::as_select())
            .load_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Vpc,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn project_create_vpc(&self, vpc: Vpc) -> Result<Vpc, Error> {
        use db::schema::vpc::dsl;

        let name = vpc.name().clone();
        let vpc = diesel::insert_into(dsl::vpc)
            .values(vpc)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(Vpc::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::Vpc,
                    name.as_str(),
                )
            })?;
        Ok(vpc)
    }

    pub async fn project_update_vpc(
        &self,
        vpc_id: &Uuid,
        updates: VpcUpdate,
    ) -> Result<(), Error> {
        use db::schema::vpc::dsl;

        diesel::update(dsl::vpc)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*vpc_id))
            .set(updates)
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
    ) -> LookupResult<Vpc> {
        use db::schema::vpc::dsl;

        dsl::vpc
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(*project_id))
            .filter(dsl::name.eq(vpc_name.clone()))
            .select(Vpc::as_select())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Vpc,
                    LookupType::ByName(vpc_name.as_str().to_owned()),
                )
            })
    }

    pub async fn project_delete_vpc(&self, vpc_id: &Uuid) -> DeleteResult {
        use db::schema::vpc::dsl;

        // Note that we don't ensure the firewall rules are empty here, because
        // we allow deleting VPCs with firewall rules present

        let now = Utc::now();
        diesel::update(dsl::vpc)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*vpc_id))
            .set(dsl::time_deleted.eq(now))
            .returning(Vpc::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Vpc,
                    LookupType::ById(*vpc_id),
                )
            })?;
        Ok(())
    }

    pub async fn vpc_list_firewall_rules(
        &self,
        vpc_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<VpcFirewallRule> {
        use db::schema::vpc_firewall_rule::dsl;

        paginated(dsl::vpc_firewall_rule, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(*vpc_id))
            .select(VpcFirewallRule::as_select())
            .load_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::VpcFirewallRule,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn vpc_delete_all_firewall_rules(
        &self,
        vpc_id: &Uuid,
    ) -> DeleteResult {
        use db::schema::vpc_firewall_rule::dsl;

        let now = Utc::now();
        diesel::update(dsl::vpc_firewall_rule)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(*vpc_id))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::Vpc,
                    LookupType::ById(*vpc_id),
                )
            })?;
        Ok(())
    }

    /// Replace all firewall rules with the given rules
    pub async fn vpc_update_firewall_rules(
        &self,
        vpc_id: &Uuid,
        rules: Vec<VpcFirewallRule>,
    ) -> UpdateResult<Vec<VpcFirewallRule>> {
        use db::schema::vpc_firewall_rule::dsl;

        let now = Utc::now();
        let delete_old_query = diesel::update(dsl::vpc_firewall_rule)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(*vpc_id))
            .set(dsl::time_deleted.eq(now));

        let insert_new_query = Vpc::insert_resource(
            *vpc_id,
            diesel::insert_into(dsl::vpc_firewall_rule).values(rules),
        );

        // TODO-scalability: Ideally this would be a CTE so we don't need to
        // hold a transaction open across multiple roundtrips from the database,
        // but for now we're using a transaction due to the severely decreased
        // legibility of CTEs via diesel right now.
        self.pool()
            .transaction(move |conn| {
                delete_old_query.execute(conn)?;

                // The generation count update on the vpc table row will take a
                // write lock on the row, ensuring that the vpc was not deleted
                // concurently.
                insert_new_query.insert_and_get_results(conn).map_err(|e| {
                    match e {
                        SyncInsertError::CollectionNotFound => {
                            diesel::result::Error::RollbackTransaction
                        }
                        SyncInsertError::DatabaseError(e) => e,
                    }
                })
            })
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::VpcFirewallRule,
                    LookupType::ById(*vpc_id),
                )
            })
    }

    pub async fn vpc_list_subnets(
        &self,
        vpc_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<VpcSubnet> {
        use db::schema::vpc_subnet::dsl;

        paginated(dsl::vpc_subnet, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(*vpc_id))
            .select(VpcSubnet::as_select())
            .load_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::VpcSubnet,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }
    pub async fn vpc_subnet_fetch_by_name(
        &self,
        vpc_id: &Uuid,
        subnet_name: &Name,
    ) -> LookupResult<VpcSubnet> {
        use db::schema::vpc_subnet::dsl;

        dsl::vpc_subnet
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(*vpc_id))
            .filter(dsl::name.eq(subnet_name.clone()))
            .select(VpcSubnet::as_select())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::VpcSubnet,
                    LookupType::ByName(subnet_name.as_str().to_owned()),
                )
            })
    }

    pub async fn vpc_create_subnet(
        &self,
        subnet: VpcSubnet,
    ) -> CreateResult<VpcSubnet> {
        use db::schema::vpc_subnet::dsl;

        let name = subnet.name().clone();
        let subnet = diesel::insert_into(dsl::vpc_subnet)
            .values(subnet)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(VpcSubnet::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::VpcSubnet,
                    name.as_str(),
                )
            })?;
        Ok(subnet)
    }

    pub async fn vpc_delete_subnet(&self, subnet_id: &Uuid) -> DeleteResult {
        use db::schema::vpc_subnet::dsl;

        let now = Utc::now();
        diesel::update(dsl::vpc_subnet)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*subnet_id))
            .set(dsl::time_deleted.eq(now))
            .returning(VpcSubnet::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::VpcSubnet,
                    LookupType::ById(*subnet_id),
                )
            })?;
        Ok(())
    }

    pub async fn vpc_update_subnet(
        &self,
        subnet_id: &Uuid,
        updates: VpcSubnetUpdate,
    ) -> Result<(), Error> {
        use db::schema::vpc_subnet::dsl;

        diesel::update(dsl::vpc_subnet)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*subnet_id))
            .set(updates)
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::VpcSubnet,
                    LookupType::ById(*subnet_id),
                )
            })?;
        Ok(())
    }

    pub async fn vpc_list_routers(
        &self,
        vpc_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<VpcRouter> {
        use db::schema::vpc_router::dsl;

        paginated(dsl::vpc_router, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(*vpc_id))
            .select(VpcRouter::as_select())
            .load_async::<db::model::VpcRouter>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::VpcRouter,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn vpc_router_fetch_by_name(
        &self,
        vpc_id: &Uuid,
        router_name: &Name,
    ) -> LookupResult<VpcRouter> {
        use db::schema::vpc_router::dsl;

        dsl::vpc_router
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(*vpc_id))
            .filter(dsl::name.eq(router_name.clone()))
            .select(VpcRouter::as_select())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::VpcRouter,
                    LookupType::ByName(router_name.as_str().to_owned()),
                )
            })
    }

    pub async fn vpc_create_router(
        &self,
        router: VpcRouter,
    ) -> CreateResult<VpcRouter> {
        use db::schema::vpc_router::dsl;

        let name = router.name().clone();
        let router = diesel::insert_into(dsl::vpc_router)
            .values(router)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(VpcRouter::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::VpcRouter,
                    name.as_str(),
                )
            })?;
        Ok(router)
    }

    pub async fn vpc_delete_router(&self, router_id: &Uuid) -> DeleteResult {
        use db::schema::vpc_router::dsl;

        let now = Utc::now();
        diesel::update(dsl::vpc_router)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*router_id))
            .set(dsl::time_deleted.eq(now))
            .returning(VpcRouter::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::VpcRouter,
                    LookupType::ById(*router_id),
                )
            })?;
        Ok(())
    }

    pub async fn vpc_update_router(
        &self,
        router_id: &Uuid,
        updates: VpcRouterUpdate,
    ) -> Result<(), Error> {
        use db::schema::vpc_router::dsl;

        diesel::update(dsl::vpc_router)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*router_id))
            .set(updates)
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::VpcRouter,
                    LookupType::ById(*router_id),
                )
            })?;
        Ok(())
    }

    pub async fn router_list_routes(
        &self,
        router_id: &Uuid,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<RouterRoute> {
        use db::schema::router_route::dsl;

        paginated(dsl::router_route, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::router_id.eq(*router_id))
            .select(RouterRoute::as_select())
            .load_async::<db::model::RouterRoute>(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::RouterRoute,
                    LookupType::Other("Listing All".to_string()),
                )
            })
    }

    pub async fn router_route_fetch_by_name(
        &self,
        router_id: &Uuid,
        route_name: &Name,
    ) -> LookupResult<RouterRoute> {
        use db::schema::router_route::dsl;

        dsl::router_route
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::router_id.eq(*router_id))
            .filter(dsl::name.eq(route_name.clone()))
            .select(RouterRoute::as_select())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::RouterRoute,
                    LookupType::ByName(route_name.as_str().to_owned()),
                )
            })
    }

    pub async fn router_create_route(
        &self,
        route: RouterRoute,
    ) -> CreateResult<RouterRoute> {
        use db::schema::router_route::dsl;
        let router_id = route.router_id;
        let name = route.name().clone();

        VpcRouter::insert_resource(
            router_id,
            diesel::insert_into(dsl::router_route).values(route),
        )
        .insert_and_get_result_async(self.pool())
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::VpcRouter,
                lookup_type: LookupType::ById(router_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool_create(
                    e,
                    ResourceType::RouterRoute,
                    name.as_str(),
                )
            }
        })
    }

    pub async fn router_delete_route(&self, route_id: &Uuid) -> DeleteResult {
        use db::schema::router_route::dsl;

        let now = Utc::now();
        diesel::update(dsl::router_route)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*route_id))
            .set(dsl::time_deleted.eq(now))
            .returning(RouterRoute::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::RouterRoute,
                    LookupType::ById(*route_id),
                )
            })?;
        Ok(())
    }

    pub async fn router_update_route(
        &self,
        route_id: &Uuid,
        route_update: RouterRouteUpdate,
    ) -> Result<(), Error> {
        use db::schema::router_route::dsl;

        diesel::update(dsl::router_route)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*route_id))
            .set(route_update)
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ResourceType::RouterRoute,
                    LookupType::ById(*route_id),
                )
            })?;
        Ok(())
    }

    // TODO-correctness: fix session method errors. the map_errs turn all errors
    // into 500s, most notably (and most frequently) session not found. they
    // don't end up as 500 in the http response because they get turned into a
    // 4xx error by calling code, the session cookie authn scheme. this is
    // necessary for now in order to avoid the possibility of leaking out a
    // too-friendly 404 to the client. once datastore has its own error type and
    // the conversion to serializable user-facing errors happens elsewhere (see
    // issue #347) these methods can safely return more accurate errors, and
    // showing/hiding that info as appropriate will be handled higher up

    pub async fn session_fetch(
        &self,
        token: String,
    ) -> LookupResult<ConsoleSession> {
        use db::schema::console_session::dsl;
        dsl::console_session
            .filter(dsl::token.eq(token.clone()))
            .select(ConsoleSession::as_select())
            .first_async(self.pool())
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error fetching session: {:?}",
                    e
                ))
            })
    }

    pub async fn session_create(
        &self,
        session: ConsoleSession,
    ) -> CreateResult<ConsoleSession> {
        use db::schema::console_session::dsl;

        diesel::insert_into(dsl::console_session)
            .values(session)
            .returning(ConsoleSession::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error creating session: {:?}",
                    e
                ))
            })
    }

    pub async fn session_update_last_used(
        &self,
        token: String,
    ) -> UpdateResult<ConsoleSession> {
        use db::schema::console_session::dsl;

        diesel::update(dsl::console_session)
            .filter(dsl::token.eq(token.clone()))
            .set((dsl::time_last_used.eq(Utc::now()),))
            .returning(ConsoleSession::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error renewing session: {:?}",
                    e
                ))
            })
    }

    // putting "hard" in the name because we don't do this with any other model
    pub async fn session_hard_delete(&self, token: String) -> DeleteResult {
        use db::schema::console_session::dsl;

        diesel::delete(dsl::console_session)
            .filter(dsl::token.eq(token.clone()))
            .execute_async(self.pool())
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error deleting session: {:?}",
                    e
                ))
            })
    }
}

#[cfg(test)]
mod test {
    use crate::context::OpContext;
    use crate::db;
    use crate::db::identity::Resource;
    use crate::db::model::{ConsoleSession, Organization, Project};
    use crate::db::DataStore;
    use chrono::{Duration, Utc};
    use omicron_common::api::external::{
        Error, IdentityMetadataCreateParams, OrganizationCreateParams,
        ProjectCreateParams,
    };
    use omicron_test_utils::dev;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_project_creation() {
        let logctx = dev::test_setup_log("test_collection_not_present");
        let opctx = OpContext::for_unit_tests(logctx.log.new(o!()));
        let mut db = dev::test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&cfg);
        let datastore = DataStore::new(Arc::new(pool));

        let organization = Organization::new(OrganizationCreateParams {
            identity: IdentityMetadataCreateParams {
                name: "org".parse().unwrap(),
                description: "desc".to_string(),
            },
        });
        let organization =
            datastore.organization_create(&opctx, organization).await.unwrap();

        let project = Project::new(
            organization.id(),
            ProjectCreateParams {
                identity: IdentityMetadataCreateParams {
                    name: "project".parse().unwrap(),
                    description: "desc".to_string(),
                },
            },
        );
        datastore.project_create(project).await.unwrap();
        let organization_after_project_create =
            datastore.organization_fetch(organization.name()).await.unwrap();
        assert!(organization_after_project_create.rcgen > organization.rcgen);

        let _ = db.cleanup().await;
    }

    #[tokio::test]
    async fn test_session_methods() {
        let logctx = dev::test_setup_log("test_collection_not_present");
        let mut db = dev::test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&cfg);
        let datastore = DataStore::new(Arc::new(pool));

        let token = "a_token".to_string();
        let session = ConsoleSession {
            token: token.clone(),
            time_created: Utc::now() - Duration::minutes(5),
            time_last_used: Utc::now() - Duration::minutes(5),
            user_id: Uuid::new_v4(),
        };

        let _ = datastore.session_create(session.clone()).await;

        // fetch the one we just created
        let fetched = datastore.session_fetch(token.clone()).await.unwrap();
        assert_eq!(session.user_id, fetched.user_id);

        // trying to insert the same one again fails
        let duplicate = datastore.session_create(session.clone()).await;
        assert!(matches!(
            duplicate,
            Err(Error::InternalError { internal_message: _ })
        ));

        // update last used (i.e., renew token)
        let renewed =
            datastore.session_update_last_used(token.clone()).await.unwrap();
        assert!(renewed.time_last_used > session.time_last_used);

        // time_last_used change persists in DB
        let fetched = datastore.session_fetch(token.clone()).await.unwrap();
        assert!(fetched.time_last_used > session.time_last_used);

        // delete it and fetch should come back with nothing
        let delete = datastore.session_hard_delete(token.clone()).await;
        assert_eq!(delete, Ok(()));

        // this will be a not found after #347
        let fetched = datastore.session_fetch(token.clone()).await;
        assert!(matches!(
            fetched,
            Err(Error::InternalError { internal_message: _ })
        ));

        // deleting an already nonexistent is considered a success
        let delete_again = datastore.session_hard_delete(token.clone()).await;
        assert_eq!(delete_again, Ok(()));

        let _ = db.cleanup().await;
    }
}
