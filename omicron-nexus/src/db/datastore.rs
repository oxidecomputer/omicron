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
use diesel::expression::{AsExpression, NonAggregate};
use diesel::query_dsl::methods as query_methods;
use diesel::query_builder::QueryFragment;
use diesel::pg::Pg;
use diesel::query_builder::*;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use diesel::query_builder::AsQuery;
use omicron_common::api;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::ListResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::Name;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::bail_unless;
use omicron_common::db::sql_row_value;
use std::sync::Arc;
use uuid::Uuid;

use super::operations::sql_execute_maybe_one;
use super::schema;
use super::schema::LookupByUniqueId;
use super::schema::LookupByUniqueNameInProject;
use super::schema::Vpc;
use super::sql::SqlSerialize;
use super::sql::SqlString;
use super::sql::SqlValueSet;
use super::sql::Table;
use super::sql_operations::sql_fetch_page_by;
use super::sql_operations::sql_fetch_row_by;
use super::sql_operations::sql_insert;
use super::sql_operations::sql_insert_unique_idempotent_and_fetch;
use super::sql_operations::sql_update_precond;
use crate::db;
use crate::db::update_and_check::{UpdateAndCheck, UpdateAndQueryResult};

// TODO: The api would be better impl'd as a trait, but it might
// be easier to get rev 1 working with a function.
//
// TODO: Maybe move this thing to a different file?


/*
trait Paginate<'a> {
    fn paginate(self, pagparams: &DataPageParams<'_, Uuid>) -> BoxedQuery<'a>;
}

#[must_use = "Queries must be executed"]
struct Paginated<P> {
    query: P,
}

impl<P> AsQuery for Paginated<P>
where
    P: Query,
{
    type SqlType = P::SqlType;
    type Query = P;

    fn as_query(self) -> Self::Query {
        self.query
    }
}

impl<P> RunQueryDsl<PgConnection> for Paginated<P> {}
*/

type BoxedQuery<'a, T> = BoxedSelectStatement<'a, <T as AsQuery>::SqlType, T, Pg>;

pub fn paginated<'a, T, E, M>(table: T, column: E, pagparams: &DataPageParams<'a, M>) -> BoxedQuery<'a, T>
where
    T: diesel::Table + AsQuery,
    T::Query: query_methods::BoxedDsl<'static, Pg, Output = BoxedQuery<'static, T>>,
    E: 'a + ExpressionMethods + Sized + diesel::AppearsOnTable<T> + NonAggregate + QueryFragment<Pg> + Copy,
    E::SqlType: diesel::sql_types::SingleValue,
    &'a M: AsExpression<E::SqlType>,
    <&'a M as AsExpression<<E as diesel::Expression>::SqlType>>::Expression: diesel::AppearsOnTable<T> + NonAggregate + QueryFragment<Pg>,
{
    let mut query = table
        .into_boxed()
        .limit(pagparams.limit.get().into());
    match pagparams.direction {
        dropshot::PaginationOrder::Ascending => {
            if let Some(marker) = pagparams.marker {
                query = query.filter(column.gt(marker));
            }
            query.order(column.asc())
        }
        dropshot::PaginationOrder::Descending => {
            if let Some(marker) = pagparams.marker {
                query = query.filter(column.lt(marker));
            }
            query.order(column.desc())
        }
    }
}

/*
impl<'a, T> Paginate<'a> for T
where
    // We start with an input query that may be boxed.
    T: diesel::Table + AsQuery + QueryDsl,
    T::Query: query_methods::BoxedDsl<'a, Pg>,
    // Once boxed, we should be able to apply a limit.

{
    fn paginate(self, pagparams: &DataPageParams<'_, Uuid>) -> BoxedQuery<'static>
    {
        use db::diesel_schema::project::dsl;
        let mut query = self
            .limit(pagparams.limit.get().into())
            .filter(dsl::id.gt(pagparams.marker.unwrap()))
            .into_boxed();
        query


            /*
        match pagparams.direction {
            dropshot::PaginationOrder::Ascending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::id.gt(marker));
                }
                query.order(dsl::id.asc())
            }
            dropshot::PaginationOrder::Descending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::id.lt(marker));
                }
                query.order(dsl::id.desc())
            }
        }
            */

    }
}
*/

pub struct DataStore {
    pool: Arc<Pool>,
}

impl DataStore {
    pub fn new(pool: Arc<Pool>) -> Self {
        DataStore { pool }
    }

    /// Create a project
    pub async fn project_create(
        &self,
        project: db::model::Project,
    ) -> CreateResult<db::model::Project> {
        use db::diesel_schema::project::dsl;
        let conn = self.pool.acquire_diesel().await?;
        diesel::insert_into(dsl::project)
            .values(&project)
            .get_result(&*conn)
            .map_err(|e| {
                Error::from_diesel_create(
                    e,
                    ResourceType::Project,
                    project.name.as_str(),
                )
            })
    }

    /// Lookup a project by name.
    pub async fn project_fetch(
        &self,
        name: &Name,
    ) -> LookupResult<db::model::Project> {
        use db::diesel_schema::project::dsl;
        let conn = self.pool.acquire_diesel().await?;
        dsl::project
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name))
            .first::<db::model::Project>(&*conn)
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
        let conn = self.pool.acquire_diesel().await?;
        let now = Utc::now();
        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name))
            .set(dsl::time_deleted.eq(now))
            .get_result::<db::model::Project>(&*conn)
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
        let conn = self.pool.acquire_diesel().await?;

        dsl::project
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name))
            .select(dsl::id)
            .get_result::<Uuid>(&*conn)
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Project,
                    LookupType::ByName(name.as_str().to_owned()),
                )
            })
    }

    // TODO: deleteme
    pub async fn projects_idk_call_paginate_for_me(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Project> {
        use db::diesel_schema::project::dsl;
        let conn = self.pool.acquire_diesel().await?;
        let query = paginated(dsl::project, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null());
        query.load::<db::model::Project>(&*conn).map_err(|e| {
            Error::from_diesel(
                e,
                ResourceType::Project,
                LookupType::Other("Listing All".to_string()),
            )
        })
    }

    pub async fn projects_list_by_id(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Project> {
        use db::diesel_schema::project::dsl;
        let conn = self.pool.acquire_diesel().await?;
        let mut query = dsl::project
            .filter(dsl::time_deleted.is_null())
            .limit(pagparams.limit.get().into())
            .into_boxed();
        let query = match pagparams.direction {
            dropshot::PaginationOrder::Ascending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::id.gt(marker));
                }
                query.order(dsl::id.asc())
            }
            dropshot::PaginationOrder::Descending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::id.lt(marker));
                }
                query.order(dsl::id.desc())
            }
        };
        query.load::<db::model::Project>(&*conn).map_err(|e| {
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
        let conn = self.pool.acquire_diesel().await?;
        let mut query = dsl::project
            .filter(dsl::time_deleted.is_null())
            .limit(pagparams.limit.get().into())
            .into_boxed();
        let query = match pagparams.direction {
            dropshot::PaginationOrder::Ascending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::name.gt(marker));
                }
                query.order(dsl::name.asc())
            }
            dropshot::PaginationOrder::Descending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::name.lt(marker));
                }
                query.order(dsl::name.desc())
            }
        };
        query.load::<db::model::Project>(&*conn).map_err(|e| {
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
        let conn = self.pool.acquire_diesel().await?;
        let updates: db::model::ProjectUpdate = update_params.clone().into();

        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::name.eq(name))
            .set(&updates)
            .get_result(&*conn)
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
        let conn = self.pool.acquire_diesel().await?;

        let instance = db::model::Instance::new(
            *instance_id,
            *project_id,
            params,
            runtime_initial.clone(),
        );
        let instance: db::model::Instance = diesel::insert_into(dsl::instance)
            .values(&instance)
            .on_conflict(dsl::id)
            .do_nothing()
            .get_result(&*conn)
            .map_err(|e| {
                Error::from_diesel_create(
                    e,
                    ResourceType::Instance,
                    instance.name.as_str(),
                )
            })?;

        bail_unless!(
            instance.instance_state.state()
                == &api::external::InstanceState::Creating,
            "newly-created Instance has unexpected state: {:?}",
            instance.instance_state
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
        let conn = self.pool.acquire_diesel().await?;

        let mut query = dsl::instance
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(project_id))
            .limit(pagparams.limit.get().into())
            .into_boxed();
        let query = match pagparams.direction {
            dropshot::PaginationOrder::Ascending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::name.gt(marker));
                }
                query.order(dsl::name.asc())
            }
            dropshot::PaginationOrder::Descending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::name.lt(marker));
                }
                query.order(dsl::name.desc())
            }
        };

        query.load::<db::model::Instance>(&*conn).map_err(|e| {
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
        let conn = self.pool.acquire_diesel().await?;

        dsl::instance
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id))
            .get_result(&*conn)
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
        let conn = self.pool.acquire_diesel().await?;

        dsl::instance
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(project_id))
            .filter(dsl::name.eq(instance_name))
            .get_result(&*conn)
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
        let conn = self.pool.acquire_diesel().await?;

        let updated = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime)
            .check_if_exists(*instance_id)
            .execute_and_check(&conn)
            .map(|r| match r {
                UpdateAndQueryResult::Updated => true,
                UpdateAndQueryResult::NotUpdatedButExists => false,
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
        use db::diesel_schema::instance::dsl;
        let conn = self.pool.acquire_diesel().await?;
        let now = Utc::now();

        let destroyed = db::model::InstanceState::new(
            api::external::InstanceState::Destroyed,
        );
        let stopped = db::model::InstanceState::new(
            api::external::InstanceState::Stopped,
        );
        let failed =
            db::model::InstanceState::new(api::external::InstanceState::Failed);

        let instance = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id))
            .filter(dsl::instance_state.eq_any(vec![stopped, failed]))
            .set((dsl::instance_state.eq(destroyed), dsl::time_deleted.eq(now)))
            .get_result::<db::model::Instance>(&*conn)
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Instance,
                    LookupType::ById(*instance_id),
                )
            })?;
        bail_unless!(instance.id == *instance_id);
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
        let conn = self.pool.acquire_diesel().await?;

        let mut query = dsl::disk
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::attach_instance_id.eq(instance_id))
            .limit(pagparams.limit.get().into())
            .into_boxed();

        // TODO: Can we make the pagparams stuff generic w.r.t. instance v disk?
        let query = match pagparams.direction {
            dropshot::PaginationOrder::Ascending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::name.gt(marker));
                }
                query.order(dsl::name.asc())
            }
            dropshot::PaginationOrder::Descending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::name.lt(marker));
                }
                query.order(dsl::name.desc())
            }
        };

        query.load::<db::model::Disk>(&*conn)
            .map(|disks| {
                disks.into_iter().map(|disk| {
                    // TODO: Maybe make an "attachment()" helper for disks?
                    db::model::DiskAttachment {
                        instance_id: disk.attach_instance_id.unwrap(),
                        disk_id: disk.id,
                        disk_name: disk.name.clone(),
                        disk_state: disk.state(),
                    }
                }).collect()
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
        let conn = self.pool.acquire_diesel().await?;

        let disk = db::model::Disk::new(
            *disk_id,
            *project_id,
            params.clone(),
            runtime_initial.clone(),
        );
        let disk: db::model::Disk = diesel::insert_into(dsl::disk)
            .values(&disk)
            .on_conflict(dsl::id)
            .do_nothing()
            .get_result(&*conn)
            .map_err(|e| {
                Error::from_diesel_create(
                    e,
                    ResourceType::Disk,
                    disk.name.as_str(),
                )
            })?;

        let runtime = disk.runtime();
        bail_unless!(
            runtime.state().state()
                == &api::external::DiskState::Creating,
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
        let conn = self.pool.acquire_diesel().await?;

        let mut query = dsl::disk
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(project_id))
            .limit(pagparams.limit.get().into())
            .into_boxed();
        let query = match pagparams.direction {
            dropshot::PaginationOrder::Ascending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::name.gt(marker));
                }
                query.order(dsl::name.asc())
            }
            dropshot::PaginationOrder::Descending => {
                if let Some(marker) = pagparams.marker {
                    query = query.filter(dsl::name.lt(marker));
                }
                query.order(dsl::name.desc())
            }
        };

        query.load::<db::model::Disk>(&*conn).map_err(|e| {
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
        let conn = self.pool.acquire_diesel().await?;

        let updated = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(disk_id))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime)
            .check_if_exists(*disk_id)
            .execute_and_check(&conn)
            .map(|r| match r {
                UpdateAndQueryResult::Updated => true,
                UpdateAndQueryResult::NotUpdatedButExists => false,
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
        let conn = self.pool.acquire_diesel().await?;

        dsl::disk
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(disk_id))
            .get_result(&*conn)
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
        let conn = self.pool.acquire_diesel().await?;

        dsl::disk
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(project_id))
            .filter(dsl::name.eq(disk_name))
            .get_result(&*conn)
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
        let conn = self.pool.acquire_diesel().await?;
        let now = Utc::now();

        let destroyed = api::external::DiskState::Destroyed.label();
        let detached = api::external::DiskState::Detached.label();
        let faulted = api::external::DiskState::Faulted.label();

        let updated = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(disk_id))
            .filter(dsl::disk_state.eq_any(vec![detached, faulted]))
            .set((dsl::disk_state.eq(destroyed), dsl::time_deleted.eq(now)))
            .check_if_exists(*disk_id)
            .execute_and_check(&*conn)
            .map(|r| match r {
                UpdateAndQueryResult::Updated => true,
                UpdateAndQueryResult::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                Error::from_diesel(
                    e,
                    ResourceType::Disk,
                    LookupType::ById(*disk_id),
                )
            })?;

        if updated {
            Ok(())
        } else {
            // TODO(smklein): Update CTE to get the existing row
            // so we can return it in this error message
            Err(Error::InvalidRequest {
                message: format!(
                    "disk cannot be deleted in current state",
                ),
            })
        }

        /*
        let client = self.pool.acquire().await?;
        let now = Utc::now();

        let mut values = SqlValueSet::new();
        values.set("time_deleted", &now);
        db::model::DiskState::new(api::external::DiskState::Destroyed)
            .sql_serialize(&mut values);

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
        */
    }

    // Create a record for a new Oximeter instance
    pub async fn oximeter_create(
        &self,
        info: &db::model::OximeterInfo,
    ) -> Result<(), Error> {
        let client = self.pool.acquire().await?;
        let mut values = SqlValueSet::new();
        info.sql_serialize(&mut values);
        sql_insert::<schema::Oximeter>(&client, &values).await
    }

    // Create a record for a new producer endpoint
    pub async fn producer_endpoint_create(
        &self,
        producer: &db::model::ProducerEndpoint,
    ) -> Result<(), Error> {
        let client = self.pool.acquire().await?;
        let mut values = SqlValueSet::new();
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
        let reg = db::model::OximeterAssignment { oximeter_id, producer_id };
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
    ) -> ListResult<db::model::Vpc> {
        let client = self.pool.acquire().await?;
        sql_fetch_page_by::<
            LookupByUniqueNameInProject,
            Vpc,
            <Vpc as Table>::Model,
        >(&client, (project_id,), pagparams, Vpc::ALL_COLUMNS)
        .await
    }

    pub async fn project_create_vpc(
        &self,
        vpc_id: &Uuid,
        project_id: &Uuid,
        params: &api::external::VpcCreateParams,
    ) -> Result<db::model::Vpc, Error> {
        let client = self.pool.acquire().await?;
        let mut values = SqlValueSet::new();
        let vpc = db::model::Vpc::new(*vpc_id, *project_id, params.clone());
        vpc.sql_serialize(&mut values);

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
        vpc_id: &Uuid,
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

        sql_update_precond::<Vpc, LookupByUniqueId>(
            &client,
            (),
            vpc_id,
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
    ) -> LookupResult<db::model::Vpc> {
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
