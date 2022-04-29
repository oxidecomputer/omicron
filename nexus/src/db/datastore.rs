// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Primary control plane interface for database read and write operations

// TODO-scalability review all queries for use of indexes (may need
// "time_deleted IS NOT NULL" conditions) Figure out how to automate this.
//
// TODO-design Better support for joins?
// The interfaces here often require that to do anything with an object, a
// caller must first look up the id and then do operations with the id.  For
// example, the caller of project_list_disks() always looks up the project to
// get the project_id, then lists disks having that project_id.  It's possible
// to implement this instead with a JOIN in the database so that we do it with
// one database round-trip.  We could use CTEs similar to what we do with
// conditional updates to distinguish the case where the project didn't exist
// vs. there were no disks in it.  This seems likely to be a fair bit more
// complicated to do safely and generally compared to what we have now.

use super::collection_insert::{
    AsyncInsertError, DatastoreCollection, SyncInsertError,
};
use super::error::diesel_pool_result_optional;
use super::identity::{Asset, Resource};
use super::pool::DbConnection;
use super::Pool;
use crate::authn;
use crate::authz;
use crate::authz::ApiResourceError;
use crate::context::OpContext;
use crate::db::fixed_data::role_assignment_builtin::BUILTIN_ROLE_ASSIGNMENTS;
use crate::db::fixed_data::role_builtin::BUILTIN_ROLES;
use crate::db::fixed_data::silo::{DEFAULT_SILO, SILO_ID};
use crate::db::lookup::LookupPath;
use crate::db::{
    self,
    error::{public_error_from_diesel_pool, ErrorHandler, TransactionError},
    model::{
        ConsoleSession, Dataset, DatasetKind, Disk, DiskRuntimeState,
        Generation, GlobalImage, IncompleteNetworkInterface, Instance,
        InstanceRuntimeState, Name, NetworkInterface, Organization,
        OrganizationUpdate, OximeterInfo, ProducerEndpoint, Project,
        ProjectUpdate, Region, RoleAssignmentBuiltin, RoleBuiltin, RouterRoute,
        RouterRouteUpdate, Silo, SiloUser, Sled, SshKey,
        UpdateAvailableArtifact, UserBuiltin, Volume, Vpc, VpcFirewallRule,
        VpcRouter, VpcRouterUpdate, VpcSubnet, VpcSubnetUpdate, VpcUpdate,
        Zpool,
    },
    pagination::paginated,
    pagination::paginated_multicolumn,
    subnet_allocation::FilterConflictingVpcSubnetRangesQuery,
    subnet_allocation::InsertNetworkInterfaceQuery,
    subnet_allocation::NetworkInterfaceError,
    subnet_allocation::SubnetError,
    update_and_check::{UpdateAndCheck, UpdateStatus},
};
use crate::external_api::params;
use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl, ConnectionManager};
use chrono::Utc;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::{QueryFragment, QueryId};
use diesel::query_dsl::methods::LoadQuery;
use diesel::upsert::excluded;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use omicron_common::api;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::{
    CreateResult, IdentityMetadataCreateParams,
};
use omicron_common::bail_unless;
use std::convert::{TryFrom, TryInto};
use std::net::Ipv6Addr;
use std::sync::Arc;
use uuid::Uuid;

// Number of unique datasets required to back a region.
// TODO: This should likely turn into a configuration option.
const REGION_REDUNDANCY_THRESHOLD: usize = 3;

// Represents a query that is ready to be executed.
//
// This helper trait lets the statement either be executed or explained.
//
// U: The output type of executing the statement.
trait RunnableQuery<U>:
    RunQueryDsl<DbConnection>
    + QueryFragment<Pg>
    + LoadQuery<DbConnection, U>
    + QueryId
{
}

impl<U, T> RunnableQuery<U> for T where
    T: RunQueryDsl<DbConnection>
        + QueryFragment<Pg>
        + LoadQuery<DbConnection, U>
        + QueryId
{
}

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
    fn pool(&self) -> &bb8::Pool<ConnectionManager<DbConnection>> {
        self.pool.pool()
    }

    pub(super) async fn pool_authorized(
        &self,
        opctx: &OpContext,
    ) -> Result<&bb8::Pool<ConnectionManager<DbConnection>>, Error> {
        opctx.authorize(authz::Action::Query, &authz::DATABASE).await?;
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
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Sled,
                        &sled.id().to_string(),
                    ),
                )
            })
    }

    pub async fn sled_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Sled> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::sled::dsl;
        paginated(dsl::sled, dsl::id, pagparams)
            .select(Sled::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Stores a new zpool in the database.
    pub async fn zpool_upsert(&self, zpool: Zpool) -> CreateResult<Zpool> {
        use db::schema::zpool::dsl;

        let sled_id = zpool.sled_id;
        Sled::insert_resource(
            sled_id,
            diesel::insert_into(dsl::zpool)
                .values(zpool.clone())
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::sled_id.eq(excluded(dsl::sled_id)),
                    dsl::total_size.eq(excluded(dsl::total_size)),
                )),
        )
        .insert_and_get_result_async(self.pool())
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Sled,
                lookup_type: LookupType::ById(sled_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Zpool,
                        &zpool.id().to_string(),
                    ),
                )
            }
        })
    }

    /// Stores a new dataset in the database.
    pub async fn dataset_upsert(
        &self,
        dataset: Dataset,
    ) -> CreateResult<Dataset> {
        use db::schema::dataset::dsl;

        let zpool_id = dataset.pool_id;
        Zpool::insert_resource(
            zpool_id,
            diesel::insert_into(dsl::dataset)
                .values(dataset.clone())
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::pool_id.eq(excluded(dsl::pool_id)),
                    dsl::ip.eq(excluded(dsl::ip)),
                    dsl::port.eq(excluded(dsl::port)),
                    dsl::kind.eq(excluded(dsl::kind)),
                )),
        )
        .insert_and_get_result_async(self.pool())
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Zpool,
                lookup_type: LookupType::ById(zpool_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Dataset,
                        &dataset.id().to_string(),
                    ),
                )
            }
        })
    }

    fn get_allocated_regions_query(
        volume_id: Uuid,
    ) -> impl RunnableQuery<(Dataset, Region)> {
        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::region::dsl as region_dsl;
        region_dsl::region
            .filter(region_dsl::volume_id.eq(volume_id))
            .inner_join(
                dataset_dsl::dataset
                    .on(region_dsl::dataset_id.eq(dataset_dsl::id)),
            )
            .select((Dataset::as_select(), Region::as_select()))
    }

    /// Gets allocated regions for a disk, and the datasets to which those
    /// regions belong.
    ///
    /// Note that this function does not validate liveness of the Disk, so it
    /// may be used in a context where the disk is being deleted.
    pub async fn get_allocated_regions(
        &self,
        volume_id: Uuid,
    ) -> Result<Vec<(Dataset, Region)>, Error> {
        Self::get_allocated_regions_query(volume_id)
            .get_results_async::<(Dataset, Region)>(self.pool())
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    fn get_allocatable_datasets_query() -> impl RunnableQuery<Dataset> {
        use db::schema::dataset::dsl;

        dsl::dataset
            // We look for valid datasets (non-deleted crucible datasets).
            .filter(dsl::size_used.is_not_null())
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::kind.eq(DatasetKind::Crucible))
            .order(dsl::size_used.asc())
            // TODO: We admittedly don't actually *fail* any request for
            // running out of space - we try to send the request down to
            // crucible agents, and expect them to fail on our behalf in
            // out-of-storage conditions. This should undoubtedly be
            // handled more explicitly.
            .select(Dataset::as_select())
            .limit(REGION_REDUNDANCY_THRESHOLD.try_into().unwrap())
    }

    async fn get_block_size_from_disk_create(
        &self,
        opctx: &OpContext,
        disk_create: &params::DiskCreate,
    ) -> Result<db::model::BlockSize, Error> {
        match &disk_create.disk_source {
            params::DiskSource::Blank { block_size } => {
                Ok(db::model::BlockSize::try_from(*block_size)
                    .map_err(|e| Error::invalid_request(&e.to_string()))?)
            }
            params::DiskSource::Snapshot { snapshot_id: _ } => {
                // Until we implement snapshots, do not allow disks to be
                // created from a snapshot.
                return Err(Error::InvalidValue {
                    label: String::from("snapshot"),
                    message: String::from("snapshots are not yet supported"),
                });
            }
            params::DiskSource::Image { image_id: _ } => {
                // Until we implement project images, do not allow disks to be
                // created from a project image.
                return Err(Error::InvalidValue {
                    label: String::from("image"),
                    message: String::from(
                        "project image are not yet supported",
                    ),
                });
            }
            params::DiskSource::GlobalImage { image_id } => {
                let (.., db_global_image) = LookupPath::new(opctx, &self)
                    .global_image_id(*image_id)
                    .fetch()
                    .await?;

                Ok(db_global_image.block_size)
            }
        }
    }

    /// Idempotently allocates enough regions to back a disk.
    ///
    /// Returns the allocated regions, as well as the datasets to which they
    /// belong.
    pub async fn region_allocate(
        &self,
        opctx: &OpContext,
        volume_id: Uuid,
        params: &params::DiskCreate,
    ) -> Result<Vec<(Dataset, Region)>, Error> {
        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::region::dsl as region_dsl;

        // ALLOCATION POLICY
        //
        // NOTE: This policy can - and should! - be changed.
        //
        // See https://rfd.shared.oxide.computer/rfd/0205 for a more
        // complete discussion.
        //
        // It is currently acting as a placeholder, showing a feasible
        // interaction between datasets and regions.
        //
        // This policy allocates regions to distinct Crucible datasets,
        // favoring datasets with the smallest existing (summed) region
        // sizes. Basically, "pick the datasets with the smallest load first".
        //
        // Longer-term, we should consider:
        // - Storage size + remaining free space
        // - Sled placement of datasets
        // - What sort of loads we'd like to create (even split across all disks
        // may not be preferable, especially if maintenance is expected)
        #[derive(Debug, thiserror::Error)]
        enum RegionAllocateError {
            #[error("Not enough datasets for replicated allocation: {0}")]
            NotEnoughDatasets(usize),
        }
        type TxnError = TransactionError<RegionAllocateError>;

        let params: params::DiskCreate = params.clone();
        let block_size =
            self.get_block_size_from_disk_create(opctx, &params).await?;
        let blocks_per_extent =
            params.extent_size() / block_size.to_bytes() as i64;

        self.pool()
            .transaction(move |conn| {
                // First, for idempotency, check if regions are already
                // allocated to this disk.
                //
                // If they are, return those regions and the associated
                // datasets.
                let datasets_and_regions =
                    Self::get_allocated_regions_query(volume_id)
                        .get_results::<(Dataset, Region)>(conn)?;
                if !datasets_and_regions.is_empty() {
                    return Ok(datasets_and_regions);
                }

                let mut datasets: Vec<Dataset> =
                    Self::get_allocatable_datasets_query()
                        .get_results::<Dataset>(conn)?;

                if datasets.len() < REGION_REDUNDANCY_THRESHOLD {
                    return Err(TxnError::CustomError(
                        RegionAllocateError::NotEnoughDatasets(datasets.len()),
                    ));
                }

                // Create identical regions on each of the following datasets.
                let source_datasets =
                    &mut datasets[0..REGION_REDUNDANCY_THRESHOLD];
                let regions: Vec<Region> = source_datasets
                    .iter()
                    .map(|dataset| {
                        Region::new(
                            dataset.id(),
                            volume_id,
                            block_size.into(),
                            blocks_per_extent,
                            params.extent_count(),
                        )
                    })
                    .collect();
                let regions = diesel::insert_into(region_dsl::region)
                    .values(regions)
                    .returning(Region::as_returning())
                    .get_results(conn)?;

                // Update the tallied sizes in the source datasets containing
                // those regions.
                let region_size = i64::from(block_size.to_bytes())
                    * blocks_per_extent
                    * params.extent_count();
                for dataset in source_datasets.iter_mut() {
                    dataset.size_used =
                        dataset.size_used.map(|v| v + region_size);
                }

                let dataset_ids: Vec<Uuid> =
                    source_datasets.iter().map(|ds| ds.id()).collect();
                diesel::update(dataset_dsl::dataset)
                    .filter(dataset_dsl::id.eq_any(dataset_ids))
                    .set(
                        dataset_dsl::size_used
                            .eq(dataset_dsl::size_used + region_size),
                    )
                    .execute(conn)?;

                // Return the regions with the datasets to which they were allocated.
                Ok(source_datasets
                    .into_iter()
                    .map(|d| d.clone())
                    .zip(regions)
                    .collect())
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(
                    RegionAllocateError::NotEnoughDatasets(_),
                ) => Error::unavail("Not enough datasets to allocate disks"),
                _ => {
                    Error::internal_error(&format!("Transaction error: {}", e))
                }
            })
    }

    /// Deletes all regions backing a disk.
    ///
    /// Also updates the storage usage on their corresponding datasets.
    pub async fn regions_hard_delete(&self, volume_id: Uuid) -> DeleteResult {
        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::region::dsl as region_dsl;

        // Remove the regions, collecting datasets they're from.
        let (dataset_id, size) = diesel::delete(region_dsl::region)
            .filter(region_dsl::volume_id.eq(volume_id))
            .returning((
                region_dsl::dataset_id,
                region_dsl::block_size
                    * region_dsl::blocks_per_extent
                    * region_dsl::extent_count,
            ))
            .get_result_async::<(Uuid, i64)>(self.pool())
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error deleting regions: {:?}",
                    e
                ))
            })?;

        // Update those datasets to which the regions belonged.
        diesel::update(dataset_dsl::dataset)
            .filter(dataset_dsl::id.eq(dataset_id))
            .set(dataset_dsl::size_used.eq(dataset_dsl::size_used - size))
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error updating dataset space: {:?}",
                    e
                ))
            })?;

        Ok(())
    }

    pub async fn volume_create(&self, volume: Volume) -> CreateResult<Volume> {
        use db::schema::volume::dsl;

        diesel::insert_into(dsl::volume)
            .values(volume.clone())
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(Volume::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Volume,
                        volume.id().to_string().as_str(),
                    ),
                )
            })
    }

    pub async fn volume_delete(&self, volume_id: Uuid) -> DeleteResult {
        use db::schema::volume::dsl;

        let now = Utc::now();
        diesel::update(dsl::volume)
            .filter(dsl::id.eq(volume_id))
            .set(dsl::time_deleted.eq(now))
            .check_if_exists::<Volume>(volume_id)
            .execute_and_check(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Volume,
                        LookupType::ById(volume_id),
                    ),
                )
            })?;
        Ok(())
    }

    pub async fn volume_get(&self, volume_id: Uuid) -> LookupResult<Volume> {
        use db::schema::volume::dsl;

        dsl::volume
            .filter(dsl::id.eq(volume_id))
            .select(Volume::as_select())
            .get_result_async(self.pool())
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Create a organization
    pub async fn organization_create(
        &self,
        opctx: &OpContext,
        organization: &params::OrganizationCreate,
    ) -> CreateResult<Organization> {
        let authz_silo = opctx.authn.silo_required()?;
        opctx.authorize(authz::Action::CreateChild, &authz_silo).await?;

        use db::schema::organization::dsl;
        let silo_id = authz_silo.id();
        let organization = Organization::new(organization.clone(), silo_id);
        let name = organization.name().as_str().to_string();

        Silo::insert_resource(
            silo_id,
            diesel::insert_into(dsl::organization).values(organization),
        )
        .insert_and_get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::InternalError {
                internal_message: format!(
                    "attempting to create an \
                    organization under non-existent silo {}",
                    silo_id
                ),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::Organization, &name),
                )
            }
        })
    }

    /// Delete a organization
    pub async fn organization_delete(
        &self,
        opctx: &OpContext,
        authz_org: &authz::Organization,
        db_org: &db::model::Organization,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_org).await?;

        use db::schema::organization::dsl;
        use db::schema::project;

        // Make sure there are no projects present within this organization.
        let project_found = diesel_pool_result_optional(
            project::dsl::project
                .filter(project::dsl::organization_id.eq(authz_org.id()))
                .filter(project::dsl::time_deleted.is_null())
                .select(project::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        if project_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "organization to be deleted contains a project"
                    .to_string(),
            });
        }

        let now = Utc::now();
        let updated_rows = diesel::update(dsl::organization)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_org.id()))
            .filter(dsl::rcgen.eq(db_org.rcgen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_org),
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

    pub async fn organizations_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Organization> {
        let authz_silo = opctx.authn.silo_required()?;
        opctx.authorize(authz::Action::ListChildren, &authz_silo).await?;

        use db::schema::organization::dsl;
        paginated(dsl::organization, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .select(Organization::as_select())
            .load_async::<Organization>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn organizations_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Organization> {
        let authz_silo = opctx.authn.silo_required()?;
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::organization::dsl;
        paginated(dsl::organization, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .select(Organization::as_select())
            .load_async::<Organization>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Updates a organization by name (clobbering update -- no etag)
    pub async fn organization_update(
        &self,
        opctx: &OpContext,
        authz_org: &authz::Organization,
        updates: OrganizationUpdate,
    ) -> UpdateResult<Organization> {
        use db::schema::organization::dsl;

        opctx.authorize(authz::Action::Modify, authz_org).await?;
        diesel::update(dsl::organization)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_org.id()))
            .set(updates)
            .returning(Organization::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_org),
                )
            })
    }

    /// Create a project
    pub async fn project_create(
        &self,
        opctx: &OpContext,
        org: &authz::Organization,
        project: Project,
    ) -> CreateResult<Project> {
        use db::schema::project::dsl;

        opctx.authorize(authz::Action::CreateChild, org).await?;

        let name = project.name().as_str().to_string();
        let organization_id = project.organization_id;
        Organization::insert_resource(
            organization_id,
            diesel::insert_into(dsl::project).values(project),
        )
        .insert_and_get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Organization,
                lookup_type: LookupType::ById(organization_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::Project, &name),
                )
            }
        })
    }

    /// Delete a project
    // TODO-correctness This needs to check whether there are any resources that
    // depend on the Project (Disks, Instances).  We can do this with a
    // generation counter that gets bumped when these resources are created.
    pub async fn project_delete(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_project).await?;

        use db::schema::project::dsl;

        let now = Utc::now();
        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_project.id()))
            .set(dsl::time_deleted.eq(now))
            .returning(Project::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_project),
                )
            })?;
        Ok(())
    }

    pub async fn projects_list_by_id(
        &self,
        opctx: &OpContext,
        authz_org: &authz::Organization,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Project> {
        use db::schema::project::dsl;

        opctx.authorize(authz::Action::ListChildren, authz_org).await?;

        paginated(dsl::project, dsl::id, pagparams)
            .filter(dsl::organization_id.eq(authz_org.id()))
            .filter(dsl::time_deleted.is_null())
            .select(Project::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn projects_list_by_name(
        &self,
        opctx: &OpContext,
        authz_org: &authz::Organization,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Project> {
        use db::schema::project::dsl;

        opctx.authorize(authz::Action::ListChildren, authz_org).await?;

        paginated(dsl::project, dsl::name, &pagparams)
            .filter(dsl::organization_id.eq(authz_org.id()))
            .filter(dsl::time_deleted.is_null())
            .select(Project::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Updates a project (clobbering update -- no etag)
    pub async fn project_update(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        updates: ProjectUpdate,
    ) -> UpdateResult<Project> {
        opctx.authorize(authz::Action::Modify, authz_project).await?;

        use db::schema::project::dsl;
        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_project.id()))
            .set(updates)
            .returning(Project::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_project),
                )
            })
    }

    // Instances

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
    // TODO-design Given that this is really oriented towards the saga
    // interface, one wonders if it's even worth having an abstraction here, or
    // if sagas shouldn't directly work with the database here (i.e., just do
    // what this function does under the hood).
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
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Instance,
                        name.as_str(),
                    ),
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
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Instance> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use db::schema::instance::dsl;
        paginated(dsl::instance, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(authz_project.id()))
            .select(Instance::as_select())
            .load_async::<Instance>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Fetches information about an Instance that the caller has previously
    /// fetched
    ///
    /// See disk_refetch().
    pub async fn instance_refetch(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> LookupResult<Instance> {
        let (.., db_instance) = LookupPath::new(opctx, self)
            .instance_id(authz_instance.id())
            .fetch()
            .await
            .map_err(|e| match e {
                // Use the "not found" message of the authz object we were
                // given, which will reflect however the caller originally
                // looked it up.
                Error::ObjectNotFound { .. } => authz_instance.not_found(),
                e => e,
            })?;
        Ok(db_instance)
    }

    // TODO-design It's tempting to return the updated state of the Instance
    // here because it's convenient for consumers and by using a RETURNING
    // clause, we could ensure that the "update" and "fetch" are atomic.
    // But in the unusual case that we _don't_ update the row because our
    // update is older than the one in the database, we would have to fetch
    // the current state explicitly.  For now, we'll just require consumers
    // to explicitly fetch the state if they want that.
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
            .filter(
                dsl::migration_id
                    .is_null()
                    .or(dsl::target_propolis_id.eq(new_runtime.propolis_uuid)),
            )
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
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Instance,
                        LookupType::ById(*instance_id),
                    ),
                )
            })?;

        Ok(updated)
    }

    pub async fn project_delete_instance(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_instance).await?;

        // This is subject to change, but for now we're going to say that an
        // instance must be "stopped" or "failed" in order to delete it.  The
        // delete operation sets "time_deleted" (just like with other objects)
        // and also sets the state to "destroyed".  By virtue of being
        // "stopped", we assume there are no dependencies on this instance
        // (e.g., disk attachments).  If that changes, we'll want to check for
        // such dependencies here.
        use api::external::InstanceState as ApiInstanceState;
        use db::model::InstanceState as DbInstanceState;
        use db::schema::instance::dsl;

        let now = Utc::now();

        let destroyed = DbInstanceState::new(ApiInstanceState::Destroyed);
        let stopped = DbInstanceState::new(ApiInstanceState::Stopped);
        let failed = DbInstanceState::new(ApiInstanceState::Failed);

        let instance_id = authz_instance.id();
        let result = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id))
            .filter(dsl::state.eq_any(vec![stopped, failed]))
            .set((dsl::state.eq(destroyed), dsl::time_deleted.eq(now)))
            .check_if_exists::<Instance>(instance_id)
            .execute_and_check(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_instance),
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

    // Disks

    /// List disks associated with a given instance.
    pub async fn instance_list_disks(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Disk> {
        use db::schema::disk::dsl;

        opctx.authorize(authz::Action::ListChildren, authz_instance).await?;

        paginated(dsl::disk, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::attach_instance_id.eq(authz_instance.id()))
            .select(Disk::as_select())
            .load_async::<Disk>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::Disk, name.as_str()),
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
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Disk> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use db::schema::disk::dsl;
        paginated(dsl::disk, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(authz_project.id()))
            .select(Disk::as_select())
            .load_async::<Disk>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn disk_update_runtime(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
        new_runtime: &DiskRuntimeState,
    ) -> Result<bool, Error> {
        // TODO-security This permission might be overloaded here.  The way disk
        // runtime updates work is that the caller in Nexus first updates the
        // Sled Agent to make a change, then updates to the database to reflect
        // that change.  So by the time we get here, we better have already done
        // an authz check, or we will have already made some unauthorized change
        // to the system!  At the same time, we don't want just anybody to be
        // able to modify the database state.  So we _do_ still want an authz
        // check here.  Arguably it's for a different kind of action, but it
        // doesn't seem that useful to split it out right now.
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let disk_id = authz_disk.id();
        use db::schema::disk::dsl;
        let updated = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(disk_id))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime.clone())
            .check_if_exists::<Disk>(disk_id)
            .execute_and_check(self.pool())
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_disk),
                )
            })?;

        Ok(updated)
    }

    /// Fetches information about a Disk that the caller has previously fetched
    ///
    /// The only difference between this function and a new fetch by id is that
    /// this function preserves the `authz_disk` that you started with -- which
    /// keeps track of how you looked it up.  So if you looked it up by name,
    /// the authz you get back will reflect that, whereas if you did a fresh
    /// lookup by id, it wouldn't.
    /// TODO-cleanup this could be provided by the Lookup API for any resource
    pub async fn disk_refetch(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
    ) -> LookupResult<Disk> {
        let (.., db_disk) = LookupPath::new(opctx, self)
            .disk_id(authz_disk.id())
            .fetch()
            .await
            .map_err(|e| match e {
                // Use the "not found" message of the authz object we were
                // given, which will reflect however the caller originally
                // looked it up.
                Error::ObjectNotFound { .. } => authz_disk.not_found(),
                e => e,
            })?;
        Ok(db_disk)
    }

    /// Updates a disk record to indicate it has been deleted.
    ///
    /// Returns the volume ID of associated with the deleted disk.
    ///
    /// Does not attempt to modify any resources (e.g. regions) which may
    /// belong to the disk.
    // TODO: Delete me (this function, not the disk!), ensure all datastore
    // access is auth-checked.
    //
    // Here's the deal: We have auth checks on access to the database - at the
    // time of writing this comment, only a subset of access is protected, and
    // "Delete Disk" is actually one of the first targets of this auth check.
    //
    // However, there are contexts where we want to delete disks *outside* of
    // calling the HTTP API-layer "delete disk" endpoint. As one example, during
    // the "undo" part of the disk creation saga, we want to allow users to
    // delete the disk they (partially) created.
    //
    // This gets a little tricky mapping back to user permissions - a user
    // SHOULD be able to create a disk with the "create" permission, without the
    // "delete" permission. To still make the call internally, we'd basically
    // need to manufacture a token that identifies the ability to "create a
    // disk, or delete a very specific disk with ID = ...".
    pub async fn project_delete_disk_no_auth(
        &self,
        disk_id: &Uuid,
    ) -> Result<Uuid, Error> {
        use db::schema::disk::dsl;
        let pool = self.pool();
        let now = Utc::now();

        let ok_to_delete_states = vec![
            api::external::DiskState::Detached,
            api::external::DiskState::Faulted,
            api::external::DiskState::Creating,
        ];

        let ok_to_delete_state_labels: Vec<_> =
            ok_to_delete_states.iter().map(|s| s.label()).collect();
        let destroyed = api::external::DiskState::Destroyed.label();

        let result = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*disk_id))
            .filter(dsl::disk_state.eq_any(ok_to_delete_state_labels))
            .filter(dsl::attach_instance_id.is_null())
            .set((dsl::disk_state.eq(destroyed), dsl::time_deleted.eq(now)))
            .check_if_exists::<Disk>(*disk_id)
            .execute_and_check(pool)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Disk,
                        LookupType::ById(*disk_id),
                    ),
                )
            })?;

        match result.status {
            UpdateStatus::Updated => Ok(result.found.volume_id),
            UpdateStatus::NotUpdatedButExists => {
                let disk = result.found;
                let disk_state = disk.state();
                if disk.time_deleted().is_some()
                    && disk_state.state()
                        == &api::external::DiskState::Destroyed
                {
                    // To maintain idempotency, if the disk has already been
                    // destroyed, don't throw an error.
                    return Ok(disk.volume_id);
                } else if !ok_to_delete_states.contains(disk_state.state()) {
                    return Err(Error::InvalidRequest {
                        message: format!(
                            "disk cannot be deleted in state \"{}\"",
                            disk.runtime_state.disk_state
                        ),
                    });
                } else if disk_state.is_attached() {
                    return Err(Error::InvalidRequest {
                        message: String::from("disk is attached"),
                    });
                } else {
                    // NOTE: This is a "catch-all" error case, more specific
                    // errors should be preferred as they're more actionable.
                    return Err(Error::InternalError {
                        internal_message: String::from(
                            "disk exists, but cannot be deleted",
                        ),
                    });
                }
            }
        }
    }

    // Network interfaces

    /// Create a network interface attached to the provided instance.
    pub async fn instance_create_network_interface(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        authz_instance: &authz::Instance,
        interface: IncompleteNetworkInterface,
    ) -> Result<NetworkInterface, NetworkInterfaceError> {
        opctx
            .authorize(authz::Action::CreateChild, authz_instance)
            .await
            .map_err(NetworkInterfaceError::External)?;
        opctx
            .authorize(authz::Action::CreateChild, authz_subnet)
            .await
            .map_err(NetworkInterfaceError::External)?;
        self.instance_create_network_interface_raw(&opctx, interface).await
    }

    pub(super) async fn instance_create_network_interface_raw(
        &self,
        opctx: &OpContext,
        interface: IncompleteNetworkInterface,
    ) -> Result<NetworkInterface, NetworkInterfaceError> {
        use db::schema::network_interface::dsl;
        let query = InsertNetworkInterfaceQuery {
            interface: interface.clone(),
            now: Utc::now(),
        };
        diesel::insert_into(dsl::network_interface)
            .values(query)
            .returning(NetworkInterface::as_returning())
            .get_result_async(
                self.pool_authorized(opctx)
                    .await
                    .map_err(NetworkInterfaceError::External)?,
            )
            .await
            .map_err(|e| NetworkInterfaceError::from_pool(e, &interface))
    }

    /// Delete all network interfaces attached to the given instance.
    // NOTE: This is mostly useful in the context of sagas, but might be helpful
    // in other situations, such as moving an instance between VPC Subnets.
    pub async fn instance_delete_all_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;

        use db::schema::network_interface::dsl;
        let now = Utc::now();
        diesel::update(dsl::network_interface)
            .filter(dsl::instance_id.eq(authz_instance.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_instance),
                )
            })?;
        Ok(())
    }

    /// Delete a `NetworkInterface` attached to a provided instance.
    pub async fn instance_delete_network_interface(
        &self,
        opctx: &OpContext,
        authz_interface: &authz::NetworkInterface,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_interface).await?;

        use db::schema::network_interface::dsl;
        let now = Utc::now();
        let interface_id = authz_interface.id();
        diesel::update(dsl::network_interface)
            .filter(dsl::id.eq(interface_id))
            .filter(dsl::time_deleted.is_null())
            .set((dsl::time_deleted.eq(now),))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_interface),
                )
            })?;
        Ok(())
    }

    /// List network interfaces associated with a given instance.
    pub async fn instance_list_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<NetworkInterface> {
        opctx.authorize(authz::Action::ListChildren, authz_instance).await?;

        use db::schema::network_interface::dsl;
        paginated(dsl::network_interface, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::instance_id.eq(authz_instance.id()))
            .select(NetworkInterface::as_select())
            .load_async::<NetworkInterface>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Oximeter,
                        "Oximeter Info",
                    ),
                )
            })?;
        Ok(())
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
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::MetricProducer,
                        "Producer Endpoint",
                    ),
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
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::MetricProducer,
                        "By Oximeter ID",
                    ),
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
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::SagaDbg, &name),
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
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::SagaDbg, "Saga Event"),
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
            .set(dsl::saga_state.eq(db::saga_types::SagaCachedState(new_state)))
            .check_if_exists::<db::saga_types::Saga>(saga_id)
            .execute_and_check(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::SagaDbg,
                        LookupType::ById(saga_id.0.into()),
                    ),
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
            .filter(dsl::saga_state.ne(db::saga_types::SagaCachedState(
                steno::SagaCachedState::Done,
            )))
            .filter(dsl::current_sec.eq(*sec_id))
            .load_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::SagaDbg,
                        LookupType::ById(sec_id.0),
                    ),
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
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::SagaDbg,
                        LookupType::ById(id.0 .0),
                    ),
                )
            })?
            .into_iter()
            .map(|db_event| steno::SagaNodeEvent::try_from(db_event))
            .collect::<Result<_, Error>>()
    }

    // VPCs

    pub async fn project_list_vpcs(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Vpc> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use db::schema::vpc::dsl;
        paginated(dsl::vpc, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(authz_project.id()))
            .select(Vpc::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn project_create_vpc(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        vpc: Vpc,
    ) -> Result<(authz::Vpc, Vpc), Error> {
        use db::schema::vpc::dsl;

        assert_eq!(authz_project.id(), vpc.project_id);
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        // TODO-correctness Shouldn't this use "insert_resource"?
        let name = vpc.name().clone();
        let vpc = diesel::insert_into(dsl::vpc)
            .values(vpc)
            .returning(Vpc::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::Vpc, name.as_str()),
                )
            })?;
        Ok((
            authz::Vpc::new(
                authz_project.clone(),
                vpc.id(),
                LookupType::ByName(vpc.name().to_string()),
            ),
            vpc,
        ))
    }

    pub async fn project_update_vpc(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        updates: VpcUpdate,
    ) -> UpdateResult<Vpc> {
        opctx.authorize(authz::Action::Modify, authz_vpc).await?;

        use db::schema::vpc::dsl;
        diesel::update(dsl::vpc)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_vpc.id()))
            .set(updates)
            .returning(Vpc::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_vpc),
                )
            })
    }

    pub async fn project_delete_vpc(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_vpc).await?;

        use db::schema::vpc::dsl;

        // Note that we don't ensure the firewall rules are empty here, because
        // we allow deleting VPCs with firewall rules present. Inserting new
        // rules is serialized with respect to the deletion by the row lock
        // associated with the VPC row, since we use the collection insert CTE
        // pattern to add firewall rules.

        let now = Utc::now();
        diesel::update(dsl::vpc)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_vpc.id()))
            .set(dsl::time_deleted.eq(now))
            .returning(Vpc::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_vpc),
                )
            })?;
        Ok(())
    }

    pub async fn vpc_list_firewall_rules(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
    ) -> ListResultVec<VpcFirewallRule> {
        // Firewall rules are modeled in the API as a single resource under the
        // Vpc (rather than individual child resources with their own CRUD
        // endpoints).  You cannot look them up individually, create them,
        // remove them, or update them.  You can only modify the whole set.  So
        // for authz, we treat them as part of the Vpc itself.
        opctx.authorize(authz::Action::Read, authz_vpc).await?;
        use db::schema::vpc_firewall_rule::dsl;

        dsl::vpc_firewall_rule
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .order(dsl::name.asc())
            .select(VpcFirewallRule::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn vpc_delete_all_firewall_rules(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, authz_vpc).await?;
        use db::schema::vpc_firewall_rule::dsl;

        let now = Utc::now();
        // TODO-performance: Paginate this update to avoid long queries
        diesel::update(dsl::vpc_firewall_rule)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_vpc),
                )
            })?;
        Ok(())
    }

    /// Replace all firewall rules with the given rules
    pub async fn vpc_update_firewall_rules(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        mut rules: Vec<VpcFirewallRule>,
    ) -> UpdateResult<Vec<VpcFirewallRule>> {
        opctx.authorize(authz::Action::Modify, authz_vpc).await?;
        for r in &rules {
            assert_eq!(r.vpc_id, authz_vpc.id());
        }

        // Sort the rules in the same order that we would return them when
        // listing them.  This is because we're going to use RETURNING to return
        // the inserted rows from the database and we want them to come back in
        // the same order that we would normally list them.
        rules.sort_by_key(|r| r.name().to_string());

        use db::schema::vpc_firewall_rule::dsl;

        let now = Utc::now();
        let delete_old_query = diesel::update(dsl::vpc_firewall_rule)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .set(dsl::time_deleted.eq(now));

        let insert_new_query = Vpc::insert_resource(
            authz_vpc.id(),
            diesel::insert_into(dsl::vpc_firewall_rule).values(rules),
        );

        #[derive(Debug)]
        enum FirewallUpdateError {
            CollectionNotFound,
        }
        type TxnError = TransactionError<FirewallUpdateError>;

        // TODO-scalability: Ideally this would be a CTE so we don't need to
        // hold a transaction open across multiple roundtrips from the database,
        // but for now we're using a transaction due to the severely decreased
        // legibility of CTEs via diesel right now.
        self.pool_authorized(opctx)
            .await?
            .transaction(move |conn| {
                delete_old_query.execute(conn)?;

                // The generation count update on the vpc table row will take a
                // write lock on the row, ensuring that the vpc was not deleted
                // concurently.
                insert_new_query.insert_and_get_results(conn).map_err(|e| {
                    match e {
                        SyncInsertError::CollectionNotFound => {
                            TxnError::CustomError(
                                FirewallUpdateError::CollectionNotFound,
                            )
                        }
                        SyncInsertError::DatabaseError(e) => e.into(),
                    }
                })
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(
                    FirewallUpdateError::CollectionNotFound,
                ) => Error::not_found_by_id(ResourceType::Vpc, &authz_vpc.id()),
                TxnError::Pool(e) => public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_vpc),
                ),
            })
    }

    pub async fn vpc_list_subnets(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<VpcSubnet> {
        opctx.authorize(authz::Action::ListChildren, authz_vpc).await?;

        use db::schema::vpc_subnet::dsl;
        paginated(dsl::vpc_subnet, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .select(VpcSubnet::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Insert a VPC Subnet, checking for unique IP address ranges.
    pub async fn vpc_create_subnet(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        subnet: VpcSubnet,
    ) -> Result<VpcSubnet, SubnetError> {
        opctx
            .authorize(authz::Action::CreateChild, authz_vpc)
            .await
            .map_err(SubnetError::External)?;
        assert_eq!(authz_vpc.id(), subnet.vpc_id);

        self.vpc_create_subnet_raw(subnet).await
    }

    pub(super) async fn vpc_create_subnet_raw(
        &self,
        subnet: VpcSubnet,
    ) -> Result<VpcSubnet, SubnetError> {
        use db::schema::vpc_subnet::dsl;
        let values = FilterConflictingVpcSubnetRangesQuery(subnet.clone());
        diesel::insert_into(dsl::vpc_subnet)
            .values(values)
            .returning(VpcSubnet::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| SubnetError::from_pool(e, &subnet))
    }

    pub async fn vpc_delete_subnet(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_subnet).await?;

        use db::schema::vpc_subnet::dsl;
        let now = Utc::now();
        diesel::update(dsl::vpc_subnet)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_subnet.id()))
            .set(dsl::time_deleted.eq(now))
            .returning(VpcSubnet::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_subnet),
                )
            })?;
        Ok(())
    }

    pub async fn vpc_update_subnet(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        updates: VpcSubnetUpdate,
    ) -> UpdateResult<VpcSubnet> {
        opctx.authorize(authz::Action::Modify, authz_subnet).await?;

        use db::schema::vpc_subnet::dsl;
        diesel::update(dsl::vpc_subnet)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_subnet.id()))
            .set(updates)
            .returning(VpcSubnet::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_subnet),
                )
            })
    }

    pub async fn subnet_list_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_subnet: &authz::VpcSubnet,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<NetworkInterface> {
        opctx.authorize(authz::Action::ListChildren, authz_subnet).await?;

        use db::schema::network_interface::dsl;
        paginated(dsl::network_interface, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::subnet_id.eq(authz_subnet.id()))
            .select(NetworkInterface::as_select())
            .load_async::<db::model::NetworkInterface>(
                self.pool_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn vpc_list_routers(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<VpcRouter> {
        opctx.authorize(authz::Action::ListChildren, authz_vpc).await?;

        use db::schema::vpc_router::dsl;
        paginated(dsl::vpc_router, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_id.eq(authz_vpc.id()))
            .select(VpcRouter::as_select())
            .load_async::<db::model::VpcRouter>(
                self.pool_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn vpc_create_router(
        &self,
        opctx: &OpContext,
        authz_vpc: &authz::Vpc,
        router: VpcRouter,
    ) -> CreateResult<(authz::VpcRouter, VpcRouter)> {
        opctx.authorize(authz::Action::CreateChild, authz_vpc).await?;

        use db::schema::vpc_router::dsl;
        let name = router.name().clone();
        let router = diesel::insert_into(dsl::vpc_router)
            .values(router)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(VpcRouter::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::VpcRouter,
                        name.as_str(),
                    ),
                )
            })?;
        Ok((
            authz::VpcRouter::new(
                authz_vpc.clone(),
                router.id(),
                LookupType::ById(router.id()),
            ),
            router,
        ))
    }

    pub async fn vpc_delete_router(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_router).await?;

        use db::schema::vpc_router::dsl;
        let now = Utc::now();
        diesel::update(dsl::vpc_router)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_router.id()))
            .set(dsl::time_deleted.eq(now))
            .returning(VpcRouter::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_router),
                )
            })?;
        Ok(())
    }

    pub async fn vpc_update_router(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
        updates: VpcRouterUpdate,
    ) -> UpdateResult<VpcRouter> {
        opctx.authorize(authz::Action::Modify, authz_router).await?;

        use db::schema::vpc_router::dsl;
        diesel::update(dsl::vpc_router)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_router.id()))
            .set(updates)
            .returning(VpcRouter::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_router),
                )
            })
    }

    pub async fn router_list_routes(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<RouterRoute> {
        opctx.authorize(authz::Action::ListChildren, authz_router).await?;

        use db::schema::router_route::dsl;
        paginated(dsl::router_route, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::vpc_router_id.eq(authz_router.id()))
            .select(RouterRoute::as_select())
            .load_async::<db::model::RouterRoute>(
                self.pool_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn router_create_route(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
        route: RouterRoute,
    ) -> CreateResult<RouterRoute> {
        assert_eq!(authz_router.id(), route.vpc_router_id);
        opctx.authorize(authz::Action::CreateChild, authz_router).await?;

        use db::schema::router_route::dsl;
        let router_id = route.vpc_router_id;
        let name = route.name().clone();

        VpcRouter::insert_resource(
            router_id,
            diesel::insert_into(dsl::router_route).values(route),
        )
        .insert_and_get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::VpcRouter,
                lookup_type: LookupType::ById(router_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::RouterRoute,
                        name.as_str(),
                    ),
                )
            }
        })
    }

    pub async fn router_delete_route(
        &self,
        opctx: &OpContext,
        authz_route: &authz::RouterRoute,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_route).await?;

        use db::schema::router_route::dsl;
        let now = Utc::now();
        diesel::update(dsl::router_route)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_route.id()))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_route),
                )
            })?;
        Ok(())
    }

    pub async fn router_update_route(
        &self,
        opctx: &OpContext,
        authz_route: &authz::RouterRoute,
        route_update: RouterRouteUpdate,
    ) -> UpdateResult<RouterRoute> {
        opctx.authorize(authz::Action::Modify, authz_route).await?;

        use db::schema::router_route::dsl;
        diesel::update(dsl::router_route)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_route.id()))
            .set(route_update)
            .returning(RouterRoute::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_route),
                )
            })
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
    // TODO-correctness this may apply at the Nexus level as well.

    pub async fn session_create(
        &self,
        opctx: &OpContext,
        session: ConsoleSession,
    ) -> CreateResult<ConsoleSession> {
        opctx
            .authorize(authz::Action::CreateChild, &authz::CONSOLE_SESSION_LIST)
            .await?;

        use db::schema::console_session::dsl;

        diesel::insert_into(dsl::console_session)
            .values(session)
            .returning(ConsoleSession::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
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
        opctx: &OpContext,
        authz_session: &authz::ConsoleSession,
    ) -> UpdateResult<authn::ConsoleSessionWithSiloId> {
        opctx.authorize(authz::Action::Modify, authz_session).await?;

        use db::schema::console_session::dsl;
        let console_session = diesel::update(dsl::console_session)
            .filter(dsl::token.eq(authz_session.id()))
            .set((dsl::time_last_used.eq(Utc::now()),))
            .returning(ConsoleSession::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error renewing session: {:?}",
                    e
                ))
            })?;

        let (.., db_silo_user) = LookupPath::new(opctx, &self)
            .silo_user_id(console_session.silo_user_id)
            .fetch()
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error fetching silo id: {:?}",
                    e
                ))
            })?;

        Ok(authn::ConsoleSessionWithSiloId {
            console_session,
            silo_id: db_silo_user.silo_id,
        })
    }

    // putting "hard" in the name because we don't do this with any other model
    pub async fn session_hard_delete(
        &self,
        opctx: &OpContext,
        authz_session: &authz::ConsoleSession,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_session).await?;

        use db::schema::console_session::dsl;
        diesel::delete(dsl::console_session)
            .filter(dsl::token.eq(authz_session.id()))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error deleting session: {:?}",
                    e
                ))
            })
    }

    pub async fn users_builtin_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<UserBuiltin> {
        use db::schema::user_builtin::dsl;
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        paginated(dsl::user_builtin, dsl::name, pagparams)
            .select(UserBuiltin::as_select())
            .load_async::<UserBuiltin>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Load built-in users into the database
    pub async fn load_builtin_users(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use db::schema::user_builtin::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        let builtin_users = [
            // Note: "db_init" is also a builtin user, but that one by necessity
            // is created with the database.
            &*authn::USER_INTERNAL_API,
            &*authn::USER_INTERNAL_READ,
            &*authn::USER_EXTERNAL_AUTHN,
            &*authn::USER_SAGA_RECOVERY,
            &*authn::USER_TEST_PRIVILEGED,
            &*authn::USER_TEST_UNPRIVILEGED,
        ]
        .iter()
        .map(|u| {
            UserBuiltin::new(
                u.id,
                params::UserBuiltinCreate {
                    identity: IdentityMetadataCreateParams {
                        name: u.name.clone(),
                        description: String::from(u.description),
                    },
                },
            )
        })
        .collect::<Vec<UserBuiltin>>();

        debug!(opctx.log, "creating silo_user entries for built-in users");

        for builtin_user in &builtin_users {
            self.silo_user_create(SiloUser::new(
                *SILO_ID,
                builtin_user.identity.id, /* silo user id */
            ))
            .await?;
        }

        info!(opctx.log, "created silo_user entries for built-in users");

        debug!(opctx.log, "attempting to create built-in users");
        let count = diesel::insert_into(dsl::user_builtin)
            .values(builtin_users)
            .on_conflict(dsl::id)
            .do_nothing()
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        info!(opctx.log, "created {} built-in users", count);

        Ok(())
    }

    /// List built-in roles
    pub async fn roles_builtin_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (String, String)>,
    ) -> ListResultVec<RoleBuiltin> {
        use db::schema::role_builtin::dsl;
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        paginated_multicolumn(
            dsl::role_builtin,
            (dsl::resource_type, dsl::role_name),
            pagparams,
        )
        .select(RoleBuiltin::as_select())
        .load_async::<RoleBuiltin>(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Load built-in roles into the database
    pub async fn load_builtin_roles(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use db::schema::role_builtin::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        let builtin_roles = BUILTIN_ROLES
            .iter()
            .map(|role_config| {
                RoleBuiltin::new(
                    role_config.resource_type,
                    &role_config.role_name,
                    &role_config.description,
                )
            })
            .collect::<Vec<RoleBuiltin>>();

        debug!(opctx.log, "attempting to create built-in roles");
        let count = diesel::insert_into(dsl::role_builtin)
            .values(builtin_roles)
            .on_conflict((dsl::resource_type, dsl::role_name))
            .do_nothing()
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        info!(opctx.log, "created {} built-in roles", count);
        Ok(())
    }

    /// Load role assignments for built-in users and built-in roles into the
    /// database
    pub async fn load_builtin_role_asgns(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use db::schema::role_assignment_builtin::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        // The built-in "test-privileged" user gets the "fleet admin" role.
        debug!(opctx.log, "attempting to create built-in role assignments");
        let count = diesel::insert_into(dsl::role_assignment_builtin)
            .values(&*BUILTIN_ROLE_ASSIGNMENTS)
            .on_conflict((
                dsl::user_builtin_id,
                dsl::resource_type,
                dsl::resource_id,
                dsl::role_name,
            ))
            .do_nothing()
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        info!(opctx.log, "created {} built-in role assignments", count);
        Ok(())
    }

    /// Return the built-in roles that the given built-in user has for the given
    /// resource
    pub async fn role_asgn_builtin_list_for(
        &self,
        opctx: &OpContext,
        user_builtin_id: Uuid,
        resource_type: ResourceType,
        resource_id: Uuid,
    ) -> Result<Vec<RoleAssignmentBuiltin>, Error> {
        use db::schema::role_assignment_builtin::dsl;

        // There is no resource-specific authorization check because all
        // authenticated users need to be able to list their own roles --
        // otherwise we can't do any authorization checks.

        // TODO-scalability TODO-security This needs to be paginated.  It's not
        // exposed via an external API right now but someone could still put us
        // into some hurt by assigning loads of roles to someone and having that
        // person attempt to access anything.
        dsl::role_assignment_builtin
            .filter(dsl::user_builtin_id.eq(user_builtin_id))
            .filter(dsl::resource_type.eq(resource_type.to_string()))
            .filter(dsl::resource_id.eq(resource_id))
            .select(RoleAssignmentBuiltin::as_select())
            .load_async::<RoleAssignmentBuiltin>(
                self.pool_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn update_available_artifact_upsert(
        &self,
        opctx: &OpContext,
        artifact: UpdateAvailableArtifact,
    ) -> CreateResult<UpdateAvailableArtifact> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        use db::schema::update_available_artifact::dsl;
        diesel::insert_into(dsl::update_available_artifact)
            .values(artifact.clone())
            .on_conflict((dsl::name, dsl::version, dsl::kind))
            .do_update()
            .set(artifact.clone())
            .returning(UpdateAvailableArtifact::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn update_available_artifact_hard_delete_outdated(
        &self,
        opctx: &OpContext,
        current_targets_role_version: i64,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // We use the `targets_role_version` column in the table to delete any
        // old rows, keeping the table in sync with the current copy of
        // artifacts.json.
        use db::schema::update_available_artifact::dsl;
        diesel::delete(dsl::update_available_artifact)
            .filter(dsl::targets_role_version.lt(current_targets_role_version))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| {
                // TODO-correctness TODO-availability This should be using
                // public_error_from_diesel_pool()
                Error::internal_error(&format!(
                    "error deleting outdated available artifacts: {:?}",
                    e
                ))
            })
    }

    pub async fn silo_user_create(
        &self,
        silo_user: SiloUser,
    ) -> CreateResult<SiloUser> {
        use db::schema::silo_user::dsl;

        diesel::insert_into(dsl::silo_user)
            .values(silo_user)
            .returning(SiloUser::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error creating silo user: {:?}",
                    e
                ))
            })
    }

    /// Load built-in silos into the database
    pub async fn load_builtin_silos(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        debug!(opctx.log, "attempting to create built-in silo");

        use db::schema::silo::dsl;
        let count = diesel::insert_into(dsl::silo)
            .values(&*DEFAULT_SILO)
            .on_conflict(dsl::id)
            .do_nothing()
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        info!(opctx.log, "created {} built-in silos", count);
        Ok(())
    }

    pub async fn silo_create(
        &self,
        opctx: &OpContext,
        silo: Silo,
    ) -> CreateResult<Silo> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        let silo_id = silo.id();

        use db::schema::silo::dsl;
        diesel::insert_into(dsl::silo)
            .values(silo)
            .returning(Silo::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Silo,
                        silo_id.to_string().as_str(),
                    ),
                )
            })
    }

    pub async fn silos_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Silo> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::silo::dsl;
        paginated(dsl::silo, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::discoverable.eq(true))
            .select(Silo::as_select())
            .load_async::<Silo>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silos_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Silo> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::silo::dsl;
        paginated(dsl::silo, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::discoverable.eq(true))
            .select(Silo::as_select())
            .load_async::<Silo>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silo_delete(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        db_silo: &db::model::Silo,
    ) -> DeleteResult {
        assert_eq!(authz_silo.id(), db_silo.id());
        opctx.authorize(authz::Action::Delete, authz_silo).await?;

        use db::schema::organization;
        use db::schema::silo;
        use db::schema::silo_user;

        // Make sure there are no organizations present within this silo.
        let id = authz_silo.id();
        let rcgen = db_silo.rcgen;
        let org_found = diesel_pool_result_optional(
            organization::dsl::organization
                .filter(organization::dsl::silo_id.eq(id))
                .filter(organization::dsl::time_deleted.is_null())
                .select(organization::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;

        if org_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "silo to be deleted contains an organization"
                    .to_string(),
            });
        }

        let now = Utc::now();
        let updated_rows = diesel::update(silo::dsl::silo)
            .filter(silo::dsl::time_deleted.is_null())
            .filter(silo::dsl::id.eq(id))
            .filter(silo::dsl::rcgen.eq(rcgen))
            .set(silo::dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_silo),
                )
            })?;

        if updated_rows == 0 {
            return Err(Error::InvalidRequest {
                message: "silo deletion failed due to concurrent modification"
                    .to_string(),
            });
        }

        info!(opctx.log, "deleted silo {}", id);

        // If silo deletion succeeded, delete all silo users
        // TODO-correctness This needs to happen in a saga or some other
        // mechanism that ensures it happens even if we crash at this point.
        // TODO-scalability This needs to happen in batches
        let updated_rows = diesel::update(silo_user::dsl::silo_user)
            .filter(silo_user::dsl::silo_id.eq(id))
            .filter(silo_user::dsl::time_deleted.is_null())
            .set(silo_user::dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_silo),
                )
            })?;

        info!(opctx.log, "deleted {} silo users for silo {}", updated_rows, id);

        Ok(())
    }

    /// Return the next available IPv6 address for an Oxide service running on
    /// the provided sled.
    pub async fn next_ipv6_address(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
    ) -> Result<Ipv6Addr, Error> {
        use db::schema::sled::dsl;
        let net = diesel::update(
            dsl::sled.find(sled_id).filter(dsl::time_deleted.is_null()),
        )
        .set(dsl::last_used_address.eq(dsl::last_used_address + 1))
        .returning(dsl::last_used_address)
        .get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| {
            public_error_from_diesel_pool(
                e,
                ErrorHandler::NotFoundByLookup(
                    ResourceType::Sled,
                    LookupType::ById(sled_id),
                ),
            )
        })?;

        // TODO-correctness: We need to ensure that this address is actually
        // within the sled's underlay prefix, once that's included in the
        // database record.
        match net {
            ipnetwork::IpNetwork::V6(net) => Ok(net.ip()),
            _ => Err(Error::InternalError {
                internal_message: String::from("Sled IP address must be IPv6"),
            }),
        }
    }

    pub async fn global_image_list_images(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<GlobalImage> {
        opctx
            .authorize(authz::Action::ListChildren, &authz::GLOBAL_IMAGE_LIST)
            .await?;

        use db::schema::global_image::dsl;
        paginated(dsl::global_image, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(GlobalImage::as_select())
            .load_async::<GlobalImage>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn global_image_create_image(
        &self,
        opctx: &OpContext,
        image: GlobalImage,
    ) -> CreateResult<GlobalImage> {
        opctx
            .authorize(authz::Action::CreateChild, &authz::GLOBAL_IMAGE_LIST)
            .await?;

        use db::schema::global_image::dsl;
        let name = image.name().clone();
        diesel::insert_into(dsl::global_image)
            .values(image)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(GlobalImage::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::Image, name.as_str()),
                )
            })
    }

    // SSH public keys

    pub async fn ssh_keys_list(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        page_params: &DataPageParams<'_, Name>,
    ) -> ListResultVec<SshKey> {
        opctx.authorize(authz::Action::ListChildren, authz_user).await?;

        use db::schema::ssh_key::dsl;
        paginated(dsl::ssh_key, dsl::name, page_params)
            .filter(dsl::silo_user_id.eq(authz_user.id()))
            .filter(dsl::time_deleted.is_null())
            .select(SshKey::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Create a new SSH public key for a user.
    pub async fn ssh_key_create(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        ssh_key: SshKey,
    ) -> CreateResult<SshKey> {
        assert_eq!(authz_user.id(), ssh_key.silo_user_id);
        opctx.authorize(authz::Action::CreateChild, authz_user).await?;

        use db::schema::ssh_key::dsl;
        diesel::insert_into(dsl::ssh_key)
            .values(ssh_key)
            .returning(SshKey::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error creating SSH key: {:?}",
                    e
                ))
            })
    }

    /// Delete an existing SSH public key.
    pub async fn ssh_key_delete(
        &self,
        opctx: &OpContext,
        authz_ssh_key: &authz::SshKey,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_ssh_key).await?;

        use db::schema::ssh_key::dsl;
        diesel::update(dsl::ssh_key)
            .filter(dsl::id.eq(authz_ssh_key.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .check_if_exists::<SshKey>(authz_ssh_key.id())
            .execute_and_check(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_ssh_key),
                )
            })?;
        Ok(())
    }

    // Test interfaces

    #[cfg(test)]
    async fn test_try_table_scan(&self, opctx: &OpContext) -> Error {
        use db::schema::project::dsl;
        let conn = self.pool_authorized(opctx).await;
        if let Err(error) = conn {
            return error;
        }
        let result = dsl::project
            .select(diesel::dsl::count_star())
            .first_async::<i64>(conn.unwrap())
            .await;
        match result {
            Ok(_) => Error::internal_error("table scan unexpectedly succeeded"),
            Err(error) => {
                public_error_from_diesel_pool(error, ErrorHandler::Server)
            }
        }
    }
}

/// Constructs a DataStore for use in test suites that has preloaded the
/// built-in users, roles, and role assignments that are needed for basic
/// operation
#[cfg(test)]
pub async fn datastore_test(
    logctx: &dropshot::test_util::LogContext,
    db: &omicron_test_utils::dev::db::CockroachInstance,
) -> (OpContext, Arc<DataStore>) {
    let cfg = db::Config { url: db.pg_config().clone() };
    let pool = Arc::new(db::Pool::new(&cfg));
    let datastore = Arc::new(DataStore::new(pool));

    // Create an OpContext with the credentials of "db-init" just for the
    // purpose of loading the built-in users, roles, and assignments.
    let opctx = OpContext::for_background(
        logctx.log.new(o!()),
        Arc::new(authz::Authz::new(&logctx.log)),
        authn::Context::internal_db_init(),
        Arc::clone(&datastore),
    );
    datastore.load_builtin_users(&opctx).await.unwrap();
    datastore.load_builtin_roles(&opctx).await.unwrap();
    datastore.load_builtin_role_asgns(&opctx).await.unwrap();
    datastore.load_builtin_silos(&opctx).await.unwrap();

    // Create an OpContext with the credentials of "test-privileged" for general
    // testing.
    let opctx =
        OpContext::for_tests(logctx.log.new(o!()), Arc::clone(&datastore));

    (opctx, datastore)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::authz;
    use crate::db::explain::ExplainableAsync;
    use crate::db::identity::Resource;
    use crate::db::lookup::LookupPath;
    use crate::db::model::{ConsoleSession, DatasetKind, Project};
    use crate::external_api::params;
    use chrono::{Duration, Utc};
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::{
        ByteCount, Error, IdentityMetadataCreateParams, LookupType, Name,
    };
    use omicron_test_utils::dev;
    use std::collections::HashSet;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_project_creation() {
        let logctx = dev::test_setup_log("test_project_creation");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let organization = params::OrganizationCreate {
            identity: IdentityMetadataCreateParams {
                name: "org".parse().unwrap(),
                description: "desc".to_string(),
            },
        };

        let organization =
            datastore.organization_create(&opctx, &organization).await.unwrap();

        let project = Project::new(
            organization.id(),
            params::ProjectCreate {
                identity: IdentityMetadataCreateParams {
                    name: "project".parse().unwrap(),
                    description: "desc".to_string(),
                },
            },
        );
        let (.., authz_org) = LookupPath::new(&opctx, &datastore)
            .organization_id(organization.id())
            .lookup_for(authz::Action::CreateChild)
            .await
            .unwrap();
        datastore.project_create(&opctx, &authz_org, project).await.unwrap();

        let (.., organization_after_project_create) =
            LookupPath::new(&opctx, &datastore)
                .organization_name(organization.name())
                .fetch()
                .await
                .unwrap();
        assert!(organization_after_project_create.rcgen > organization.rcgen);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_session_methods() {
        let logctx = dev::test_setup_log("test_session_methods");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let token = "a_token".to_string();
        let silo_user_id = Uuid::new_v4();

        let session = ConsoleSession {
            token: token.clone(),
            time_created: Utc::now() - Duration::minutes(5),
            time_last_used: Utc::now() - Duration::minutes(5),
            silo_user_id,
        };

        let _ =
            datastore.session_create(&opctx, session.clone()).await.unwrap();

        // Associate silo with user
        let silo_user = datastore
            .silo_user_create(SiloUser::new(*SILO_ID, silo_user_id))
            .await
            .unwrap();

        let (.., db_silo_user) = LookupPath::new(&opctx, &datastore)
            .silo_user_id(session.silo_user_id)
            .fetch()
            .await
            .unwrap();
        assert_eq!(silo_user.silo_id, db_silo_user.silo_id,);

        // fetch the one we just created
        let (.., fetched) = LookupPath::new(&opctx, &datastore)
            .console_session_token(&token)
            .fetch()
            .await
            .unwrap();
        assert_eq!(session.silo_user_id, fetched.silo_user_id);

        // trying to insert the same one again fails
        let duplicate = datastore.session_create(&opctx, session.clone()).await;
        assert!(matches!(
            duplicate,
            Err(Error::InternalError { internal_message: _ })
        ));

        // update last used (i.e., renew token)
        let authz_session = authz::ConsoleSession::new(
            authz::FLEET,
            token.clone(),
            LookupType::ByCompositeId(token.clone()),
        );
        let renewed = datastore
            .session_update_last_used(&opctx, &authz_session)
            .await
            .unwrap();
        assert!(
            renewed.console_session.time_last_used > session.time_last_used
        );

        // time_last_used change persists in DB
        let (.., fetched) = LookupPath::new(&opctx, &datastore)
            .console_session_token(&token)
            .fetch()
            .await
            .unwrap();
        assert!(fetched.time_last_used > session.time_last_used);

        // delete it and fetch should come back with nothing
        let delete =
            datastore.session_hard_delete(&opctx, &authz_session).await;
        assert_eq!(delete, Ok(()));

        // this will be a not found after #347
        let fetched = LookupPath::new(&opctx, &datastore)
            .console_session_token(&token)
            .fetch()
            .await;
        assert!(matches!(
            fetched,
            Err(Error::ObjectNotFound { type_name: _, lookup_type: _ })
        ));

        // deleting an already nonexistent is considered a success
        let delete_again =
            datastore.session_hard_delete(&opctx, &authz_session).await;
        assert_eq!(delete_again, Ok(()));

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Creates a test sled, returns its UUID.
    async fn create_test_sled(datastore: &DataStore) -> Uuid {
        let bogus_addr = SocketAddrV6::new(
            Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1),
            8080,
            0,
            0,
        );
        let sled_id = Uuid::new_v4();
        let sled = Sled::new(sled_id, bogus_addr.clone());
        datastore.sled_upsert(sled).await.unwrap();
        sled_id
    }

    fn test_zpool_size() -> ByteCount {
        ByteCount::from_gibibytes_u32(100)
    }

    // Creates a test zpool, returns its UUID.
    async fn create_test_zpool(datastore: &DataStore, sled_id: Uuid) -> Uuid {
        let zpool_id = Uuid::new_v4();
        let zpool = Zpool::new(
            zpool_id,
            sled_id,
            &crate::internal_api::params::ZpoolPutRequest {
                size: test_zpool_size(),
            },
        );
        datastore.zpool_upsert(zpool).await.unwrap();
        zpool_id
    }

    fn create_test_disk_create_params(
        name: &str,
        size: ByteCount,
    ) -> params::DiskCreate {
        params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from(name.to_string()).unwrap(),
                description: name.to_string(),
            },
            disk_source: params::DiskSource::Blank {
                block_size: params::BlockSize::try_from(4096).unwrap(),
            },
            size,
        }
    }

    #[tokio::test]
    async fn test_region_allocation() {
        let logctx = dev::test_setup_log("test_region_allocation");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&cfg);
        let datastore = Arc::new(DataStore::new(Arc::new(pool)));
        let opctx =
            OpContext::for_tests(logctx.log.new(o!()), datastore.clone());

        // Create a sled...
        let sled_id = create_test_sled(&datastore).await;

        // ... and a zpool within that sled...
        let zpool_id = create_test_zpool(&datastore, sled_id).await;

        // ... and datasets within that zpool.
        let dataset_count = REGION_REDUNDANCY_THRESHOLD * 2;
        let bogus_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let dataset_ids: Vec<Uuid> =
            (0..dataset_count).map(|_| Uuid::new_v4()).collect();
        for id in &dataset_ids {
            let dataset =
                Dataset::new(*id, zpool_id, bogus_addr, DatasetKind::Crucible);
            datastore.dataset_upsert(dataset).await.unwrap();
        }

        // Allocate regions from the datasets for this disk.
        let params = create_test_disk_create_params(
            "disk1",
            ByteCount::from_mebibytes_u32(500),
        );
        let volume1_id = Uuid::new_v4();
        // Currently, we only allocate one Region Set per volume.
        let expected_region_count = REGION_REDUNDANCY_THRESHOLD;
        let dataset_and_regions = datastore
            .region_allocate(&opctx, volume1_id, &params)
            .await
            .unwrap();

        // Verify the allocation.
        assert_eq!(expected_region_count, dataset_and_regions.len());
        let mut disk1_datasets = HashSet::new();
        for (dataset, region) in dataset_and_regions {
            assert!(disk1_datasets.insert(dataset.id()));
            assert_eq!(volume1_id, region.volume_id());
            assert_eq!(ByteCount::from(4096), region.block_size());
            assert_eq!(params.extent_size() / 4096, region.blocks_per_extent());
            assert_eq!(params.extent_count(), region.extent_count());
        }

        // Allocate regions for a second disk. Observe that we allocate from
        // the three previously unused datasets.
        let params = create_test_disk_create_params(
            "disk2",
            ByteCount::from_mebibytes_u32(500),
        );
        let volume2_id = Uuid::new_v4();
        let dataset_and_regions = datastore
            .region_allocate(&opctx, volume2_id, &params)
            .await
            .unwrap();
        assert_eq!(expected_region_count, dataset_and_regions.len());
        let mut disk2_datasets = HashSet::new();
        for (dataset, region) in dataset_and_regions {
            assert!(disk2_datasets.insert(dataset.id()));
            assert_eq!(volume2_id, region.volume_id());
            assert_eq!(ByteCount::from(4096), region.block_size());
            assert_eq!(params.extent_size() / 4096, region.blocks_per_extent());
            assert_eq!(params.extent_count(), region.extent_count());
        }

        // Double-check that the datasets used for the first disk weren't
        // used when allocating the second disk.
        assert_eq!(0, disk1_datasets.intersection(&disk2_datasets).count());

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_region_allocation_is_idempotent() {
        let logctx =
            dev::test_setup_log("test_region_allocation_is_idempotent");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&cfg);
        let datastore = Arc::new(DataStore::new(Arc::new(pool)));
        let opctx =
            OpContext::for_tests(logctx.log.new(o!()), datastore.clone());

        // Create a sled...
        let sled_id = create_test_sled(&datastore).await;

        // ... and a zpool within that sled...
        let zpool_id = create_test_zpool(&datastore, sled_id).await;

        // ... and datasets within that zpool.
        let dataset_count = REGION_REDUNDANCY_THRESHOLD;
        let bogus_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let dataset_ids: Vec<Uuid> =
            (0..dataset_count).map(|_| Uuid::new_v4()).collect();
        for id in &dataset_ids {
            let dataset =
                Dataset::new(*id, zpool_id, bogus_addr, DatasetKind::Crucible);
            datastore.dataset_upsert(dataset).await.unwrap();
        }

        // Allocate regions from the datasets for this volume.
        let params = create_test_disk_create_params(
            "disk",
            ByteCount::from_mebibytes_u32(500),
        );
        let volume_id = Uuid::new_v4();
        let mut dataset_and_regions1 = datastore
            .region_allocate(&opctx, volume_id, &params)
            .await
            .unwrap();
        let mut dataset_and_regions2 = datastore
            .region_allocate(&opctx, volume_id, &params)
            .await
            .unwrap();

        // Give them a consistent order so we can easily compare them.
        let sort_vec = |v: &mut Vec<(Dataset, Region)>| {
            v.sort_by(|(d1, r1), (d2, r2)| {
                let order = d1.id().cmp(&d2.id());
                match order {
                    std::cmp::Ordering::Equal => r1.id().cmp(&r2.id()),
                    _ => order,
                }
            });
        };
        sort_vec(&mut dataset_and_regions1);
        sort_vec(&mut dataset_and_regions2);

        // Validate that the two calls to allocate return the same data.
        assert_eq!(dataset_and_regions1.len(), dataset_and_regions2.len());
        for i in 0..dataset_and_regions1.len() {
            assert_eq!(dataset_and_regions1[i], dataset_and_regions2[i],);
        }

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_region_allocation_not_enough_datasets() {
        let logctx =
            dev::test_setup_log("test_region_allocation_not_enough_datasets");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&cfg);
        let datastore = Arc::new(DataStore::new(Arc::new(pool)));
        let opctx =
            OpContext::for_tests(logctx.log.new(o!()), datastore.clone());

        // Create a sled...
        let sled_id = create_test_sled(&datastore).await;

        // ... and a zpool within that sled...
        let zpool_id = create_test_zpool(&datastore, sled_id).await;

        // ... and datasets within that zpool.
        let dataset_count = REGION_REDUNDANCY_THRESHOLD - 1;
        let bogus_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let dataset_ids: Vec<Uuid> =
            (0..dataset_count).map(|_| Uuid::new_v4()).collect();
        for id in &dataset_ids {
            let dataset =
                Dataset::new(*id, zpool_id, bogus_addr, DatasetKind::Crucible);
            datastore.dataset_upsert(dataset).await.unwrap();
        }

        // Allocate regions from the datasets for this volume.
        let params = create_test_disk_create_params(
            "disk1",
            ByteCount::from_mebibytes_u32(500),
        );
        let volume1_id = Uuid::new_v4();
        let err = datastore
            .region_allocate(&opctx, volume1_id, &params)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("Not enough datasets to allocate disks"));

        assert!(matches!(err, Error::ServiceUnavailable { .. }));

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    // TODO: This test should be updated when the correct handling
    // of this out-of-space case is implemented.
    #[tokio::test]
    async fn test_region_allocation_out_of_space_does_not_fail_yet() {
        let logctx = dev::test_setup_log(
            "test_region_allocation_out_of_space_does_not_fail_yet",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&cfg);
        let datastore = Arc::new(DataStore::new(Arc::new(pool)));
        let opctx =
            OpContext::for_tests(logctx.log.new(o!()), datastore.clone());

        // Create a sled...
        let sled_id = create_test_sled(&datastore).await;

        // ... and a zpool within that sled...
        let zpool_id = create_test_zpool(&datastore, sled_id).await;

        // ... and datasets within that zpool.
        let dataset_count = REGION_REDUNDANCY_THRESHOLD;
        let bogus_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let dataset_ids: Vec<Uuid> =
            (0..dataset_count).map(|_| Uuid::new_v4()).collect();
        for id in &dataset_ids {
            let dataset =
                Dataset::new(*id, zpool_id, bogus_addr, DatasetKind::Crucible);
            datastore.dataset_upsert(dataset).await.unwrap();
        }

        // Allocate regions from the datasets for this disk.
        //
        // Note that we ask for a disk which is as large as the zpool,
        // so we shouldn't have space for redundancy.
        let disk_size = test_zpool_size();
        let params = create_test_disk_create_params("disk1", disk_size);
        let volume1_id = Uuid::new_v4();

        // NOTE: This *should* be an error, rather than succeeding.
        datastore.region_allocate(&opctx, volume1_id, &params).await.unwrap();

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    // Validate that queries which should be executable without a full table
    // scan are, in fact, runnable without a FULL SCAN.
    #[tokio::test]
    async fn test_queries_do_not_require_full_table_scan() {
        use omicron_common::api::external;
        let logctx =
            dev::test_setup_log("test_queries_do_not_require_full_table_scan");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&cfg);
        let datastore = DataStore::new(Arc::new(pool));

        let explanation = DataStore::get_allocated_regions_query(Uuid::nil())
            .explain_async(datastore.pool())
            .await
            .unwrap();
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        let explanation = DataStore::get_allocatable_datasets_query()
            .explain_async(datastore.pool())
            .await
            .unwrap();
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        let subnet = db::model::VpcSubnet::new(
            Uuid::nil(),
            Uuid::nil(),
            external::IdentityMetadataCreateParams {
                name: external::Name::try_from(String::from("name")).unwrap(),
                description: String::from("description"),
            },
            external::Ipv4Net("172.30.0.0/22".parse().unwrap()),
            external::Ipv6Net("fd00::/64".parse().unwrap()),
        );
        let values = FilterConflictingVpcSubnetRangesQuery(subnet);
        let query =
            diesel::insert_into(db::schema::vpc_subnet::dsl::vpc_subnet)
                .values(values)
                .returning(VpcSubnet::as_returning());
        println!("{}", diesel::debug_query(&query));
        let explanation = query.explain_async(datastore.pool()).await.unwrap();
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation,
        );

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    // Test sled-specific IPv6 address allocation
    #[tokio::test]
    async fn test_sled_ipv6_address_allocation() {
        use crate::db::model::STATIC_IPV6_ADDRESS_OFFSET;
        use std::net::Ipv6Addr;

        let logctx = dev::test_setup_log("test_sled_ipv6_address_allocation");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(db::Pool::new(&cfg));
        let datastore = Arc::new(DataStore::new(Arc::clone(&pool)));
        let opctx =
            OpContext::for_tests(logctx.log.new(o!()), datastore.clone());

        let addr1 = "[fd00:1de::1]:12345".parse().unwrap();
        let sled1_id = "0de4b299-e0b4-46f0-d528-85de81a7095f".parse().unwrap();
        let sled1 = db::model::Sled::new(sled1_id, addr1);
        datastore.sled_upsert(sled1).await.unwrap();

        let addr2 = "[fd00:1df::1]:12345".parse().unwrap();
        let sled2_id = "66285c18-0c79-43e0-e54f-95271f271314".parse().unwrap();
        let sled2 = db::model::Sled::new(sled2_id, addr2);
        datastore.sled_upsert(sled2).await.unwrap();

        let ip = datastore.next_ipv6_address(&opctx, sled1_id).await.unwrap();
        let expected_ip = Ipv6Addr::new(
            0xfd00,
            0x1de,
            0,
            0,
            0,
            0,
            0,
            2 + STATIC_IPV6_ADDRESS_OFFSET,
        );
        assert_eq!(ip, expected_ip);
        let ip = datastore.next_ipv6_address(&opctx, sled1_id).await.unwrap();
        let expected_ip = Ipv6Addr::new(
            0xfd00,
            0x1de,
            0,
            0,
            0,
            0,
            0,
            3 + STATIC_IPV6_ADDRESS_OFFSET,
        );
        assert_eq!(ip, expected_ip);

        let ip = datastore.next_ipv6_address(&opctx, sled2_id).await.unwrap();
        let expected_ip = Ipv6Addr::new(
            0xfd00,
            0x1df,
            0,
            0,
            0,
            0,
            0,
            2 + STATIC_IPV6_ADDRESS_OFFSET,
        );
        assert_eq!(ip, expected_ip);

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ssh_keys() {
        let logctx = dev::test_setup_log("test_ssh_keys");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create a new Silo user so that we can lookup their keys.
        let silo_user_id = Uuid::new_v4();
        let silo_user = datastore
            .silo_user_create(SiloUser::new(*SILO_ID, silo_user_id))
            .await
            .unwrap();
        assert_eq!(silo_user.id(), silo_user_id);

        let (.., authz_user) = LookupPath::new(&opctx, &datastore)
            .silo_user_id(silo_user_id)
            .lookup_for(authz::Action::CreateChild)
            .await
            .unwrap();
        assert_eq!(authz_user.id(), silo_user_id);

        // Create a new SSH public key for the new user.
        let key_name = Name::try_from(String::from("sshkey")).unwrap();
        let public_key = "ssh-test AAAAAAAAKEY".to_string();
        let ssh_key = SshKey::new(
            silo_user_id,
            params::SshKeyCreate {
                identity: IdentityMetadataCreateParams {
                    name: key_name.clone(),
                    description: "my SSH public key".to_string(),
                },
                public_key,
            },
        );
        let created = datastore
            .ssh_key_create(&opctx, &authz_user, ssh_key.clone())
            .await
            .unwrap();
        assert_eq!(created.silo_user_id, ssh_key.silo_user_id);
        assert_eq!(created.public_key, ssh_key.public_key);

        // Lookup the key we just created.
        let (authz_silo, authz_silo_user, authz_ssh_key, found) =
            LookupPath::new(&opctx, &datastore)
                .silo_user_id(silo_user_id)
                .ssh_key_name(&key_name.into())
                .fetch()
                .await
                .unwrap();
        assert_eq!(authz_silo.id(), *SILO_ID);
        assert_eq!(authz_silo_user.id(), silo_user_id);
        assert_eq!(found.silo_user_id, ssh_key.silo_user_id);
        assert_eq!(found.public_key, ssh_key.public_key);

        // Trying to insert the same one again fails.
        let duplicate = datastore
            .ssh_key_create(&opctx, &authz_user, ssh_key.clone())
            .await;
        assert!(matches!(
            duplicate,
            Err(Error::InternalError { internal_message: _ })
        ));

        // Delete the key we just created.
        datastore.ssh_key_delete(&opctx, &authz_ssh_key).await.unwrap();

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_table_scan() {
        let logctx = dev::test_setup_log("test_table_scan");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let error = datastore.test_try_table_scan(&opctx).await;
        println!("error from attempted table scan: {:#}", error);
        match error {
            Error::InternalError { internal_message } => {
                assert!(internal_message.contains(
                    "contains a full table/index scan which is \
                    explicitly disallowed"
                ));
            }
            error => panic!(
                "expected internal error with specific message, found {:?}",
                error
            ),
        }

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
