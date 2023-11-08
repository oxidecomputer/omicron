// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`IpPool`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::fixed_data::silo::INTERNAL_SILO_ID;
use crate::db::identity::Resource;
use crate::db::model::IpPool;
use crate::db::model::IpPoolRange;
use crate::db::model::IpPoolResource;
use crate::db::model::IpPoolUpdate;
use crate::db::model::Name;
use crate::db::pagination::paginated;
use crate::db::pool::DbConnection;
use crate::db::queries::ip_pool::FilterOverlappingIpRanges;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use ipnetwork::IpNetwork;
use nexus_db_model::ExternalIp;
use nexus_db_model::IpPoolResourceType;
use nexus_types::external_api::shared::IpRange;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;
use uuid::Uuid;

impl DataStore {
    /// List IP Pools
    pub async fn ip_pools_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<IpPool> {
        use db::schema::ip_pool;
        use db::schema::ip_pool_resource;

        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(ip_pool::table, ip_pool::id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                ip_pool::table,
                ip_pool::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .left_outer_join(ip_pool_resource::table)
        .filter(
            ip_pool_resource::resource_id
                .ne(*INTERNAL_SILO_ID)
                // resource_id is not nullable -- null here means the
                // pool has no entry in the join table
                .or(ip_pool_resource::resource_id.is_null()),
        )
        .filter(ip_pool::time_deleted.is_null())
        .select(IpPool::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Look up whether the given pool is available to users in the given silo,
    /// i.e., whether there is an entry in the association table associating the
    /// pool with either that silo or the fleet
    pub async fn ip_pool_fetch_association(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> LookupResult<IpPoolResource> {
        use db::schema::ip_pool;
        use db::schema::ip_pool_resource;

        let authz_silo = opctx.authn.silo_required()?;

        ip_pool::table
            .inner_join(ip_pool_resource::table)
            .filter(
                (ip_pool_resource::resource_type
                    .eq(IpPoolResourceType::Silo)
                    .and(ip_pool_resource::resource_id.eq(authz_silo.id())))
                .or(ip_pool_resource::resource_type
                    .eq(IpPoolResourceType::Fleet)),
            )
            .filter(ip_pool::id.eq(authz_pool.id()))
            .filter(ip_pool::time_deleted.is_null())
            .select(IpPoolResource::as_select())
            .first_async::<IpPoolResource>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Look up the default IP pool for the current silo. If there is no default
    /// at silo scope, fall back to the next level up, namely the fleet default.
    /// There should always be a default pool at the fleet level, though this
    /// query can theoretically fail if someone is able to delete that pool or
    /// make another one the default and delete that.
    pub async fn ip_pools_fetch_default(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<IpPool> {
        use db::schema::ip_pool;
        use db::schema::ip_pool_resource;

        let authz_silo_id = opctx.authn.silo_required()?.id();

        // TODO: Need auth check here. Only fleet viewers can list children on
        // IP_POOL_LIST, so if we check that, nobody can make instances. This
        // used to check CreateChild on an individual IP pool, but now we're not
        // looking up by name so the check is more complicated
        //
        // opctx
        //     .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
        //     .await?;

        // join ip_pool to ip_pool_resource and filter

        ip_pool::table
            .inner_join(ip_pool_resource::table)
            .filter(
                (ip_pool_resource::resource_type
                    .eq(IpPoolResourceType::Silo)
                    .and(ip_pool_resource::resource_id.eq(authz_silo_id)))
                .or(ip_pool_resource::resource_type
                    .eq(IpPoolResourceType::Fleet)),
            )
            .filter(ip_pool_resource::is_default.eq(true))
            .filter(ip_pool::time_deleted.is_null())
            // Order by most specific first so we get the most specific.
            // resource_type is an enum in the DB and therefore gets its order
            // from the definition; it's not lexicographic. So correctness here
            // relies on the types being most-specific-first in the definition.
            // There are tests for this.
            .order(ip_pool_resource::resource_type.asc())
            .select(IpPool::as_select())
            .first_async::<IpPool>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Looks up an IP pool intended for internal services.
    ///
    /// This method may require an index by Availability Zone in the future.
    pub async fn ip_pools_service_lookup(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<(authz::IpPool, IpPool)> {
        use db::schema::ip_pool;
        use db::schema::ip_pool_resource;

        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;

        // Look up IP pool by its association with the internal silo.
        // We assume there is only one pool for that silo, or at least,
        // if there is more than one, it doesn't matter which one we pick.
        let (authz_pool, pool) = ip_pool::table
            .inner_join(ip_pool_resource::table)
            .filter(ip_pool::time_deleted.is_null())
            .filter(
                ip_pool_resource::resource_type
                    .eq(IpPoolResourceType::Silo)
                    .and(ip_pool_resource::resource_id.eq(*INTERNAL_SILO_ID)),
            )
            .select(IpPool::as_select())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|ip_pool| {
                (
                    authz::IpPool::new(
                        authz::FLEET,
                        ip_pool.id(),
                        LookupType::ByCompositeId(
                            "Service IP Pool".to_string(),
                        ),
                    ),
                    ip_pool,
                )
            })?;
        Ok((authz_pool, pool))
    }

    /// Creates a new IP pool.
    pub async fn ip_pool_create(
        &self,
        opctx: &OpContext,
        pool: IpPool,
    ) -> CreateResult<IpPool> {
        use db::schema::ip_pool::dsl;
        opctx
            .authorize(authz::Action::CreateChild, &authz::IP_POOL_LIST)
            .await?;
        let pool_name = pool.name().as_str().to_string();

        diesel::insert_into(dsl::ip_pool)
            .values(pool)
            .returning(IpPool::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(ResourceType::IpPool, &pool_name),
                )
            })
    }

    pub async fn ip_pool_delete(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        db_pool: &IpPool,
    ) -> DeleteResult {
        use db::schema::ip_pool::dsl;
        use db::schema::ip_pool_range;
        opctx.authorize(authz::Action::Delete, authz_pool).await?;

        // Verify there are no IP ranges still in this pool
        let range = ip_pool_range::dsl::ip_pool_range
            .filter(ip_pool_range::dsl::ip_pool_id.eq(authz_pool.id()))
            .filter(ip_pool_range::dsl::time_deleted.is_null())
            .select(ip_pool_range::dsl::id)
            .limit(1)
            .first_async::<Uuid>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if range.is_some() {
            return Err(Error::InvalidRequest {
                message:
                    "IP Pool cannot be deleted while it contains IP ranges"
                        .to_string(),
            });
        }

        // Delete the pool, conditional on the rcgen not having changed. This
        // protects the delete from occuring if clients created a new IP range
        // in between the above check for children and this query.
        let now = Utc::now();
        let updated_rows = diesel::update(dsl::ip_pool)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_pool.id()))
            .filter(dsl::rcgen.eq(db_pool.rcgen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
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

    /// Check whether the pool is internal by checking that it exists and is
    /// associated with the internal silo
    pub async fn ip_pool_is_internal(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> LookupResult<bool> {
        use db::schema::ip_pool;
        use db::schema::ip_pool_resource;

        let result = ip_pool::table
            .inner_join(ip_pool_resource::table)
            .filter(ip_pool::id.eq(authz_pool.id()))
            .filter(
                ip_pool_resource::resource_type
                    .eq(IpPoolResourceType::Silo)
                    .and(ip_pool_resource::resource_id.eq(*INTERNAL_SILO_ID)),
            )
            .filter(ip_pool::time_deleted.is_null())
            .select(IpPool::as_select())
            .load_async::<IpPool>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // if there is a result, the pool is associated with the internal silo,
        // which makes it the internal pool
        Ok(result.len() > 0)
    }

    pub async fn ip_pool_update(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        updates: IpPoolUpdate,
    ) -> UpdateResult<IpPool> {
        use db::schema::ip_pool::dsl;
        opctx.authorize(authz::Action::Modify, authz_pool).await?;

        diesel::update(dsl::ip_pool)
            .filter(dsl::id.eq(authz_pool.id()))
            .filter(dsl::time_deleted.is_null())
            .set(updates)
            .returning(IpPool::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                )
            })
    }

    pub async fn ip_pool_association_list(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<IpPoolResource> {
        use db::schema::ip_pool;
        use db::schema::ip_pool_resource;

        paginated(
            ip_pool_resource::table,
            ip_pool_resource::ip_pool_id,
            pagparams,
        )
        .inner_join(ip_pool::table)
        .filter(ip_pool::id.eq(authz_pool.id()))
        .filter(ip_pool::time_deleted.is_null())
        .select(IpPoolResource::as_select())
        .load_async::<IpPoolResource>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn ip_pool_associate_resource(
        &self,
        opctx: &OpContext,
        ip_pool_resource: IpPoolResource,
    ) -> CreateResult<IpPoolResource> {
        use db::schema::ip_pool_resource::dsl;
        opctx
            .authorize(authz::Action::CreateChild, &authz::IP_POOL_LIST)
            .await?;

        diesel::insert_into(dsl::ip_pool_resource)
            .values(ip_pool_resource.clone())
            // We have two constraints that are relevant here, and we need to
            // make this behave correctly with respect to both. If the entry
            // matches an existing (ip pool, silo/fleet), we want to update
            // is_default because we want to handle the case where someone is
            // trying to change is_default on an existing association. But
            // you can only have one default pool for a given resource, so
            // if that update violates the unique index ensuring one default,
            // the insert should still fail.
            // note that this on_conflict has to have all three because that's
            // how the pk is defined. if it only has the IDs and not the type,
            // CRDB complains that the tuple doesn't match any constraints it's
            // aware of
            .on_conflict((
                dsl::ip_pool_id,
                dsl::resource_type,
                dsl::resource_id,
            ))
            .do_update()
            .set(dsl::is_default.eq(ip_pool_resource.is_default))
            .returning(IpPoolResource::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::IpPoolResource,
                        // TODO: make string more useful
                        &ip_pool_resource.ip_pool_id.to_string(),
                    ),
                )
            })
    }

    // TODO: write a test for this
    async fn ensure_no_ips_outstanding(
        &self,
        opctx: &OpContext,
        // TODO: this could take the authz_pool, it's just more annoying to test that way
        ip_pool_id: Uuid,
        // TODO: we need to know the resource type because it affects the IPs query
        resource_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::external_ip;
        use db::schema::instance;
        use db::schema::project;
        opctx
            .authorize(authz::Action::CreateChild, &authz::IP_POOL_LIST)
            .await?;

        // We can only delete the association if there are no IPs allocated
        // from this pool in the associated resource.

        // most of the query is the same between silo and fleet
        let base_query = |table: external_ip::table| {
            table
                .inner_join(
                    instance::table
                        .on(external_ip::parent_id.eq(instance::id.nullable())),
                )
                .filter(external_ip::is_service.eq(false))
                .filter(external_ip::parent_id.is_not_null())
                .filter(external_ip::time_deleted.is_null())
                .filter(external_ip::ip_pool_id.eq(ip_pool_id))
                .filter(instance::time_deleted.is_not_null())
                .select(ExternalIp::as_select())
                .limit(1)
        };

        let is_silo = true; // TODO obviously this is not how this works
        let existing_ips = if is_silo {
            // if it's a silo association, we also have to join through IPs to instances
            // to projects to get the silo ID
            base_query(external_ip::table)
                .inner_join(
                    project::table.on(instance::project_id.eq(project::id)),
                )
                .filter(project::silo_id.eq(resource_id))
                .load_async::<ExternalIp>(
                    &*self.pool_connection_authorized(opctx).await?,
                )
                .await
        } else {
            // If it's a fleet association, we can't delete it if there are any IPs
            // allocated from the pool anywhere
            base_query(external_ip::table)
                .load_async::<ExternalIp>(
                    &*self.pool_connection_authorized(opctx).await?,
                )
                .await
        }
        .map_err(|e| {
            Error::internal_error(&format!(
                "error checking for outstanding IPs before deleting IP pool association to resource: {:?}",
                e
            ))
        })?;

        if !existing_ips.is_empty() {
            return Err(Error::InvalidRequest {
                message: "IP addresses from this pool are in use in the associated silo/fleet".to_string()
            });
        }

        Ok(())
    }

    /// Delete IP pool assocation with resource unless there are outstanding
    /// IPs allocated from the pool in the associated silo (or the fleet, if
    /// it's a fleet association).
    pub async fn ip_pool_dissociate_resource(
        &self,
        opctx: &OpContext,
        // TODO: this could take the authz_pool, it's just more annoying to test that way
        ip_pool_id: Uuid,
        // TODO: we need to know the resource type because it affects the IPs query
        resource_id: Uuid,
    ) -> DeleteResult {
        use db::schema::ip_pool_resource;
        opctx
            .authorize(authz::Action::CreateChild, &authz::IP_POOL_LIST)
            .await?;

        // We can only delete the association if there are no IPs allocated
        // from this pool in the associated resource.
        self.ensure_no_ips_outstanding(opctx, ip_pool_id, resource_id).await?;

        diesel::delete(ip_pool_resource::table)
            .filter(ip_pool_resource::ip_pool_id.eq(ip_pool_id))
            .filter(ip_pool_resource::resource_id.eq(resource_id))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error deleting IP pool association to resource: {:?}",
                    e
                ))
            })
    }

    pub async fn ip_pool_list_ranges(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        pag_params: &DataPageParams<'_, IpNetwork>,
    ) -> ListResultVec<IpPoolRange> {
        use db::schema::ip_pool_range::dsl;
        opctx.authorize(authz::Action::ListChildren, authz_pool).await?;
        paginated(dsl::ip_pool_range, dsl::first_address, pag_params)
            .filter(dsl::ip_pool_id.eq(authz_pool.id()))
            .filter(dsl::time_deleted.is_null())
            .select(IpPoolRange::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                )
            })
    }

    pub async fn ip_pool_add_range(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        range: &IpRange,
    ) -> CreateResult<IpPoolRange> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::ip_pool_add_range_on_connection(&conn, opctx, authz_pool, range)
            .await
    }

    /// Variant of [Self::ip_pool_add_range] which may be called from a
    /// transaction context.
    pub(crate) async fn ip_pool_add_range_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        range: &IpRange,
    ) -> CreateResult<IpPoolRange> {
        use db::schema::ip_pool_range::dsl;
        opctx.authorize(authz::Action::CreateChild, authz_pool).await?;
        let pool_id = authz_pool.id();
        let new_range = IpPoolRange::new(range, pool_id);
        let filter_subquery = FilterOverlappingIpRanges { range: new_range };
        let insert_query =
            diesel::insert_into(dsl::ip_pool_range).values(filter_subquery);
        IpPool::insert_resource(pool_id, insert_query)
            .insert_and_get_result_async(conn)
            .await
            .map_err(|e| {
                use diesel::result::Error::NotFound;

                match e {
                    AsyncInsertError::CollectionNotFound => {
                        Error::ObjectNotFound {
                            type_name: ResourceType::IpPool,
                            lookup_type: LookupType::ById(pool_id),
                        }
                    }
                    AsyncInsertError::DatabaseError(NotFound) => {
                        // We've filtered out the IP addresses the client provided,
                        // i.e., there's some overlap with existing addresses.
                        Error::invalid_request(
                            format!(
                                "The provided IP range {}-{} overlaps with \
                            an existing range",
                                range.first_address(),
                                range.last_address(),
                            )
                            .as_str(),
                        )
                    }
                    AsyncInsertError::DatabaseError(err) => {
                        public_error_from_diesel(err, ErrorHandler::Server)
                    }
                }
            })
    }

    pub async fn ip_pool_delete_range(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        range: &IpRange,
    ) -> DeleteResult {
        use db::schema::external_ip;
        use db::schema::ip_pool_range::dsl;
        opctx.authorize(authz::Action::Modify, authz_pool).await?;

        let pool_id = authz_pool.id();
        let first_address = range.first_address();
        let last_address = range.last_address();
        let first_net = ipnetwork::IpNetwork::from(first_address);
        let last_net = ipnetwork::IpNetwork::from(last_address);

        // Fetch the range itself, if it exists. We'll need to protect against
        // concurrent inserts of new external IPs from the target range by
        // comparing the rcgen.
        let conn = self.pool_connection_authorized(opctx).await?;
        let range = dsl::ip_pool_range
            .filter(dsl::ip_pool_id.eq(pool_id))
            .filter(dsl::first_address.eq(first_net))
            .filter(dsl::last_address.eq(last_net))
            .filter(dsl::time_deleted.is_null())
            .select(IpPoolRange::as_select())
            .get_result_async::<IpPoolRange>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .ok_or_else(|| {
                Error::invalid_request(
                    format!(
                        "The provided range {}-{} does not exist",
                        first_address, last_address,
                    )
                    .as_str(),
                )
            })?;

        // Find external IPs allocated out of this pool and range.
        let range_id = range.id;
        let has_children = diesel::dsl::select(diesel::dsl::exists(
            external_ip::table
                .filter(external_ip::dsl::ip_pool_id.eq(pool_id))
                .filter(external_ip::dsl::ip_pool_range_id.eq(range_id))
                .filter(external_ip::dsl::time_deleted.is_null()),
        ))
        .get_result_async::<bool>(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if has_children {
            return Err(Error::invalid_request(
                "IP pool ranges cannot be deleted while \
                    external IP addresses are allocated from them",
            ));
        }

        // Delete the range, conditional on the rcgen not having changed. This
        // protects the delete from occuring if clients allocated a new external
        // IP address in between the above check for children and this query.
        let rcgen = range.rcgen;
        let now = Utc::now();
        let updated_rows = diesel::update(
            dsl::ip_pool_range
                .find(range_id)
                .filter(dsl::time_deleted.is_null())
                .filter(dsl::rcgen.eq(rcgen)),
        )
        .set(dsl::time_deleted.eq(now))
        .execute_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if updated_rows == 1 {
            Ok(())
        } else {
            Err(Error::invalid_request(
                "IP range deletion failed due to concurrent modification",
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::db::datastore::datastore_test;
    use crate::db::fixed_data::FLEET_ID;
    use crate::db::model::{IpPool, IpPoolResource, IpPoolResourceType};
    use assert_matches::assert_matches;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::identity::Resource;
    use omicron_common::api::external::{Error, IdentityMetadataCreateParams};
    use omicron_test_utils::dev;

    // TODO: add calls to the list endpoint throughout all this

    #[tokio::test]
    async fn test_default_ip_pools() {
        let logctx = dev::test_setup_log("test_default_ip_pools");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // we start out with the default fleet-level pool already created,
        // so when we ask for a default silo, we get it back
        let fleet_default_pool =
            datastore.ip_pools_fetch_default(&opctx).await.unwrap();

        assert_eq!(fleet_default_pool.identity.name.as_str(), "default");

        // unique index prevents second fleet-level default
        let identity = IdentityMetadataCreateParams {
            name: "another-fleet-default".parse().unwrap(),
            description: "".to_string(),
        };
        let second_default = datastore
            .ip_pool_create(&opctx, IpPool::new(&identity))
            .await
            .expect("Failed to create pool");
        let err = datastore
            .ip_pool_associate_resource(
                &opctx,
                IpPoolResource {
                    ip_pool_id: second_default.id(),
                    resource_type: IpPoolResourceType::Fleet,
                    resource_id: *FLEET_ID,
                    is_default: true,
                },
            )
            .await
            .expect_err("Failed to fail to make IP pool fleet default");

        assert_matches!(err, Error::ObjectAlreadyExists { .. });

        // now test logic preferring most specific available default

        let silo_id = opctx.authn.silo_required().unwrap().id();

        // create a non-default pool for the silo
        let identity = IdentityMetadataCreateParams {
            name: "pool1-for-silo".parse().unwrap(),
            description: "".to_string(),
        };
        let pool1_for_silo = datastore
            .ip_pool_create(&opctx, IpPool::new(&identity))
            .await
            .expect("Failed to create IP pool");
        datastore
            .ip_pool_associate_resource(
                &opctx,
                IpPoolResource {
                    ip_pool_id: pool1_for_silo.id(),
                    resource_type: IpPoolResourceType::Silo,
                    resource_id: silo_id,
                    is_default: false,
                },
            )
            .await
            .expect("Failed to associate IP pool with silo");

        // because that one was not a default, when we ask for the silo default
        // pool, we still get the fleet default
        let ip_pool = datastore
            .ip_pools_fetch_default(&opctx)
            .await
            .expect("Failed to get default IP pool");
        assert_eq!(ip_pool.id(), fleet_default_pool.id());

        // now we can change that association to is_default=true and
        // it should update rather than erroring out
        datastore
            .ip_pool_associate_resource(
                &opctx,
                IpPoolResource {
                    ip_pool_id: pool1_for_silo.id(),
                    resource_type: IpPoolResourceType::Silo,
                    resource_id: silo_id,
                    is_default: true,
                },
            )
            .await
            .expect("Failed to make IP pool default for silo");

        // now when we ask for the default pool again, we get the one we just changed
        let ip_pool = datastore
            .ip_pools_fetch_default(&opctx)
            .await
            .expect("Failed to get silo's default IP pool");
        assert_eq!(ip_pool.name().as_str(), "pool1-for-silo");

        // and we can't create a second default pool for the silo
        let identity = IdentityMetadataCreateParams {
            name: "second-default-for-silo".parse().unwrap(),
            description: "".to_string(),
        };
        let second_silo_default = datastore
            .ip_pool_create(&opctx, IpPool::new(&identity))
            .await
            .expect("Failed to create pool");
        let err = datastore
            .ip_pool_associate_resource(
                &opctx,
                IpPoolResource {
                    ip_pool_id: second_silo_default.id(),
                    resource_type: IpPoolResourceType::Silo,
                    resource_id: silo_id,
                    is_default: true,
                },
            )
            .await
            .expect_err("Failed to fail to set a second default pool for silo");
        assert_matches!(err, Error::ObjectAlreadyExists { .. });

        // now remove the association and we should get the default fleet pool again
        datastore
            .ip_pool_dissociate_resource(&opctx, pool1_for_silo.id(), silo_id)
            .await
            .expect("Failed to dissociate IP pool from silo");

        let ip_pool = datastore
            .ip_pools_fetch_default(&opctx)
            .await
            .expect("Failed to get default IP pool");
        assert_eq!(ip_pool.id(), fleet_default_pool.id());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
