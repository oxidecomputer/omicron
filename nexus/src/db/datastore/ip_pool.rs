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
use crate::db::error::diesel_pool_result_optional;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::db::model::IpPool;
use crate::db::model::IpPoolRange;
use crate::db::model::IpPoolUpdate;
use crate::db::model::Name;
use crate::db::pagination::paginated;
use crate::db::queries::ip_pool::FilterOverlappingIpRanges;
use crate::external_api::params;
use crate::external_api::shared::IpRange;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use ipnetwork::IpNetwork;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl DataStore {
    /// List IP Pools by their name
    pub async fn ip_pools_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<IpPool> {
        use db::schema::ip_pool::dsl;
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;
        paginated(dsl::ip_pool, dsl::name, pagparams)
            .filter(dsl::rack_id.is_null())
            .filter(dsl::time_deleted.is_null())
            .select(db::model::IpPool::as_select())
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// List IP Pools by their IDs
    pub async fn ip_pools_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<IpPool> {
        use db::schema::ip_pool::dsl;
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;
        paginated(dsl::ip_pool, dsl::id, pagparams)
            .filter(dsl::rack_id.is_null())
            .filter(dsl::time_deleted.is_null())
            .select(db::model::IpPool::as_select())
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Looks up an IP pool by a particular Rack ID.
    ///
    /// An index exists to look up pools by rack ID, but it is not a primary
    /// key, which requires this lookup function to be used instead of the
    /// [`LookupPath`] utility.
    pub async fn ip_pools_lookup_by_rack_id(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
    ) -> LookupResult<(authz::IpPool, IpPool)> {
        use db::schema::ip_pool::dsl;

        // Ensure the caller has the ability to look up these IP pools.
        // If they don't, return "not found" instead of "forbidden".
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await
            .map_err(|e| match e {
                Error::Forbidden => {
                    LookupType::ByCompositeId(format!("Rack ID: {rack_id}"))
                        .into_not_found(ResourceType::IpPool)
                }
                _ => e,
            })?;

        // Look up this IP pool by rack ID.
        let (authz_pool, pool) = dsl::ip_pool
            .filter(dsl::rack_id.eq(Some(rack_id)))
            .filter(dsl::time_deleted.is_null())
            .select(IpPool::as_select())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
            .map(|ip_pool| {
                (
                    authz::IpPool::new(
                        authz::FLEET,
                        ip_pool.id(),
                        LookupType::ByCompositeId(format!(
                            "Rack ID: {rack_id}"
                        )),
                    ),
                    ip_pool,
                )
            })?;
        Ok((authz_pool, pool))
    }

    pub fn ip_pools_lookup_by_rack_id_sync(
        conn: &mut crate::db::pool::DbConnection,
        rack_id: Uuid,
    ) -> Result<IpPool, diesel::result::Error> {
        use db::schema::ip_pool::dsl;

        // Look up this IP pool by rack ID.
        dsl::ip_pool
            .filter(dsl::rack_id.eq(Some(rack_id)))
            .filter(dsl::time_deleted.is_null())
            .select(IpPool::as_select())
            .get_result(conn)
    }

    /// Creates a new IP pool.
    ///
    /// - If `rack_id` is provided, this IP pool is used for Oxide
    /// services.
    pub async fn ip_pool_create(
        &self,
        opctx: &OpContext,
        new_pool: &params::IpPoolCreate,
        rack_id: Option<Uuid>,
    ) -> CreateResult<IpPool> {
        use db::schema::ip_pool::dsl;
        opctx
            .authorize(authz::Action::CreateChild, &authz::IP_POOL_LIST)
            .await?;
        let project_id = match new_pool.project.clone() {
            None => None,
            Some(project) => {
                if let Some(_) = &rack_id {
                    return Err(Error::invalid_request(
                        "Internal Service IP pools cannot be project-scoped",
                    ));
                }

                let (.., authz_project) = LookupPath::new(opctx, self)
                    .organization_name(&Name(project.organization))
                    .project_name(&Name(project.project))
                    .lookup_for(authz::Action::Read)
                    .await?;
                Some(authz_project.id())
            }
        };
        let pool = IpPool::new(&new_pool.identity, project_id, rack_id);
        let pool_name = pool.name().as_str().to_string();
        diesel::insert_into(dsl::ip_pool)
            .values(pool)
            .returning(IpPool::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
        let range = diesel_pool_result_optional(
            ip_pool_range::dsl::ip_pool_range
                .filter(ip_pool_range::dsl::ip_pool_id.eq(authz_pool.id()))
                .filter(ip_pool_range::dsl::time_deleted.is_null())
                .select(ip_pool_range::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
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
            .filter(dsl::rack_id.is_null())
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_pool.id()))
            .filter(dsl::rcgen.eq(db_pool.rcgen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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

    pub async fn ip_pool_update(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        updates: IpPoolUpdate,
    ) -> UpdateResult<IpPool> {
        use db::schema::ip_pool::dsl;
        opctx.authorize(authz::Action::Modify, authz_pool).await?;
        diesel::update(dsl::ip_pool)
            .filter(dsl::rack_id.is_null())
            .filter(dsl::id.eq(authz_pool.id()))
            .filter(dsl::time_deleted.is_null())
            .set(updates)
            .returning(IpPool::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                )
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
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                )
            })
    }

    pub async fn ip_pool_add_range(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        db_pool: &IpPool,
        range: &IpRange,
    ) -> CreateResult<IpPoolRange> {
        use db::schema::ip_pool_range::dsl;
        opctx.authorize(authz::Action::CreateChild, authz_pool).await?;
        let pool_id = authz_pool.id();
        let new_range = IpPoolRange::new(range, pool_id, db_pool.project_id);
        let filter_subquery = FilterOverlappingIpRanges { range: new_range };
        let insert_query =
            diesel::insert_into(dsl::ip_pool_range).values(filter_subquery);
        IpPool::insert_resource(pool_id, insert_query)
            .insert_and_get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                use async_bb8_diesel::ConnectionError::Query;
                use async_bb8_diesel::PoolError::Connection;
                use diesel::result::Error::NotFound;

                match e {
                    AsyncInsertError::DatabaseError(Connection(Query(
                        NotFound,
                    ))) => {
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
                    AsyncInsertError::CollectionNotFound => {
                        Error::ObjectNotFound {
                            type_name: ResourceType::IpPool,
                            lookup_type: LookupType::ById(pool_id),
                        }
                    }
                    AsyncInsertError::DatabaseError(err) => {
                        public_error_from_diesel_pool(err, ErrorHandler::Server)
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
        let range = diesel_pool_result_optional(
            dsl::ip_pool_range
                .filter(dsl::ip_pool_id.eq(pool_id))
                .filter(dsl::first_address.eq(first_net))
                .filter(dsl::last_address.eq(last_net))
                .filter(dsl::time_deleted.is_null())
                .select(IpPoolRange::as_select())
                .get_result_async::<IpPoolRange>(
                    self.pool_authorized(opctx).await?,
                )
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?
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
        .get_result_async::<bool>(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
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
        .execute_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        if updated_rows == 1 {
            Ok(())
        } else {
            Err(Error::invalid_request(
                "IP range deletion failed due to concurrent modification",
            ))
        }
    }
}
