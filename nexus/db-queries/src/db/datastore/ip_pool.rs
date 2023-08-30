// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`IpPool`]s.

use super::DataStore;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::diesel_pool_result_optional;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::fixed_data::silo::INTERNAL_SILO_ID;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::db::model::IpPool;
use crate::db::model::IpPoolRange;
use crate::db::model::IpPoolUpdate;
use crate::db::model::Name;
use crate::db::pagination::paginated;
use crate::db::pool::DbConnection;
use crate::db::queries::ip_pool::FilterOverlappingIpRanges;
use async_bb8_diesel::{AsyncRunQueryDsl, PoolError};
use chrono::Utc;
use diesel::prelude::*;
use ipnetwork::IpNetwork;
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
        use db::schema::ip_pool::dsl;
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::ip_pool, dsl::id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::ip_pool,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        // != excludes nulls so we explicitly include them
        .filter(dsl::silo_id.ne(*INTERNAL_SILO_ID).or(dsl::silo_id.is_null()))
        .filter(dsl::time_deleted.is_null())
        .select(db::model::IpPool::as_select())
        .get_results_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Looks up the default IP pool for a given scope, i.e., a given
    /// combination of silo and project ID (or none). If there is no default at
    /// a given scope, fall back up a level. There should always be a default at
    /// fleet level, though this query can theoretically fail.
    pub async fn ip_pools_fetch_default_for(
        &self,
        opctx: &OpContext,
        action: authz::Action,
        silo_id: Option<Uuid>,
        project_id: Option<Uuid>,
    ) -> LookupResult<IpPool> {
        use db::schema::ip_pool::dsl;
        opctx.authorize(action, &authz::IP_POOL_LIST).await?;

        dsl::ip_pool
            .filter(dsl::silo_id.eq(silo_id).or(dsl::silo_id.is_null()))
            .filter(
                dsl::project_id.eq(project_id).or(dsl::project_id.is_null()),
            )
            .filter(dsl::default.eq(true))
            .filter(dsl::time_deleted.is_null())
            // this will sort by most specific first, i.e.,
            //
            //   (silo, project)
            //   (silo, null)
            //   (null, null)
            //
            // then by only taking the first result, we get the most specific one
            .order((
                dsl::project_id.asc().nulls_last(),
                dsl::silo_id.asc().nulls_last(),
            ))
            .select(IpPool::as_select())
            .first_async::<IpPool>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Looks up an IP pool by name.
    pub(crate) async fn ip_pools_fetch_for(
        &self,
        opctx: &OpContext,
        action: authz::Action,
        name: &Name,
    ) -> LookupResult<(authz::IpPool, IpPool)> {
        let (.., authz_pool, pool) = LookupPath::new(opctx, &self)
            .ip_pool_name(&name)
            .fetch_for(action)
            .await?;
        // Can't look up the internal pool
        if pool.silo_id == Some(*INTERNAL_SILO_ID) {
            return Err(authz_pool.not_found());
        }

        Ok((authz_pool, pool))
    }

    /// Looks up an IP pool intended for internal services.
    ///
    /// This method may require an index by Availability Zone in the future.
    pub async fn ip_pools_service_lookup(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<(authz::IpPool, IpPool)> {
        use db::schema::ip_pool::dsl;

        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;

        // Look up this IP pool by rack ID.
        let (authz_pool, pool) = dsl::ip_pool
            .filter(dsl::silo_id.eq(*INTERNAL_SILO_ID))
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
            // != excludes nulls so we explicitly include them
            .filter(
                dsl::silo_id.ne(*INTERNAL_SILO_ID).or(dsl::silo_id.is_null()),
            )
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
            // != excludes nulls so we explicitly include them
            .filter(
                dsl::silo_id.ne(*INTERNAL_SILO_ID).or(dsl::silo_id.is_null()),
            )
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
        range: &IpRange,
    ) -> CreateResult<IpPoolRange> {
        let conn = self.pool_authorized(opctx).await?;
        Self::ip_pool_add_range_on_connection(conn, opctx, authz_pool, range)
            .await
    }

    /// Variant of [Self::ip_pool_add_range] which may be called from a
    /// transaction context.
    pub(crate) async fn ip_pool_add_range_on_connection<ConnErr>(
        conn: &(impl async_bb8_diesel::AsyncConnection<DbConnection, ConnErr>
              + Sync),
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        range: &IpRange,
    ) -> CreateResult<IpPoolRange>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
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

#[cfg(test)]
mod test {
    use crate::authz;
    use crate::db::datastore::datastore_test;
    use crate::db::model::IpPool;
    use assert_matches::assert_matches;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::identity::Resource;
    use omicron_common::api::external::{Error, IdentityMetadataCreateParams};
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_default_ip_pools() {
        let logctx = dev::test_setup_log("test_default_ip_pools");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let action = authz::Action::ListChildren;

        // we start out with the default fleet-level pool already created,
        // so when we ask for the fleet default (no silo or project) we get it back
        let fleet_default_pool = datastore
            .ip_pools_fetch_default_for(&opctx, action, None, None)
            .await
            .unwrap();

        assert_eq!(fleet_default_pool.identity.name.as_str(), "default");
        assert!(fleet_default_pool.default);
        assert_eq!(fleet_default_pool.silo_id, None);
        assert_eq!(fleet_default_pool.project_id, None);

        // unique index prevents second fleet-level default
        let identity = IdentityMetadataCreateParams {
            name: "another-fleet-default".parse().unwrap(),
            description: "".to_string(),
        };
        let err = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new(&identity, None, /*default= */ true),
            )
            .await
            .expect_err("Failed to fail to create a second default fleet pool");
        assert_matches!(err, Error::ObjectAlreadyExists { .. });

        // now the interesting thing is that when we fetch the default pool for
        // a particular silo or a particular project, if those scopes do not
        // have a default IP pool, we will still get back the fleet default

        // default for "current" silo is still the fleet default one because it
        // has no default of its own
        let silo_id = opctx.authn.silo_required().unwrap().id();
        let ip_pool = datastore
            .ip_pools_fetch_default_for(&opctx, action, Some(silo_id), None)
            .await
            .expect("Failed to get silo's default IP pool");
        assert_eq!(ip_pool.id(), fleet_default_pool.id());

        // create a non-default pool for the silo
        let identity = IdentityMetadataCreateParams {
            name: "non-default-for-silo".parse().unwrap(),
            description: "".to_string(),
        };
        let _ = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new(&identity, Some(silo_id), /*default= */ false),
            )
            .await;

        // because that one was not a default, when we ask for silo default
        // pool, we still get the fleet default
        let ip_pool = datastore
            .ip_pools_fetch_default_for(&opctx, action, Some(silo_id), None)
            .await
            .expect("Failed to get fleet default IP pool");
        assert_eq!(ip_pool.id(), fleet_default_pool.id());

        // now create a default pool for the silo
        let identity = IdentityMetadataCreateParams {
            name: "default-for-silo".parse().unwrap(),
            description: "".to_string(),
        };
        let _ = datastore
            .ip_pool_create(&opctx, IpPool::new(&identity, Some(silo_id), true))
            .await;

        // now when we ask for the silo default pool, we get the one we just made
        let ip_pool = datastore
            .ip_pools_fetch_default_for(&opctx, action, Some(silo_id), None)
            .await
            .expect("Failed to get silo's default IP pool");
        assert_eq!(ip_pool.name().as_str(), "default-for-silo");

        // and of course, if we ask for the fleet default again we still get that one
        let ip_pool = datastore
            .ip_pools_fetch_default_for(&opctx, action, None, None)
            .await
            .expect("Failed to get fleet default IP pool");
        assert_eq!(ip_pool.id(), fleet_default_pool.id());

        // and we can't create a second default pool for the silo
        let identity = IdentityMetadataCreateParams {
            name: "second-default-for-silo".parse().unwrap(),
            description: "".to_string(),
        };
        let err = datastore
            .ip_pool_create(&opctx, IpPool::new(&identity, Some(silo_id), true))
            .await
            .expect_err("Failed to fail to create second default pool");
        assert_matches!(err, Error::ObjectAlreadyExists { .. });

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
