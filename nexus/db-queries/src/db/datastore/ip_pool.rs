// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`IpPool`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::SERVICE_IPV4_POOL_NAME;
use crate::db::datastore::SERVICE_IPV6_POOL_NAME;
use crate::db::identity::Resource;
use crate::db::model::IpKind;
use crate::db::model::IpPool;
use crate::db::model::IpPoolRange;
use crate::db::model::IpPoolReservationType;
use crate::db::model::IpPoolResource;
use crate::db::model::IpPoolResourceType;
use crate::db::model::IpPoolUpdate;
use crate::db::model::Name;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::queries::ip_pool::FilterOverlappingIpRanges;
use crate::db::raw_query_builder::QueryBuilder;
use crate::db::raw_query_builder::SelectableSql;
use crate::db::raw_query_builder::TypedSqlQuery;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use ipnetwork::IpNetwork;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_errors::public_error_from_diesel_lookup;
use nexus_db_lookup::DbConnection;
use nexus_db_lookup::LookupPath;
use nexus_db_model::InternetGateway;
use nexus_db_model::InternetGatewayIpPool;
use nexus_db_model::IpVersion;
use nexus_db_model::Project;
use nexus_db_model::Vpc;
use nexus_db_schema::enums::IpKindEnum;
use nexus_db_schema::enums::IpPoolReservationTypeEnum;
use nexus_types::external_api::shared::IpRange;
use nexus_types::silo::INTERNAL_SILO_ID;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use ref_cast::RefCast;
use uuid::Uuid;

/// Helper type with both an authz IP Pool and the actual DB record.
#[derive(Debug, Clone)]
pub struct ServiceIpPool {
    pub authz_pool: authz::IpPool,
    pub db_pool: IpPool,
}

/// Helper type with service IP Pool information for both IP versions.
#[derive(Debug, Clone)]
pub struct ServiceIpPools {
    pub ipv4: ServiceIpPool,
    pub ipv6: ServiceIpPool,
}

impl ServiceIpPools {
    /// Return the IP Pool appropriate for a range, based on its version.
    pub fn pool_for_range(&self, range: &IpRange) -> &ServiceIpPool {
        if range.first_address().is_ipv4() { &self.ipv4 } else { &self.ipv6 }
    }

    /// Return the IP Pool appropriate for an IP version.
    pub fn pool_for_version(&self, version: IpVersion) -> &IpPool {
        match version {
            IpVersion::V4 => &self.ipv4.db_pool,
            IpVersion::V6 => &self.ipv6.db_pool,
        }
    }
}

// Error message emitted when a user attempts to link an IP Pool and internal
// Silo, but the pool is already reserved for internal use, or vice versa.
const BAD_SILO_LINK_ERROR: &str = "IP Pools cannot be both linked to external \
    Silos and reserved for internal Oxide usage.";

// Error message emitted when a user attempts to unlink an IP Pool from a Silo
// while the pool has external IP addresses allocated from it.
const POOL_HAS_IPS_ERROR: &str =
    "IP addresses from this pool are in use in the linked silo";

// Error message emitted when a user attempts to unlink an IP Pool from the
// Oxide internal Silo, without at least one other IP Pool linked to it.
const LAST_POOL_ERROR: &str = "Cannot delete the last IP Pool reserved for \
    Oxide internal usage. Create and reserve at least one more IP Pool \
    before deleting this one.";

impl DataStore {
    /// List IP Pools by their reservation type and optionally IP version, paginated.
    pub async fn ip_pools_list_paginated(
        &self,
        opctx: &OpContext,
        reservation_type: IpPoolReservationType,
        version: Option<IpVersion>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<IpPool> {
        use nexus_db_schema::schema::ip_pool;
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;
        let mut query = match pagparams {
            PaginatedBy::Id(by_id) => {
                paginated(ip_pool::table, ip_pool::id, by_id)
            }
            PaginatedBy::Name(by_name) => paginated(
                ip_pool::table,
                ip_pool::name,
                &by_name.map_name(|n| Name::ref_cast(n)),
            ),
        };
        query = match version {
            Some(ver) => query.filter(ip_pool::ip_version.eq(ver)),
            None => query,
        };
        query
            .filter(ip_pool::time_deleted.is_null())
            .filter(ip_pool::reservation_type.eq(reservation_type))
            .select(IpPool::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List IP Pools
    ///
    /// This returns the pools available for external customer use.
    pub async fn ip_pools_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<IpPool> {
        self.ip_pools_list_paginated(
            opctx,
            IpPoolReservationType::ExternalSilos,
            None,
            pagparams,
        )
        .await
    }

    /// Look up whether the given pool is available to users in the current
    /// silo, i.e., whether there is an entry in the association table linking
    /// the pool with that silo
    //
    // TODO-correctness: This seems difficult to use without TOCTOU issues. It's
    // currently used to ensure there's a link between a Silo and an IP Pool
    // when allocating an external address for an instance in that Silo. But
    // that works by checking that the link exists, and then in a separate
    // query, allocating an address out of it. Suppose the silo was unlinked
    // after this check, but before the external address allocation query ran.
    // Then one could end up with an address from an unlinked IP Pool, which
    // seems wrong.
    //
    // See https://github.com/oxidecomputer/omicron/issues/8992
    pub async fn ip_pool_fetch_link(
        &self,
        opctx: &OpContext,
        ip_pool_id: Uuid,
    ) -> LookupResult<IpPoolResource> {
        use nexus_db_schema::schema::ip_pool;
        use nexus_db_schema::schema::ip_pool_resource;

        let authz_silo = opctx.authn.silo_required().internal_context(
            "fetching link from an IP pool to current silo",
        )?;

        ip_pool::table
            .inner_join(ip_pool_resource::table)
            .filter(
                ip_pool_resource::resource_type
                    .eq(IpPoolResourceType::Silo)
                    .and(ip_pool_resource::resource_id.eq(authz_silo.id())),
            )
            .filter(ip_pool::id.eq(ip_pool_id))
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
    ) -> LookupResult<(authz::IpPool, IpPool)> {
        use nexus_db_schema::schema::ip_pool;
        use nexus_db_schema::schema::ip_pool_resource;

        let authz_silo_id = opctx.authn.silo_required()?.id();

        // TODO: Need auth check here. Only fleet viewers can list children on
        // IP_POOL_LIST, so if we check that, nobody can make instances. This
        // used to check CreateChild on an individual IP pool, but now we're not
        // looking up by name so the check is more complicated
        //
        // opctx
        //     .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
        //     .await?;

        let lookup_type =
            LookupType::ByOther("default IP pool for current silo".to_string());

        ip_pool::table
            .inner_join(ip_pool_resource::table)
            .filter(
                ip_pool_resource::resource_type.eq(IpPoolResourceType::Silo),
            )
            .filter(ip_pool_resource::resource_id.eq(authz_silo_id))
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
            .map_err(|e| {
                public_error_from_diesel_lookup(
                    e,
                    ResourceType::IpPool,
                    &lookup_type,
                )
            })
            .map(|ip_pool| {
                let authz_pool =
                    authz::IpPool::new(authz::FLEET, ip_pool.id(), lookup_type);
                (authz_pool, ip_pool)
            })
    }

    /// Look up internal service IP Pools for both IP versions.
    ///
    /// This is useful when you need to handle resources like external IPs where
    /// the actual address might be from either IP version.
    //
    // TODO-remove: Use list_ip_pools_for_internal instead.
    //
    // See https://github.com/oxidecomputer/omicron/issues/8947.
    pub async fn ip_pools_service_lookup_both_versions(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<ServiceIpPools> {
        let ipv4 = self.ip_pools_service_lookup(opctx, IpVersion::V4).await?;
        let ipv6 = self.ip_pools_service_lookup(opctx, IpVersion::V6).await?;
        Ok(ServiceIpPools {
            ipv4: ServiceIpPool { authz_pool: ipv4.0, db_pool: ipv4.1 },
            ipv6: ServiceIpPool { authz_pool: ipv6.0, db_pool: ipv6.1 },
        })
    }

    /// Look up IP pool intended for internal services by their well-known
    /// names. There are separate IP Pools for IPv4 and IPv6 address ranges.
    ///
    /// This method may require an index by Availability Zone in the future.
    //
    // TODO-remove: Use ip_pools_list_paginated with the right enum type
    // instead.
    //
    // See https://github.com/oxidecomputer/omicron/issues/8947.
    pub async fn ip_pools_service_lookup(
        &self,
        opctx: &OpContext,
        ip_version: IpVersion,
    ) -> LookupResult<(authz::IpPool, IpPool)> {
        let name = match ip_version {
            IpVersion::V4 => SERVICE_IPV4_POOL_NAME,
            IpVersion::V6 => SERVICE_IPV6_POOL_NAME,
        };
        let name =
            Name(name.parse().expect("should be able to parse builtin names"));
        LookupPath::new(&opctx, self).ip_pool_name(&name).fetch().await
    }

    /// Creates a new IP pool.
    pub async fn ip_pool_create(
        &self,
        opctx: &OpContext,
        pool: IpPool,
    ) -> CreateResult<IpPool> {
        use nexus_db_schema::schema::ip_pool::dsl;
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

    /// Delete an IP Pool, and any links between it an any Silos.
    ///
    /// This fails if there are still IP Ranges in the pool, or if we're
    /// deleting the last pool reserved for Oxide use.
    pub async fn ip_pool_delete(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        db_pool: &IpPool,
    ) -> DeleteResult {
        use nexus_db_schema::schema::ip_pool::dsl;
        use nexus_db_schema::schema::ip_pool_range;
        use nexus_db_schema::schema::ip_pool_resource;
        opctx.authorize(authz::Action::Delete, authz_pool).await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        // Verify there are no IP ranges still in this pool
        let range = ip_pool_range::dsl::ip_pool_range
            .filter(ip_pool_range::dsl::ip_pool_id.eq(authz_pool.id()))
            .filter(ip_pool_range::dsl::time_deleted.is_null())
            .select(ip_pool_range::dsl::id)
            .limit(1)
            .first_async::<Uuid>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if range.is_some() {
            return Err(Error::invalid_request(
                "IP Pool cannot be deleted while it contains IP ranges",
            ));
        }

        // Add a small subquery, if needed, to ensure that we don't delete this
        // IP Pool if it's the last reserved pool. There has to always be at
        // least one of these.
        let enough_reserved_pools = if matches!(
            db_pool.reservation_type,
            IpPoolReservationType::ExternalSilos
        ) {
            diesel::dsl::sql::<sql_types::Bool>("TRUE")
        } else {
            diesel::dsl::sql::<sql_types::Bool>(&count_reserved_pools_subquery(
                db_pool.reservation_type,
            ))
        };

        // Delete the pool, conditional on the rcgen not having changed. This
        // protects the delete from occuring if clients created a new IP range
        // in between the above check for children and this query.
        let now = Utc::now();
        let updated_rows = diesel::update(dsl::ip_pool)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_pool.id()))
            .filter(dsl::rcgen.eq(db_pool.rcgen))
            .filter(enough_reserved_pools)
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*conn)
            .await
            .map_err(|e| match e {
                DieselError::DatabaseError(
                    DatabaseErrorKind::Unknown,
                    ref info,
                ) if info.message().ends_with("invalid bool value") => {
                    Error::invalid_request(LAST_POOL_ERROR)
                }
                _ => public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                ),
            })?;

        if updated_rows == 0 {
            return Err(Error::invalid_request(
                "deletion failed due to concurrent modification",
            ));
        }

        // Rather than treating outstanding links as a blocker for pool delete,
        // just delete them. If we've gotten this far, we know there are no
        // ranges in the pool, which means it can't be in use.

        // delete any links from this pool to any other resources (silos)
        diesel::delete(ip_pool_resource::table)
            .filter(ip_pool_resource::ip_pool_id.eq(authz_pool.id()))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Check whether the pool is internal by checking that it exists and is
    /// associated with the internal silo
    //
    // TODO-remove: This should go away when we let operators reserve any IP
    // Pools for internal Oxide usage. The pool belongs to them even in that
    // case, and so we should show it to them.
    //
    // See https://github.com/oxidecomputer/omicron/issues/8947.
    pub async fn ip_pool_is_internal(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> LookupResult<bool> {
        use nexus_db_schema::schema::ip_pool;
        ip_pool::table
            .find(authz_pool.id())
            .filter(ip_pool::time_deleted.is_null())
            .select(
                ip_pool::reservation_type
                    .eq(IpPoolReservationType::OxideInternal),
            )
            .first_async::<bool>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map(|result| result.unwrap_or(false))
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn ip_pool_update(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        updates: IpPoolUpdate,
    ) -> UpdateResult<IpPool> {
        use nexus_db_schema::schema::ip_pool::dsl;
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

    /// Reserve an IP Pool for a specific use.
    pub async fn ip_pool_reserve(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        db_pool: &IpPool,
        reservation_type: IpPoolReservationType,
    ) -> UpdateResult<()> {
        if db_pool.reservation_type == reservation_type {
            return Err(Error::invalid_request(format!(
                "IP Pool already has reservation type '{}'",
                reservation_type,
            )));
        }
        let n_rows = reserve_ip_pool_query(db_pool, reservation_type)
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| match e {
                DieselError::DatabaseError(
                    DatabaseErrorKind::Unknown,
                    ref info,
                ) => {
                    let message = info.message();
                    if message.ends_with("invalid bool value") {
                        Error::invalid_request(BAD_SILO_LINK_ERROR)
                    } else if message.contains("division by zero") {
                        Error::invalid_request(POOL_HAS_IPS_ERROR)
                    } else if message.starts_with("could not parse")
                        && message.contains("as type int")
                    {
                        Error::invalid_request(LAST_POOL_ERROR)
                    } else {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    }
                }
                _ => public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                ),
            })?;
        if n_rows == 0 {
            Err(Error::invalid_request(
                "update failed due to concurrent modification",
            ))
        } else {
            Ok(())
        }
    }

    /// Return the number of IPs allocated from and the capacity of the provided
    /// IP Pool.
    pub async fn ip_pool_utilization(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> Result<(i64, u128), Error> {
        opctx.authorize(authz::Action::Read, authz_pool).await?;
        opctx.authorize(authz::Action::ListChildren, authz_pool).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let (allocated, ranges) = self
            .transaction_retry_wrapper("ip_pool_utilization")
            .transaction(&conn, |conn| async move {
                let allocated = self
                    .ip_pool_allocated_count_on_connection(&conn, authz_pool)
                    .await?;
                let ranges = self
                    .ip_pool_list_ranges_batched_on_connection(
                        &conn, authz_pool,
                    )
                    .await?;
                Ok((allocated, ranges))
            })
            .await
            .map_err(|e| match &e {
                DieselError::NotFound => public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                ),
                _ => public_error_from_diesel(e, ErrorHandler::Server),
            })?;
        let capacity = Self::accumulate_ip_range_sizes(ranges)?;
        Ok((allocated, capacity))
    }

    /// Return the total number of IPs allocated from the provided pool.
    #[cfg(test)]
    async fn ip_pool_allocated_count(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> Result<i64, Error> {
        opctx.authorize(authz::Action::Read, authz_pool).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.ip_pool_allocated_count_on_connection(&conn, authz_pool)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    async fn ip_pool_allocated_count_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_pool: &authz::IpPool,
    ) -> Result<i64, DieselError> {
        use nexus_db_schema::schema::external_ip;
        external_ip::table
            .filter(external_ip::ip_pool_id.eq(authz_pool.id()))
            .filter(external_ip::time_deleted.is_null())
            .select(diesel::dsl::count_distinct(external_ip::ip))
            .first_async::<i64>(conn)
            .await
    }

    /// Return the total capacity of the provided pool.
    #[cfg(test)]
    async fn ip_pool_total_capacity(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> Result<u128, Error> {
        opctx.authorize(authz::Action::Read, authz_pool).await?;
        opctx.authorize(authz::Action::ListChildren, authz_pool).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.ip_pool_list_ranges_batched_on_connection(&conn, authz_pool)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                )
            })
            .and_then(Self::accumulate_ip_range_sizes)
    }

    async fn ip_pool_list_ranges_batched_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_pool: &authz::IpPool,
    ) -> Result<Vec<(IpNetwork, IpNetwork)>, DieselError> {
        use nexus_db_schema::schema::ip_pool_range;
        ip_pool_range::table
            .filter(ip_pool_range::ip_pool_id.eq(authz_pool.id()))
            .filter(ip_pool_range::time_deleted.is_null())
            .select((ip_pool_range::first_address, ip_pool_range::last_address))
            // This is a rare unpaginated DB query, which means we are
            // vulnerable to a resource exhaustion attack in which someone
            // creates a very large number of ranges in order to make this
            // query slow. In order to mitigate that, we limit the query to the
            // (current) max allowed page size, effectively making this query
            // exactly as vulnerable as if it were paginated. If there are more
            // than 10,000 ranges in a pool, we will undercount, but I have a
            // hard time seeing that as a practical problem.
            .limit(10000)
            .get_results_async::<(IpNetwork, IpNetwork)>(conn)
            .await
    }

    fn accumulate_ip_range_sizes(
        ranges: Vec<(IpNetwork, IpNetwork)>,
    ) -> Result<u128, Error> {
        let mut count: u128 = 0;
        for range in ranges.into_iter() {
            let first = range.0.ip();
            let last = range.1.ip();
            let r = IpRange::try_from((first, last))
                .map_err(|e| Error::internal_error(e.as_str()))?;
            match r {
                IpRange::V4(r) => count += u128::from(r.len()),
                IpRange::V6(r) => count += r.len(),
            }
        }
        Ok(count)
    }

    /// List Silos linked to the given IP Pool.
    pub async fn ip_pool_silo_list(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<IpPoolResource> {
        use nexus_db_schema::schema::ip_pool;
        use nexus_db_schema::schema::ip_pool_resource;
        use nexus_db_schema::schema::silo;

        paginated(
            ip_pool_resource::table,
            ip_pool_resource::resource_id,
            pagparams,
        )
        .inner_join(ip_pool::table)
        .inner_join(silo::table.on(silo::id.eq(ip_pool_resource::resource_id)))
        .filter(ip_pool::id.eq(authz_pool.id()))
        .filter(ip_pool::time_deleted.is_null())
        .filter(silo::time_deleted.is_null())
        .filter(silo::discoverable.eq(true))
        .select(IpPoolResource::as_select())
        .load_async::<IpPoolResource>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List IP Pools linked to the given Silo.
    ///
    /// Returns (IpPool, IpPoolResource) so we can know in the calling code
    /// whether the pool is default for the silo
    pub async fn silo_ip_pool_list(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<(IpPool, IpPoolResource)> {
        use nexus_db_schema::schema::ip_pool;
        use nexus_db_schema::schema::ip_pool_resource;

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
        .inner_join(ip_pool_resource::table)
        .filter(ip_pool_resource::resource_id.eq(authz_silo.id()))
        .filter(ip_pool_resource::resource_type.eq(IpPoolResourceType::Silo))
        .filter(ip_pool::time_deleted.is_null())
        .select(<(IpPool, IpPoolResource)>::as_select())
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Insert a link between an IP Pool and a Silo.
    pub async fn ip_pool_link_silo(
        &self,
        opctx: &OpContext,
        ip_pool_resource: IpPoolResource,
    ) -> CreateResult<IpPoolResource> {
        if ip_pool_resource.resource_id == INTERNAL_SILO_ID {
            return Err(Error::invalid_request(
                "IP Pools should not be linked to the internal Oxide silo. \
                    Reserve the Pool for `oxide_internal` use instead.",
            ));
        }
        opctx
            .authorize(authz::Action::CreateChild, &authz::IP_POOL_LIST)
            .await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let result = link_ip_pool_to_external_silo_query(&ip_pool_resource)
            .get_result_async(&*conn)
            .await
            .map_err(|e| {
                match e {
                    DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => {
                        public_error_from_diesel(
                            e,
                            ErrorHandler::Conflict(
                                ResourceType::IpPoolResource,
                                &format!(
                                    "ip_pool_id: {}, resource_id: {}, resource_type: {:?}",
                                    ip_pool_resource.ip_pool_id,
                                    ip_pool_resource.resource_id,
                                    ip_pool_resource.resource_type,
                                ),
                            )
                        )
                    }
                    // Handle intentional errors in the query.
                    DieselError::DatabaseError(DatabaseErrorKind::Unknown, ref info) => {
                        let is_uuid_cast_error = |msg: &str, sentinel: &str| -> bool {
                            // We're unfortunately allocating here, but this
                            // error path isn't expected to be common.
                            let expected = format!(
                                "uuid: incorrect UUID length: {}",
                                sentinel,
                            );
                            msg.ends_with(&expected)
                        };
                        let msg = info.message();
                        if is_uuid_cast_error(msg, BAD_SILO_LINK_SENTINEL) {
                            Error::invalid_request(BAD_SILO_LINK_ERROR)
                        } else if is_uuid_cast_error(msg, IP_POOL_DELETED_SENTINEL) {
                            Error::not_found_by_id(ResourceType::IpPool, &ip_pool_resource.ip_pool_id)
                        } else if is_uuid_cast_error(msg, SILO_DELETED_SENTINEL) {
                            Error::not_found_by_id(ResourceType::Silo, &ip_pool_resource.resource_id)
                        } else {
                            public_error_from_diesel(e, ErrorHandler::Server)
                        }
                    }
                    _ => public_error_from_diesel(e, ErrorHandler::Server),
                }
            })?;

        if ip_pool_resource.is_default {
            self.link_default_gateway(
                opctx,
                ip_pool_resource.resource_id,
                ip_pool_resource.ip_pool_id,
                &conn,
            )
            .await?;
        }

        Ok(result)
    }

    // TODO-correctness: This seems like it should be in a transaction. At
    // least, the nested-loops can mostly be re-expressed as a join between the
    // silos, projects, vpcs, and Internet gateway tables.
    //
    // See https://github.com/oxidecomputer/omicron/issues/8992.
    async fn link_default_gateway(
        &self,
        opctx: &OpContext,
        silo_id: Uuid,
        ip_pool_id: Uuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> UpdateResult<()> {
        use nexus_db_schema::schema::internet_gateway::dsl as igw_dsl;
        use nexus_db_schema::schema::internet_gateway_ip_pool::dsl as igw_ip_pool_dsl;
        use nexus_db_schema::schema::project::dsl as project_dsl;
        use nexus_db_schema::schema::vpc::dsl as vpc_dsl;

        let projects = project_dsl::project
            .filter(project_dsl::time_deleted.is_null())
            .filter(project_dsl::silo_id.eq(silo_id))
            .select(Project::as_select())
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        for project in &projects {
            let vpcs = vpc_dsl::vpc
                .filter(vpc_dsl::time_deleted.is_null())
                .filter(vpc_dsl::project_id.eq(project.id()))
                .select(Vpc::as_select())
                .load_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            for vpc in &vpcs {
                let igws = igw_dsl::internet_gateway
                    .filter(igw_dsl::time_deleted.is_null())
                    .filter(igw_dsl::name.eq("default"))
                    .filter(igw_dsl::vpc_id.eq(vpc.id()))
                    .select(InternetGateway::as_select())
                    .load_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

                for igw in &igws {
                    let igw_pool = InternetGatewayIpPool::new(
                        Uuid::new_v4(),
                        ip_pool_id,
                        igw.id(),
                        IdentityMetadataCreateParams {
                            name: "default".parse().unwrap(),
                            description: String::from(
                                "Default internet gateway ip pool",
                            ),
                        },
                    );

                    let _ipp: InternetGatewayIpPool =
                        match InternetGateway::insert_resource(
                            igw.id(),
                            diesel::insert_into(
                                igw_ip_pool_dsl::internet_gateway_ip_pool,
                            )
                            .values(igw_pool),
                        )
                        .insert_and_get_result_async(&conn)
                        .await {
                            Ok(x) => x,
                            Err(e) => match e {
                                AsyncInsertError::CollectionNotFound => {
                                    return Err(Error::not_found_by_name(
                                        ResourceType::InternetGateway,
                                        &"default".parse().unwrap(),
                                    ))
                                }
                                AsyncInsertError::DatabaseError(e) => match e {
                                    diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _) =>
                                    {
                                        return Ok(());
                                    }
                                    _ => return Err(public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )),
                                },
                            }
                        };
                }
                self.vpc_increment_rpw_version(opctx, vpc.id()).await?;
            }
        }
        Ok(())
    }

    // TODO-correctness: This should probably be in a transaction, collecting
    // all the Internet gateway IDs via a JOIN and then soft-deleting them all.
    //
    // See https://github.com/oxidecomputer/omicron/issues/8992.
    async fn unlink_ip_pool_gateway(
        &self,
        opctx: &OpContext,
        silo_id: Uuid,
        ip_pool_id: Uuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> UpdateResult<()> {
        use nexus_db_schema::schema::internet_gateway::dsl as igw_dsl;
        use nexus_db_schema::schema::internet_gateway_ip_pool::dsl as igw_ip_pool_dsl;
        use nexus_db_schema::schema::project::dsl as project_dsl;
        use nexus_db_schema::schema::vpc::dsl as vpc_dsl;

        let projects = project_dsl::project
            .filter(project_dsl::time_deleted.is_null())
            .filter(project_dsl::silo_id.eq(silo_id))
            .select(Project::as_select())
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        for project in &projects {
            let vpcs = vpc_dsl::vpc
                .filter(vpc_dsl::time_deleted.is_null())
                .filter(vpc_dsl::project_id.eq(project.id()))
                .select(Vpc::as_select())
                .load_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            for vpc in &vpcs {
                let igws = igw_dsl::internet_gateway
                    .filter(igw_dsl::time_deleted.is_null())
                    .filter(igw_dsl::vpc_id.eq(vpc.id()))
                    .select(InternetGateway::as_select())
                    .load_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

                for igw in &igws {
                    diesel::update(igw_ip_pool_dsl::internet_gateway_ip_pool)
                        .filter(igw_ip_pool_dsl::time_deleted.is_null())
                        .filter(
                            igw_ip_pool_dsl::internet_gateway_id.eq(igw.id()),
                        )
                        .filter(igw_ip_pool_dsl::ip_pool_id.eq(ip_pool_id))
                        .set(igw_ip_pool_dsl::time_deleted.eq(Utc::now()))
                        .execute_async(conn)
                        .await
                        .map_err(|e| {
                            public_error_from_diesel(e, ErrorHandler::Server)
                        })?;
                }
                self.vpc_increment_rpw_version(opctx, vpc.id()).await?;
            }
        }
        Ok(())
    }

    pub async fn ip_pool_set_default(
        &self,
        opctx: &OpContext,
        authz_ip_pool: &authz::IpPool,
        authz_silo: &authz::Silo,
        is_default: bool,
    ) -> UpdateResult<IpPoolResource> {
        use nexus_db_schema::schema::ip_pool_resource::dsl;

        opctx.authorize(authz::Action::Modify, authz_ip_pool).await?;
        opctx.authorize(authz::Action::Modify, authz_silo).await?;

        let ip_pool_id = authz_ip_pool.id();
        let silo_id = authz_silo.id();

        let conn = self.pool_connection_authorized(opctx).await?;

        // if we're making is_default false, we can just do that without
        // checking any other stuff
        if !is_default {
            let updated_link = diesel::update(dsl::ip_pool_resource)
                .filter(dsl::resource_id.eq(silo_id))
                .filter(dsl::ip_pool_id.eq(ip_pool_id))
                .filter(dsl::resource_type.eq(IpPoolResourceType::Silo))
                .set(dsl::is_default.eq(false))
                .returning(IpPoolResource::as_returning())
                .get_result_async(&*conn)
                .await
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "Transaction error: {:?}",
                        e
                    ))
                })?;
            return Ok(updated_link);
        }

        // Errors returned from the below transactions.
        #[derive(Debug)]
        enum IpPoolResourceUpdateError {
            FailedToUnsetDefault(DieselError),
        }
        type TxnError = TransactionError<IpPoolResourceUpdateError>;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("ip_pool_set_default")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    // note this is matching the specified silo, but could be any pool
                    let existing_default_for_silo = dsl::ip_pool_resource
                        .filter(dsl::resource_type.eq(IpPoolResourceType::Silo))
                        .filter(dsl::resource_id.eq(silo_id))
                        .filter(dsl::is_default.eq(true))
                        .select(IpPoolResource::as_select())
                        .get_result_async(&conn)
                        .await;

                    // if there is an existing default, we need to unset it before we can
                    // set the new default
                    if let Ok(existing_default) = existing_default_for_silo {
                        // if the pool we're making default is already default for this
                        // silo, don't error: just noop
                        if existing_default.ip_pool_id == ip_pool_id {
                            return Ok(existing_default);
                        }

                        let unset_default =
                            diesel::update(dsl::ip_pool_resource)
                                .filter(
                                    dsl::resource_id
                                        .eq(existing_default.resource_id),
                                )
                                .filter(
                                    dsl::ip_pool_id
                                        .eq(existing_default.ip_pool_id),
                                )
                                .filter(
                                    dsl::resource_type
                                        .eq(existing_default.resource_type),
                                )
                                .set(dsl::is_default.eq(false))
                                .execute_async(&conn)
                                .await;
                        if let Err(e) = unset_default {
                            return Err(err.bail(TxnError::CustomError(
                                IpPoolResourceUpdateError::FailedToUnsetDefault(
                                    e,
                                ),
                            )));
                        }
                    }

                    let updated_link = diesel::update(dsl::ip_pool_resource)
                        .filter(dsl::resource_id.eq(silo_id))
                        .filter(dsl::ip_pool_id.eq(ip_pool_id))
                        .filter(dsl::resource_type.eq(IpPoolResourceType::Silo))
                        .set(dsl::is_default.eq(true))
                        .returning(IpPoolResource::as_returning())
                        .get_result_async(&conn)
                        .await?;
                    Ok(updated_link)
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(TxnError::CustomError(
                    IpPoolResourceUpdateError::FailedToUnsetDefault(err),
                )) => public_error_from_diesel(err, ErrorHandler::Server),
                Some(TxnError::Database(err)) => {
                    public_error_from_diesel(err, ErrorHandler::Server)
                }
                None => public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::IpPoolResource,
                        // TODO: would be nice to put the actual names and/or ids in
                        // here but LookupType on each of the two silos doesn't have
                        // a nice to_string yet or a way of composing them
                        LookupType::ByCompositeId("(pool, silo)".to_string()),
                    ),
                ),
            })
    }

    /// Delete IP pool assocation with resource unless there are outstanding
    /// IPs allocated from the pool in the associated silo
    pub async fn ip_pool_unlink_silo(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        authz_silo: &authz::Silo,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, authz_pool).await?;
        opctx.authorize(authz::Action::Modify, authz_silo).await?;

        if authz_silo.id() == INTERNAL_SILO_ID {
            return Err(Error::internal_error(
                "Cannot unlink an internally-reserved IP Pool. \
                    Use the `reservation_type` column instead.",
            ));
        }

        let conn = self.pool_connection_authorized(opctx).await?;
        unlink_ip_pool_from_external_silo_query(
            authz_pool.id(),
            authz_silo.id(),
        )
        .execute_async(&*conn)
        .await
        .map_err(|e| match e {
            DieselError::DatabaseError(
                DatabaseErrorKind::Unknown,
                ref info,
            ) => {
                let msg = info.message();
                // Intentional bool-parsing error, which we use to detect
                // when there are still external IPs in the Silo.
                if msg.contains("could not parse")
                    && msg.ends_with("invalid bool value")
                {
                    Error::invalid_request(POOL_HAS_IPS_ERROR)
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            }
            _ => public_error_from_diesel(e, ErrorHandler::Server),
        })?;

        // TODO-correctness: We probably want to do this in the same transaction
        // as above. See https://github.com/oxidecomputer/omicron/issues/8992.
        self.unlink_ip_pool_gateway(
            opctx,
            authz_silo.id(),
            authz_pool.id(),
            &conn,
        )
        .await?;

        Ok(())
    }

    pub async fn ip_pool_list_ranges(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        pag_params: &DataPageParams<'_, IpNetwork>,
    ) -> ListResultVec<IpPoolRange> {
        use nexus_db_schema::schema::ip_pool_range::dsl;
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

    /// List all IP pool ranges for a given pool, making as many queries as
    /// needed to get them all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn ip_pool_list_ranges_batched(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> ListResultVec<IpPoolRange> {
        opctx.check_complex_operations_allowed()?;
        let mut ip_ranges = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .ip_pool_list_ranges(opctx, &authz_pool, &p.current_pagparams())
                .await?;
            // The use of `last_address` here assumes `paginator` is sorting
            // in Ascending order (which it does - see the implementation of
            // `current_pagparams()`).
            paginator = p.found_batch(&batch, &|r| r.last_address);
            ip_ranges.extend(batch);
        }
        Ok(ip_ranges)
    }

    pub async fn ip_pool_add_range(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        pool: &IpPool,
        range: &IpRange,
    ) -> CreateResult<IpPoolRange> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::ip_pool_add_range_on_connection(
            &conn, opctx, authz_pool, pool, range,
        )
        .await
    }

    /// Variant of [Self::ip_pool_add_range] which may be called from a
    /// transaction context.
    pub(crate) async fn ip_pool_add_range_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        pool: &IpPool,
        range: &IpRange,
    ) -> CreateResult<IpPoolRange> {
        use nexus_db_schema::schema::ip_pool_range::dsl;
        opctx.authorize(authz::Action::CreateChild, authz_pool).await?;

        // Sanity check that the provided DB and authz pools match.
        if pool.id() != authz_pool.id() {
            return Err(Error::internal_error(&format!(
                "DB and authz IP Pool object IDs must match, but \
                DB ID is '{}' and authz ID is '{}'",
                pool.id(),
                authz_pool.id(),
            )));
        }

        // First ensure the IP range matches the IP version of the pool.
        let pool_id = authz_pool.id();
        if pool.ip_version != range.version().into() {
            return Err(Error::invalid_request(format!(
                "Cannot add IP{} address range to \
                IP{} pool with ID \"{}\"",
                range.version(),
                pool.ip_version,
                pool_id,
            )));
        }

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
        use nexus_db_schema::schema::external_ip;
        use nexus_db_schema::schema::ip_pool_range::dsl;
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

// Sentinel we try to cast as a UUID in the database, when linking an IP Pool to
// a Silo of the wrong "type" -- i.e., linking to an external Silo if the Pool
// is already linked to our internal Silo, or vice versa.
const BAD_SILO_LINK_SENTINEL: &str = "bad-link-type";

// Sentinel we try to cast as a UUID in the database when the IP Pool is
// deleted between selecting it and trying to insert the link.
const IP_POOL_DELETED_SENTINEL: &str = "ip-pool-deleted";

// Sentinel we try to cast as a UUID in the database when the Silo is deleted
// between selecting it and trying to insert the link.
const SILO_DELETED_SENTINEL: &str = "silo-deleted";

// Sentinel we try to cast as an integer when removing the reservation on the
// last internal pool of a given type.
const LAST_POOL_SENTINEL: &str = "last-pool";

// Query to conditionally link an IP Pool to an external customer Silo.
//
// This method returns a SQL query to conditionally insert a link between an IP
// Pool and a Silo. It maintains the invariant that a pool can be reserved for
// Oxide internal usage XOR linked to customer silos. It also checks that the
// pool and silo still exist when the query is run.
//
// The full query is:
//
// ```sql
// WITH
//   -- Select the IP Pool by ID, used to ensure it still exists when we run
//   -- this query. Also select the reservation type, and fail if the pool is
//   -- currently reserved for Oxide.
//   ip_pool AS (
//      SELECT CAST(IF(
//          reservation_type != 'external_silos',
//          'bad-link-type',
//          $1)
//      AS UUID)
//      FROM ip_pool
//      WHERE id = $2 AND time_deleted IS NULL
//   )
//   -- Select the Silo by ID, used to ensure it still exists when we run this
//   -- query
//   silo AS (SELECT id FROM silo WHERE id = $3 AND time_deleted IS NULL),
// INSERT
// INTO
//   ip_pool_resource (ip_pool_id, resource_type, resource_id, is_default)
// SELECT
//   -- If the pool exists, take its ID as a string. If it does not exist, take
//   -- the string `'ip-pool-deleted'`. Attempt to cast the result to a UUID.
//   -- This is the "true or cast error" trick we use in many places.
//   CAST(COALESCE(CAST(ip.id AS STRING), 'ip-pool-deleted') AS UUID),
//   -- The resource type, always 'silo' here.
//   $4,
//   -- If the silo exists, take its ID as a string. If it does not exist, take
//   -- the string `'silo-deleted'`. Attempt to cast the result to a UUID.
//   -- This is the "true or cast error" trick we use in many places.
//   CAST(COALESCE(CAST(s.id AS STRING), 'silo-deleted') AS UUID),
//    $5
// FROM
//   (SELECT 1) AS dummy
//   LEFT JOIN ip_pool AS ip ON true
//   LEFT JOIN silo AS s ON true
// RETURNING
//  *
// ```
fn link_ip_pool_to_external_silo_query(
    ip_pool_resource: &IpPoolResource,
) -> TypedSqlQuery<SelectableSql<IpPoolResource>> {
    let mut builder = QueryBuilder::new();
    builder
        .sql("WITH ip_pool AS (SELECT CAST(IF(reservation_type != ")
        .param()
        .bind::<IpPoolReservationTypeEnum, _>(
            IpPoolReservationType::ExternalSilos,
        )
        .sql(", '")
        .sql(BAD_SILO_LINK_SENTINEL)
        .sql("', ")
        .param()
        .bind::<sql_types::Text, _>(ip_pool_resource.ip_pool_id.to_string())
        .sql(") AS UUID) AS id FROM ip_pool WHERE id = ")
        .param()
        .bind::<sql_types::Uuid, _>(ip_pool_resource.ip_pool_id)
        .sql(
            " \
            AND time_deleted IS NULL), \
            silo AS (SELECT id FROM silo WHERE id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(ip_pool_resource.resource_id)
        .sql(
            " AND time_deleted IS NULL) \
            INSERT INTO ip_pool_resource (\
                ip_pool_id, \
                resource_type, \
                resource_id, \
                is_default\
            ) SELECT CAST(COALESCE(CAST(ip.id AS STRING), '",
        )
        .sql(IP_POOL_DELETED_SENTINEL)
        .sql("') AS UUID), ")
        .param()
        .bind::<nexus_db_schema::enums::IpPoolResourceTypeEnum, _>(
            ip_pool_resource.resource_type,
        )
        .sql(", CAST(COALESCE(CAST(s.id AS STRING), '")
        .sql(SILO_DELETED_SENTINEL)
        .sql("') AS UUID), ")
        .param()
        .bind::<sql_types::Bool, _>(ip_pool_resource.is_default)
        .sql(
            " FROM (SELECT 1) AS dummy \
            LEFT JOIN ip_pool AS ip ON TRUE \
            LEFT JOIN silo AS s ON TRUE \
            RETURNING *",
        );
    builder.query()
}

// Query to conditionally unlink an IP Pool from an external / customer Silo.
//
// This deletes the link iff there are no outstanding instance external IPs or
// floating IPs allocated out of the pool, to objects in the Silo.
//
// The full query is:
//
// ```
// -- This CTE returns one row if there are any external IPs attached to
// -- instances, in any projects in the Silo.
// WITH instance_ips AS (
//   SELECT 1
//   FROM external_ip
//   INNER JOIN instance ON external_ip.parent_id = instance.id
//   INNER JOIN project ON instance.project_id = project.id
//   WHERE
//       external_ip.is_service = FALSE AND
//       external_ip.parent_id IS NOT NULL AND
//       external_ip.time_deleted IS NULL AND
//       external_ip.ip_pool_id = $1 AND
//       external_ip.kind != 'floating' AND
//       instance.time_deleted IS NULL AND
//       project.silo_id = $2
//   LIMIT 1
// ),
// -- This CTE returns one row if there are any Floating IPs in the Silo,
// -- whether they're attached or not.
// floating_ips AS (
//   SELECT 1
//   FROM external_ip
//   INNER JOIN project ON external_ip.project_id = project.id
//   WHERE
//       external_ip.is_service = FALSE AND
//       external_ip.time_deleted IS NULL AND
//       external_ip.project_id IS NOT NULL AND
//       external_ip.ip_pool_id = $3 AND
//       external_ip.kind = 'floating' AND
//       project.silo_id = $4 AND
//       project.time_deleted IS NULL
//   LIMIT 1
// )
// -- Delete the requested link by primary key, but conditionally.
// DELETE FROM ip_pool_resource
// WHERE
//   ip_pool_id = $7 AND
//   resource_type = 'silo' AND
//   resource_id = $8 AND
//   -- If there are any external IPs, this generates an error casting 'eips' to
//   -- a boolean, which we detect and handle.
//   CAST(IF(EXISTS(
//      SELECT 1 FROM instance_ips
//      UNION ALL
//      SELECT 1 FROM floating_ips
//  ), 'eips', 'true') AS BOOL)
// ```
fn unlink_ip_pool_from_external_silo_query(
    ip_pool_id: Uuid,
    silo_id: Uuid,
) -> TypedSqlQuery<()> {
    let mut builder = QueryBuilder::new();
    builder
        .sql(
            "WITH instance_ips AS (\
    SELECT 1 \
    FROM external_ip \
    INNER JOIN instance ON external_ip.parent_id = instance.id \
    INNER JOIN project ON instance.project_id = project.id \
    WHERE \
        external_ip.is_service = FALSE AND \
        external_ip.parent_id IS NOT NULL AND \
        external_ip.time_deleted IS NULL AND \
        external_ip.ip_pool_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(ip_pool_id)
        .sql(" AND external_ip.kind != ")
        .param()
        .bind::<IpKindEnum, _>(IpKind::Floating)
        .sql(" AND instance.time_deleted IS NULL AND project.silo_id = ")
        .param()
        .bind::<sql_types::Uuid, _>(silo_id)
        .sql(
            " LIMIT 1), floating_ips AS (\
        SELECT 1 \
        FROM external_ip \
        INNER JOIN project ON external_ip.project_id = project.id \
        WHERE \
            external_ip.is_service = FALSE AND \
            external_ip.time_deleted IS NULL AND \
            external_ip.project_id IS NOT NULL AND \
            external_ip.ip_pool_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(ip_pool_id)
        .sql(" AND external_ip.kind = ")
        .param()
        .bind::<IpKindEnum, _>(IpKind::Floating)
        .sql(" AND project.silo_id = ")
        .param()
        .bind::<sql_types::Uuid, _>(silo_id)
        .sql(
            " AND project.time_deleted IS NULL LIMIT 1) \
            DELETE FROM ip_pool_resource \
            WHERE ip_pool_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(ip_pool_id)
        .sql(" AND resource_type = ")
        .param()
        .bind::<nexus_db_schema::enums::IpPoolResourceTypeEnum, _>(
            IpPoolResourceType::Silo,
        )
        .sql(" AND resource_id = ")
        .param()
        .bind::<sql_types::Uuid, _>(silo_id)
        .sql(
            " AND \
            CAST(IF(\
                EXISTS(\
                    SELECT 1 FROM instance_ips \
                    UNION ALL \
                    SELECT 1 FROM floating_ips\
                ), \
                'has-eips', \
                'true'\
            ) AS BOOL)",
        );
    builder.query()
}

// Generate a small helper subquery which fails with a bool-cast error if there
// are fewer than 2 IP Pools reserved for the provided use. It must be internal.
fn count_reserved_pools_subquery(
    reservation_type: IpPoolReservationType,
) -> String {
    assert!(!matches!(reservation_type, IpPoolReservationType::ExternalSilos));
    format!(
        "CAST(IF(\
        (SELECT COUNT(1) \
             FROM ip_pool \
             WHERE time_deleted IS NULL AND reservation_type = '{}' LIMIT 2\
        ) >= 2, \
        'true', \
        '{}') \
        AS BOOL)",
        reservation_type, LAST_POOL_SENTINEL,
    )
}

// Conditionally reserve an IP Pool for a specific use.
//
// # Panics
//
// Panics if the current and new reservation type are the same.
fn reserve_ip_pool_query(
    pool: &IpPool,
    reservation_type: IpPoolReservationType,
) -> TypedSqlQuery<()> {
    assert_ne!(pool.reservation_type, reservation_type);
    match pool.reservation_type {
        IpPoolReservationType::ExternalSilos => {
            reserve_external_ip_pool_query(pool, reservation_type)
        }
        IpPoolReservationType::OxideInternal => {
            reserve_internal_ip_pool_query(pool, reservation_type)
        }
    }
}

// Query to conditionally reserve an IP Pool that is currently reserved for
// external silo use.
//
// Checks that the pool is not currently linked to any silos first. Note that
// this means there cannot be any silo-specific resources using the pool.
fn reserve_external_ip_pool_query(
    ip_pool: &IpPool,
    new_reservation_type: IpPoolReservationType,
) -> TypedSqlQuery<()> {
    let mut builder = QueryBuilder::new();
    builder
        .sql("UPDATE ip_pool SET reservation_type = ")
        .param()
        .bind::<IpPoolReservationTypeEnum, _>(new_reservation_type)
        .sql(", time_modified = NOW() WHERE id = ")
        .param()
        .bind::<sql_types::Uuid, _>(ip_pool.id())
        .sql(" AND time_deleted IS NULL AND reservation_type = ")
        .param()
        .bind::<IpPoolReservationTypeEnum, _>(ip_pool.reservation_type)
        .sql(
            " AND CAST(IF(EXISTS(\
                SELECT 1 \
                FROM ip_pool_resource \
                WHERE ip_pool_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(ip_pool.id())
        .sql(" LIMIT 1), '")
        .sql(BAD_SILO_LINK_SENTINEL)
        .sql("', 'TRUE') AS BOOL)");
    builder.query()
}

// Query to conditionally reserve an IP Pool that is currently reserved for Oxide
// internal use.
//
// Checks that:
//
// - There are no external IPs in use by Oxide resources.
// - There is at least one other internal pool of the same reservation type.
fn reserve_internal_ip_pool_query(
    ip_pool: &IpPool,
    new_reservation_type: IpPoolReservationType,
) -> TypedSqlQuery<()> {
    let mut builder = QueryBuilder::new();
    builder
        .sql("UPDATE ip_pool SET reservation_type = ")
        .param()
        .bind::<IpPoolReservationTypeEnum, _>(new_reservation_type)
        .sql(", time_modified = NOW() WHERE id = ")
        .param()
        .bind::<sql_types::Uuid, _>(ip_pool.id())
        .sql(" AND time_deleted IS NULL AND reservation_type = ")
        .param()
        .bind::<IpPoolReservationTypeEnum, _>(ip_pool.reservation_type)
        // Generate div-by-zero error if there are IPs
        .sql(
            " AND (\
            SELECT CAST(IF(EXISTS(\
                SELECT 1 \
                FROM external_ip \
                WHERE ip_pool_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(ip_pool.id())
        .sql(
            " AND \
            external_ip.is_service AND \
            time_deleted IS NULL \
            LIMIT 1\
        ), 1/0, 1) AS BOOL))",
        )
        // Generate int-cast error if this is the last pool of this reservation
        // type.
        .sql(
            " AND CAST(IF(\
                (SELECT COUNT(1) \
                    FROM ip_pool \
                    WHERE time_deleted IS NULL \
                    AND reservation_type = ",
        )
        .param()
        .bind::<IpPoolReservationTypeEnum, _>(ip_pool.reservation_type)
        .sql(
            " \
                LIMIT 2\
                ) >= 2, \
            '1', ",
        )
        .param()
        .bind::<sql_types::Text, _>(LAST_POOL_SENTINEL)
        .sql(") AS INT) = 1");
    builder.query()
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;
    use std::num::NonZeroU32;

    use crate::authz;
    use crate::db::datastore::ip_pool::{
        BAD_SILO_LINK_ERROR, LAST_POOL_ERROR, POOL_HAS_IPS_ERROR,
        link_ip_pool_to_external_silo_query, reserve_ip_pool_query,
        unlink_ip_pool_from_external_silo_query,
    };
    use crate::db::explain::ExplainableAsync as _;
    use crate::db::model::{
        IpPool, IpPoolResource, IpPoolResourceType, Project,
    };
    use crate::db::pagination::Paginator;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncRunQueryDsl as _;
    use diesel::{
        ExpressionMethods as _, QueryDsl as _, SelectableHelper as _,
    };
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::{IpPoolIdentity, IpPoolReservationType, IpVersion};
    use nexus_sled_agent_shared::inventory::ZoneKind;
    use nexus_types::deployment::{
        OmicronZoneExternalFloatingIp, OmicronZoneExternalIp,
    };
    use nexus_types::external_api::params;
    use nexus_types::identity::Resource;
    use nexus_types::silo::INTERNAL_SILO_ID;
    use omicron_common::address::{IpRange, Ipv4Range, Ipv6Range};
    use omicron_common::api::external::http_pagination::PaginatedBy;
    use omicron_common::api::external::{
        DataPageParams, Error, IdentityMetadataCreateParams, LookupType,
    };
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{
        ExternalIpUuid, GenericUuid as _, OmicronZoneUuid,
    };

    #[tokio::test]
    async fn test_default_ip_pools() {
        let logctx = dev::test_setup_log("test_default_ip_pools");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // we start out with no default pool, so we expect not found
        let error = datastore.ip_pools_fetch_default(&opctx).await.unwrap_err();
        assert_matches!(error, Error::ObjectNotFound { .. });

        let pagparams_id = DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };
        let pagbyid = PaginatedBy::Id(pagparams_id);

        let all_pools = datastore
            .ip_pools_list(&opctx, &pagbyid)
            .await
            .expect("Should list IP pools");
        assert_eq!(all_pools.len(), 0);

        let authz_silo = opctx.authn.silo_required().unwrap();

        let silo_pools = datastore
            .silo_ip_pool_list(&opctx, &authz_silo, &pagbyid)
            .await
            .expect("Should list silo IP pools");
        assert_eq!(silo_pools.len(), 0);

        let authz_silo = opctx.authn.silo_required().unwrap();
        let silo_id = authz_silo.id();

        // create a non-default pool for the silo
        let identity = IdentityMetadataCreateParams {
            name: "pool1-for-silo".parse().unwrap(),
            description: "".to_string(),
        };
        let pool1_for_silo = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new(
                    &identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
            )
            .await
            .expect("Failed to create IP pool");

        // shows up in full list but not silo list
        let all_pools = datastore
            .ip_pools_list(&opctx, &pagbyid)
            .await
            .expect("Should list IP pools");
        assert_eq!(all_pools.len(), 1);
        let silo_pools = datastore
            .silo_ip_pool_list(&opctx, &authz_silo, &pagbyid)
            .await
            .expect("Should list silo IP pools");
        assert_eq!(silo_pools.len(), 0);

        // make default should fail when there is no link yet
        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            pool1_for_silo.id(),
            LookupType::ById(pool1_for_silo.id()),
        );
        let error = datastore
            .ip_pool_set_default(&opctx, &authz_pool, &authz_silo, true)
            .await
            .expect_err("Should not be able to make non-existent link default");
        assert_matches!(error, Error::ObjectNotFound { .. });

        // now link to silo
        let link_body = IpPoolResource {
            ip_pool_id: pool1_for_silo.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: silo_id,
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link_body)
            .await
            .expect("Failed to associate IP pool with silo");

        // because that one was not a default, when we ask for the silo default
        // pool, we still get nothing
        let error = datastore.ip_pools_fetch_default(&opctx).await.unwrap_err();
        assert_matches!(error, Error::ObjectNotFound { .. });

        // now it shows up in the silo list
        let silo_pools = datastore
            .silo_ip_pool_list(&opctx, &authz_silo, &pagbyid)
            .await
            .expect("Should list silo IP pools");
        assert_eq!(silo_pools.len(), 1);
        assert_eq!(silo_pools[0].0.id(), pool1_for_silo.id());
        assert_eq!(silo_pools[0].1.is_default, false);

        // linking an already linked silo errors due to PK conflict
        let err = datastore
            .ip_pool_link_silo(&opctx, link_body)
            .await
            .expect_err("Creating the same link again should conflict");
        assert_matches!(err, Error::ObjectAlreadyExists { .. });

        // now make it default
        datastore
            .ip_pool_set_default(&opctx, &authz_pool, &authz_silo, true)
            .await
            .expect("Should be able to make pool default");

        // setting default if already default is allowed
        datastore
            .ip_pool_set_default(&opctx, &authz_pool, &authz_silo, true)
            .await
            .expect("Should be able to make pool default again");

        // now when we ask for the default pool again, we get that one
        let (authz_pool1_for_silo, ip_pool) = datastore
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
            .ip_pool_create(
                &opctx,
                IpPool::new(
                    &identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
            )
            .await
            .expect("Failed to create pool");
        let err = datastore
            .ip_pool_link_silo(
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

        // now remove the association and we should get nothing again
        let authz_silo =
            authz::Silo::new(authz::Fleet, silo_id, LookupType::ById(silo_id));
        let q =
            nexus_db_schema::schema::ip_pool_resource::dsl::ip_pool_resource
                .select(IpPoolResource::as_select())
                .filter(
                    nexus_db_schema::schema::ip_pool_resource::dsl::resource_id
                        .eq(authz_silo.id()),
                )
                .get_results_async(
                    &*datastore
                        .pool_connection_authorized(opctx)
                        .await
                        .unwrap(),
                )
                .await
                .unwrap();
        println!("{q:#?}");
        datastore
            .ip_pool_unlink_silo(&opctx, &authz_pool1_for_silo, &authz_silo)
            .await
            .expect("Failed to unlink IP pool from silo");

        let q =
            nexus_db_schema::schema::ip_pool_resource::dsl::ip_pool_resource
                .select(IpPoolResource::as_select())
                .filter(
                    nexus_db_schema::schema::ip_pool_resource::dsl::resource_id
                        .eq(authz_silo.id()),
                )
                .get_results_async(
                    &*datastore
                        .pool_connection_authorized(opctx)
                        .await
                        .unwrap(),
                )
                .await
                .unwrap();
        println!("{q:#?}");

        // no default
        let error = datastore.ip_pools_fetch_default(&opctx).await.unwrap_err();
        assert_matches!(error, Error::ObjectNotFound { .. });

        // and silo pools list is empty again
        let silo_pools = datastore
            .silo_ip_pool_list(&opctx, &authz_silo, &pagbyid)
            .await
            .expect("Should list silo IP pools");
        assert_eq!(silo_pools.len(), 0);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_internal_ip_pools() {
        let logctx = dev::test_setup_log("test_internal_ip_pools");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        for version in [IpVersion::V4, IpVersion::V6] {
            // confirm internal pools appear as internal
            let (authz_pool, pool) = datastore
                .ip_pools_service_lookup(&opctx, version)
                .await
                .unwrap();
            assert_eq!(pool.ip_version, version);

            let is_internal =
                datastore.ip_pool_is_internal(&opctx, &authz_pool).await;
            assert_eq!(is_internal, Ok(true));

            // another random pool should not be considered internal
            let identity = IdentityMetadataCreateParams {
                name: format!("other-{version}-pool").parse().unwrap(),
                description: "".to_string(),
            };
            let other_pool = datastore
                .ip_pool_create(
                    &opctx,
                    IpPool::new(
                        &identity,
                        version,
                        IpPoolReservationType::ExternalSilos,
                    ),
                )
                .await
                .expect("Failed to create IP pool");
            assert_eq!(other_pool.ip_version, version);

            let authz_other_pool = authz::IpPool::new(
                authz::FLEET,
                other_pool.id(),
                LookupType::ById(other_pool.id()),
            );
            let is_internal =
                datastore.ip_pool_is_internal(&opctx, &authz_other_pool).await;
            assert_eq!(is_internal, Ok(false));

            // now link it to the current silo, and it is still not internal.
            let silo_id = opctx.authn.silo_required().unwrap().id();
            let is_default = matches!(version, IpVersion::V4);
            let link = IpPoolResource {
                ip_pool_id: other_pool.id(),
                resource_type: IpPoolResourceType::Silo,
                resource_id: silo_id,
                is_default,
            };
            datastore
                .ip_pool_link_silo(&opctx, link)
                .await
                .expect("Failed to link IP pool to silo");

            let is_internal =
                datastore.ip_pool_is_internal(&opctx, &authz_other_pool).await;
            assert_eq!(is_internal, Ok(false));
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // We're breaking out the utilization tests for IPv4 and IPv6 pools, since
    // pools only contain one version now.
    //
    // See https://github.com/oxidecomputer/omicron/issues/8888.
    #[tokio::test]
    async fn test_ipv4_ip_pool_utilization() {
        let logctx = dev::test_setup_log("test_ipv4_ip_pool_utilization");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let authz_silo = opctx.authn.silo_required().unwrap();
        let project = Project::new(
            authz_silo.id(),
            params::ProjectCreate {
                identity: IdentityMetadataCreateParams {
                    name: "my-project".parse().unwrap(),
                    description: "".to_string(),
                },
            },
        );
        let (.., project) =
            datastore.project_create(&opctx, project).await.unwrap();

        // create an IP pool for the silo, add a range to it, and link it to the silo
        let identity = IdentityMetadataCreateParams {
            name: "my-pool".parse().unwrap(),
            description: "".to_string(),
        };
        let pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new(
                    &identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
            )
            .await
            .expect("Failed to create IP pool");
        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            pool.id(),
            LookupType::ById(pool.id()),
        );

        // capacity of zero because there are no ranges
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 0);

        let range = IpRange::V4(
            Ipv4Range::new(
                std::net::Ipv4Addr::new(10, 0, 0, 1),
                std::net::Ipv4Addr::new(10, 0, 0, 5),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &pool, &range)
            .await
            .expect("Could not add range");

        // now it has a capacity of 5 because we added the range
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 5);

        let link = IpPoolResource {
            ip_pool_id: pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: authz_silo.id(),
            is_default: true,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Could not link pool to silo");

        let ip_count = datastore
            .ip_pool_allocated_count(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(ip_count, 0);

        let identity = IdentityMetadataCreateParams {
            name: "my-ip".parse().unwrap(),
            description: "".to_string(),
        };
        let ip = datastore
            .allocate_floating_ip(&opctx, project.id(), identity, None, None)
            .await
            .expect("Could not allocate floating IP");
        assert_eq!(ip.ip.to_string(), "10.0.0.1/32");

        let ip_count = datastore
            .ip_pool_allocated_count(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(ip_count, 1);

        // allocating one has nothing to do with total capacity
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 5);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ipv6_ip_pool_utilization() {
        let logctx = dev::test_setup_log("test_ipv6_ip_pool_utilization");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let authz_silo = opctx.authn.silo_required().unwrap();
        let project = Project::new(
            authz_silo.id(),
            params::ProjectCreate {
                identity: IdentityMetadataCreateParams {
                    name: "my-project".parse().unwrap(),
                    description: "".to_string(),
                },
            },
        );
        let (.., project) =
            datastore.project_create(&opctx, project).await.unwrap();

        // create an IP pool for the silo, add a range to it, and link it to the silo
        let identity = IdentityMetadataCreateParams {
            name: "my-pool".parse().unwrap(),
            description: "".to_string(),
        };
        let pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new(
                    &identity,
                    IpVersion::V6,
                    IpPoolReservationType::ExternalSilos,
                ),
            )
            .await
            .expect("Failed to create IP pool");
        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            pool.id(),
            LookupType::ById(pool.id()),
        );
        let link = IpPoolResource {
            ip_pool_id: pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: authz_silo.id(),
            is_default: true,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Could not link pool to silo");

        // capacity of zero because there are no ranges
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 0);

        // Add an IPv6 range
        let ipv6_range = IpRange::V6(
            Ipv6Range::new(
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 10),
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 1, 20),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &pool, &ipv6_range)
            .await
            .expect("Could not add range");
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 11 + 65536);

        let ip_count = datastore
            .ip_pool_allocated_count(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(ip_count, 0);

        let identity = IdentityMetadataCreateParams {
            name: "my-ip".parse().unwrap(),
            description: "".to_string(),
        };
        let ip = datastore
            .allocate_floating_ip(&opctx, project.id(), identity, None, None)
            .await
            .expect("Could not allocate floating IP");
        assert_eq!(ip.ip.to_string(), "fd00::a/128");

        let ip_count = datastore
            .ip_pool_allocated_count(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(ip_count, 1);

        // allocating one has nothing to do with total capacity
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 11 + 65536);

        // add a giant range for fun
        let ipv6_range = IpRange::V6(
            Ipv6Range::new(
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 1, 21),
                std::net::Ipv6Addr::new(
                    0xfd00, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
                ),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &pool, &ipv6_range)
            .await
            .expect("Could not add range");

        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 1208925819614629174706166);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_insert_range_in_pool_with_different_ip_version() {
        let logctx = dev::test_setup_log(
            "cannot_insert_range_in_pool_with_different_ip_version",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // IP pool versions, and ranges of the opposite version.
        let versions = [IpVersion::V4, IpVersion::V6];
        let ranges = [
            IpRange::V6(
                Ipv6Range::new(
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 1, 21),
                    std::net::Ipv6Addr::new(
                        0xfd00, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
                    ),
                )
                .unwrap(),
            ),
            IpRange::V4(
                Ipv4Range::new(
                    std::net::Ipv4Addr::new(10, 0, 0, 1),
                    std::net::Ipv4Addr::new(10, 0, 0, 5),
                )
                .unwrap(),
            ),
        ];

        for (version, range) in versions.into_iter().zip(ranges) {
            // Create the pool
            let identity = IdentityMetadataCreateParams {
                name: format!("ip{version}-pool-for-silo").parse().unwrap(),
                description: "".to_string(),
            };
            let pool = datastore
                .ip_pool_create(
                    &opctx,
                    IpPool::new(
                        &identity,
                        version,
                        IpPoolReservationType::ExternalSilos,
                    ),
                )
                .await
                .expect("Failed to create IP pool");
            let authz_pool = authz::IpPool::new(
                authz::FLEET,
                pool.id(),
                LookupType::ById(pool.id()),
            );

            // Ensure we cannot insert a range of the other version.
            let res = datastore
                .ip_pool_add_range(&opctx, &authz_pool, &pool, &range)
                .await;
            assert!(
                res.is_err(),
                "Should have failed to insert an IP{} range in a IP{} pool",
                range.version(),
                version,
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn paginate_ip_pools_by_delegation_type() {
        let logctx =
            dev::test_setup_log("paginate_ip_pools_by_delegation_type");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Insert a bunch of pools, not linked to any silo, and so reserved for
        // customer use.
        const N_POOLS: usize = 20;
        let mut customer_pools = Vec::with_capacity(N_POOLS);
        for i in 0..N_POOLS {
            // Create the pool
            let identity = IdentityMetadataCreateParams {
                name: format!("ip-pool-{i}").parse().unwrap(),
                description: "".to_string(),
            };
            let pool = datastore
                .ip_pool_create(
                    opctx,
                    IpPool::new(
                        &identity,
                        IpVersion::V4,
                        IpPoolReservationType::ExternalSilos,
                    ),
                )
                .await
                .expect("Failed to create IP pool");
            customer_pools.push(pool);
        }
        customer_pools.sort_by_key(|pool| pool.id());

        // Create a bunch which _are_ reserved for Oxide's usage.
        let mut oxide_pools = Vec::with_capacity(N_POOLS);
        for i in 0..N_POOLS {
            // Create the pool
            let identity = IdentityMetadataCreateParams {
                name: format!("oxide-ip-pool-{i}").parse().unwrap(),
                description: "".to_string(),
            };
            let pool = datastore
                .ip_pool_create(
                    opctx,
                    IpPool::new(
                        &identity,
                        IpVersion::V4,
                        IpPoolReservationType::OxideInternal,
                    ),
                )
                .await
                .expect("Failed to create reserved IP pool");
            oxide_pools.push(pool);
        }
        assert_eq!(oxide_pools.len(), N_POOLS);

        let fetch_paginated = |reservation_type| async move {
            let mut found = Vec::with_capacity(N_POOLS);
            let mut paginator = Paginator::new(
                NonZeroU32::new(5).unwrap(),
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(page) = paginator.next() {
                let batch = datastore
                    .ip_pools_list_paginated(
                        opctx,
                        reservation_type,
                        None,
                        &PaginatedBy::Id(page.current_pagparams()),
                    )
                    .await
                    .expect("Should be able to list pools with pagination");
                paginator = page.found_batch(&batch, &|pool| pool.id());
                found.extend(batch.into_iter());
            }
            found
        };

        // Paginate all the customer-reserved.
        let customer_pools_found =
            fetch_paginated(IpPoolReservationType::ExternalSilos).await;
        assert_eq!(customer_pools.len(), customer_pools_found.len());
        assert_eq!(customer_pools, customer_pools_found);

        // Paginate all those reserved for Oxide.
        //
        // Note that we have 2 extra pools today, which are the builtin service
        // pools. These will go away in the future, so we'll unfortunately need
        // to update this test at that time. Until then, fetch those service
        // pools explicitly and add them.
        let oxide_reserved_found =
            fetch_paginated(IpPoolReservationType::OxideInternal).await;
        let pools = datastore
            .ip_pools_service_lookup_both_versions(opctx)
            .await
            .unwrap();
        oxide_pools.push(pools.ipv4.db_pool);
        oxide_pools.push(pools.ipv6.db_pool);
        oxide_pools.sort_by_key(|pool| pool.id());
        assert_eq!(oxide_pools.len(), oxide_reserved_found.len());
        assert_eq!(oxide_pools, oxide_reserved_found);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Ensure we have the right query contents.
    #[tokio::test]
    async fn expectorate_insert_ip_pool_external_silo_link() {
        let res = IpPoolResource {
            ip_pool_id: uuid::uuid!("aaa84fbd-85a5-4fcb-b34f-23b7e56145c7"),
            resource_type: IpPoolResourceType::Silo,
            resource_id: INTERNAL_SILO_ID,
            is_default: false,
        };
        let query = link_ip_pool_to_external_silo_query(&res);
        expectorate_query_contents(
            &query,
            "tests/output/ip_pool_external_silo_link.sql",
        )
        .await;
    }

    // Explain the SQL query inserting an IP Pool link to a customer silo
    #[tokio::test]
    async fn can_explain_link_ip_pool_to_silo_query() {
        let logctx =
            dev::test_setup_log("can_explain_link_ip_pool_to_silo_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let res = IpPoolResource {
            ip_pool_id: uuid::uuid!("aaa84fbd-85a5-4fcb-b34f-23b7e56145c7"),
            resource_type: IpPoolResourceType::Silo,
            resource_id: INTERNAL_SILO_ID,
            is_default: false,
        };

        let query = link_ip_pool_to_external_silo_query(&res);
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_link_oxide_internal_pool_to_external_silo() {
        let logctx = dev::test_setup_log(
            "cannot_link_oxide_internal_pool_to_external_silo",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create the pool
        let identity = IdentityMetadataCreateParams {
            name: "internal-ip-pool".parse().unwrap(),
            description: "".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                opctx,
                IpPool::new(
                    &identity,
                    IpVersion::V4,
                    IpPoolReservationType::OxideInternal,
                ),
            )
            .await
            .expect("Failed to create IP pool");

        // We should fail to link it to some other silo now.
        let link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: uuid::uuid!("cfb16a9d-764e-4c5d-8d0d-cf737885b84a"),
            is_default: false,
        };
        let res = datastore.ip_pool_link_silo(&opctx, link).await;
        let Err(Error::InvalidRequest { message }) = &res else {
            panic!(
                "Expected to fail linking an internally-reserved \
                IP Pool to an external Silo, found: {res:#?}",
            );
        };
        assert_eq!(message.external_message(), BAD_SILO_LINK_ERROR);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_reserve_externally_linked_pool_for_internal_use() {
        let logctx = dev::test_setup_log(
            "cannot_reserve_externally_linked_pool_for_internal_use",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create the pool, reserved for external silos.
        let identity = IdentityMetadataCreateParams {
            name: "external-ip-pool".parse().unwrap(),
            description: "".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                opctx,
                IpPool::new(
                    &identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
            )
            .await
            .expect("Failed to create IP pool");

        // Link to an external silo.
        let external_link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: nexus_types::silo::DEFAULT_SILO_ID,
            is_default: false,
        };
        let _ = datastore
            .ip_pool_link_silo(&opctx, external_link)
            .await
            .expect("Should be able to link unlinked pool to default silo");

        // We should fail to reserve it for Oxide-internal use now.
        let (authz_pool, db_pool) = LookupPath::new(opctx, datastore)
            .ip_pool_id(ip_pool.id())
            .fetch_for(authz::Action::Modify)
            .await
            .unwrap();
        let res = datastore
            .ip_pool_reserve(
                opctx,
                &authz_pool,
                &db_pool,
                IpPoolReservationType::OxideInternal,
            )
            .await;
        let Err(Error::InvalidRequest { message }) = &res else {
            panic!(
                "Expected to fail delegating an IP Pool \
                when it's already linked to an external silo, found {res:#?}"
            );
        };
        assert_eq!(message.external_message(), BAD_SILO_LINK_ERROR);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Ensure that we fail to link to a silo that is deleted.
    #[tokio::test]
    async fn cannot_link_pool_to_deleted_silo() {
        let logctx = dev::test_setup_log("cannot_link_pool_to_deleted_silo");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create the pool
        let identity = IdentityMetadataCreateParams {
            name: "external-ip-pool".parse().unwrap(),
            description: "".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                opctx,
                IpPool::new(
                    &identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
            )
            .await
            .expect("Failed to create IP pool");

        // Delete the silo, directly to avoid a bunch of machinery.
        use nexus_db_schema::schema::silo::dsl;
        let c =
            diesel::update(dsl::silo.find(nexus_types::silo::DEFAULT_SILO_ID))
                .set(dsl::time_deleted.eq(diesel::dsl::now))
                .execute_async(
                    &*datastore
                        .pool_connection_authorized(opctx)
                        .await
                        .unwrap(),
                )
                .await
                .expect("Should be able to soft-delete silo");
        assert_eq!(c, 1, "Should have deleted something");

        // Now try link to it.
        let external_link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: nexus_types::silo::DEFAULT_SILO_ID,
            is_default: false,
        };
        let err = datastore
            .ip_pool_link_silo(&opctx, external_link)
            .await
            .expect_err("Should have failed to link IP Pool to deleted Silo");
        assert_matches!(err, Error::ObjectNotFound { .. });

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Ensure that we fail to link a deleted pool to a silo.
    #[tokio::test]
    async fn cannot_link_silo_to_deleted_pool() {
        let logctx = dev::test_setup_log("cannot_link_silo_to_deleted_pool");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create the pool
        let identity = IdentityMetadataCreateParams {
            name: "external-ip-pool".parse().unwrap(),
            description: "".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                opctx,
                IpPool::new(
                    &identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
            )
            .await
            .expect("Failed to create IP pool");

        // Delete the pool, directly to avoid a bunch of machinery.
        use nexus_db_schema::schema::ip_pool::dsl;
        let c = diesel::update(dsl::ip_pool.find(ip_pool.id()))
            .set(dsl::time_deleted.eq(diesel::dsl::now))
            .execute_async(
                &*datastore.pool_connection_authorized(opctx).await.unwrap(),
            )
            .await
            .expect("Should be able to soft-delete IP Pool");
        assert_eq!(c, 1, "Should have deleted something");

        // Now try link to it.
        let external_link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: nexus_types::silo::DEFAULT_SILO_ID,
            is_default: false,
        };
        let err = datastore
            .ip_pool_link_silo(&opctx, external_link)
            .await
            .expect_err("Should have failed to link deleted IP Pool to Silo");
        assert_matches!(err, Error::ObjectNotFound { .. });

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn can_explain_unlink_ip_pool_from_external_silo_query() {
        let logctx = dev::test_setup_log(
            "can_explain_unlink_ip_pool_from_external_silo_query",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let ip_pool_id = uuid::uuid!("aaa84fbd-85a5-4fcb-b34f-23b7e56145c7");
        let silo_id = nexus_types::silo::DEFAULT_SILO_ID;
        let query =
            unlink_ip_pool_from_external_silo_query(ip_pool_id, silo_id);
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_delete_last_internally_reserved_ip_pool() {
        let logctx = dev::test_setup_log(
            "cannot_delete_last_internally_reserved_ip_pool",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Fetch the pools.
        let pools = datastore
            .ip_pools_service_lookup_both_versions(opctx)
            .await
            .unwrap();

        // We should be able to delete one of these.
        let _ = datastore
            .ip_pool_delete(opctx, &pools.ipv4.authz_pool, &pools.ipv4.db_pool)
            .await
            .expect(
                "Should be able to delete internally-reserved \
                IP Pool when at least one remains",
            );

        // Check there's only one left.
        let pagparams = &PaginatedBy::Id(DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit: 100.try_into().unwrap(),
        });
        let l = datastore
            .ip_pools_list_paginated(
                opctx,
                IpPoolReservationType::OxideInternal,
                None,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(l.len(), 1);

        // We should _not_ be able to delete the other now, because there's only
        // one left.
        let res = datastore
            .ip_pool_delete(opctx, &pools.ipv6.authz_pool, &pools.ipv6.db_pool)
            .await;

        let Err(Error::InvalidRequest { message }) = &res else {
            panic!(
                "Should not be able to delete internally-reserved \
                IP Pool when only one remains, found {res:#?}"
            );
        };
        assert_eq!(message.external_message(), LAST_POOL_ERROR);

        let l = datastore
            .ip_pools_list_paginated(
                opctx,
                IpPoolReservationType::OxideInternal,
                None,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(l.len(), 1);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_externally_reserve_last_internally_reserved_ip_pool() {
        let logctx = dev::test_setup_log(
            "cannot_externally_reserve_last_internally_reserved_ip_pool",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Fetch the pools.
        let pools = datastore
            .ip_pools_service_lookup_both_versions(opctx)
            .await
            .unwrap();

        // We should be able to reserve one of these for external use.
        let _ = datastore
            .ip_pool_reserve(
                opctx,
                &pools.ipv4.authz_pool,
                &pools.ipv4.db_pool,
                IpPoolReservationType::ExternalSilos,
            )
            .await
            .expect(
                "Should be able to externally reserve IP Pool \
                when at least one internally-reserved pool remains",
            );

        // Check there's only one left.
        let pagparams = &PaginatedBy::Id(DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit: 100.try_into().unwrap(),
        });
        let l = datastore
            .ip_pools_list_paginated(
                opctx,
                IpPoolReservationType::OxideInternal,
                None,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(l.len(), 1);

        // We should _not_ be able to reserve the other for external use now,
        // because there's only one left for internal use.
        let res = datastore
            .ip_pool_reserve(
                opctx,
                &pools.ipv6.authz_pool,
                &pools.ipv6.db_pool,
                IpPoolReservationType::ExternalSilos,
            )
            .await;
        let Err(Error::InvalidRequest { message }) = &res else {
            panic!(
                "Should not be able to externally-reserve an \
                internally-reserved IP Pool when only one remains, \
                found {res:#?}"
            );
        };
        assert_eq!(message.external_message(), LAST_POOL_ERROR);

        let l = datastore
            .ip_pools_list_paginated(
                opctx,
                IpPoolReservationType::OxideInternal,
                None,
                &pagparams,
            )
            .await
            .unwrap();
        assert_eq!(l.len(), 1);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn cannot_externally_reserve_ip_pool_with_outstanding_external_ips() {
        let logctx = dev::test_setup_log(
            "cannot_externally_reserve_ip_pool_with_outstanding_external_ips",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Get pool, add a range, allocate an external IP.
        let pools = datastore
            .ip_pools_service_lookup_both_versions(opctx)
            .await
            .unwrap();
        let ip_range = IpRange::V4(Ipv4Range {
            first: Ipv4Addr::new(1, 1, 1, 1),
            last: Ipv4Addr::new(1, 1, 1, 10),
        });
        datastore
            .ip_pool_add_range(
                opctx,
                &pools.ipv4.authz_pool,
                &pools.ipv4.db_pool,
                &ip_range,
            )
            .await
            .unwrap();

        // Create an IP for an Omicron zone.
        let eip = datastore
            .external_ip_allocate_omicron_zone(
                opctx,
                OmicronZoneUuid::from_untyped_uuid(uuid::uuid!(
                    "b7b641d6-f52c-4fd5-b5a5-66ac3918c8b4"
                )),
                ZoneKind::BoundaryNtp,
                OmicronZoneExternalIp::Floating(
                    OmicronZoneExternalFloatingIp {
                        id: ExternalIpUuid::from_untyped_uuid(uuid::uuid!(
                            "4a7f86aa-5cab-42dd-afa5-7eee6304cb8c"
                        )),
                        ip: ip_range.first_address(),
                    },
                ),
            )
            .await
            .expect("Should be able to create zone external IP");

        // Should not be able to externally-reserve the IPv4 pool now, since
        // we've got an address in use.
        let res = datastore
            .ip_pool_reserve(
                opctx,
                &pools.ipv4.authz_pool,
                &pools.ipv4.db_pool,
                IpPoolReservationType::ExternalSilos,
            )
            .await;
        let Err(Error::InvalidRequest { message }) = &res else {
            panic!(
                "Should not be able to externally reserve internal \
                IP Pool when an address is in use, found {res:#?}"
            );
        };
        assert_eq!(message.external_message(), POOL_HAS_IPS_ERROR);

        // Delete the address, and now we can reserve the pool for external use.
        let _ = datastore
            .deallocate_external_ip(opctx, eip.id)
            .await
            .expect("Should be able to delete external IP");
        let _ = datastore.ip_pool_reserve(
            opctx,
            &pools.ipv4.authz_pool,
            &pools.ipv4.db_pool,
            IpPoolReservationType::ExternalSilos,
        ).await
            .expect(
                "Should be able to delete internal IP Pool when more than one remains, \
                after deleting external IP address"
            );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn can_explain_reserve_external_ip_pool_query() {
        let logctx =
            dev::test_setup_log("can_explain_reserve_external_ip_pool_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let ip_pool = IpPool {
            identity: IpPoolIdentity::new(
                uuid::uuid!("93fea64d-5d0a-4cc6-8f94-7c527ee640a9"),
                IdentityMetadataCreateParams {
                    name: "some-pool".parse().unwrap(),
                    description: String::new(),
                },
            ),
            ip_version: IpVersion::V4,
            rcgen: 0,
            reservation_type: IpPoolReservationType::ExternalSilos,
        };
        let query = reserve_ip_pool_query(
            &ip_pool,
            IpPoolReservationType::OxideInternal,
        );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_reserve_external_ip_pool_query() {
        let ip_pool = IpPool {
            identity: IpPoolIdentity::new(
                uuid::uuid!("93fea64d-5d0a-4cc6-8f94-7c527ee640a9"),
                IdentityMetadataCreateParams {
                    name: "some-pool".parse().unwrap(),
                    description: String::new(),
                },
            ),
            ip_version: IpVersion::V4,
            rcgen: 0,
            reservation_type: IpPoolReservationType::ExternalSilos,
        };
        let query = reserve_ip_pool_query(
            &ip_pool,
            IpPoolReservationType::OxideInternal,
        );
        expectorate_query_contents(
            &query,
            "tests/output/reserve_external_ip_pool.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn can_explain_reserve_internal_ip_pool_query() {
        let logctx =
            dev::test_setup_log("can_explain_reserve_internal_ip_pool_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let ip_pool = IpPool {
            identity: IpPoolIdentity::new(
                uuid::uuid!("93fea64d-5d0a-4cc6-8f94-7c527ee640a9"),
                IdentityMetadataCreateParams {
                    name: "some-pool".parse().unwrap(),
                    description: String::new(),
                },
            ),
            ip_version: IpVersion::V4,
            rcgen: 0,
            reservation_type: IpPoolReservationType::OxideInternal,
        };
        let query = reserve_ip_pool_query(
            &ip_pool,
            IpPoolReservationType::ExternalSilos,
        );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_reserve_internal_ip_pool_query() {
        let ip_pool = IpPool {
            identity: IpPoolIdentity::new(
                uuid::uuid!("93fea64d-5d0a-4cc6-8f94-7c527ee640a9"),
                IdentityMetadataCreateParams {
                    name: "some-pool".parse().unwrap(),
                    description: String::new(),
                },
            ),
            ip_version: IpVersion::V4,
            rcgen: 0,
            reservation_type: IpPoolReservationType::OxideInternal,
        };
        let query = reserve_ip_pool_query(
            &ip_pool,
            IpPoolReservationType::ExternalSilos,
        );
        expectorate_query_contents(
            &query,
            "tests/output/reserve_internal_ip_pool.sql",
        )
        .await;
    }
}
