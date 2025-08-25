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
use crate::db::model::ExternalIp;
use crate::db::model::IpKind;
use crate::db::model::IpPool;
use crate::db::model::IpPoolRange;
use crate::db::model::IpPoolResource;
use crate::db::model::IpPoolResourceType;
use crate::db::model::IpPoolUpdate;
use crate::db::model::Name;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::queries::ip_pool::FilterOverlappingIpRanges;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
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
use nexus_types::external_api::shared::IpRange;
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

pub struct IpsAllocated {
    pub ipv4: i64,
    pub ipv6: i64,
}

pub struct IpsCapacity {
    pub ipv4: u32,
    pub ipv6: u128,
}

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

impl DataStore {
    /// List IP Pools
    pub async fn ip_pools_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<IpPool> {
        use nexus_db_schema::schema::ip_pool;

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
        .filter(ip_pool::name.ne(SERVICE_IPV4_POOL_NAME))
        .filter(ip_pool::name.ne(SERVICE_IPV6_POOL_NAME))
        .filter(ip_pool::time_deleted.is_null())
        .select(IpPool::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Look up whether the given pool is available to users in the current
    /// silo, i.e., whether there is an entry in the association table linking
    /// the pool with that silo
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
    // NOTE: It'd be better to do one roundtrip to the DB, but this is
    // rarely-used right now. We also want to return the authz and database
    // objects, so we need the lookup-path mechanism.
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

        // Delete the pool, conditional on the rcgen not having changed. This
        // protects the delete from occuring if clients created a new IP range
        // in between the above check for children and this query.
        let now = Utc::now();
        let updated_rows = diesel::update(dsl::ip_pool)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_pool.id()))
            .filter(dsl::rcgen.eq(db_pool.rcgen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                )
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
    pub async fn ip_pool_is_internal(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> LookupResult<bool> {
        use nexus_db_schema::schema::ip_pool;

        ip_pool::table
            .filter(ip_pool::id.eq(authz_pool.id()))
            .filter(
                ip_pool::name
                    .eq(SERVICE_IPV4_POOL_NAME)
                    .or(ip_pool::name.eq(SERVICE_IPV6_POOL_NAME)),
            )
            .filter(ip_pool::time_deleted.is_null())
            .select(ip_pool::id)
            .first_async::<Uuid>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            // if there is a result, the pool is associated with the internal silo,
            // which makes it the internal pool
            .map(|result| Ok(result.is_some()))
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
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

    pub async fn ip_pool_allocated_count(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> Result<IpsAllocated, Error> {
        opctx.authorize(authz::Action::Read, authz_pool).await?;

        use diesel::dsl::sql;
        use diesel::sql_types::BigInt;
        use nexus_db_schema::schema::external_ip;

        let (ipv4, ipv6) = external_ip::table
            .filter(external_ip::ip_pool_id.eq(authz_pool.id()))
            .filter(external_ip::time_deleted.is_null())
            // need to do distinct IP because SNAT IPs are shared between
            // multiple instances, and each gets its own row in the table
            .select((
                sql::<BigInt>(
                    "count(distinct ip) FILTER (WHERE family(ip) = 4)",
                ),
                sql::<BigInt>(
                    "count(distinct ip) FILTER (WHERE family(ip) = 6)",
                ),
            ))
            .first_async::<(i64, i64)>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(IpsAllocated { ipv4, ipv6 })
    }

    pub async fn ip_pool_total_capacity(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> Result<IpsCapacity, Error> {
        opctx.authorize(authz::Action::Read, authz_pool).await?;
        opctx.authorize(authz::Action::ListChildren, authz_pool).await?;

        use nexus_db_schema::schema::ip_pool_range;

        let ranges = ip_pool_range::table
            .filter(ip_pool_range::ip_pool_id.eq(authz_pool.id()))
            .filter(ip_pool_range::time_deleted.is_null())
            .select(IpPoolRange::as_select())
            // This is a rare unpaginated DB query, which means we are
            // vulnerable to a resource exhaustion attack in which someone
            // creates a very large number of ranges in order to make this
            // query slow. In order to mitigate that, we limit the query to the
            // (current) max allowed page size, effectively making this query
            // exactly as vulnerable as if it were paginated. If there are more
            // than 10,000 ranges in a pool, we will undercount, but I have a
            // hard time seeing that as a practical problem.
            .limit(10000)
            .get_results_async::<IpPoolRange>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                )
            })?;

        let mut ipv4: u32 = 0;
        let mut ipv6: u128 = 0;

        for range in &ranges {
            let r = IpRange::from(range);
            match r {
                IpRange::V4(r) => ipv4 += r.len(),
                IpRange::V6(r) => ipv6 += r.len(),
            }
        }
        Ok(IpsCapacity { ipv4, ipv6 })
    }

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

    pub async fn ip_pool_link_silo(
        &self,
        opctx: &OpContext,
        ip_pool_resource: IpPoolResource,
    ) -> CreateResult<IpPoolResource> {
        use nexus_db_schema::schema::ip_pool_resource::dsl;
        opctx
            .authorize(authz::Action::CreateChild, &authz::IP_POOL_LIST)
            .await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        let result = diesel::insert_into(dsl::ip_pool_resource)
            .values(ip_pool_resource.clone())
            .get_result_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::IpPoolResource,
                        &format!(
                            "ip_pool_id: {:?}, resource_id: {:?}, resource_type: {:?}",
                            ip_pool_resource.ip_pool_id,
                            ip_pool_resource.resource_id,
                            ip_pool_resource.resource_type,
                        )
                    ),
                )
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
                None => {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByLookup(
                            ResourceType::IpPoolResource,
                            // TODO: would be nice to put the actual names and/or ids in
                            // here but LookupType on each of the two silos doesn't have
                            // a nice to_string yet or a way of composing them
                            LookupType::ByCompositeId(
                                "(pool, silo)".to_string(),
                            ),
                        ),
                    )
                }
            })
    }

    /// Ephemeral and snat IPs are associated with a silo through an instance,
    /// so in order to see if there are any such IPs outstanding in the given
    /// silo, we have to join IP -> Instance -> Project -> Silo
    async fn ensure_no_instance_ips_outstanding(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        authz_silo: &authz::Silo,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::external_ip;
        use nexus_db_schema::schema::instance;
        use nexus_db_schema::schema::project;

        let existing_ips = external_ip::table
            .inner_join(
                instance::table
                    .on(external_ip::parent_id.eq(instance::id.nullable())),
            )
            .inner_join(project::table.on(instance::project_id.eq(project::id)))
            .filter(external_ip::is_service.eq(false))
            .filter(external_ip::parent_id.is_not_null())
            .filter(external_ip::time_deleted.is_null())
            .filter(external_ip::ip_pool_id.eq(authz_pool.id()))
            // important, floating IPs are handled separately
            .filter(external_ip::kind.eq(IpKind::Ephemeral).or(external_ip::kind.eq(IpKind::SNat)))
            .filter(instance::time_deleted.is_null())
            // we have to join through IPs to instances to projects to get the silo ID
            .filter(project::silo_id.eq(authz_silo.id()))
            .select(ExternalIp::as_select())
            .limit(1)
            .load_async::<ExternalIp>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error checking for outstanding IPs before deleting IP pool association to resource: {:?}",
                    e
                ))
            })?;

        if !existing_ips.is_empty() {
            return Err(Error::invalid_request(
                "IP addresses from this pool are in use in the linked silo",
            ));
        }

        Ok(())
    }

    /// Floating IPs are associated with a silo through a project, so this one
    /// is a little simpler than ephemeral. We join IP -> Project -> Silo.
    async fn ensure_no_floating_ips_outstanding(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        authz_silo: &authz::Silo,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::external_ip;
        use nexus_db_schema::schema::project;

        let existing_ips = external_ip::table
            .inner_join(project::table.on(external_ip::project_id.eq(project::id.nullable())))
            .filter(external_ip::is_service.eq(false))
            .filter(external_ip::time_deleted.is_null())
            // all floating IPs have a project
            .filter(external_ip::project_id.is_not_null())
            .filter(external_ip::ip_pool_id.eq(authz_pool.id()))
            .filter(external_ip::kind.eq(IpKind::Floating))
            // we have to join through IPs to projects to get the silo ID
            .filter(project::silo_id.eq(authz_silo.id()))
            .filter(project::time_deleted.is_null())
            .select(ExternalIp::as_select())
            .limit(1)
            .load_async::<ExternalIp>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error checking for outstanding IPs before deleting IP pool association to resource: {:?}",
                    e
                ))
            })?;

        if !existing_ips.is_empty() {
            return Err(Error::invalid_request(
                "IP addresses from this pool are in use in the linked silo",
            ));
        }

        Ok(())
    }

    /// Delete IP pool assocation with resource unless there are outstanding
    /// IPs allocated from the pool in the associated silo
    pub async fn ip_pool_unlink_silo(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        authz_silo: &authz::Silo,
    ) -> DeleteResult {
        use nexus_db_schema::schema::ip_pool_resource;

        opctx.authorize(authz::Action::Modify, authz_pool).await?;
        opctx.authorize(authz::Action::Modify, authz_silo).await?;

        // We can only delete the association if there are no IPs allocated
        // from this pool in the associated resource.
        self.ensure_no_instance_ips_outstanding(opctx, authz_pool, authz_silo)
            .await?;
        self.ensure_no_floating_ips_outstanding(opctx, authz_pool, authz_silo)
            .await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        diesel::delete(ip_pool_resource::table)
            .filter(ip_pool_resource::ip_pool_id.eq(authz_pool.id()))
            .filter(ip_pool_resource::resource_id.eq(authz_silo.id()))
            .execute_async(&*conn)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error deleting IP pool association to resource: {:?}",
                    e
                ))
            })?;

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

#[cfg(test)]
mod test {
    use std::num::NonZeroU32;

    use crate::authz;
    use crate::db::model::{
        IpPool, IpPoolResource, IpPoolResourceType, Project,
    };
    use crate::db::pub_test_utils::TestDatabase;
    use assert_matches::assert_matches;
    use nexus_db_model::IpVersion;
    use nexus_types::external_api::params;
    use nexus_types::identity::Resource;
    use omicron_common::address::{IpRange, Ipv4Range, Ipv6Range};
    use omicron_common::api::external::http_pagination::PaginatedBy;
    use omicron_common::api::external::{
        DataPageParams, Error, IdentityMetadataCreateParams, LookupType,
    };
    use omicron_test_utils::dev;

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
            .ip_pool_create(&opctx, IpPool::new(&identity, IpVersion::V4))
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
            .ip_pool_link_silo(&opctx, link_body.clone())
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
            .ip_pool_create(&opctx, IpPool::new(&identity, IpVersion::V4))
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
        datastore
            .ip_pool_unlink_silo(&opctx, &authz_pool1_for_silo, &authz_silo)
            .await
            .expect("Failed to unlink IP pool from silo");

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
                .ip_pool_create(&opctx, IpPool::new(&identity, version))
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
            //
            // We're only making the IPv4 pool the default right now. See
            // https://github.com/oxidecomputer/omicron/issues/8884 for more.
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
            .ip_pool_create(&opctx, IpPool::new(&identity, IpVersion::V4))
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
        assert_eq!(max_ips.ipv4, 0);
        assert_eq!(max_ips.ipv6, 0);

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
        assert_eq!(max_ips.ipv4, 5);
        assert_eq!(max_ips.ipv6, 0);

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
        assert_eq!(ip_count.ipv4, 0);
        assert_eq!(ip_count.ipv6, 0);

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
        assert_eq!(ip_count.ipv4, 1);
        assert_eq!(ip_count.ipv6, 0);

        // allocating one has nothing to do with total capacity
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips.ipv4, 5);
        assert_eq!(max_ips.ipv6, 0);

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
            .ip_pool_create(&opctx, IpPool::new(&identity, IpVersion::V6))
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
        assert_eq!(max_ips.ipv4, 0);
        assert_eq!(max_ips.ipv6, 0);

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
        assert_eq!(max_ips.ipv4, 0);
        assert_eq!(max_ips.ipv6, 11 + 65536);

        let ip_count = datastore
            .ip_pool_allocated_count(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(ip_count.ipv4, 0);
        assert_eq!(ip_count.ipv6, 0);

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
        assert_eq!(ip_count.ipv4, 0);
        assert_eq!(ip_count.ipv6, 1);

        // allocating one has nothing to do with total capacity
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips.ipv4, 0);
        assert_eq!(max_ips.ipv6, 11 + 65536);

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
        assert_eq!(max_ips.ipv4, 0);
        assert_eq!(max_ips.ipv6, 1208925819614629174706166);

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
                .ip_pool_create(&opctx, IpPool::new(&identity, version))
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
}
