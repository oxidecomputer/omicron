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
use crate::db::datastore::SERVICE_IP_POOL_NAME;
use crate::db::error::public_error_from_diesel;
use crate::db::error::public_error_from_diesel_lookup;
use crate::db::error::ErrorHandler;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::db::model::ExternalIp;
use crate::db::model::IpKind;
use crate::db::model::IpPool;
use crate::db::model::IpPoolRange;
use crate::db::model::IpPoolResource;
use crate::db::model::IpPoolResourceType;
use crate::db::model::IpPoolUpdate;
use crate::db::model::Name;
use crate::db::pagination::paginated;
use crate::db::pool::DbConnection;
use crate::db::queries::ip_pool::FilterOverlappingIpRanges;
use crate::db::TransactionError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use ipnetwork::IpNetwork;
use nexus_types::external_api::shared::IpRange;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
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
        .filter(ip_pool::name.ne(SERVICE_IP_POOL_NAME))
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
        use db::schema::ip_pool;
        use db::schema::ip_pool_resource;

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

    /// Look up IP pool intended for internal services by its well-known name.
    ///
    /// This method may require an index by Availability Zone in the future.
    pub async fn ip_pools_service_lookup(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<(authz::IpPool, IpPool)> {
        let name = SERVICE_IP_POOL_NAME.parse().unwrap();
        LookupPath::new(&opctx, self).ip_pool_name(&Name(name)).fetch().await
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
        use db::schema::ip_pool_resource;
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
        use db::schema::ip_pool;

        ip_pool::table
            .filter(ip_pool::id.eq(authz_pool.id()))
            .filter(ip_pool::name.eq(SERVICE_IP_POOL_NAME))
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

    pub async fn ip_pool_silo_list(
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

    /// Returns (IpPool, IpPoolResource) so we can know in the calling code
    /// whether the pool is default for the silo
    pub async fn silo_ip_pool_list(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<(IpPool, IpPoolResource)> {
        use db::schema::ip_pool;
        use db::schema::ip_pool_resource;

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
        use db::schema::ip_pool_resource::dsl;
        opctx
            .authorize(authz::Action::CreateChild, &authz::IP_POOL_LIST)
            .await?;

        diesel::insert_into(dsl::ip_pool_resource)
            .values(ip_pool_resource.clone())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
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
            })
    }

    pub async fn ip_pool_set_default(
        &self,
        opctx: &OpContext,
        authz_ip_pool: &authz::IpPool,
        authz_silo: &authz::Silo,
        is_default: bool,
    ) -> UpdateResult<IpPoolResource> {
        use db::schema::ip_pool_resource::dsl;

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

        conn.transaction_async(|conn| async move {
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

                let unset_default = diesel::update(dsl::ip_pool_resource)
                    .filter(dsl::resource_id.eq(existing_default.resource_id))
                    .filter(dsl::ip_pool_id.eq(existing_default.ip_pool_id))
                    .filter(
                        dsl::resource_type.eq(existing_default.resource_type),
                    )
                    .set(dsl::is_default.eq(false))
                    .execute_async(&conn)
                    .await;
                if let Err(e) = unset_default {
                    return Err(TxnError::CustomError(
                        IpPoolResourceUpdateError::FailedToUnsetDefault(e),
                    ));
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
        })
        .await
        .map_err(|e| match e {
            TransactionError::CustomError(
                IpPoolResourceUpdateError::FailedToUnsetDefault(e),
            ) => public_error_from_diesel(e, ErrorHandler::Server),
            TransactionError::Database(e) => public_error_from_diesel(
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

    /// Ephemeral and snat IPs are associated with a silo through an instance,
    /// so in order to see if there are any such IPs outstanding in the given
    /// silo, we have to join IP -> Instance -> Project -> Silo
    async fn ensure_no_instance_ips_outstanding(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
        authz_silo: &authz::Silo,
    ) -> Result<(), Error> {
        use db::schema::external_ip;
        use db::schema::instance;
        use db::schema::project;

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
        use db::schema::external_ip;
        use db::schema::project;

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
        use db::schema::ip_pool_resource;

        opctx.authorize(authz::Action::Modify, authz_pool).await?;
        opctx.authorize(authz::Action::Modify, authz_silo).await?;

        // We can only delete the association if there are no IPs allocated
        // from this pool in the associated resource.
        self.ensure_no_instance_ips_outstanding(opctx, authz_pool, authz_silo)
            .await?;
        self.ensure_no_floating_ips_outstanding(opctx, authz_pool, authz_silo)
            .await?;

        diesel::delete(ip_pool_resource::table)
            .filter(ip_pool_resource::ip_pool_id.eq(authz_pool.id()))
            .filter(ip_pool_resource::resource_id.eq(authz_silo.id()))
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
    use std::num::NonZeroU32;

    use crate::authz;
    use crate::db::datastore::datastore_test;
    use crate::db::model::{IpPool, IpPoolResource, IpPoolResourceType};
    use assert_matches::assert_matches;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::identity::Resource;
    use omicron_common::api::external::http_pagination::PaginatedBy;
    use omicron_common::api::external::{
        DataPageParams, Error, IdentityMetadataCreateParams, LookupType,
    };
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_default_ip_pools() {
        let logctx = dev::test_setup_log("test_default_ip_pools");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

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
            .ip_pool_create(&opctx, IpPool::new(&identity))
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
            .ip_pool_create(&opctx, IpPool::new(&identity))
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

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_internal_ip_pool() {
        let logctx = dev::test_setup_log("test_internal_ip_pool");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // confirm internal pool appears as internal
        let (authz_pool, _pool) =
            datastore.ip_pools_service_lookup(&opctx).await.unwrap();

        let is_internal =
            datastore.ip_pool_is_internal(&opctx, &authz_pool).await;
        assert_eq!(is_internal, Ok(true));

        // another random pool should not be considered internal
        let identity = IdentityMetadataCreateParams {
            name: "other-pool".parse().unwrap(),
            description: "".to_string(),
        };
        let other_pool = datastore
            .ip_pool_create(&opctx, IpPool::new(&identity))
            .await
            .expect("Failed to create IP pool");

        let authz_other_pool = authz::IpPool::new(
            authz::FLEET,
            other_pool.id(),
            LookupType::ById(other_pool.id()),
        );
        let is_internal =
            datastore.ip_pool_is_internal(&opctx, &authz_other_pool).await;
        assert_eq!(is_internal, Ok(false));

        // now link it to the current silo, and it is still not internal
        let silo_id = opctx.authn.silo_required().unwrap().id();
        let link = IpPoolResource {
            ip_pool_id: other_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: silo_id,
            is_default: true,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Failed to make IP pool default for silo");

        let is_internal =
            datastore.ip_pool_is_internal(&opctx, &authz_other_pool).await;
        assert_eq!(is_internal, Ok(false));

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
