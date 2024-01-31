// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP Pools, collections of external IP addresses for guest instances

use crate::external_api::params;
use crate::external_api::shared::IpRange;
use ipnetwork::IpNetwork;
use nexus_db_queries::authz;
use nexus_db_queries::authz::ApiResource;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::Name;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;
use uuid::Uuid;

/// Helper to make it easier to 404 on attempts to manipulate internal pools
fn not_found_from_lookup(pool_lookup: &lookup::IpPool<'_>) -> Error {
    match pool_lookup {
        lookup::IpPool::Name(_, name) => {
            Error::not_found_by_name(ResourceType::IpPool, &name)
        }
        lookup::IpPool::OwnedName(_, name) => {
            Error::not_found_by_name(ResourceType::IpPool, &name)
        }
        lookup::IpPool::PrimaryKey(_, id) => {
            Error::not_found_by_id(ResourceType::IpPool, &id)
        }
        lookup::IpPool::Error(_, error) => error.to_owned(),
    }
}

impl super::Nexus {
    pub fn ip_pool_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        pool: &'a NameOrId,
    ) -> LookupResult<lookup::IpPool<'a>> {
        match pool {
            NameOrId::Name(name) => {
                let pool = LookupPath::new(opctx, &self.db_datastore)
                    .ip_pool_name(Name::ref_cast(name));
                Ok(pool)
            }
            NameOrId::Id(id) => {
                let pool =
                    LookupPath::new(opctx, &self.db_datastore).ip_pool_id(*id);
                Ok(pool)
            }
        }
    }

    pub(crate) async fn ip_pool_create(
        &self,
        opctx: &OpContext,
        pool_params: &params::IpPoolCreate,
    ) -> CreateResult<db::model::IpPool> {
        let pool = db::model::IpPool::new(&pool_params.identity);
        self.db_datastore.ip_pool_create(opctx, pool).await
    }

    /// List IP pools in current silo
    pub(crate) async fn current_silo_ip_pool_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<(db::model::IpPool, db::model::IpPoolResource)> {
        let authz_silo =
            opctx.authn.silo_required().internal_context("listing IP pools")?;

        // From the developer user's point of view, we treat IP pools linked to
        // their silo as silo resources, so they can list them if they can list
        // silo children
        opctx.authorize(authz::Action::ListChildren, &authz_silo).await?;

        self.db_datastore.silo_ip_pool_list(opctx, &authz_silo, pagparams).await
    }

    // Look up pool by name or ID, but only return it if it's linked to the
    // current silo
    pub async fn silo_ip_pool_fetch<'a>(
        &'a self,
        opctx: &'a OpContext,
        pool: &'a NameOrId,
    ) -> LookupResult<(db::model::IpPool, db::model::IpPoolResource)> {
        let (authz_pool, pool) =
            self.ip_pool_lookup(opctx, pool)?.fetch().await?;

        // 404 if no link is found in the current silo
        let link = self.db_datastore.ip_pool_fetch_link(opctx, pool.id()).await;
        match link {
            Ok(link) => Ok((pool, link)),
            Err(_) => Err(authz_pool.not_found()),
        }
    }

    /// List silos for a given pool
    pub(crate) async fn ip_pool_silo_list(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::IpPoolResource> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::ListChildren).await?;

        // check ability to list silos in general
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        self.db_datastore.ip_pool_silo_list(opctx, &authz_pool, pagparams).await
    }

    // List pools for a given silo
    pub(crate) async fn silo_ip_pool_list(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<(db::model::IpPool, db::model::IpPoolResource)> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Read).await?;
        // check ability to list pools in general
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;
        self.db_datastore.silo_ip_pool_list(opctx, &authz_silo, pagparams).await
    }

    pub(crate) async fn ip_pool_link_silo(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        silo_link: &params::IpPoolLinkSilo,
    ) -> CreateResult<db::model::IpPoolResource> {
        let (authz_pool,) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;
        let (authz_silo,) = self
            .silo_lookup(&opctx, silo_link.silo.clone())?
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .ip_pool_link_silo(
                opctx,
                db::model::IpPoolResource {
                    ip_pool_id: authz_pool.id(),
                    resource_type: db::model::IpPoolResourceType::Silo,
                    resource_id: authz_silo.id(),
                    is_default: silo_link.is_default,
                },
            )
            .await
    }

    pub(crate) async fn ip_pool_unlink_silo(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        silo_lookup: &lookup::Silo<'_>,
    ) -> DeleteResult {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;

        self.db_datastore
            .ip_pool_unlink_silo(opctx, &authz_pool, &authz_silo)
            .await
    }

    pub(crate) async fn ip_pool_silo_update(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        silo_lookup: &lookup::Silo<'_>,
        update: &params::IpPoolSiloUpdate,
    ) -> CreateResult<db::model::IpPoolResource> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;

        self.db_datastore
            .ip_pool_set_default(
                opctx,
                &authz_pool,
                &authz_silo,
                update.is_default,
            )
            .await
    }

    pub(crate) async fn ip_pools_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::IpPool> {
        self.db_datastore.ip_pools_list(opctx, pagparams).await
    }

    pub(crate) async fn ip_pool_delete(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
    ) -> DeleteResult {
        let (.., authz_pool, db_pool) =
            pool_lookup.fetch_for(authz::Action::Delete).await?;

        let is_internal =
            self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await?;
        if is_internal {
            return Err(not_found_from_lookup(pool_lookup));
        }

        self.db_datastore.ip_pool_delete(opctx, &authz_pool, &db_pool).await
    }

    pub(crate) async fn ip_pool_update(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        updates: &params::IpPoolUpdate,
    ) -> UpdateResult<db::model::IpPool> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;

        let is_internal =
            self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await?;
        if is_internal {
            return Err(not_found_from_lookup(pool_lookup));
        }

        self.db_datastore
            .ip_pool_update(opctx, &authz_pool, updates.clone().into())
            .await
    }

    pub(crate) async fn ip_pool_list_ranges(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        pagparams: &DataPageParams<'_, IpNetwork>,
    ) -> ListResultVec<db::model::IpPoolRange> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::ListChildren).await?;

        let is_internal =
            self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await?;
        if is_internal {
            return Err(not_found_from_lookup(pool_lookup));
        }

        self.db_datastore
            .ip_pool_list_ranges(opctx, &authz_pool, pagparams)
            .await
    }

    pub(crate) async fn ip_pool_add_range(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        range: &IpRange,
    ) -> UpdateResult<db::model::IpPoolRange> {
        let (.., authz_pool, _db_pool) =
            pool_lookup.fetch_for(authz::Action::Modify).await?;

        let is_internal =
            self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await?;
        if is_internal {
            return Err(not_found_from_lookup(pool_lookup));
        }
        self.db_datastore.ip_pool_add_range(opctx, &authz_pool, range).await
    }

    pub(crate) async fn ip_pool_delete_range(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        range: &IpRange,
    ) -> DeleteResult {
        let (.., authz_pool, _db_pool) =
            pool_lookup.fetch_for(authz::Action::Modify).await?;

        let is_internal =
            self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await?;

        if is_internal {
            return Err(not_found_from_lookup(pool_lookup));
        }

        self.db_datastore.ip_pool_delete_range(opctx, &authz_pool, range).await
    }

    // The "ip_pool_service_..." functions look up IP pools for Oxide service usage,
    // rather than for VMs.
    //
    // TODO(https://github.com/oxidecomputer/omicron/issues/1276): Should be
    // accessed via AZ UUID, probably.

    pub(crate) async fn ip_pool_service_fetch(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<db::model::IpPool> {
        let (authz_pool, db_pool) =
            self.db_datastore.ip_pools_service_lookup(opctx).await?;
        opctx.authorize(authz::Action::Read, &authz_pool).await?;
        Ok(db_pool)
    }

    pub(crate) async fn ip_pool_service_list_ranges(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, IpNetwork>,
    ) -> ListResultVec<db::model::IpPoolRange> {
        let (authz_pool, ..) =
            self.db_datastore.ip_pools_service_lookup(opctx).await?;
        opctx.authorize(authz::Action::Read, &authz_pool).await?;
        self.db_datastore
            .ip_pool_list_ranges(opctx, &authz_pool, pagparams)
            .await
    }

    pub(crate) async fn ip_pool_service_add_range(
        &self,
        opctx: &OpContext,
        range: &IpRange,
    ) -> UpdateResult<db::model::IpPoolRange> {
        let (authz_pool, ..) =
            self.db_datastore.ip_pools_service_lookup(opctx).await?;
        opctx.authorize(authz::Action::Modify, &authz_pool).await?;
        self.db_datastore.ip_pool_add_range(opctx, &authz_pool, range).await
    }

    pub(crate) async fn ip_pool_service_delete_range(
        &self,
        opctx: &OpContext,
        range: &IpRange,
    ) -> DeleteResult {
        let (authz_pool, ..) =
            self.db_datastore.ip_pools_service_lookup(opctx).await?;
        opctx.authorize(authz::Action::Modify, &authz_pool).await?;
        self.db_datastore.ip_pool_delete_range(opctx, &authz_pool, range).await
    }
}
