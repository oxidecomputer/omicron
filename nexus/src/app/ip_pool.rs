// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP Pools, collections of external IP addresses for guest instances

use crate::external_api::params;
use crate::external_api::shared::IpRange;
use ipnetwork::IpNetwork;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::fixed_data::FLEET_ID;
// use nexus_db_queries::db::fixed_data::silo::INTERNAL_SILO_ID;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::Name;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;

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

    pub(crate) async fn ip_pool_associate_resource(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        assoc_create: &params::IpPoolAssociationCreate,
    ) -> CreateResult<db::model::IpPoolResource> {
        // TODO: check for perms on specified resource? or unnecessary because this is an operator action?
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;
        let (resource_type, resource_id, is_default) = match assoc_create {
            params::IpPoolAssociationCreate::Silo(assoc_silo) => {
                let (silo,) = self
                    .silo_lookup(&opctx, assoc_silo.silo.clone())?
                    .lookup_for(authz::Action::Read)
                    .await?;
                (
                    db::model::IpPoolResourceType::Silo,
                    silo.id(),
                    assoc_silo.is_default,
                )
            }
            params::IpPoolAssociationCreate::Fleet(assoc_fleet) => {
                // we don't need to be assured of the fleet's existence
                (
                    db::model::IpPoolResourceType::Fleet,
                    *FLEET_ID,
                    assoc_fleet.is_default,
                )
            }
        };
        self.db_datastore
            .ip_pool_associate_resource(
                opctx,
                db::model::IpPoolResource {
                    ip_pool_id: authz_pool.id(),
                    resource_type,
                    resource_id,
                    is_default,
                },
            )
            .await
    }

    pub(crate) async fn ip_pool_dissociate_resource(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        ip_pool_dissoc: &params::IpPoolAssociationDelete,
    ) -> DeleteResult {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .ip_pool_dissociate_resource(
                opctx,
                authz_pool.id(),
                ip_pool_dissoc.resource_id,
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
