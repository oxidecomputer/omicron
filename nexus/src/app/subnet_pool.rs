// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subnet Pools, collections of IP subnets for external subnet allocation
//!
//! TODO(#9453): This module contains stub implementations that return
//! "not implemented" errors. Full implementation requires:
//! - Database schema and models (see nexus/db-model/)
//! - Datastore methods (see nexus/db-queries/src/db/datastore/)
//! - Authorization resources (see nexus/auth/src/authz/)
//! - Replacing these stubs with real implementations

use crate::app::Unimpl;
use nexus_auth::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::{params, views};
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use oxnet::IpNet;
use uuid::Uuid;

// TODO(#9453): Remove this helper once real lookup logic is implemented.
// When using LookupPath for database lookups, errors are handled internally
// by the lookup machinery, making this helper unnecessary.
fn not_found_error(pool: &NameOrId, resource_type: ResourceType) -> Error {
    match pool {
        NameOrId::Name(name) => Error::not_found_by_name(resource_type, &name),
        NameOrId::Id(id) => Error::not_found_by_id(resource_type, &id),
    }
}

impl super::Nexus {
    // === Subnet Pool CRUD ===

    pub(crate) async fn subnet_pool_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<views::SubnetPool> {
        self.datastore()
            .list_subnet_pools(opctx, pagparams)
            .await
            .map(|list| list.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn subnet_pool_create(
        &self,
        opctx: &OpContext,
        pool_params: params::SubnetPoolCreate,
    ) -> Result<views::SubnetPool, Error> {
        self.datastore()
            .create_subnet_pool(opctx, pool_params)
            .await
            .map(Into::into)
    }

    pub(crate) async fn subnet_pool_view(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
    ) -> LookupResult<views::SubnetPool> {
        self.datastore()
            .lookup_subnet_pool(opctx, pool)
            .fetch()
            .await
            .map(|(_authz_pool, db_pool)| db_pool.into())
    }

    pub(crate) async fn subnet_pool_update(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        params: params::SubnetPoolUpdate,
    ) -> UpdateResult<views::SubnetPool> {
        let (authz_pool, _db_pool) = self
            .datastore()
            .lookup_subnet_pool(opctx, pool)
            .fetch_for(authz::Action::Modify)
            .await?;
        self.datastore()
            .update_subnet_pool(opctx, &authz_pool, params.into())
            .await
            .map(Into::into)
    }

    pub(crate) async fn subnet_pool_delete(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
    ) -> DeleteResult {
        let (authz_pool, db_pool) = self
            .datastore()
            .lookup_subnet_pool(opctx, pool)
            .fetch_for(authz::Action::Delete)
            .await?;
        self.datastore().delete_subnet_pool(opctx, &authz_pool, &db_pool).await
    }

    // === Subnet Range Management ===

    pub(crate) async fn subnet_pool_member_list(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        pagparams: &DataPageParams<'_, IpNet>,
    ) -> ListResultVec<views::SubnetPoolMember> {
        let (authz_pool, _db_pool) = self
            .datastore()
            .lookup_subnet_pool(opctx, pool)
            .fetch_for(authz::Action::ListChildren)
            .await?;
        // This is a little awkward, but the database query is paginated by our
        // model IpNet type, and the input here is an `oxnet::IpNet`. We can't
        // use `DataPageParams::map_name()`, because that accepts a function
        // that returns a _reference_, but we need to make an owned value here.
        //
        // Build a new set of pag params instead, since the actual IP CIDR is
        // the same in either case.
        let marker = pagparams.marker.map(|net| (*net).into());
        let new_pagparams = DataPageParams {
            marker: marker.as_ref(),
            direction: pagparams.direction,
            limit: pagparams.limit,
        };
        self.datastore()
            .list_subnet_pool_members(opctx, &authz_pool, &new_pagparams)
            .await
            .map(|items| items.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn subnet_pool_member_add(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        params: &params::SubnetPoolMemberAdd,
    ) -> Result<views::SubnetPoolMember, Error> {
        let (authz_pool, db_pool) = self
            .datastore()
            .lookup_subnet_pool(opctx, pool)
            .fetch_for(authz::Action::CreateChild)
            .await?;
        self.datastore()
            .add_subnet_pool_member(opctx, &authz_pool, &db_pool, params)
            .await
            .map(Into::into)
    }

    pub(crate) async fn subnet_pool_member_remove(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        params: &params::SubnetPoolMemberRemove,
    ) -> DeleteResult {
        let (authz_pool, _db_pool) = self
            .datastore()
            .lookup_subnet_pool(opctx, pool)
            .fetch_for(authz::Action::Modify)
            .await?;
        self.datastore()
            .delete_subnet_pool_member(opctx, &authz_pool, params.subnet.into())
            .await
    }

    // === Silo Linkage ===

    // TODO(#9453): Implement using subnet_pool_lookup and datastore list
    pub(crate) async fn subnet_pool_silo_list(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        _pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<views::SubnetPoolSiloLink> {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // TODO(#9453): Implement using subnet_pool_lookup and datastore insert
    pub(crate) async fn subnet_pool_silo_link(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        _params: &params::SubnetPoolLinkSilo,
    ) -> Result<views::SubnetPoolSiloLink, Error> {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // TODO(#9453): Implement using subnet_pool_lookup, silo_lookup, and datastore
    pub(crate) async fn subnet_pool_silo_update(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        _silo: &NameOrId,
        _params: &params::SubnetPoolSiloUpdate,
    ) -> UpdateResult<views::SubnetPoolSiloLink> {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // TODO(#9453): Implement using subnet_pool_lookup, silo_lookup, and datastore
    pub(crate) async fn subnet_pool_silo_unlink(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        _silo: &NameOrId,
    ) -> DeleteResult {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // === Utilization ===

    // TODO(#9453): Implement using subnet_pool_lookup and datastore aggregation
    pub(crate) async fn subnet_pool_utilization_view(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
    ) -> LookupResult<views::SubnetPoolUtilization> {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }
}
