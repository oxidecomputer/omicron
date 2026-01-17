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

    // TODO(#9453): Implement using datastore subnet_pool_list method
    pub(crate) async fn subnet_pool_list(
        &self,
        opctx: &OpContext,
        _pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<views::SubnetPool> {
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    // TODO(#9453): Implement using datastore subnet_pool_create method
    pub(crate) async fn subnet_pool_create(
        &self,
        opctx: &OpContext,
        _pool_params: &params::SubnetPoolCreate,
    ) -> Result<views::SubnetPool, Error> {
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    // TODO(#9453): Implement using subnet_pool_lookup and fetch
    pub(crate) async fn subnet_pool_view(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
    ) -> LookupResult<views::SubnetPool> {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // TODO(#9453): Implement using subnet_pool_lookup and datastore update
    pub(crate) async fn subnet_pool_update(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        _params: &params::SubnetPoolUpdate,
    ) -> UpdateResult<views::SubnetPool> {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // TODO(#9453): Implement using subnet_pool_lookup and datastore delete
    pub(crate) async fn subnet_pool_delete(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
    ) -> DeleteResult {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // === Subnet Range Management ===

    // TODO(#9453): Implement using subnet_pool_lookup and datastore list
    pub(crate) async fn subnet_pool_member_list(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        _pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<views::SubnetPoolMember> {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // TODO(#9453): Implement using subnet_pool_lookup and datastore insert
    pub(crate) async fn subnet_pool_subnet_add(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        _params: &params::SubnetPoolMemberAdd,
    ) -> Result<views::SubnetPoolMember, Error> {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // TODO(#9453): Implement using subnet_pool_lookup and datastore delete
    pub(crate) async fn subnet_pool_subnet_remove(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        _params: &params::SubnetPoolMemberRemove,
    ) -> DeleteResult {
        let not_found = not_found_error(pool, ResourceType::SubnetPool);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
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
