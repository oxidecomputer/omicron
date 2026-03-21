// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subnet Pools, collections of IP subnets for external subnet allocation

use nexus_auth::authz;
use nexus_db_lookup::lookup;
use nexus_db_model::SubnetPool;
use nexus_db_model::SubnetPoolSiloLink;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::subnet_pool as subnet_pool_types;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use oxnet::IpNet;
use uuid::Uuid;

impl super::Nexus {
    // === Subnet Pool CRUD ===

    pub(crate) async fn subnet_pool_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<subnet_pool_types::SubnetPool> {
        self.datastore()
            .list_subnet_pools(opctx, pagparams)
            .await
            .map(|list| list.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn subnet_pool_create(
        &self,
        opctx: &OpContext,
        pool_params: subnet_pool_types::SubnetPoolCreate,
    ) -> Result<subnet_pool_types::SubnetPool, Error> {
        self.datastore()
            .create_subnet_pool(opctx, pool_params)
            .await
            .map(Into::into)
    }

    pub(crate) async fn subnet_pool_view(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
    ) -> LookupResult<subnet_pool_types::SubnetPool> {
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
        params: subnet_pool_types::SubnetPoolUpdate,
    ) -> UpdateResult<subnet_pool_types::SubnetPool> {
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
    ) -> ListResultVec<subnet_pool_types::SubnetPoolMember> {
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
        params: &subnet_pool_types::SubnetPoolMemberAdd,
    ) -> Result<subnet_pool_types::SubnetPoolMember, Error> {
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
        params: &subnet_pool_types::SubnetPoolMemberRemove,
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

    pub(crate) async fn subnet_pool_silo_list(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<subnet_pool_types::SubnetPoolSiloLink> {
        let (authz_pool, _db_pool) = self
            .datastore()
            .lookup_subnet_pool(opctx, pool)
            .fetch_for(authz::Action::ListChildren)
            .await?;
        self.datastore()
            .list_silos_linked_to_subnet_pool(opctx, &authz_pool, pagparams)
            .await
            .map(|items| items.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn silo_subnet_pool_list(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<(SubnetPool, SubnetPoolSiloLink)> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Read).await?;

        // check ability to list pools in general
        opctx
            .authorize(authz::Action::ListChildren, &authz::SUBNET_POOL_LIST)
            .await?;

        self.datastore()
            .silo_subnet_pool_list(opctx, &authz_silo, pagparams)
            .await
    }

    /// List Subnet Pools linked to the user's current silo.
    pub(crate) async fn current_silo_subnet_pool_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<(SubnetPool, SubnetPoolSiloLink)> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("listing subnet pools")?;

        // From the developer user's point of view, we treat Subnet Pools linked
        // to their silo as silo resources, so they can list them if they can
        // list silo children.
        opctx.authorize(authz::Action::ListChildren, &authz_silo).await?;

        self.datastore()
            .silo_subnet_pool_list(opctx, &authz_silo, pagparams)
            .await
    }

    /// Look up a subnet pool linked to the current silo. Returns 404 if the
    /// pool exists but isn't linked to the current silo.
    ///
    /// Authorization comes from the silo link: if the pool is linked to the
    /// caller's silo, they can view it; otherwise they get a 404. This is
    /// the same pattern used by `silo_ip_pool_fetch` for IP pools.
    pub(crate) async fn silo_subnet_pool_fetch(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
    ) -> LookupResult<(SubnetPool, SubnetPoolSiloLink)> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("fetching subnet pool")?;
        opctx.authorize(authz::Action::ListChildren, &authz_silo).await?;

        self.datastore().silo_subnet_pool_fetch(opctx, &authz_silo, pool).await
    }

    pub(crate) async fn subnet_pool_silo_link(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
        params: subnet_pool_types::SubnetPoolLinkSilo,
    ) -> Result<subnet_pool_types::SubnetPoolSiloLink, Error> {
        let subnet_pool_types::SubnetPoolLinkSilo { silo, is_default } = params;
        let (authz_pool, _db_pool) = self
            .datastore()
            .lookup_subnet_pool(opctx, pool)
            .fetch_for(authz::Action::Modify)
            .await?;
        let (authz_silo, _db_silo) = self
            .silo_lookup(opctx, silo)?
            .fetch_for(authz::Action::Modify)
            .await?;
        self.datastore()
            .link_subnet_pool_to_silo(
                opctx,
                &authz_pool,
                &authz_silo,
                is_default,
            )
            .await
            .map(Into::into)
    }

    pub(crate) async fn subnet_pool_silo_update(
        &self,
        opctx: &OpContext,
        pool: NameOrId,
        silo: NameOrId,
        params: subnet_pool_types::SubnetPoolSiloUpdate,
    ) -> UpdateResult<subnet_pool_types::SubnetPoolSiloLink> {
        let subnet_pool_types::SubnetPoolSiloUpdate { is_default } = params;
        let (authz_pool, _db_pool) = self
            .datastore()
            .lookup_subnet_pool(opctx, &pool)
            .fetch_for(authz::Action::Modify)
            .await?;
        let (authz_silo, _db_silo) = self
            .silo_lookup(opctx, silo)?
            .fetch_for(authz::Action::Modify)
            .await?;
        self.datastore()
            .update_subnet_pool_silo_link(
                opctx,
                &authz_pool,
                &authz_silo,
                is_default,
            )
            .await
            .map(Into::into)
    }

    pub(crate) async fn subnet_pool_silo_unlink(
        &self,
        opctx: &OpContext,
        pool: NameOrId,
        silo: NameOrId,
    ) -> DeleteResult {
        let (authz_pool, db_pool) = self
            .datastore()
            .lookup_subnet_pool(opctx, &pool)
            .fetch_for(authz::Action::Modify)
            .await?;
        let (authz_silo, _db_silo) = self
            .silo_lookup(opctx, silo)?
            .fetch_for(authz::Action::Modify)
            .await?;
        self.datastore()
            .unlink_subnet_pool_from_silo(
                opctx,
                &authz_pool,
                &db_pool,
                &authz_silo,
            )
            .await
    }

    // === Utilization ===

    pub(crate) async fn subnet_pool_utilization_view(
        &self,
        opctx: &OpContext,
        pool: &NameOrId,
    ) -> LookupResult<subnet_pool_types::SubnetPoolUtilization> {
        let pool_lookup = self.datastore().lookup_subnet_pool(opctx, pool);
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Read).await?;

        let (allocated, capacity) = self
            .db_datastore
            .subnet_pool_utilization(opctx, &authz_pool)
            .await?;

        let Some(remaining) = capacity.checked_sub(allocated) else {
            return Err(Error::internal_error(
                format!(
                    "Computed an impossible negative count of remaining \
                    addresses. Capacity = {capacity}, allocated = {allocated}"
                )
                .as_str(),
            ));
        };
        let remaining = remaining as f64;
        let capacity = capacity as f64;
        Ok(subnet_pool_types::SubnetPoolUtilization { remaining, capacity })
    }
}
