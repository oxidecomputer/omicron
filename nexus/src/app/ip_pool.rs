// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP Pools, collections of external IP addresses for guest instances

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::external_api::params;
use crate::external_api::shared::IpRange;
use ipnetwork::IpNetwork;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl super::Nexus {
    pub async fn ip_pool_create(
        &self,
        opctx: &OpContext,
        new_pool: &params::IpPoolCreate,
    ) -> CreateResult<db::model::IpPool> {
        self.db_datastore.ip_pool_create(opctx, new_pool, None).await
    }

    pub async fn ip_pool_services_create(
        &self,
        opctx: &OpContext,
        new_pool: &params::IpPoolCreate,
        rack_id: Uuid,
    ) -> CreateResult<db::model::IpPool> {
        self.db_datastore.ip_pool_create(opctx, new_pool, Some(rack_id)).await
    }

    pub async fn ip_pools_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::IpPool> {
        self.db_datastore.ip_pools_list_by_name(opctx, pagparams).await
    }

    pub async fn ip_pools_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::IpPool> {
        self.db_datastore.ip_pools_list_by_id(opctx, pagparams).await
    }

    pub async fn ip_pool_fetch(
        &self,
        opctx: &OpContext,
        pool_name: &Name,
    ) -> LookupResult<db::model::IpPool> {
        let (.., db_pool) = LookupPath::new(opctx, &self.db_datastore)
            .ip_pool_name(pool_name)
            .fetch()
            .await?;
        Ok(db_pool)
    }

    pub async fn ip_pool_delete(
        &self,
        opctx: &OpContext,
        pool_name: &Name,
    ) -> DeleteResult {
        let (.., authz_pool, db_pool) =
            LookupPath::new(opctx, &self.db_datastore)
                .ip_pool_name(pool_name)
                .fetch_for(authz::Action::Delete)
                .await?;
        self.db_datastore.ip_pool_delete(opctx, &authz_pool, &db_pool).await
    }

    pub async fn ip_pool_update(
        &self,
        opctx: &OpContext,
        pool_name: &Name,
        updates: &params::IpPoolUpdate,
    ) -> UpdateResult<db::model::IpPool> {
        let (.., authz_pool) = LookupPath::new(opctx, &self.db_datastore)
            .ip_pool_name(pool_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .ip_pool_update(opctx, &authz_pool, updates.clone().into())
            .await
    }

    pub async fn ip_pool_list_ranges(
        &self,
        opctx: &OpContext,
        pool_name: &Name,
        pagparams: &DataPageParams<'_, IpNetwork>,
    ) -> ListResultVec<db::model::IpPoolRange> {
        let (.., authz_pool, db_pool) =
            LookupPath::new(opctx, &self.db_datastore)
                .ip_pool_name(pool_name)
                .fetch_for(authz::Action::ListChildren)
                .await?;
        if db_pool.rack_id.is_some() {
            return Err(Error::not_found_by_name(
                ResourceType::IpPool,
                pool_name,
            ));
        }

        self.db_datastore
            .ip_pool_list_ranges(opctx, &authz_pool, pagparams)
            .await
    }

    pub async fn ip_pool_add_range(
        &self,
        opctx: &OpContext,
        pool_name: &Name,
        range: &IpRange,
    ) -> UpdateResult<db::model::IpPoolRange> {
        let (.., authz_pool, db_pool) =
            LookupPath::new(opctx, &self.db_datastore)
                .ip_pool_name(pool_name)
                .fetch_for(authz::Action::Modify)
                .await?;
        if db_pool.rack_id.is_some() {
            return Err(Error::not_found_by_name(
                ResourceType::IpPool,
                pool_name,
            ));
        }
        self.db_datastore
            .ip_pool_add_range(opctx, &authz_pool, &db_pool, range)
            .await
    }

    pub async fn ip_pool_delete_range(
        &self,
        opctx: &OpContext,
        pool_name: &Name,
        range: &IpRange,
    ) -> DeleteResult {
        let (.., authz_pool, db_pool) =
            LookupPath::new(opctx, &self.db_datastore)
                .ip_pool_name(pool_name)
                .fetch_for(authz::Action::Modify)
                .await?;
        if db_pool.rack_id.is_some() {
            return Err(Error::not_found_by_name(
                ResourceType::IpPool,
                pool_name,
            ));
        }
        self.db_datastore.ip_pool_delete_range(opctx, &authz_pool, range).await
    }

    // The "ip_pool_service_..." functions look up IP pools for Oxide service usage,
    // rather than for VMs. As such, they're identified by rack UUID, not
    // by pool names.
    //
    // TODO(https://github.com/oxidecomputer/omicron/issues/1276): Should be
    // AZ UUID, probably.

    pub async fn ip_pool_service_fetch(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
    ) -> LookupResult<db::model::IpPool> {
        let (authz_pool, db_pool) = self
            .db_datastore
            .ip_pools_lookup_by_rack_id(opctx, rack_id)
            .await?;
        opctx.authorize(authz::Action::Read, &authz_pool).await?;
        Ok(db_pool)
    }

    pub async fn ip_pool_service_list_ranges(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        pagparams: &DataPageParams<'_, IpNetwork>,
    ) -> ListResultVec<db::model::IpPoolRange> {
        let (authz_pool, ..) = self
            .db_datastore
            .ip_pools_lookup_by_rack_id(opctx, rack_id)
            .await?;
        opctx.authorize(authz::Action::Read, &authz_pool).await?;
        self.db_datastore
            .ip_pool_list_ranges(opctx, &authz_pool, pagparams)
            .await
    }

    pub async fn ip_pool_service_add_range(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        range: &IpRange,
    ) -> UpdateResult<db::model::IpPoolRange> {
        let (authz_pool, db_pool) = self
            .db_datastore
            .ip_pools_lookup_by_rack_id(opctx, rack_id)
            .await?;
        opctx.authorize(authz::Action::Modify, &authz_pool).await?;
        self.db_datastore
            .ip_pool_add_range(opctx, &authz_pool, &db_pool, range)
            .await
    }

    pub async fn ip_pool_service_delete_range(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        range: &IpRange,
    ) -> DeleteResult {
        let (authz_pool, ..) = self
            .db_datastore
            .ip_pools_lookup_by_rack_id(opctx, rack_id)
            .await?;
        opctx.authorize(authz::Action::Modify, &authz_pool).await?;
        self.db_datastore.ip_pool_delete_range(opctx, &authz_pool, range).await
    }
}
