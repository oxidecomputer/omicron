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
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::IpRange;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use std::net::IpAddr;
use uuid::Uuid;

impl super::Nexus {
    pub async fn ip_pool_create(
        &self,
        opctx: &OpContext,
        new_pool: &params::IpPoolCreate,
    ) -> CreateResult<db::model::IpPool> {
        self.db_datastore.ip_pool_create(opctx, new_pool).await
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
        pagparams: &DataPageParams<'_, IpAddr>,
    ) -> ListResultVec<db::model::IpPoolRange> {
        let (.., authz_pool) = LookupPath::new(opctx, &self.db_datastore)
            .ip_pool_name(pool_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
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
        let (.., authz_pool) = LookupPath::new(opctx, &self.db_datastore)
            .ip_pool_name(pool_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore.ip_pool_add_range(opctx, &authz_pool, range).await
    }

    pub async fn ip_pool_delete_range(
        &self,
        opctx: &OpContext,
        pool_name: &Name,
        range: &IpRange,
    ) -> DeleteResult {
        let (.., authz_pool) = LookupPath::new(opctx, &self.db_datastore)
            .ip_pool_name(pool_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore.ip_pool_delete_range(opctx, &authz_pool, range).await
    }
}
