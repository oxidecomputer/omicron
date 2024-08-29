// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internet gateways

use crate::external_api::params;
use nexus_auth::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;

impl super::Nexus {
    //Internet gateways
    pub fn internet_gateway_lookup<'a>(
        &'a self,
        _opctx: &'a OpContext,
        _igw_selector: params::InternetGatewaySelector,
    ) -> LookupResult<lookup::InternetGateway<'a>> {
        todo!();
    }

    pub(crate) async fn internet_gateway_create(
        &self,
        _opctx: &OpContext,
        _vpc_lookup: &lookup::Vpc<'_>,
        _params: &params::InternetGatewayCreate,
    ) -> CreateResult<db::model::InternetGateway> {
        todo!();
    }

    pub(crate) async fn internet_gateway_list(
        &self,
        _opctx: &OpContext,
        _vpc_lookup: &lookup::Vpc<'_>,
        _pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::InternetGateway> {
        todo!();
    }

    pub(crate) async fn internet_gateway_delete(
        &self,
        _opctx: &OpContext,
        _lookup: &lookup::InternetGateway<'_>,
    ) -> DeleteResult {
        todo!();
    }

    pub(crate) async fn internet_gateway_ip_pool_list(
        &self,
        _opctx: &OpContext,
        _gateway_lookup: &lookup::InternetGateway<'_>,
        _pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::InternetGatewayIpPool> {
        todo!();
    }

    pub fn internet_gateway_ip_pool_lookup<'a>(
        &'a self,
        _opctx: &'a OpContext,
        _route_selector: params::InternetGatewayIpPoolSelector,
    ) -> LookupResult<lookup::InternetGatewayIpPool<'a>> {
        todo!();
    }

    pub(crate) async fn internet_gateway_ip_pool_attach(
        &self,
        _opctx: &OpContext,
        _lookup: &lookup::InternetGateway<'_>,
        _params: &params::InternetGatewayIpPoolCreate,
    ) -> CreateResult<db::model::InternetGatewayIpPool> {
        todo!();
    }

    pub(crate) async fn internet_gateway_ip_pool_detach(
        &self,
        _opctx: &OpContext,
        _route_lookup: &lookup::InternetGatewayIpPool<'_>,
    ) -> DeleteResult {
        todo!();
    }

    pub fn internet_gateway_ip_address_lookup<'a>(
        &'a self,
        _opctx: &'a OpContext,
        _route_selector: params::InternetGatewayIpAddressSelector,
    ) -> LookupResult<lookup::InternetGatewayIpAddress<'a>> {
        todo!();
    }

    pub(crate) async fn internet_gateway_ip_address_list(
        &self,
        _opctx: &OpContext,
        _gateway_lookup: &lookup::InternetGateway<'_>,
        _pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::InternetGatewayIpAddress> {
        todo!();
    }

    pub(crate) async fn internet_gateway_ip_address_attach(
        &self,
        _opctx: &OpContext,
        _lookup: &lookup::InternetGateway<'_>,
        _params: &params::InternetGatewayIpAddressCreate,
    ) -> CreateResult<db::model::InternetGatewayIpAddress> {
        todo!();
    }

    pub(crate) async fn internet_gateway_ip_address_detach(
        &self,
        _opctx: &OpContext,
        _route_lookup: &lookup::InternetGatewayIpAddress<'_>,
    ) -> DeleteResult {
        todo!();
    }
}
