// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internet gateways

use crate::external_api::params;
use nexus_auth::authz;
use nexus_auth::context::OpContext;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::db;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::http_pagination::PaginatedBy;
use uuid::Uuid;

impl super::Nexus {
    //Internet gateways

    /// Lookup an internet gateway.
    pub fn internet_gateway_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        igw_selector: params::InternetGatewaySelector,
    ) -> LookupResult<lookup::InternetGateway<'a>> {
        match igw_selector {
            params::InternetGatewaySelector {
                gateway: NameOrId::Id(id),
                vpc: None,
                project: None,
            } => {
                let gw = LookupPath::new(opctx, &self.db_datastore)
                    .internet_gateway_id(id);
                Ok(gw)
            }
            params::InternetGatewaySelector {
                gateway: NameOrId::Name(name),
                vpc: Some(vpc),
                project,
            } => {
                let gw = self
                    .vpc_lookup(opctx, params::VpcSelector { project, vpc })?
                    .internet_gateway_name_owned(name.into());
                Ok(gw)
            }
            params::InternetGatewaySelector {
                gateway: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing gateway as an ID vpc and project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "gateway should either be an ID or vpc should be specified",
            )),
        }
    }

    /// Create an internet gateway.
    pub(crate) async fn internet_gateway_create(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        params: &params::InternetGatewayCreate,
    ) -> CreateResult<db::model::InternetGateway> {
        let (.., authz_vpc) =
            vpc_lookup.lookup_for(authz::Action::CreateChild).await?;

        // Check networking restrictions: if the actor's silo restricts networking
        // actions, only Silo Admins can create internet gateways
        self.check_networking_restrictions(opctx).await?;

        let id = Uuid::new_v4();
        let router =
            db::model::InternetGateway::new(id, authz_vpc.id(), params.clone());
        let (_, router) = self
            .db_datastore
            .vpc_create_internet_gateway(&opctx, &authz_vpc, router)
            .await?;

        self.vpc_needed_notify_sleds();

        Ok(router)
    }

    /// List internet gateways within a VPC.
    pub(crate) async fn internet_gateway_list(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::InternetGateway> {
        let (.., authz_vpc) =
            vpc_lookup.lookup_for(authz::Action::ListChildren).await?;
        let igws = self
            .db_datastore
            .internet_gateway_list(opctx, &authz_vpc, pagparams)
            .await?;
        Ok(igws)
    }

    /// Delete an internet gateway.
    ///
    /// If there are routes that reference the gateway being deleted and
    /// `cascade` is true all referencing routes are deleted, otherwise an
    /// error is returned.
    pub(crate) async fn internet_gateway_delete(
        &self,
        opctx: &OpContext,
        lookup: &lookup::InternetGateway<'_>,
        cascade: bool,
    ) -> DeleteResult {
        let (.., authz_vpc, authz_igw, _db_igw) =
            lookup.fetch_for(authz::Action::Delete).await?;

        // Check networking restrictions: if the actor's silo restricts networking
        // actions, only Silo Admins can delete internet gateways
        self.check_networking_restrictions(opctx).await?;

        let out = self
            .db_datastore
            .vpc_delete_internet_gateway(
                opctx,
                &authz_igw,
                authz_vpc.id(),
                cascade,
            )
            .await?;

        self.vpc_needed_notify_sleds();

        Ok(out)
    }

    /// List IP pools associated with an internet gateway.
    pub(crate) async fn internet_gateway_ip_pool_list(
        &self,
        opctx: &OpContext,
        gateway_lookup: &lookup::InternetGateway<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::InternetGatewayIpPool> {
        let (.., authz_vpc) =
            gateway_lookup.lookup_for(authz::Action::ListChildren).await?;
        let pools = self
            .db_datastore
            .internet_gateway_list_ip_pools(opctx, &authz_vpc, pagparams)
            .await?;
        Ok(pools)
    }

    /// Lookup an IP pool associated with an internet gateway.
    pub fn internet_gateway_ip_pool_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        pool_selector: params::InternetGatewayIpPoolSelector,
    ) -> LookupResult<lookup::InternetGatewayIpPool<'a>> {
        match pool_selector {
            params::InternetGatewayIpPoolSelector {
                pool: NameOrId::Id(id),
                gateway: None,
                vpc: None,
                project: None,
            } => {
                let route = LookupPath::new(opctx, &self.db_datastore)
                    .internet_gateway_ip_pool_id(id);
                Ok(route)
            }
            params::InternetGatewayIpPoolSelector {
                pool: NameOrId::Name(name),
                gateway: Some(gateway),
                vpc,
                project,
            } => {
                let route = self
                    .internet_gateway_lookup(
                        opctx,
                        params::InternetGatewaySelector {
                            project,
                            vpc,
                            gateway,
                        },
                    )?
                    .internet_gateway_ip_pool_name_owned(name.into());
                Ok(route)
            }
            params::InternetGatewayIpPoolSelector {
                pool: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing pool as an ID gateway, subnet, vpc, and project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "pool should either be an ID or gateway should be specified",
            )),
        }
    }

    /// Attach an IP pool to an internet gateway.
    pub(crate) async fn internet_gateway_ip_pool_attach(
        &self,
        opctx: &OpContext,
        lookup: &lookup::InternetGateway<'_>,
        params: &params::InternetGatewayIpPoolCreate,
    ) -> CreateResult<db::model::InternetGatewayIpPool> {
        let (.., authz_igw, _) =
            lookup.fetch_for(authz::Action::CreateChild).await?;

        // Check networking restrictions: if the actor's silo restricts networking
        // actions, only Silo Admins can attach IP pools to internet gateways
        self.check_networking_restrictions(opctx).await?;

        // need to use this method so it works for non-fleet users
        let (authz_pool, ..) =
            self.silo_ip_pool_fetch(&opctx, &params.ip_pool).await?;

        let id = Uuid::new_v4();
        let route = db::model::InternetGatewayIpPool::new(
            id,
            authz_pool.id(),
            authz_igw.id(),
            params.identity.clone(),
        );
        let route = self
            .db_datastore
            .internet_gateway_attach_ip_pool(&opctx, &authz_igw, route)
            .await?;

        self.vpc_needed_notify_sleds();

        Ok(route)
    }

    /// Detach an IP pool from an internet gateway.
    ///
    /// If there are routes that depend on the IP pool being detached and
    /// `cascade` is true then those routes are deleted, otherwise an
    /// error is returned.
    pub(crate) async fn internet_gateway_ip_pool_detach(
        &self,
        opctx: &OpContext,
        lookup: &lookup::InternetGatewayIpPool<'_>,
        cascade: bool,
    ) -> DeleteResult {
        let (.., authz_vpc, _authz_igw, authz_pool, db_pool) =
            lookup.fetch_for(authz::Action::Delete).await?;

        // Check networking restrictions: if the actor's silo restricts networking
        // actions, only Silo Admins can detach IP pools from internet gateways
        self.check_networking_restrictions(opctx).await?;

        let (.., igw) = LookupPath::new(opctx, &self.db_datastore)
            .internet_gateway_id(db_pool.internet_gateway_id)
            .fetch()
            .await?;

        let out = self
            .db_datastore
            .internet_gateway_detach_ip_pool(
                opctx,
                igw.name().to_string(),
                &authz_pool,
                db_pool.ip_pool_id,
                authz_vpc.id(),
                cascade,
            )
            .await?;

        self.vpc_needed_notify_sleds();

        Ok(out)
    }

    /// List IP addresses attached to an internet gateway.
    pub(crate) async fn internet_gateway_ip_address_list(
        &self,
        opctx: &OpContext,
        gateway_lookup: &lookup::InternetGateway<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::InternetGatewayIpAddress> {
        let (.., authz_igw) =
            gateway_lookup.lookup_for(authz::Action::ListChildren).await?;
        let pools = self
            .db_datastore
            .internet_gateway_list_ip_addresses(opctx, &authz_igw, pagparams)
            .await?;
        Ok(pools)
    }

    /// Lookup an IP address attached to an internet gateway.
    pub fn internet_gateway_ip_address_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        address_selector: params::InternetGatewayIpAddressSelector,
    ) -> LookupResult<lookup::InternetGatewayIpAddress<'a>> {
        match address_selector {
            params::InternetGatewayIpAddressSelector {
                address: NameOrId::Id(id),
                gateway: None,
                vpc: None,
                project: None,
            } => {
                let route = LookupPath::new(opctx, &self.db_datastore)
                    .internet_gateway_ip_address_id(id);
                Ok(route)
            }
            params::InternetGatewayIpAddressSelector {
                address: NameOrId::Name(name),
                gateway: Some(gateway),
                vpc,
                project,
            } => {
                let route = self
                    .internet_gateway_lookup(
                        opctx,
                        params::InternetGatewaySelector {
                            project,
                            vpc,
                            gateway,
                        },
                    )?
                    .internet_gateway_ip_address_name_owned(name.into());
                Ok(route)
            }
            params::InternetGatewayIpAddressSelector {
                address: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing address as an ID gateway, subnet, vpc, and project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "address should either be an ID or gateway should be specified",
            )),
        }
    }

    /// Attach an IP address to an internet gateway.
    pub(crate) async fn internet_gateway_ip_address_attach(
        &self,
        opctx: &OpContext,
        lookup: &lookup::InternetGateway<'_>,
        params: &params::InternetGatewayIpAddressCreate,
    ) -> CreateResult<db::model::InternetGatewayIpAddress> {
        let (.., authz_igw, _) =
            lookup.fetch_for(authz::Action::CreateChild).await?;

        // Check networking restrictions: if the actor's silo restricts networking
        // actions, only Silo Admins can attach IP addresses to internet gateways
        self.check_networking_restrictions(opctx).await?;

        let id = Uuid::new_v4();
        let route = db::model::InternetGatewayIpAddress::new(
            id,
            authz_igw.id(),
            params.clone(),
        );
        let route = self
            .db_datastore
            .internet_gateway_attach_ip_address(&opctx, &authz_igw, route)
            .await?;

        self.vpc_needed_notify_sleds();

        Ok(route)
    }

    /// Detach an IP pool from an internet gateway.
    ///
    /// If there are routes that depend on the IP address being detached and
    /// `cascade` is true then those routes are deleted, otherwise an
    /// error is returned.
    pub(crate) async fn internet_gateway_ip_address_detach(
        &self,
        opctx: &OpContext,
        lookup: &lookup::InternetGatewayIpAddress<'_>,
        cascade: bool,
    ) -> DeleteResult {
        let (.., authz_vpc, _authz_igw, authz_addr, db_addr) =
            lookup.fetch_for(authz::Action::Delete).await?;

        // Check networking restrictions: if the actor's silo restricts networking
        // actions, only Silo Admins can detach IP addresses from internet gateways
        self.check_networking_restrictions(opctx).await?;

        let (.., igw) = LookupPath::new(opctx, &self.db_datastore)
            .internet_gateway_id(db_addr.internet_gateway_id)
            .fetch()
            .await?;

        let out = self
            .db_datastore
            .internet_gateway_detach_ip_address(
                opctx,
                igw.name().to_string(),
                &authz_addr,
                db_addr.address.ip(),
                authz_vpc.id(),
                cascade,
            )
            .await?;

        self.vpc_needed_notify_sleds();

        Ok(out)
    }
}
