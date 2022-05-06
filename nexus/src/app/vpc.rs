// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::db::model::VpcRouter;
use crate::db::model::VpcRouterKind;
use crate::db::subnet_allocation::SubnetError;
use crate::defaults;
use crate::external_api::params;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use uuid::Uuid;

impl super::Nexus {
    pub async fn vpc_list_firewall_rules(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> ListResultVec<db::model::VpcFirewallRule> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::Read)
            .await?;
        let rules = self
            .db_datastore
            .vpc_list_firewall_rules(&opctx, &authz_vpc)
            .await?;
        Ok(rules)
    }

    pub async fn vpc_update_firewall_rules(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &VpcFirewallRuleUpdateParams,
    ) -> UpdateResult<Vec<db::model::VpcFirewallRule>> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        let rules = db::model::VpcFirewallRule::vec_from_params(
            authz_vpc.id(),
            params.clone(),
        );
        self.db_datastore
            .vpc_update_firewall_rules(opctx, &authz_vpc, rules)
            .await
    }

    pub async fn vpc_list_subnets(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::VpcSubnet> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore.vpc_list_subnets(opctx, &authz_vpc, pagparams).await
    }

    pub async fn vpc_subnet_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        subnet_name: &Name,
    ) -> LookupResult<db::model::VpcSubnet> {
        let (.., db_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_subnet_name(subnet_name)
            .fetch()
            .await?;
        Ok(db_vpc)
    }

    // TODO: When a subnet is created it should add a route entry into the VPC's
    // system router
    pub async fn vpc_create_subnet(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &params::VpcSubnetCreate,
    ) -> CreateResult<db::model::VpcSubnet> {
        let (.., authz_vpc, db_vpc) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .fetch()
                .await?;

        // Validate IPv4 range
        if !params.ipv4_block.network().is_private() {
            return Err(external::Error::invalid_request(
                "VPC Subnet IPv4 address ranges must be from a private range",
            ));
        }
        if params.ipv4_block.prefix() < defaults::MIN_VPC_IPV4_SUBNET_PREFIX
            || params.ipv4_block.prefix() > defaults::MAX_VPC_IPV4_SUBNET_PREFIX
        {
            return Err(external::Error::invalid_request(&format!(
                concat!(
                    "VPC Subnet IPv4 address ranges must have prefix ",
                    "length between {} and {}, inclusive"
                ),
                defaults::MIN_VPC_IPV4_SUBNET_PREFIX,
                defaults::MAX_VPC_IPV4_SUBNET_PREFIX
            )));
        }

        // Allocate an ID and insert the record.
        //
        // If the client provided an IPv6 range, we try to insert that or fail
        // with a conflict error.
        //
        // If they did _not_, we randomly generate a subnet valid for the VPC's
        // prefix, and the insert that. There's a small retry loop if we get
        // unlucky and conflict with an existing IPv6 range. In the case we
        // cannot find a subnet within a small number of retries, we fail the
        // request with a 503.
        //
        // TODO-robustness: We'd really prefer to allocate deterministically.
        // See <https://github.com/oxidecomputer/omicron/issues/685> for
        // details.
        let subnet_id = Uuid::new_v4();
        match params.ipv6_block {
            None => {
                const NUM_RETRIES: usize = 2;
                let mut retry = 0;
                let result = loop {
                    let ipv6_block = db_vpc
                        .ipv6_prefix
                        .random_subnet(
                            external::Ipv6Net::VPC_SUBNET_IPV6_PREFIX_LENGTH,
                        )
                        .map(|block| block.0)
                        .ok_or_else(|| {
                            external::Error::internal_error(
                                "Failed to create random IPv6 subnet",
                            )
                        })?;
                    let subnet = db::model::VpcSubnet::new(
                        subnet_id,
                        authz_vpc.id(),
                        params.identity.clone(),
                        params.ipv4_block,
                        ipv6_block,
                    );
                    let result = self
                        .db_datastore
                        .vpc_create_subnet(opctx, &authz_vpc, subnet)
                        .await;
                    match result {
                        // Allow NUM_RETRIES retries, after the first attempt.
                        //
                        // Note that we only catch IPv6 overlaps. The client
                        // always specifies the IPv4 range, so we fail the
                        // request if that overlaps with an existing range.
                        Err(SubnetError::OverlappingIpRange(ip))
                            if retry <= NUM_RETRIES && ip.is_ipv6() =>
                        {
                            debug!(
                                self.log,
                                "autogenerated random IPv6 range overlap";
                                "subnet_id" => ?subnet_id,
                                "ipv6_block" => %ipv6_block.0
                            );
                            retry += 1;
                            continue;
                        }
                        other => break other,
                    }
                };
                match result {
                    Err(SubnetError::OverlappingIpRange(ip))
                        if ip.is_ipv6() =>
                    {
                        // TODO-monitoring TODO-debugging
                        //
                        // We should maintain a counter for this occurrence, and
                        // export that via `oximeter`, so that we can see these
                        // failures through the timeseries database. The main
                        // goal here is for us to notice that this is happening
                        // before it becomes a major issue for customers.
                        let vpc_id = authz_vpc.id();
                        error!(
                            self.log,
                            "failed to generate unique random IPv6 address \
                            range in {} retries",
                            NUM_RETRIES;
                            "vpc_id" => ?vpc_id,
                            "subnet_id" => ?subnet_id,
                        );
                        Err(external::Error::internal_error(
                            "Unable to allocate unique IPv6 address range \
                            for VPC Subnet",
                        ))
                    }
                    Err(SubnetError::OverlappingIpRange(_)) => {
                        // Overlapping IPv4 ranges, which is always a client error.
                        Err(result.unwrap_err().into_external())
                    }
                    Err(SubnetError::External(e)) => Err(e),
                    Ok(subnet) => Ok(subnet),
                }
            }
            Some(ipv6_block) => {
                if !ipv6_block.is_vpc_subnet(&db_vpc.ipv6_prefix) {
                    return Err(external::Error::invalid_request(&format!(
                        concat!(
                        "VPC Subnet IPv6 address range '{}' is not valid for ",
                        "VPC with IPv6 prefix '{}'",
                    ),
                        ipv6_block, db_vpc.ipv6_prefix.0 .0,
                    )));
                }
                let subnet = db::model::VpcSubnet::new(
                    subnet_id,
                    db_vpc.id(),
                    params.identity.clone(),
                    params.ipv4_block,
                    ipv6_block,
                );
                self.db_datastore
                    .vpc_create_subnet(opctx, &authz_vpc, subnet)
                    .await
                    .map_err(SubnetError::into_external)
            }
        }
    }

    // TODO: When a subnet is deleted it should remove its entry from the VPC's
    // system router.
    pub async fn vpc_delete_subnet(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        subnet_name: &Name,
    ) -> DeleteResult {
        let (.., authz_subnet) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_subnet_name(subnet_name)
            .lookup_for(authz::Action::Delete)
            .await?;
        self.db_datastore.vpc_delete_subnet(opctx, &authz_subnet).await
    }

    pub async fn vpc_list_routers(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::VpcRouter> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        let routers = self
            .db_datastore
            .vpc_list_routers(opctx, &authz_vpc, pagparams)
            .await?;
        Ok(routers)
    }

    pub async fn vpc_router_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
    ) -> LookupResult<db::model::VpcRouter> {
        let (.., db_router) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .fetch()
            .await?;
        Ok(db_router)
    }

    pub async fn vpc_create_router(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        kind: &VpcRouterKind,
        params: &params::VpcRouterCreate,
    ) -> CreateResult<db::model::VpcRouter> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        let id = Uuid::new_v4();
        let router = db::model::VpcRouter::new(
            id,
            authz_vpc.id(),
            *kind,
            params.clone(),
        );
        let (_, router) = self
            .db_datastore
            .vpc_create_router(&opctx, &authz_vpc, router)
            .await?;
        Ok(router)
    }

    // TODO: When a router is deleted all its routes should be deleted
    // TODO: When a router is deleted it should be unassociated w/ any subnets it may be associated with
    //       or trigger an error
    pub async fn vpc_delete_router(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
    ) -> DeleteResult {
        let (.., authz_router, db_router) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .vpc_router_name(router_name)
                .fetch()
                .await?;
        // TODO-performance shouldn't this check be part of the "update"
        // database query?  This shouldn't affect correctness, assuming that a
        // router kind cannot be changed, but it might be able to save us a
        // database round-trip.
        if db_router.kind == VpcRouterKind::System {
            return Err(Error::MethodNotAllowed {
                internal_message: "Cannot delete system router".to_string(),
            });
        }
        self.db_datastore.vpc_delete_router(opctx, &authz_router).await
    }

    pub async fn vpc_update_router(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        params: &params::VpcRouterUpdate,
    ) -> UpdateResult<VpcRouter> {
        let (.., authz_router) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .vpc_update_router(opctx, &authz_router, params.clone().into())
            .await
    }
}
