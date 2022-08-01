// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPCs and firewall rules

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::{Asset, Resource};
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::db::model::VpcRouterKind;
use crate::db::queries::vpc_subnet::SubnetError;
use crate::external_api::params;
use nexus_defaults as defaults;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteCreateParams;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;

use futures::future::join_all;
use uuid::Uuid;

impl super::Nexus {
    // VPCs

    pub async fn project_create_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::VpcCreate,
    ) -> CreateResult<db::model::Vpc> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        let vpc_id = Uuid::new_v4();
        let system_router_id = Uuid::new_v4();
        let default_route_id = Uuid::new_v4();
        let default_subnet_id = Uuid::new_v4();

        // TODO: This is both fake and utter nonsense. It should be eventually
        // replaced with the proper behavior for creating the default route
        // which may not even happen here. Creating the vpc, its system router,
        // and that routers default route should all be a part of the same
        // transaction.
        let vpc = db::model::IncompleteVpc::new(
            vpc_id,
            authz_project.id(),
            system_router_id,
            params.clone(),
        )?;
        let (authz_vpc, db_vpc) = self
            .db_datastore
            .project_create_vpc(opctx, &authz_project, vpc.clone())
            .await?;

        // TODO: Ultimately when the VPC is created a system router w/ an
        // appropriate setup should also be created.  Given that the underlying
        // systems aren't wired up yet this is a naive implementation to
        // populate the database with a starting router. Eventually this code
        // should be replaced with a saga that'll handle creating the VPC and
        // its underlying system
        let router = db::model::VpcRouter::new(
            system_router_id,
            vpc_id,
            VpcRouterKind::System,
            params::VpcRouterCreate {
                identity: IdentityMetadataCreateParams {
                    name: "system".parse().unwrap(),
                    description: "Routes are automatically added to this \
                        router as vpc subnets are created"
                        .into(),
                },
            },
        );
        let (authz_router, _) = self
            .db_datastore
            .vpc_create_router(&opctx, &authz_vpc, router)
            .await?;
        let route = db::model::RouterRoute::new(
            default_route_id,
            system_router_id,
            RouterRouteKind::Default,
            RouterRouteCreateParams {
                identity: IdentityMetadataCreateParams {
                    name: "default".parse().unwrap(),
                    description: "The default route of a vpc".to_string(),
                },
                target: RouteTarget::InternetGateway(
                    "outbound".parse().unwrap(),
                ),
                destination: RouteDestination::Vpc(
                    params.identity.name.clone(),
                ),
            },
        );

        self.db_datastore
            .router_create_route(opctx, &authz_router, route)
            .await?;

        // Allocate the first /64 sub-range from the requested or created
        // prefix.
        let ipv6_block = external::Ipv6Net(
            ipnetwork::Ipv6Network::new(db_vpc.ipv6_prefix.network(), 64)
                .map_err(|_| {
                    external::Error::internal_error(
                        "Failed to allocate default IPv6 subnet",
                    )
                })?,
        );

        // TODO: batch this up with everything above
        let subnet = db::model::VpcSubnet::new(
            default_subnet_id,
            vpc_id,
            IdentityMetadataCreateParams {
                name: "default".parse().unwrap(),
                description: format!(
                    "The default subnet for {}",
                    params.identity.name
                ),
            },
            *defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK,
            ipv6_block,
        );

        // Create the subnet record in the database. Overlapping IP ranges
        // should be translated into an internal error. That implies that
        // there's already an existing VPC Subnet, but we're explicitly creating
        // the _first_ VPC in the project. Something is wrong, and likely a bug
        // in our code.
        self.db_datastore
            .vpc_create_subnet(opctx, &authz_vpc, subnet)
            .await
            .map_err(|err| match err {
                SubnetError::OverlappingIpRange(ip) => {
                    let ipv4_block = &defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK;
                    error!(
                        self.log,
                        concat!(
                            "failed to create default VPC Subnet, IP address ",
                            "range '{}' overlaps with existing",
                        ),
                        ip;
                        "vpc_id" => ?vpc_id,
                        "subnet_id" => ?default_subnet_id,
                        "ipv4_block" => ?**ipv4_block,
                        "ipv6_block" => ?ipv6_block,
                    );
                    external::Error::internal_error(
                        "Failed to create default VPC Subnet, \
                            found overlapping IP address ranges",
                    )
                }
                SubnetError::External(e) => e,
            })?;
        let rules = db::model::VpcFirewallRule::vec_from_params(
            authz_vpc.id(),
            defaults::DEFAULT_FIREWALL_RULES.clone(),
        );
        self.db_datastore
            .vpc_update_firewall_rules(opctx, &authz_vpc, rules.clone())
            .await?;
        self.send_sled_agents_firewall_rules(&db_vpc, &rules).await?;
        Ok(db_vpc)
    }

    pub async fn project_list_vpcs(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Vpc> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .project_list_vpcs(&opctx, &authz_project, pagparams)
            .await
    }

    pub async fn vpc_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> LookupResult<db::model::Vpc> {
        let (.., db_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .fetch()
            .await?;
        Ok(db_vpc)
    }

    pub async fn vpc_fetch_by_id(
        &self,
        opctx: &OpContext,
        vpc_id: &Uuid,
    ) -> LookupResult<db::model::Vpc> {
        let (.., db_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .vpc_id(*vpc_id)
            .fetch()
            .await?;
        Ok(db_vpc)
    }

    pub async fn project_update_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &params::VpcUpdate,
    ) -> UpdateResult<db::model::Vpc> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .project_update_vpc(opctx, &authz_vpc, params.clone().into())
            .await
    }

    pub async fn project_delete_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> DeleteResult {
        let (.., authz_vpc, db_vpc) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .fetch()
                .await?;

        let authz_vpc_router = authz::VpcRouter::new(
            authz_vpc.clone(),
            db_vpc.system_router_id,
            LookupType::ById(db_vpc.system_router_id),
        );

        // Possibly delete the VPC, then the router and firewall.
        //
        // We must delete the VPC first. This will fail if the VPC still
        // contains at least one subnet, since those are independent containers
        // that track network interfaces as child resources. If we delete the
        // router first, it'll succeed even if the VPC contains Subnets, which
        // means the router is now gone from an otherwise-live subnet.
        //
        // This is a good example of need for the original comment:
        //
        // TODO: This should eventually use a saga to call the
        // networking subsystem to have it clean up the networking resources
        self.db_datastore
            .project_delete_vpc(opctx, &db_vpc, &authz_vpc)
            .await?;
        self.db_datastore.vpc_delete_router(&opctx, &authz_vpc_router).await?;

        // Delete all firewall rules after deleting the VPC, to ensure no
        // firewall rules get added between rules deletion and VPC deletion.
        self.db_datastore
            .vpc_delete_all_firewall_rules(&opctx, &authz_vpc)
            .await
    }

    // Firewall rules

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
        let (.., authz_vpc, db_vpc) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .fetch_for(authz::Action::Modify)
                .await?;
        let rules = db::model::VpcFirewallRule::vec_from_params(
            authz_vpc.id(),
            params.clone(),
        );
        let rules = self
            .db_datastore
            .vpc_update_firewall_rules(opctx, &authz_vpc, rules)
            .await?;
        self.send_sled_agents_firewall_rules(&db_vpc, &rules).await?;
        Ok(rules)
    }

    async fn send_sled_agents_firewall_rules(
        &self,
        vpc: &db::model::Vpc,
        rules: &[db::model::VpcFirewallRule],
    ) -> Result<(), Error> {
        info!(self.log, "sending firewall rules to sled agents");

        let rules_for_sled =
            self.resolve_firewall_rules_for_sled_agent(&vpc, rules).await?;
        info!(self.log, "resolved {} rules for sleds", rules_for_sled.len());
        let sled_rules_request =
            sled_agent_client::types::VpcFirewallRulesEnsureBody {
                rules: rules_for_sled,
            };

        let vpc_to_sleds =
            self.db_datastore.vpc_resolve_to_sleds(vpc.id()).await?;
        info!(self.log, "resolved sleds for vpc {}", vpc.name(); "vpc_to_sled" => ?vpc_to_sleds);
        let mut sled_requests = Vec::with_capacity(vpc_to_sleds.len());
        for sled in &vpc_to_sleds {
            let sled_id = sled.id();
            let vpc_id = vpc.id();
            let sled_rules_request = sled_rules_request.clone();
            sled_requests.push(async move {
                self.sled_client(&sled_id)
                    .await?
                    .vpc_firewall_rules_put(&vpc_id, &sled_rules_request)
                    .await
                    .map_err(|e| Error::internal_error(&e.to_string()))
            });
        }

        let results = join_all(sled_requests).await;
        // TODO-correctness: handle more than one failure in the sled-agent requests
        for (sled, result) in vpc_to_sleds.iter().zip(results) {
            if let Err(e) = result {
                warn!(self.log, "failed to update firewall rules on sled agent";
                      "sled_id" => %sled.id(),
                      "vpc_id" => %vpc.id(),
                      "error" => %e);
                return Err(e);
            }
        }
        info!(
            self.log,
            "updated firewall rules on {} sleds",
            vpc_to_sleds.len()
        );

        Ok(())
    }

    async fn resolve_firewall_rules_for_sled_agent(
        &self,
        vpc: &db::model::Vpc,
        rules: &[db::model::VpcFirewallRule],
    ) -> Result<Vec<sled_agent_client::types::VpcFirewallRule>, Error> {
        Ok(rules
            .iter()
            .map(|rule| sled_agent_client::types::VpcFirewallRule {
                status: rule.status.0.into(),
                direction: rule.direction.0.into(),
                targets: vec![], // XXX
                filter_hosts: rule.filter_hosts.as_ref().map(|hosts| {
                    hosts
                        .iter()
                        .filter_map(|host| match host {
                            db::model::VpcFirewallRuleHostFilter(
                                external::VpcFirewallRuleHostFilter::Ip(
                                    std::net::IpAddr::V4(addr),
                                ),
                            ) => Some(sled_agent_client::types::IpNet::V4(
                                external::Ipv4Net(
                                    ipnetwork::Ipv4Network::new(
                                        addr.clone(),
                                        32,
                                    )
                                    .unwrap(),
                                )
                                .into(),
                            )),
                            _ => None, // XXX
                        })
                        .collect()
                }),
                filter_ports: rule
                    .filter_ports
                    .as_ref()
                    .map(|ports| ports.iter().map(|v| v.0.into()).collect()),
                filter_protocols: rule.filter_protocols.as_ref().map(
                    |protocols| protocols.iter().map(|v| v.0.into()).collect(),
                ),
                action: rule.action.0.into(),
                priority: rule.priority.0 .0,
            })
            .collect::<Vec<_>>())
    }
}
