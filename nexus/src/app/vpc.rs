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
use sled_agent_client::types::{IpNet, NetworkInterface};

use futures::future::join_all;
use ipnetwork::IpNetwork;
use std::collections::{HashMap, HashSet};
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

        // Save and send the default firewall rules for the new VPC.
        let rules = self
            .default_firewall_rules_for_vpc(
                authz_vpc.id(),
                params.identity.name.clone().into(),
            )
            .await?;
        self.db_datastore
            .vpc_update_firewall_rules(opctx, &authz_vpc, rules.clone())
            .await?;
        self.send_sled_agents_firewall_rules(opctx, &db_vpc, &rules).await?;

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
        self.send_sled_agents_firewall_rules(opctx, &db_vpc, &rules).await?;
        Ok(rules)
    }

    /// Customize the default firewall rules for a particular VPC.
    async fn default_firewall_rules_for_vpc(
        &self,
        vpc_id: Uuid,
        vpc_name: Name,
    ) -> Result<Vec<db::model::VpcFirewallRule>, Error> {
        let mut rules = db::model::VpcFirewallRule::vec_from_params(
            vpc_id,
            defaults::DEFAULT_FIREWALL_RULES.clone(),
        );
        for rule in rules.iter_mut() {
            for target in rule.targets.iter_mut() {
                match target.0 {
                    external::VpcFirewallRuleTarget::Vpc(ref mut name)
                        if name.as_str() == "default" =>
                    {
                        *name = vpc_name.clone().into()
                    }
                    _ => {
                        return Err(external::Error::internal_error(
                            "unexpected target in default firewall rule",
                        ))
                    }
                }
                if let Some(ref mut filter_hosts) = rule.filter_hosts {
                    for host in filter_hosts.iter_mut() {
                        match host.0 {
                            external::VpcFirewallRuleHostFilter::Vpc(
                                ref mut name,
                            ) if name.as_str() == "default" => {
                                *name = vpc_name.clone().into()
                            }
                            _ => return Err(external::Error::internal_error(
                                "unexpected host filter in default firewall rule"
                            )),
                        }
                    }
                }
            }
        }
        debug!(self.log, "default firewall rules for vpc {}", vpc_name; "rules" => ?&rules);
        Ok(rules)
    }

    async fn send_sled_agents_firewall_rules(
        &self,
        opctx: &OpContext,
        vpc: &db::model::Vpc,
        rules: &[db::model::VpcFirewallRule],
    ) -> Result<(), Error> {
        let rules_for_sled = self
            .resolve_firewall_rules_for_sled_agent(opctx, &vpc, rules)
            .await?;
        debug!(self.log, "resolved {} rules for sleds", rules_for_sled.len());
        let sled_rules_request =
            sled_agent_client::types::VpcFirewallRulesEnsureBody {
                rules: rules_for_sled,
            };

        let vpc_to_sleds =
            self.db_datastore.vpc_resolve_to_sleds(vpc.id()).await?;
        debug!(self.log, "resolved sleds for vpc {}", vpc.name(); "vpc_to_sled" => ?vpc_to_sleds);

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

        debug!(self.log, "sending firewall rules to sled agents");
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

    pub(crate) async fn resolve_firewall_rules_for_sled_agent(
        &self,
        opctx: &OpContext,
        vpc: &db::model::Vpc,
        rules: &[db::model::VpcFirewallRule],
    ) -> Result<Vec<sled_agent_client::types::VpcFirewallRule>, Error> {
        // Collect the names of instances, subnets, and VPCs that are either
        // targets or host filters. We have to find the sleds for all the
        // targets, and we'll need information about the IP addresses or
        // subnets for things that are specified as host filters as well.
        let mut instances: HashSet<Name> = HashSet::new();
        let mut subnets: HashSet<Name> = HashSet::new();
        let mut vpcs: HashSet<Name> = HashSet::new();
        for rule in rules {
            for target in &rule.targets {
                match &target.0 {
                    external::VpcFirewallRuleTarget::Instance(name) => {
                        instances.insert(name.clone().into());
                    }
                    external::VpcFirewallRuleTarget::Subnet(name) => {
                        subnets.insert(name.clone().into());
                    }
                    external::VpcFirewallRuleTarget::Vpc(name) => {
                        if name != vpc.name() {
                            return Err(Error::invalid_request(
                                "cross-VPC firewall target unsupported",
                            ));
                        }
                        vpcs.insert(name.clone().into());
                    }
                    external::VpcFirewallRuleTarget::Ip(_)
                    | external::VpcFirewallRuleTarget::IpNet(_) => {
                        vpcs.insert(vpc.name().clone().into());
                    }
                }
            }

            for host in rule.filter_hosts.iter().flatten() {
                match &host.0 {
                    external::VpcFirewallRuleHostFilter::Instance(name) => {
                        instances.insert(name.clone().into());
                    }
                    external::VpcFirewallRuleHostFilter::Subnet(name) => {
                        subnets.insert(name.clone().into());
                    }
                    external::VpcFirewallRuleHostFilter::Vpc(name) => {
                        if name != vpc.name() {
                            return Err(Error::invalid_request(
                                "cross-VPC firewall host filter unsupported",
                            ));
                        }
                        vpcs.insert(name.clone().into());
                    }
                    // We don't need to resolve anything for Ip(Net)s.
                    external::VpcFirewallRuleHostFilter::Ip(_) => (),
                    external::VpcFirewallRuleHostFilter::IpNet(_) => (),
                }
            }
        }

        // Resolve named instances, VPCs, and subnets.
        // TODO-correctness: It's possible the resolving queries produce
        // inconsistent results due to concurrent changes. They should be
        // transactional.
        type NetMap = HashMap<external::Name, Vec<IpNetwork>>;
        type NicMap = HashMap<external::Name, Vec<NetworkInterface>>;
        let no_networks: Vec<IpNetwork> = Vec::new();
        let no_interfaces: Vec<NetworkInterface> = Vec::new();

        let mut instance_interfaces: NicMap = HashMap::new();
        for instance_name in &instances {
            let (.., authz_instance) =
                LookupPath::new(opctx, &self.db_datastore)
                    .project_id(vpc.project_id)
                    .instance_name(instance_name)
                    .lookup_for(authz::Action::ListChildren)
                    .await?;
            for iface in self
                .db_datastore
                .derive_guest_network_interface_info(opctx, &authz_instance)
                .await?
            {
                instance_interfaces
                    .entry(instance_name.0.clone())
                    .or_insert_with(Vec::new)
                    .push(iface);
            }
        }

        let mut vpc_interfaces: NicMap = HashMap::new();
        for vpc_name in &vpcs {
            let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
                .project_id(vpc.project_id)
                .vpc_name(vpc_name)
                .lookup_for(authz::Action::ListChildren)
                .await?;
            for iface in self
                .db_datastore
                .derive_vpc_network_interface_info(opctx, &authz_vpc)
                .await?
            {
                vpc_interfaces
                    .entry(vpc_name.0.clone())
                    .or_insert_with(Vec::new)
                    .push(iface);
            }
        }

        let mut subnet_interfaces: NicMap = HashMap::new();
        for subnet_name in &subnets {
            let (.., authz_subnet) = LookupPath::new(opctx, &self.db_datastore)
                .project_id(vpc.project_id)
                .vpc_name(&Name::from(vpc.name().clone()))
                .vpc_subnet_name(subnet_name)
                .lookup_for(authz::Action::ListChildren)
                .await?;
            for iface in self
                .db_datastore
                .derive_subnet_network_interface_info(opctx, &authz_subnet)
                .await?
            {
                subnet_interfaces
                    .entry(subnet_name.0.clone())
                    .or_insert_with(Vec::new)
                    .push(iface);
            }
        }

        let subnet_networks: NetMap = self
            .db_datastore
            .resolve_subnets_to_ips(vpc, subnets)
            .await?
            .into_iter()
            .map(|(name, v)| (name.0, v))
            .collect();

        debug!(
            self.log,
            "resolved names for firewall rules";
            "instance_interfaces" => ?instance_interfaces,
            "vpc_interfaces" => ?vpc_interfaces,
            "subnet_interfaces" => ?subnet_interfaces,
            "subnet_networks" => ?subnet_networks,
        );

        // Compile resolved rules for the sled agents.
        let mut sled_agent_rules = Vec::with_capacity(rules.len());
        for rule in rules {
            // TODO: what is the correct behavior when a name is not found?
            // Options:
            // (1) Fail update request (though note this can still arise
            //     from things like instance deletion)
            // (2) Allow update request, ignore this rule (but store it
            //     in case it becomes valid later). This is consistent
            //     with the semantics of the rules. Rules with bad
            //     references should likely at least be flagged to users.
            // We currently adopt option (2), as this allows users to add
            // firewall rules (including default rules) before instances
            // and their interfaces are instantiated.

            // Collect unique network interface targets.
            // This would be easier if `NetworkInterface` were `Hash`,
            // but that's not easy because it's a generated type. We
            // use the pair (VNI, MAC) as a unique interface identifier.
            let mut nics = HashSet::new();
            let mut targets = Vec::with_capacity(rule.targets.len());
            let mut push_target_nic = |nic: &NetworkInterface| {
                if nics.insert((*nic.vni, (*nic.mac).clone())) {
                    targets.push(nic.clone());
                }
            };
            for target in &rule.targets {
                match &target.0 {
                    external::VpcFirewallRuleTarget::Vpc(name) => {
                        vpc_interfaces
                            .get(&name)
                            .unwrap_or(&no_interfaces)
                            .iter()
                            .for_each(&mut push_target_nic);
                    }
                    external::VpcFirewallRuleTarget::Subnet(name) => {
                        subnet_interfaces
                            .get(&name)
                            .unwrap_or(&no_interfaces)
                            .iter()
                            .for_each(&mut push_target_nic);
                    }
                    external::VpcFirewallRuleTarget::Instance(name) => {
                        instance_interfaces
                            .get(&name)
                            .unwrap_or(&no_interfaces)
                            .iter()
                            .for_each(&mut push_target_nic);
                    }
                    external::VpcFirewallRuleTarget::Ip(addr) => {
                        vpc_interfaces
                            .get(vpc.name())
                            .unwrap_or(&no_interfaces)
                            .iter()
                            .filter(|nic| nic.ip == *addr)
                            .for_each(&mut push_target_nic);
                    }
                    external::VpcFirewallRuleTarget::IpNet(net) => {
                        vpc_interfaces
                            .get(vpc.name())
                            .unwrap_or(&no_interfaces)
                            .iter()
                            .filter(|nic| {
                                use external::IpNet;
                                use std::net::IpAddr;
                                match (IpNet::from(*net), IpAddr::from(nic.ip))
                                {
                                    (IpNet::V4(net), IpAddr::V4(ip)) => {
                                        net.contains(ip)
                                    }
                                    (IpNet::V6(net), IpAddr::V6(ip)) => {
                                        net.contains(ip)
                                    }
                                    (_, _) => false,
                                }
                            })
                            .for_each(&mut push_target_nic);
                    }
                }
            }
            if !rule.targets.is_empty() && targets.is_empty() {
                // Target not found; skip this rule.
                continue;
            }

            let filter_hosts = match &rule.filter_hosts {
                None => None,
                Some(hosts) => {
                    let mut host_addrs = Vec::with_capacity(hosts.len());
                    for host in hosts {
                        match &host.0 {
                            external::VpcFirewallRuleHostFilter::Instance(
                                name,
                            ) => {
                                for interface in instance_interfaces
                                    .get(&name)
                                    .unwrap_or(&no_interfaces)
                                {
                                    host_addrs.push(IpNet::from(interface.ip))
                                }
                            }
                            external::VpcFirewallRuleHostFilter::Subnet(
                                name,
                            ) => {
                                for subnet in subnet_networks
                                    .get(&name)
                                    .unwrap_or(&no_networks)
                                {
                                    host_addrs.push(IpNet::from(*subnet));
                                }
                            }
                            external::VpcFirewallRuleHostFilter::Ip(addr) => {
                                host_addrs.push(IpNet::from(*addr))
                            }
                            external::VpcFirewallRuleHostFilter::IpNet(net) => {
                                host_addrs.push(IpNet::from(*net))
                            }
                            external::VpcFirewallRuleHostFilter::Vpc(name) => {
                                for interface in vpc_interfaces
                                    .get(&name)
                                    .unwrap_or(&no_interfaces)
                                {
                                    host_addrs.push(IpNet::from(interface.ip))
                                }
                            }
                        }
                    }
                    if !hosts.is_empty() && host_addrs.is_empty() {
                        // Filter host not found; skip this rule.
                        continue;
                    }
                    Some(host_addrs)
                }
            };

            let filter_ports = rule
                .filter_ports
                .as_ref()
                .map(|ports| ports.iter().map(|v| v.0.into()).collect());

            let filter_protocols =
                rule.filter_protocols.as_ref().map(|protocols| {
                    protocols.iter().map(|v| v.0.into()).collect()
                });

            sled_agent_rules.push(sled_agent_client::types::VpcFirewallRule {
                status: rule.status.0.into(),
                direction: rule.direction.0.into(),
                targets,
                filter_hosts,
                filter_ports,
                filter_protocols,
                action: rule.action.0.into(),
                priority: rule.priority.0 .0,
            });
        }
        debug!(
            self.log,
            "resolved firewall rules for sled agents";
            "sled_agent_rules" => ?sled_agent_rules,
        );

        Ok(sled_agent_rules)
    }
}
