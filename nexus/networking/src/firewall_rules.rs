// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functionality related to firewall rules.

use futures::future::join_all;
use ipnetwork::IpNetwork;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::Name;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use omicron_common::api::external::IpNet;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::internal::nexus::HostIdentifier;
use omicron_common::api::internal::shared::NetworkInterface;
use slog::debug;
use slog::info;
use slog::warn;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use uuid::Uuid;

pub async fn vpc_list_firewall_rules(
    datastore: &DataStore,
    opctx: &OpContext,
    vpc_lookup: &lookup::Vpc<'_>,
) -> ListResultVec<db::model::VpcFirewallRule> {
    let (.., authz_vpc) = vpc_lookup.lookup_for(authz::Action::Read).await?;
    let rules = datastore.vpc_list_firewall_rules(&opctx, &authz_vpc).await?;
    Ok(rules)
}

pub async fn resolve_firewall_rules_for_sled_agent(
    datastore: &DataStore,
    opctx: &OpContext,
    vpc: &db::model::Vpc,
    rules: &[db::model::VpcFirewallRule],
    log: &Logger,
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
        if let Ok((.., authz_instance)) = LookupPath::new(opctx, datastore)
            .project_id(vpc.project_id)
            .instance_name(instance_name)
            .lookup_for(authz::Action::ListChildren)
            .await
        {
            for iface in datastore
                .derive_guest_network_interface_info(opctx, &authz_instance)
                .await?
            {
                instance_interfaces
                    .entry(instance_name.0.clone())
                    .or_insert_with(Vec::new)
                    .push(iface);
            }
        }
    }

    let mut vpc_interfaces: NicMap = HashMap::new();
    for vpc_name in &vpcs {
        if let Ok((.., authz_vpc)) = LookupPath::new(opctx, datastore)
            .project_id(vpc.project_id)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::ListChildren)
            .await
        {
            for iface in datastore
                .derive_vpc_network_interface_info(opctx, &authz_vpc)
                .await?
            {
                vpc_interfaces
                    .entry(vpc_name.0.clone())
                    .or_insert_with(Vec::new)
                    .push(iface);
            }
        }
    }

    let mut subnet_interfaces: NicMap = HashMap::new();
    for subnet_name in &subnets {
        if let Ok((.., authz_subnet)) = LookupPath::new(opctx, datastore)
            .project_id(vpc.project_id)
            .vpc_name(&Name::from(vpc.name().clone()))
            .vpc_subnet_name(subnet_name)
            .lookup_for(authz::Action::ListChildren)
            .await
        {
            for iface in datastore
                .derive_subnet_network_interface_info(opctx, &authz_subnet)
                .await?
            {
                subnet_interfaces
                    .entry(subnet_name.0.clone())
                    .or_insert_with(Vec::new)
                    .push(iface);
            }
        }
    }

    let subnet_networks: NetMap = datastore
        .resolve_vpc_subnets_to_ip_networks(vpc, subnets)
        .await?
        .into_iter()
        .map(|(name, v)| (name.0, v))
        .collect();

    debug!(
        log,
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
            if nics.insert((nic.vni, *nic.mac)) {
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
                        .filter(|nic| match (net, nic.ip) {
                            (IpNet::V4(net), IpAddr::V4(ip)) => {
                                net.contains(ip)
                            }
                            (IpNet::V6(net), IpAddr::V6(ip)) => {
                                net.contains(ip)
                            }
                            (_, _) => false,
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
                        external::VpcFirewallRuleHostFilter::Instance(name) => {
                            for interface in instance_interfaces
                                .get(&name)
                                .unwrap_or(&no_interfaces)
                            {
                                host_addrs.push(
                                    HostIdentifier::Ip(IpNet::from(
                                        interface.ip,
                                    ))
                                    .into(),
                                )
                            }
                        }
                        external::VpcFirewallRuleHostFilter::Subnet(name) => {
                            for subnet in subnet_networks
                                .get(&name)
                                .unwrap_or(&no_networks)
                            {
                                host_addrs.push(
                                    HostIdentifier::Ip(IpNet::from(*subnet))
                                        .into(),
                                );
                            }
                        }
                        external::VpcFirewallRuleHostFilter::Ip(addr) => {
                            host_addrs.push(
                                HostIdentifier::Ip(IpNet::from(*addr)).into(),
                            )
                        }
                        external::VpcFirewallRuleHostFilter::IpNet(net) => {
                            host_addrs.push(HostIdentifier::Ip(*net).into())
                        }
                        external::VpcFirewallRuleHostFilter::Vpc(name) => {
                            for interface in vpc_interfaces
                                .get(&name)
                                .unwrap_or(&no_interfaces)
                            {
                                host_addrs.push(
                                    HostIdentifier::Vpc(interface.vni).into(),
                                )
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

        let filter_protocols = rule
            .filter_protocols
            .as_ref()
            .map(|protocols| protocols.iter().map(|v| v.0.into()).collect());

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
        log,
        "resolved firewall rules for sled agents";
        "sled_agent_rules" => ?sled_agent_rules,
    );

    Ok(sled_agent_rules)
}

pub async fn send_sled_agents_firewall_rules(
    datastore: &DataStore,
    opctx: &OpContext,
    vpc: &db::model::Vpc,
    rules: &[db::model::VpcFirewallRule],
    sleds_filter: &[Uuid],
    sled_lookup_opctx: &OpContext,
    log: &Logger,
) -> Result<(), Error> {
    let rules_for_sled = resolve_firewall_rules_for_sled_agent(
        datastore, opctx, &vpc, rules, log,
    )
    .await?;
    debug!(log, "resolved {} rules for sleds", rules_for_sled.len());
    let sled_rules_request =
        sled_agent_client::types::VpcFirewallRulesEnsureBody {
            vni: vpc.vni.0,
            rules: rules_for_sled,
        };

    let vpc_to_sleds =
        datastore.vpc_resolve_to_sleds(vpc.id(), sleds_filter).await?;
    debug!(
        log, "resolved sleds for vpc {}", vpc.name();
        "vpc_to_sled" => ?vpc_to_sleds,
    );

    let mut sled_requests = Vec::with_capacity(vpc_to_sleds.len());
    for sled in &vpc_to_sleds {
        let sled_id = sled.id();
        let vpc_id = vpc.id();
        let sled_rules_request = sled_rules_request.clone();
        sled_requests.push(async move {
            crate::sled_client(datastore, sled_lookup_opctx, sled_id, log)
                .await?
                .vpc_firewall_rules_put(&vpc_id, &sled_rules_request)
                .await
                .map_err(|e| Error::internal_error(&e.to_string()))
        });
    }

    debug!(log, "sending firewall rules to sled agents");
    let results = join_all(sled_requests).await;
    // TODO-correctness: handle more than one failure in the sled-agent requests
    //   https://github.com/oxidecomputer/omicron/issues/1791
    for (sled, result) in vpc_to_sleds.iter().zip(results) {
        if let Err(e) = result {
            warn!(log, "failed to update firewall rules on sled agent";
                      "sled_id" => %sled.id(),
                      "vpc_id" => %vpc.id(),
                      "error" => %e);
            return Err(e);
        }
    }
    info!(log, "updated firewall rules on {} sleds", vpc_to_sleds.len());

    Ok(())
}

/// Ensure firewall rules for internal services get reflected on all the
/// relevant sleds.
pub async fn plumb_service_firewall_rules(
    datastore: &DataStore,
    opctx: &OpContext,
    sleds_filter: &[Uuid],
    sled_lookup_opctx: &OpContext,
    log: &Logger,
) -> Result<(), Error> {
    let svcs_vpc = LookupPath::new(opctx, datastore)
        .vpc_id(*db::fixed_data::vpc::SERVICES_VPC_ID);
    let svcs_fw_rules =
        vpc_list_firewall_rules(datastore, opctx, &svcs_vpc).await?;
    let (_, _, _, svcs_vpc) = svcs_vpc.fetch().await?;
    send_sled_agents_firewall_rules(
        datastore,
        opctx,
        &svcs_vpc,
        &svcs_fw_rules,
        sleds_filter,
        sled_lookup_opctx,
        log,
    )
    .await?;
    Ok(())
}
