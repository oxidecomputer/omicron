// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functionality related to firewall rules.

use futures::future::join_all;
use ipnetwork::IpNetwork;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::fixed_data::vpc::SERVICES_VPC_ID;
use nexus_db_queries::db::fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::db::model::Name;
use omicron_common::api::external;
use omicron_common::api::external::AllowedSourceIps;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::internal::nexus::HostIdentifier;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::ResolvedVpcFirewallRule;
use omicron_uuid_kinds::SledUuid;
use oxnet::IpNet;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::IpAddr;
use uuid::Uuid;

/// Error returned when attempting to propagate service firewall rules to
/// sled-agents.
#[derive(Debug)]
pub enum ServiceFirewallRulesError {
    /// Failed to look up or resolve firewall rules (e.g., a database error).
    Lookup(Error),
    /// Firewall rules were resolved but some sled-agents could not be reached.
    SledPush(Vec<(SledUuid, Error)>),
}

impl fmt::Display for ServiceFirewallRulesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceFirewallRulesError::Lookup(e) => {
                write!(f, "failed to look up or resolve firewall rules: {e}")
            }
            ServiceFirewallRulesError::SledPush(failures) => {
                let maybe_s = if failures.len() == 1 { "" } else { "s" };
                let ids = failures
                    .iter()
                    .map(|(id, _e)| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "failed to push firewall rules to {} sled{}: [{}]",
                    failures.len(),
                    maybe_s,
                    ids,
                )
            }
        }
    }
}

pub async fn vpc_list_firewall_rules(
    datastore: &DataStore,
    opctx: &OpContext,
    vpc_lookup: &nexus_db_lookup::lookup::Vpc<'_>,
) -> ListResultVec<db::model::VpcFirewallRule> {
    let (.., authz_vpc) = vpc_lookup.lookup_for(authz::Action::Read).await?;
    let rules = datastore.vpc_list_firewall_rules(&opctx, &authz_vpc).await?;
    Ok(rules)
}

/// Resolve a set of VPC firewall rules into the form expected by sled-agents.
///
/// This function does not inject any Nexus-specific allow rules (i.e., the
/// inbound-allow rule derived from the IP allowlist). Callers that are
/// propagating rules for the services VPC must inject that rule themselves
/// before calling this function; see [`plumb_service_firewall_rules`]. An error
/// is returned if this not the case.
pub async fn resolve_firewall_rules_for_sled_agent(
    datastore: &DataStore,
    opctx: &OpContext,
    vpc: &db::model::Vpc,
    rules: &[db::model::VpcFirewallRule],
    log: &Logger,
) -> Result<Vec<ResolvedVpcFirewallRule>, Error> {
    // Check the caller has added the rules implementing the IP allowlist if
    // needed.
    if vpc.id() == *SERVICES_VPC_ID
        && !rules
            .iter()
            .any(|r| r.name().as_str().starts_with(NEXUS_VPC_FW_RULE_NAME))
    {
        return Err(Error::internal_error(
            "These firewall rules target the Oxide services VPC, \
            but do not include any rules implementing the IP allowlist \
            for Nexus. Callers must inject these manually. See \
            `plumb_service_firewall_rules` and `make_nexus_allowlist_rules` \
            more details.",
        ));
    }

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
    let mut vpc_vni_map = HashMap::new();
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
                vpc_vni_map.insert(&vpc_name.0, iface.vni);
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
                        .filter(|nic| nic.ip_config.has_addr(addr))
                        .for_each(&mut push_target_nic);
                }
                external::VpcFirewallRuleTarget::IpNet(net) => {
                    vpc_interfaces
                        .get(vpc.name())
                        .unwrap_or(&no_interfaces)
                        .iter()
                        .filter(|nic| match net {
                            IpNet::V4(net) => nic
                                .ip_config
                                .ipv4_addr()
                                .map(|ip| net.contains(*ip))
                                .unwrap_or(false),
                            IpNet::V6(net) => nic
                                .ip_config
                                .ipv6_addr()
                                .map(|ip| net.contains(*ip))
                                .unwrap_or(false),
                        })
                        .for_each(&mut push_target_nic);
                }
            }
        }
        if !rule.targets.is_empty() && targets.is_empty() {
            // Target not found; skip this rule.
            continue;
        }

        // Construct the set of filter hosts from the DB rule.
        let filter_hosts = match &rule.filter_hosts {
            None => None,
            Some(hosts) => {
                let mut host_addrs = HashSet::with_capacity(hosts.len());
                for host in hosts {
                    match &host.0 {
                        external::VpcFirewallRuleHostFilter::Instance(name) => {
                            for interface in instance_interfaces
                                .get(&name)
                                .unwrap_or(&no_interfaces)
                            {
                                // Insert both IPv4 and / or IPv6 addresses.
                                if let Some(ipv4) =
                                    interface.ip_config.ipv4_addr()
                                {
                                    host_addrs.insert(HostIdentifier::Ip(
                                        IpNet::host_net(IpAddr::V4(*ipv4)),
                                    ));
                                }
                                if let Some(ipv6) =
                                    interface.ip_config.ipv6_addr()
                                {
                                    host_addrs.insert(HostIdentifier::Ip(
                                        IpNet::host_net(IpAddr::V6(*ipv6)),
                                    ));
                                }
                            }
                        }
                        external::VpcFirewallRuleHostFilter::Subnet(name) => {
                            for subnet in subnet_networks
                                .get(name)
                                .unwrap_or(&no_networks)
                            {
                                host_addrs.insert(HostIdentifier::Ip(
                                    IpNet::from(*subnet),
                                ));
                            }
                        }
                        external::VpcFirewallRuleHostFilter::Ip(addr) => {
                            host_addrs.insert(HostIdentifier::Ip(
                                IpNet::host_net(*addr),
                            ));
                        }
                        external::VpcFirewallRuleHostFilter::IpNet(net) => {
                            host_addrs.insert(HostIdentifier::Ip(*net));
                        }
                        external::VpcFirewallRuleHostFilter::Vpc(name) => {
                            if let Some(vni) = vpc_vni_map.get(name) {
                                host_addrs.insert(HostIdentifier::Vpc(*vni));
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
            .map(|ports| ports.iter().map(|v| v.0).collect());

        let filter_protocols = rule
            .filter_protocols
            .as_ref()
            .map(|protocols| protocols.iter().map(|v| v.0).collect());

        sled_agent_rules.push(ResolvedVpcFirewallRule {
            status: rule.status.0,
            direction: rule.direction.0,
            targets,
            filter_hosts,
            filter_ports,
            filter_protocols,
            action: rule.action.0,
            priority: rule.priority.0,
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
    sleds_filter: &[SledUuid],
    sled_lookup_opctx: &OpContext,
    log: &Logger,
) -> Result<(), ServiceFirewallRulesError> {
    let rules_for_sled = resolve_firewall_rules_for_sled_agent(
        datastore, opctx, &vpc, rules, log,
    )
    .await
    .map_err(ServiceFirewallRulesError::Lookup)?;
    debug!(log, "resolved {} rules for sleds", rules_for_sled.len());
    let sled_rules_request =
        sled_agent_client::types::VpcFirewallRulesEnsureBody {
            vni: vpc.vni.0,
            rules: rules_for_sled,
        };

    let vpc_to_sleds = datastore
        .vpc_resolve_to_sleds(vpc.id(), sleds_filter)
        .await
        .map_err(ServiceFirewallRulesError::Lookup)?;
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
    let mut sled_failures = Vec::new();
    for (sled, result) in vpc_to_sleds.iter().zip(results) {
        if let Err(e) = result {
            sled_failures.push((sled.id(), e));
        }
    }
    if !sled_failures.is_empty() {
        return Err(ServiceFirewallRulesError::SledPush(sled_failures));
    }
    info!(log, "updated firewall rules on {} sleds", vpc_to_sleds.len());

    Ok(())
}

/// Ensure firewall rules for internal services get reflected on all the
/// relevant sleds.
///
/// This includes a synthetic inbound-allow rule for the Nexus subnet,
/// constructed on-demand from the current IP allowlist. Because OPTE has a
/// default-deny policy, Nexus zones will be unreachable until this function
/// succeeds at least once.
pub async fn plumb_service_firewall_rules(
    datastore: &DataStore,
    opctx: &OpContext,
    sleds_filter: &[SledUuid],
    sled_lookup_opctx: &OpContext,
    log: &Logger,
) -> Result<(), ServiceFirewallRulesError> {
    let svcs_vpc = LookupPath::new(opctx, datastore)
        .vpc_id(*db::fixed_data::vpc::SERVICES_VPC_ID);
    let mut svcs_fw_rules =
        vpc_list_firewall_rules(datastore, opctx, &svcs_vpc)
            .await
            .map_err(ServiceFirewallRulesError::Lookup)?;
    let (_, _, _, svcs_vpc) =
        svcs_vpc.fetch().await.map_err(ServiceFirewallRulesError::Lookup)?;

    // Construct a synthetic inbound-allow rule for the Nexus subnet based on
    // the current IP allowlist. This rule is not stored in the DB; it is
    // generated here and propagated to sled-agents on each invocation. The
    // allowlist determines which source IPs are permitted; OPTE's default-deny
    // policy blocks all other traffic.
    let allowed_ips = lookup_allowed_source_ips(datastore, opctx, log)
        .await
        .map_err(ServiceFirewallRulesError::Lookup)?;
    let mut nexus_rules = make_nexus_allowlist_rules(allowed_ips)
        .map_err(ServiceFirewallRulesError::Lookup)?;
    svcs_fw_rules.append(&mut nexus_rules);

    send_sled_agents_firewall_rules(
        datastore,
        opctx,
        &svcs_vpc,
        &svcs_fw_rules,
        sleds_filter,
        sled_lookup_opctx,
        log,
    )
    .await
}

/// The base name for the VPC firewall rules for Nexus implementing the IP allowlist.
pub const NEXUS_VPC_FW_RULE_NAME: &str = "nexus-inbound";

/// Maximum number of entries in the IP allowlist.
///
/// This is enforced at the API layer in `allow_list_upsert` (which imports
/// this constant). It must be kept consistent with
/// `MAX_NEXUS_INBOUND_RULES * VPC_FIREWALL_RULE_MAX_FILTER_LEN`; see the
/// static assertion below.
pub const MAX_ALLOWLIST_LENGTH: usize = 1000;

/// Number of nexus-inbound rules used to cover the full allowlist.
///
/// Because each firewall rule can carry at most
/// [`VPC_FIREWALL_RULE_MAX_FILTER_LEN`] host-filter entries, a list of up to
/// `MAX_ALLOWLIST_LENGTH` IPs is split across this many rules (named
/// `nexus-inbound-1`, `nexus-inbound-2`, …).
const MAX_NEXUS_INBOUND_RULES: usize = 4;

// If this assertion fires, MAX_ALLOWLIST_LENGTH exceeds what
// MAX_NEXUS_INBOUND_RULES rules of VPC_FIREWALL_RULE_MAX_FILTER_LEN hosts
// each can represent. Either increase MAX_NEXUS_INBOUND_RULES or reduce
// MAX_ALLOWLIST_LENGTH, and verify that sled-agent / OPTE can still handle
// the resulting number of rules and hosts per rule.
static_assertions::const_assert!(
    MAX_ALLOWLIST_LENGTH
        <= MAX_NEXUS_INBOUND_RULES * external::VPC_FIREWALL_RULE_MAX_FILTER_LEN
);

/// Build the inbound-allow firewall rules for the Nexus subnet from the given
/// IP allowlist, to be propagated to sled-agents.
///
/// Each rule allows inbound TCP traffic on ports 80 and 443 from a subset of
/// the allowed source IPs (or from any source if the allowlist is
/// unrestricted). Rules target the Nexus VPC subnet and are not stored in the
/// database.
///
/// When the allowlist is a `List`, the IPs are split across up to
/// `MAX_NEXUS_INBOUND_RULES` rules (named `nexus-inbound-1`,
/// `nexus-inbound-2`, …) of at most `VPC_FIREWALL_RULE_MAX_FILTER_LEN`
/// entries each. When the allowlist is `Any`, a single rule named
/// `nexus-inbound` with no host filter is returned.
fn make_nexus_allowlist_rules(
    allowed_ips: AllowedSourceIps,
) -> Result<Vec<db::model::VpcFirewallRule>, Error> {
    match allowed_ips {
        AllowedSourceIps::Any => {
            Ok(vec![make_one_nexus_inbound_rule(NEXUS_VPC_FW_RULE_NAME, None)?])
        }
        AllowedSourceIps::List(list) => list
            .as_slice()
            .chunks(external::VPC_FIREWALL_RULE_MAX_FILTER_LEN)
            .enumerate()
            .map(|(i, chunk)| {
                let name = format!("{NEXUS_VPC_FW_RULE_NAME}-{}", i + 1);
                let hosts = Some(
                    chunk
                        .iter()
                        .copied()
                        .map(external::VpcFirewallRuleHostFilter::IpNet)
                        .collect(),
                );
                make_one_nexus_inbound_rule(&name, hosts)
            })
            .collect(),
    }
}

fn make_one_nexus_inbound_rule(
    name: &str,
    hosts: Option<Vec<external::VpcFirewallRuleHostFilter>>,
) -> Result<db::model::VpcFirewallRule, Error> {
    let update = external::VpcFirewallRuleUpdate {
        name: name.parse().expect("nexus-inbound rule names are always valid"),
        description: String::new(),
        status: external::VpcFirewallRuleStatus::Enabled,
        direction: external::VpcFirewallRuleDirection::Inbound,
        targets: vec![external::VpcFirewallRuleTarget::Subnet(
            NEXUS_VPC_SUBNET.name().clone(),
        )],
        filters: external::VpcFirewallRuleFilter {
            hosts,
            protocols: Some(vec![external::VpcFirewallRuleProtocol::Tcp]),
            ports: Some(vec![
                external::L4PortRange {
                    first: 80.try_into().unwrap(),
                    last: 80.try_into().unwrap(),
                },
                external::L4PortRange {
                    first: 443.try_into().unwrap(),
                    last: 443.try_into().unwrap(),
                },
            ]),
        },
        action: external::VpcFirewallRuleAction::Allow,
        priority: external::VpcFirewallRulePriority(65534),
    };
    db::model::VpcFirewallRule::new(Uuid::new_v4(), *SERVICES_VPC_ID, &update)
        .map_err(|e| Error::internal_error(&e.to_string()))
}

/// Return the list of allowed IPs from the database.
async fn lookup_allowed_source_ips(
    datastore: &DataStore,
    opctx: &OpContext,
    log: &Logger,
) -> Result<AllowedSourceIps, Error> {
    match datastore.allow_list_view(opctx).await {
        Ok(allowed) => {
            slog::trace!(log, "fetched allowlist from DB"; "allowed" => ?allowed);
            allowed.allowed_source_ips()
        }
        Err(e) => {
            error!(log, "failed to fetch allowlist from DB"; "err" => ?e);
            Err(e)
        }
    }
}
