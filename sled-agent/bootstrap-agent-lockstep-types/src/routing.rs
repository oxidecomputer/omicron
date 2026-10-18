// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Optional named routing configuration for RSS and its Nexus handoff.

use anyhow::{bail, ensure};
use nexus_types_versions::latest::networking::{
    BgpPeerKind, RouterConfigurationSwitch,
};
use nexus_types_versions::router_configuration::*;
use omicron_common::api::external::{Name, NameOrId};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::{
    ImportExportPolicy, NumberedRouter, RackNetworkConfig, RouterPeerType,
};
use sled_agent_types::router_config::*;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

// These secret-free leaf types have the same meaning in commissioning and RSS.
// Reuse the existing dependency rather than maintaining duplicate definitions.
pub use wicketd_commission_types::rack_setup::{
    UserSpecifiedInitialRouterAssignment as InitialRouterAssignment,
    UserSpecifiedRoutingAnnounceSet as RoutingAnnounceSet,
};

#[derive(Clone, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    #[serde(default)]
    pub announce_sets: Vec<RoutingAnnounceSet>,
    #[serde(default)]
    pub router_configurations: Vec<InitialRouterConfiguration>,
    // Required, including when deliberately empty.
    pub control_plane_router_configurations: Vec<InitialRouterAssignment>,
    pub recovery_silo_router_configurations: Vec<InitialRouterAssignment>,
}

// The nested API peers include TCP-MD5 keys. Never log their Debug output.
impl std::fmt::Debug for RoutingConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutingConfig")
            .field("announce_sets", &self.announce_sets)
            .field("router_configurations", &"<redacted>")
            .field(
                "control_plane_router_configurations",
                &self.control_plane_router_configurations,
            )
            .field(
                "recovery_silo_router_configurations",
                &self.recovery_silo_router_configurations,
            )
            .finish()
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InitialRouterConfiguration {
    pub name: Name,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub switches: Vec<RouterConfigurationSwitch>,
}

/// IDs are allocated once by RSS and preserved across handoff retries.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RoutingInitialization {
    pub config: RoutingConfig,
    pub router_ids: BTreeMap<Name, Uuid>,
}

impl RoutingConfig {
    pub fn validate(&self, legacy: &RackNetworkConfig) -> anyhow::Result<()> {
        let mut sets = BTreeSet::new();
        for set in &self.announce_sets {
            ensure!(
                sets.insert(&set.name),
                "duplicate announce set {}",
                set.name
            );
            ensure!(
                !legacy
                    .bgp
                    .iter()
                    .any(|b| set.name.as_str()
                        == format!("as{}-announce", b.asn)),
                "announce set {} conflicts with legacy initialization",
                set.name
            );
            let mut prefixes = BTreeSet::new();
            for prefix in &set.prefixes {
                ensure!(
                    prefixes.insert(prefix.to_string()),
                    "duplicate prefix in {}",
                    set.name
                );
                validate_prefix(*prefix)?;
            }
        }
        let mut names = BTreeSet::new();
        // mgd demultiplexes peers by address/interface across all routers.
        let mut bgp_addresses = BTreeSet::new();
        let mut bgp_ports = BTreeSet::new();
        let mut bfd_addresses = BTreeSet::new();
        for port in legacy.ports.iter() {
            for peer in &port.bgp_peers {
                match peer.addr {
                    RouterPeerType::Numbered(peer) => {
                        bgp_addresses.insert((
                            port.switch,
                            IpAddr::from(peer.target_addr()),
                        ));
                    }
                    RouterPeerType::Unnumbered(_) => {
                        bgp_ports.insert((port.switch, peer.port.clone()));
                    }
                }
            }
        }
        for peer in &legacy.bfd {
            bfd_addresses.insert((peer.switch, peer.remote));
        }
        for rc in &self.router_configurations {
            ensure!(
                !is_reserved_router_configuration_name(&rc.name),
                "reserved router configuration name {}",
                rc.name
            );
            ensure!(
                names.insert(&rc.name),
                "duplicate router configuration {:?}",
                rc.name
            );
            let mut slots = BTreeSet::new();
            for block in &rc.switches {
                ensure!(
                    slots.insert(block.switch),
                    "duplicate switch block in {}",
                    rc.name
                );
                if let Some(bgp) = &block.bgp_config {
                    let NameOrId::Name(set) = &bgp.bgp_announce_set else {
                        bail!(
                            "initial announce sets must be referenced by name"
                        );
                    };
                    ensure!(sets.contains(set), "unknown announce set {set}");
                }
                ensure!(
                    block.bgp_config.is_some() || block.bgp_peers.is_empty(),
                    "BGP peers in {} need a BGP configuration",
                    rc.name
                );
                unique_names(
                    block.bgp_peers.iter().map(|p| &p.name),
                    "BGP peer",
                )?;
                for peer in &block.bgp_peers {
                    match &peer.peer {
                        BgpPeerKind::Numbered { addr, src_addr } => {
                            NumberedRouter::new(*addr, *src_addr)?;
                            ensure!(
                                bgp_addresses.insert((
                                    block.switch,
                                    IpAddr::from(*addr)
                                )),
                                "BGP peer {} is already configured on {:?}",
                                IpAddr::from(*addr),
                                block.switch
                            );
                        }
                        BgpPeerKind::Unnumbered { port, .. } => {
                            ensure!(
                                legacy
                                    .ports
                                    .iter()
                                    .any(|p| p.switch == block.switch
                                        && p.port == port.as_str()),
                                "unknown port {port} on {:?}",
                                block.switch
                            );
                            ensure!(
                                bgp_ports
                                    .insert((block.switch, port.to_string())),
                                "BGP port {port} is already configured on {:?}",
                                block.switch
                            );
                        }
                    }
                }
                unique_names(
                    block.routes.iter().map(|r| &r.name),
                    "static route",
                )?;
                for route in &block.routes {
                    ensure!(
                        matches!(route.dst, IpNet::V4(_)) == route.gw.is_ipv4(),
                        "route {} has mismatched address families",
                        route.name
                    );
                    validate_prefix(route.dst)?;
                }
                unique_names(
                    block.bfd_peers.iter().map(|p| &p.name),
                    "BFD peer",
                )?;
                for peer in &block.bfd_peers {
                    ensure!(
                        peer.local
                            .is_none_or(|local| local.is_ipv4()
                                == peer.remote.is_ipv4()),
                        "BFD peer {} has mismatched address families",
                        peer.name
                    );
                    ensure!(
                        bfd_addresses.insert((block.switch, peer.remote)),
                        "BFD peer {} is already configured on {:?}",
                        peer.remote,
                        block.switch
                    );
                }
            }
        }
        for list in [
            &self.control_plane_router_configurations,
            &self.recovery_silo_router_configurations,
        ] {
            ensure!(
                list.len() <= 64,
                "at most 64 router assignments are allowed"
            );
            let mut priorities = BTreeSet::new();
            let mut assigned = BTreeSet::new();
            for entry in list {
                ensure!(
                    names.contains(&entry.router_configuration)
                        || builtin_id(&entry.router_configuration).is_some(),
                    "unknown router configuration {:?}",
                    entry.router_configuration
                );
                ensure!(
                    priorities.insert(entry.priority),
                    "duplicate router priority {}",
                    entry.priority
                );
                ensure!(
                    assigned.insert(&entry.router_configuration),
                    "duplicate router assignment {}",
                    entry.router_configuration
                );
            }
        }
        Ok(())
    }

    pub fn allocate(&self) -> RoutingInitialization {
        RoutingInitialization {
            config: self.clone(),
            router_ids: self
                .router_configurations
                .iter()
                .map(|rc| (rc.name.clone(), Uuid::new_v4()))
                .collect(),
        }
    }
}

// Match mgd's prefix admission checks before changing the rack.
fn validate_prefix(prefix: IpNet) -> anyhow::Result<()> {
    ensure!(prefix.is_network_address(), "prefix {prefix} has host bits set");
    let addr = prefix.addr();
    ensure!(
        !addr.is_loopback()
            && !addr.is_multicast()
            && !(addr.is_unspecified() && prefix.is_host_net())
            && !matches!(addr, IpAddr::V6(a) if a.is_unicast_link_local()),
        "prefix {prefix} is not valid for routing"
    );
    Ok(())
}

fn unique_names<'a>(
    names: impl Iterator<Item = &'a Name>,
    kind: &str,
) -> anyhow::Result<()> {
    let mut seen = BTreeSet::new();
    for name in names {
        ensure!(seen.insert(name), "duplicate {kind} name {name}");
    }
    Ok(())
}

fn builtin_id(name: &Name) -> Option<Uuid> {
    if name == default_switch0_router_configuration_name() {
        Some(DEFAULT_SWITCH0_ROUTER_CONFIGURATION_ID)
    } else if name == default_switch1_router_configuration_name() {
        Some(DEFAULT_SWITCH1_ROUTER_CONFIGURATION_ID)
    } else {
        None
    }
}

impl RoutingInitialization {
    pub fn validate(&self, legacy: &RackNetworkConfig) -> anyhow::Result<()> {
        self.config.validate(legacy)?;
        ensure!(
            self.router_ids.len() == self.config.router_configurations.len(),
            "router ID map does not match the configuration"
        );
        let mut ids = BTreeSet::new();
        for rc in &self.config.router_configurations {
            let id = self.router_ids.get(&rc.name).ok_or_else(|| {
                anyhow::anyhow!("missing router ID for {}", rc.name)
            })?;
            ensure!(
                !id.is_nil()
                    && !is_builtin_router_configuration_id(id)
                    && ids.insert(id),
                "invalid or duplicate router ID for {}",
                rc.name
            );
        }
        Ok(())
    }

    pub fn resolve(&self, name: &Name) -> Uuid {
        builtin_id(name).unwrap_or_else(|| self.router_ids[name])
    }

    pub fn control_plane_router_list(&self) -> Vec<RouterConfigListEntry> {
        let mut resolved = BTreeMap::<Option<Uuid>, u16>::new();
        for entry in &self.config.control_plane_router_configurations {
            let id = self.resolve(&entry.router_configuration);
            let router_id =
                (!is_builtin_router_configuration_id(&id)).then_some(id);
            resolved
                .entry(router_id)
                .and_modify(|p| *p = (*p).min(entry.priority))
                .or_insert(entry.priority);
        }
        let mut list: Vec<_> = resolved
            .into_iter()
            .map(|(router_id, priority)| RouterConfigListEntry {
                router_id,
                priority,
            })
            .collect();
        list.sort_by_key(|e| e.priority);
        list
    }

    /// Called only after validation; uses the same UUID for each switch block.
    pub fn switch_router_configs(&self) -> SwitchRouterConfigs {
        let mut rendered = SwitchRouterConfigs::new();
        for rc in &self.config.router_configurations {
            for block in &rc.switches {
                let bgp = block.bgp_config.as_ref().map(|bgp| {
                    let NameOrId::Name(set) = &bgp.bgp_announce_set else {
                        unreachable!("validated name reference")
                    };
                    let mut originate = self
                        .config
                        .announce_sets
                        .iter()
                        .find(|s| &s.name == set)
                        .unwrap()
                        .prefixes
                        .clone();
                    originate.sort_by_key(|p| p.to_string());
                    let mut result = RouterConfigBgpSpec {
                        asn: bgp.asn,
                        originate,
                        max_paths: Some(bgp.max_paths.as_nonzero_u8()),
                        checker: None,
                        shaper: None,
                        peers: Vec::new(),
                        unnumbered_peers: Vec::new(),
                    };
                    let mut peers: Vec<_> = block.bgp_peers.iter().collect();
                    peers.sort_by_key(|p| &p.name);
                    for peer in peers {
                        let policy = |p: &ImportExportPolicy| match p {
                            ImportExportPolicy::NoFiltering => None,
                            ImportExportPolicy::Allow(v) => Some(v.clone()),
                        };
                        let parameters = RouterConfigBgpPeerParameters {
                            hold_time: peer.hold_time.into(),
                            idle_hold_time: peer.idle_hold_time.into(),
                            delay_open: peer.delay_open.into(),
                            connect_retry: peer.connect_retry.into(),
                            keepalive: peer.keepalive.into(),
                            remote_asn: peer.remote_asn,
                            min_ttl: peer.min_ttl,
                            md5_auth_key: peer.md5_auth_key.clone(),
                            multi_exit_discriminator: peer
                                .multi_exit_discriminator,
                            communities: peer.communities.clone(),
                            local_pref: peer.local_pref,
                            enforce_first_as: peer.enforce_first_as,
                            vlan_id: peer.vlan_id,
                            allowed_import: policy(&peer.allowed_import),
                            allowed_export: policy(&peer.allowed_export),
                        };
                        match &peer.peer {
                            BgpPeerKind::Numbered { addr, src_addr } => {
                                result.peers.push(RouterConfigBgpPeer {
                                    name: peer.name.to_string(),
                                    addr: IpAddr::from(*addr),
                                    src_addr: src_addr.map(|a| IpAddr::from(a)),
                                    parameters,
                                })
                            }
                            BgpPeerKind::Unnumbered {
                                port,
                                router_lifetime,
                            } => result.unnumbered_peers.push(
                                RouterConfigUnnumberedBgpPeer {
                                    name: peer.name.to_string(),
                                    port: port.to_string(),
                                    router_lifetime: router_lifetime.as_u16(),
                                    parameters,
                                },
                            ),
                        }
                    }
                    result
                });
                let mut spec = RouterConfigSpec {
                    name: rc.name.to_string(),
                    id: self.router_ids[&rc.name],
                    bgp,
                    static4: Vec::new(),
                    static6: Vec::new(),
                    bfd_peers: Vec::new(),
                };
                let mut routes: Vec<_> = block.routes.iter().collect();
                routes.sort_by_key(|r| &r.name);
                for route in routes {
                    let rib_priority = route.rib_priority.unwrap_or(
                        mg_api_types::rdb::DEFAULT_RIB_PRIORITY_STATIC,
                    );
                    match route.dst {
                        IpNet::V4(prefix) => {
                            spec.static4.push(RouterConfigStaticRoute4 {
                                prefix,
                                nexthop: route.gw,
                                vlan_id: route.vlan_id,
                                rib_priority,
                            })
                        }
                        IpNet::V6(prefix) => {
                            let IpAddr::V6(nexthop) = route.gw else {
                                unreachable!("validated gateway")
                            };
                            spec.static6.push(RouterConfigStaticRoute6 {
                                prefix,
                                nexthop,
                                vlan_id: route.vlan_id,
                                rib_priority,
                            });
                        }
                    }
                }
                let mut peers: Vec<_> = block.bfd_peers.iter().collect();
                peers.sort_by_key(|p| &p.name);
                for peer in peers {
                    spec.bfd_peers.push(RouterConfigBfdPeer {
                        peer: peer.remote,
                        listen: peer.local.unwrap_or(
                            if peer.remote.is_ipv4() {
                                Ipv4Addr::UNSPECIFIED.into()
                            } else {
                                Ipv6Addr::UNSPECIFIED.into()
                            },
                        ),
                        required_rx: peer.required_rx.into(),
                        detection_threshold: peer.detection_threshold,
                        mode: peer.mode,
                    });
                }
                rendered.entry(block.switch).or_default().push(spec);
            }
        }
        for specs in rendered.values_mut() {
            specs.sort_by_key(|s| s.id);
        }
        rendered
    }
}
