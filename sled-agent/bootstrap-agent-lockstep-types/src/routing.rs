// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Optional named routing configuration for RSS and its Nexus handoff.
//!
//! [`RoutingConfig`] is what the rack setup file holds, written like the
//! file's `rack_network_config` section. [`RoutingConfig::resolve`] turns it
//! into the Nexus API's types; [`ResolvedRoutingConfig`] is what RSS hands to
//! Nexus.

use anyhow::{bail, ensure};
use nexus_types_versions::latest::networking::{
    BfdPeer, BgpPeerKind, RouterConfigurationBgpConfig,
    RouterConfigurationBgpPeer, RouterConfigurationSwitch, StaticRoute,
};
use nexus_types_versions::router_configuration::*;
use omicron_common::api::external::{Name, NameOrId};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::{
    ImportExportPolicy, NumberedRouter, RackNetworkConfig,
    RouterLifetimeConfig, RouterPeerIpAddr, RouterPeerType, SwitchSlot,
};
use sled_agent_types::router_config::*;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

// These secret-free leaf types have the same meaning in commissioning and RSS.
// Reuse the existing dependency rather than maintaining duplicate definitions.
use wicketd_commission_types::rack_setup::UserSpecifiedRouterBgpPeer;
pub use wicketd_commission_types::rack_setup::{
    UserSpecifiedInitialRouterAssignment as InitialRouterAssignment,
    UserSpecifiedRouterBfdPeer as InitialBfdPeer,
    UserSpecifiedRouterBgp as InitialBgp,
    UserSpecifiedRouterStaticRoute as InitialStaticRoute,
    UserSpecifiedRoutingAnnounceSet as InitialAnnounceSet,
};

/// The `routing` section of the rack setup file.
#[derive(Clone, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    /// Announce sets, keyed by name.
    #[serde(default)]
    pub announce_sets: BTreeMap<Name, InitialAnnounceSet>,
    /// Router configurations, keyed by name.
    #[serde(default)]
    pub router_configurations: BTreeMap<Name, InitialRouterConfiguration>,
    // Required, including when deliberately empty.
    pub control_plane_router_configurations: Vec<InitialRouterAssignment>,
    pub recovery_silo_router_configurations: Vec<InitialRouterAssignment>,
}

// The peers include TCP-MD5 keys. Never log their Debug output.
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
    #[serde(default)]
    pub description: String,
    pub switch0: Option<InitialRouterSwitch>,
    pub switch1: Option<InitialRouterSwitch>,
}

/// The settings of a router configuration on one switch
#[derive(Clone, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InitialRouterSwitch {
    pub bgp: Option<InitialBgp>,
    #[serde(default)]
    pub bgp_peers: Vec<InitialBgpPeer>,
    #[serde(default)]
    pub routes: Vec<InitialStaticRoute>,
    #[serde(default)]
    pub bfd_peers: Vec<InitialBfdPeer>,
}

/// A BGP peer within a router configuration
///
/// A numbered peer is given by `addr`, an unnumbered peer by `port`.
#[derive(Clone, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InitialBgpPeer {
    /// Name of this BGP peer entry
    pub name: Name,

    /// Address of the peer, for a numbered session
    pub addr: Option<RouterPeerIpAddr>,

    /// Local address to use for a numbered session. Must have the same
    /// address family as the peer. If omitted, the system selects the
    /// source address.
    pub src_addr: Option<RouterPeerIpAddr>,

    /// Name of the external port the peer is reachable on, such as `qsfp0`,
    /// for an unnumbered session
    pub port: Option<Name>,

    /// Router lifetime in seconds advertised on an unnumbered link.
    /// Defaults to 0 (disabled).
    pub router_lifetime: Option<RouterLifetimeConfig>,

    /// Require that the peer has this autonomous system number. If not set,
    /// any peer ASN is accepted.
    pub remote_asn: Option<u32>,

    /// Import prefix filtering policy
    #[serde(default)]
    pub allowed_import: ImportExportPolicy,

    /// Export prefix filtering policy
    #[serde(default)]
    pub allowed_export: ImportExportPolicy,

    /// Hold time between keepalives in seconds
    #[serde(default = "bgp_defaults::hold_time")]
    pub hold_time: u32,

    /// Keepalive request interval in seconds
    #[serde(default = "bgp_defaults::keepalive")]
    pub keepalive: u32,

    /// TCP connection retry interval in seconds
    #[serde(default = "bgp_defaults::connect_retry")]
    pub connect_retry: u32,

    /// Delay in seconds before sending open request after TCP session
    /// establishment
    #[serde(default = "bgp_defaults::delay_open")]
    pub delay_open: u32,

    /// Time to hold peer in idle state in seconds
    #[serde(default = "bgp_defaults::idle_hold_time")]
    pub idle_hold_time: u32,

    /// Local preference for received routes
    pub local_pref: Option<u32>,

    /// BGP communities to include in updates
    #[serde(default)]
    pub communities: Vec<u32>,

    /// Multi-exit discriminator for updates sent to this peer
    pub multi_exit_discriminator: Option<u32>,

    /// Enforce that the first AS in paths matches the peer's AS
    #[serde(default)]
    pub enforce_first_as: bool,

    /// TCP-MD5 authentication key
    pub md5_auth_key: Option<String>,

    /// Minimum IP TTL for peer messages (GTSM)
    pub min_ttl: Option<u8>,

    /// VLAN ID associated with this peer
    pub vlan_id: Option<u16>,
}

impl InitialBgpPeer {
    /// A commissioning peer with its separately uploaded key, if it has one.
    pub fn from_user_specified(
        peer: UserSpecifiedRouterBgpPeer,
        md5_auth_key: Option<String>,
    ) -> Self {
        Self {
            name: peer.name,
            addr: peer.addr,
            src_addr: peer.src_addr,
            port: peer.port,
            router_lifetime: peer.router_lifetime,
            remote_asn: peer.remote_asn,
            allowed_import: peer.allowed_import,
            allowed_export: peer.allowed_export,
            hold_time: peer.hold_time,
            keepalive: peer.keepalive,
            connect_retry: peer.connect_retry,
            delay_open: peer.delay_open,
            idle_hold_time: peer.idle_hold_time,
            local_pref: peer.local_pref,
            communities: peer.communities,
            multi_exit_discriminator: peer.multi_exit_discriminator,
            enforce_first_as: peer.enforce_first_as,
            md5_auth_key,
            min_ttl: peer.min_ttl,
            vlan_id: peer.vlan_id,
        }
    }

    fn kind(&self) -> anyhow::Result<BgpPeerKind> {
        match (self.addr, &self.port) {
            (Some(addr), None) => {
                ensure!(
                    self.router_lifetime.is_none(),
                    "BGP peer {} has a router lifetime, which only applies \
                     to a peer given by `port`",
                    self.name
                );
                Ok(BgpPeerKind::Numbered { addr, src_addr: self.src_addr })
            }
            (None, Some(port)) => {
                ensure!(
                    self.src_addr.is_none(),
                    "BGP peer {} has a source address, which only applies \
                     to a peer given by `addr`",
                    self.name
                );
                Ok(BgpPeerKind::Unnumbered {
                    port: port.clone(),
                    router_lifetime: self.router_lifetime.unwrap_or_default(),
                })
            }
            _ => bail!(
                "BGP peer {} needs exactly one of `addr` and `port`",
                self.name
            ),
        }
    }
}

/// A named set of prefixes
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RoutingAnnounceSet {
    pub name: Name,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub prefixes: Vec<IpNet>,
}

/// [`RoutingConfig`] with the names that Nexus needs.
#[derive(Clone, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResolvedRoutingConfig {
    #[serde(default)]
    pub announce_sets: Vec<RoutingAnnounceSet>,
    #[serde(default)]
    pub router_configurations: Vec<ResolvedRouterConfiguration>,
    pub control_plane_router_configurations: Vec<InitialRouterAssignment>,
    pub recovery_silo_router_configurations: Vec<InitialRouterAssignment>,
}

// The nested API peers include TCP-MD5 keys. Never log their Debug output.
impl std::fmt::Debug for ResolvedRoutingConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedRoutingConfig")
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
pub struct ResolvedRouterConfiguration {
    pub name: Name,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub switches: Vec<RouterConfigurationSwitch>,
}

/// IDs are allocated once by RSS and preserved across handoff retries.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RoutingInitialization {
    pub config: ResolvedRoutingConfig,
    pub router_ids: BTreeMap<Name, Uuid>,
}

impl RoutingConfig {
    pub fn validate(&self, legacy: &RackNetworkConfig) -> anyhow::Result<()> {
        self.resolve()?.validate(legacy)
    }

    /// The same configuration in the Nexus API's types.
    pub fn resolve(&self) -> anyhow::Result<ResolvedRoutingConfig> {
        let announce_sets = self
            .announce_sets
            .iter()
            .map(|(name, set)| RoutingAnnounceSet {
                name: name.clone(),
                description: set.description.clone(),
                prefixes: set.prefixes.clone(),
            })
            .collect();
        let mut router_configurations = Vec::new();
        for (name, rc) in &self.router_configurations {
            let mut switches = Vec::new();
            for (switch, block) in [
                (SwitchSlot::Switch0, &rc.switch0),
                (SwitchSlot::Switch1, &rc.switch1),
            ] {
                let Some(block) = block else { continue };
                let bgp_config = block.bgp.as_ref().map(|bgp| {
                    RouterConfigurationBgpConfig {
                        asn: bgp.asn,
                        max_paths: bgp.max_paths,
                        bgp_announce_set: NameOrId::Name(
                            bgp.announce_set.clone(),
                        ),
                    }
                });
                let mut bgp_peers = Vec::new();
                for peer in &block.bgp_peers {
                    bgp_peers.push(RouterConfigurationBgpPeer {
                        name: peer.name.clone(),
                        peer: peer.kind()?,
                        remote_asn: peer.remote_asn,
                        allowed_import: peer.allowed_import.clone(),
                        allowed_export: peer.allowed_export.clone(),
                        hold_time: peer.hold_time,
                        keepalive: peer.keepalive,
                        connect_retry: peer.connect_retry,
                        delay_open: peer.delay_open,
                        idle_hold_time: peer.idle_hold_time,
                        local_pref: peer.local_pref,
                        communities: peer.communities.clone(),
                        multi_exit_discriminator: peer.multi_exit_discriminator,
                        enforce_first_as: peer.enforce_first_as,
                        md5_auth_key: peer.md5_auth_key.clone(),
                        min_ttl: peer.min_ttl,
                        vlan_id: peer.vlan_id,
                    });
                }
                let routes = block
                    .routes
                    .iter()
                    .map(|route| StaticRoute {
                        name: route.name.clone(),
                        dst: route.destination,
                        gw: route.nexthop,
                        rib_priority: route.rib_priority,
                        vlan_id: route.vlan_id,
                    })
                    .collect();
                let bfd_peers = block
                    .bfd_peers
                    .iter()
                    .map(|peer| BfdPeer {
                        name: peer.name.clone(),
                        remote: peer.remote,
                        local: peer.local,
                        mode: peer.mode,
                        detection_threshold: peer.detection_threshold,
                        required_rx: peer.required_rx,
                    })
                    .collect();
                switches.push(RouterConfigurationSwitch {
                    switch,
                    bgp_config,
                    bgp_peers,
                    routes,
                    bfd_peers,
                });
            }
            router_configurations.push(ResolvedRouterConfiguration {
                name: name.clone(),
                description: rc.description.clone(),
                switches,
            });
        }
        Ok(ResolvedRoutingConfig {
            announce_sets,
            router_configurations,
            control_plane_router_configurations: self
                .control_plane_router_configurations
                .clone(),
            recovery_silo_router_configurations: self
                .recovery_silo_router_configurations
                .clone(),
        })
    }

    /// Called only after validation.
    pub fn allocate(&self) -> RoutingInitialization {
        let config = self.resolve().expect("validated routing configuration");
        let router_ids = config
            .router_configurations
            .iter()
            .map(|rc| (rc.name.clone(), Uuid::new_v4()))
            .collect();
        RoutingInitialization { config, router_ids }
    }
}

impl ResolvedRoutingConfig {
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
                        !(matches!(route.dst, IpNet::V6(_))
                            && route.gw.is_ipv4()),
                        "route {} has an IPv6 destination and an IPv4 gateway",
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
    (name == default_router_configuration_name())
        .then_some(DEFAULT_ROUTER_CONFIGURATION_ID)
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
        let mut list: Vec<_> = self
            .config
            .control_plane_router_configurations
            .iter()
            .map(|entry| {
                let id = self.resolve(&entry.router_configuration);
                RouterConfigListEntry {
                    router_id: (!is_builtin_router_configuration_id(&id))
                        .then_some(id),
                    priority: entry.priority,
                }
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
