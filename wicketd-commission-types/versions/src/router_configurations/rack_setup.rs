// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Named routing input. Authentication secrets remain outside this configuration.

use crate::v1::rack_setup::{AllowedSourceIps, BgpAuthKeyId};
use crate::v2::rack_setup::ServiceIpPoolConfig;
use crate::v5::rack_setup::UserSpecifiedRackNetworkConfig;
use iddqd::IdOrdMap;
use nexus_types_versions::router_configuration::bgp_defaults;
use omicron_common::api::external::Name;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v1::early_networking::{
    BfdMode, ImportExportPolicy, SwitchSlot,
};
use sled_agent_types_versions::v20::early_networking::{
    MaxPathConfig, RouterLifetimeConfig,
};
use sled_agent_types_versions::v30::early_networking::RouterPeerIpAddr;
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::num::NonZeroU8;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(try_from = "UnvalidatedPutRssUserConfigInsensitive")]
pub struct PutRssUserConfigInsensitive {
    /// The slot numbers of the sleds to bring up during RSS.
    ///
    /// wicketd maps these back to sleds with the correct identifiers based on
    /// the bootstrap sleds it reports.
    pub bootstrap_sleds: BTreeSet<u16>,
    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,
    /// The external DNS server addresses.
    pub dns_servers: Vec<IpAddr>,
    /// The service IP pools which may be used for internal services.
    pub service_ip_pools: IdOrdMap<ServiceIpPoolConfig>,
    /// Service IP addresses on which external DNS servers are run.
    pub external_dns_ips: Vec<IpAddr>,
    /// The DNS zone name delegated to the rack for external DNS.
    pub external_dns_zone_name: String,
    /// The user-specified rack network configuration.
    pub rack_network_config: UserSpecifiedRackNetworkConfig,
    /// Optional named routing and explicit initial assignments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<UserSpecifiedRoutingConfig>,
    /// IPs or subnets allowed to make requests to user-facing services.
    pub allowed_source_ips: AllowedSourceIps,
    /// Enable the fleet-wide jumbo-frames opt-in.
    #[serde(default)]
    pub external_jumbo_frames_opt_in_enabled: bool,
}

// Shadow of `PutRssUserConfigInsensitive` that deserializes the pools as a
// plain `Vec` (the natural TOML/JSON array shape) before the `TryFrom` folds
// them into an `IdOrdMap`, failing on duplicate pool names.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnvalidatedPutRssUserConfigInsensitive {
    bootstrap_sleds: BTreeSet<u16>,
    ntp_servers: Vec<String>,
    dns_servers: Vec<IpAddr>,
    service_ip_pools: Vec<ServiceIpPoolConfig>,
    external_dns_ips: Vec<IpAddr>,
    external_dns_zone_name: String,
    rack_network_config: UserSpecifiedRackNetworkConfig,
    #[serde(default)]
    routing: Option<UserSpecifiedRoutingConfig>,
    allowed_source_ips: AllowedSourceIps,
    #[serde(default)]
    external_jumbo_frames_opt_in_enabled: bool,
}

impl TryFrom<UnvalidatedPutRssUserConfigInsensitive>
    for PutRssUserConfigInsensitive
{
    type Error = String;

    fn try_from(
        value: UnvalidatedPutRssUserConfigInsensitive,
    ) -> Result<Self, Self::Error> {
        let service_ip_pools =
            IdOrdMap::from_iter_unique(value.service_ip_pools)
                .map_err(|e| format!("duplicate service IP pool name: {e}"))?;
        Ok(Self {
            bootstrap_sleds: value.bootstrap_sleds,
            ntp_servers: value.ntp_servers,
            dns_servers: value.dns_servers,
            service_ip_pools,
            external_dns_ips: value.external_dns_ips,
            external_dns_zone_name: value.external_dns_zone_name,
            rack_network_config: value.rack_network_config,
            routing: value.routing,
            allowed_source_ips: value.allowed_source_ips,
            external_jumbo_frames_opt_in_enabled: value
                .external_jumbo_frames_opt_in_enabled,
        })
    }
}

impl From<crate::v5::rack_setup::PutRssUserConfigInsensitive>
    for PutRssUserConfigInsensitive
{
    fn from(old: crate::v5::rack_setup::PutRssUserConfigInsensitive) -> Self {
        Self {
            bootstrap_sleds: old.bootstrap_sleds,
            ntp_servers: old.ntp_servers,
            dns_servers: old.dns_servers,
            service_ip_pools: old.service_ip_pools,
            external_dns_ips: old.external_dns_ips,
            external_dns_zone_name: old.external_dns_zone_name,
            rack_network_config: old.rack_network_config,
            routing: None,
            allowed_source_ips: old.allowed_source_ips,
            external_jumbo_frames_opt_in_enabled: old
                .external_jumbo_frames_opt_in_enabled,
        }
    }
}

/// Named routers and the two explicit initial assignment lists.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedRoutingConfig {
    /// Announce sets, keyed by name.
    #[serde(default)]
    pub announce_sets: BTreeMap<Name, UserSpecifiedRoutingAnnounceSet>,
    /// Router configurations, keyed by name.
    #[serde(default)]
    pub router_configurations:
        BTreeMap<Name, UserSpecifiedInitialRouterConfiguration>,
    pub control_plane_router_configurations:
        Vec<UserSpecifiedInitialRouterAssignment>,
    pub recovery_silo_router_configurations:
        Vec<UserSpecifiedInitialRouterAssignment>,
}

/// A set of prefixes for BGP configurations to announce
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedRoutingAnnounceSet {
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub prefixes: Vec<IpNet>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedInitialRouterConfiguration {
    #[serde(default)]
    pub description: String,
    pub switch0: Option<UserSpecifiedRouterConfigurationSwitch>,
    pub switch1: Option<UserSpecifiedRouterConfigurationSwitch>,
}

impl UserSpecifiedInitialRouterConfiguration {
    /// The switches this router configuration has settings for.
    pub fn switches(
        &self,
    ) -> impl Iterator<Item = (SwitchSlot, &UserSpecifiedRouterConfigurationSwitch)>
    {
        [
            (SwitchSlot::Switch0, &self.switch0),
            (SwitchSlot::Switch1, &self.switch1),
        ]
        .into_iter()
        .filter_map(|(switch, block)| Some((switch, block.as_ref()?)))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedInitialRouterAssignment {
    pub router_configuration: Name,
    pub priority: u16,
}

/// The settings of a router configuration on one switch
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedRouterConfigurationSwitch {
    pub bgp: Option<UserSpecifiedRouterBgp>,
    #[serde(default)]
    pub bgp_peers: Vec<UserSpecifiedRouterBgpPeer>,
    #[serde(default)]
    pub routes: Vec<UserSpecifiedRouterStaticRoute>,
    #[serde(default)]
    pub bfd_peers: Vec<UserSpecifiedRouterBfdPeer>,
}

/// The BGP configuration of a router configuration on one switch
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedRouterBgp {
    /// The autonomous system number
    pub asn: u32,

    /// Name of the announce set that holds the prefixes to announce
    pub announce_set: Name,

    /// Maximum number of equal-cost paths for BGP ECMP
    #[serde(default)]
    pub max_paths: MaxPathConfig,
}

/// A static route within a router configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedRouterStaticRoute {
    /// Name of this static route entry
    pub name: Name,

    /// The destination of the route
    pub destination: IpNet,

    /// The nexthop/gateway address
    pub nexthop: IpAddr,

    /// Route RIB priority. Higher priority indicates precedence within and
    /// across protocols.
    pub rib_priority: Option<u8>,

    /// VLAN ID for 802.1Q tagged L2 segment the gateway is reachable over
    pub vlan_id: Option<u16>,
}

/// A BFD peer within a router configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedRouterBfdPeer {
    /// Name of this BFD peer entry
    pub name: Name,

    /// Address of the remote peer to establish a BFD session with
    pub remote: IpAddr,

    /// Local address to listen on for BFD traffic. If `None` then the
    /// unspecified address (0.0.0.0 or ::) is used.
    pub local: Option<IpAddr>,

    /// Select either single-hop (RFC 5881) or multi-hop (RFC 5883)
    pub mode: BfdMode,

    /// The negotiated Control packet transmission interval, multiplied by this
    /// variable, will be the Detection Time for this session (as seen by the
    /// remote system). Must be at least 1.
    pub detection_threshold: NonZeroU8,

    /// The minimum interval, in microseconds, between received BFD
    /// Control packets that this system requires.
    pub required_rx: u32,
}

impl UserSpecifiedRoutingConfig {
    /// Key IDs referenced by named peers, grouped for the existing upload flow.
    pub fn get_bgp_auth_key_ids(&self) -> BTreeSet<BgpAuthKeyId> {
        self.router_configurations
            .values()
            .flat_map(|rc| rc.switches())
            .flat_map(|(_, block)| &block.bgp_peers)
            .filter_map(|peer| peer.auth_key_id.clone())
            .collect()
    }
}

/// A BGP peer within a router configuration
///
/// A numbered peer is given by `addr`, an unnumbered peer by `port`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedRouterBgpPeer {
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

    /// Authentication key uploaded separately through the existing key endpoint.
    pub auth_key_id: Option<BgpAuthKeyId>,

    /// Minimum IP TTL for peer messages (GTSM)
    pub min_ttl: Option<u8>,

    /// VLAN ID associated with this peer
    pub vlan_id: Option<u16>,
}
