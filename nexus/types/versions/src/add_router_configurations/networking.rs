// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `ADD_ROUTER_CONFIGURATIONS` version.
//!
//! Changes in this version:
//!
//! * New [`RouterConfiguration`] view along with [`RouterConfigurationCreate`],
//!   [`RouterConfigurationUpdate`] and [`RouterConfigurationSelector`], for the
//!   new `/v1/system/networking/router-configurations` endpoints.
//! * New [`RouterConfigurationBgpConfig`] and
//!   [`RouterConfigurationBgpConfigSet`] for the `bgp-config` sub-resource of
//!   a router configuration.
//! * New [`RouterConfigurationBgpPeer`] and [`BgpPeerKind`] for the
//!   `bgp-peers` sub-resources of a router configuration.
//! * New [`StaticRoute`] and [`BfdPeer`] for the `routes` and `bfd-peers`
//!   sub-resources of a router configuration.
//! * New [`RouterConfigurationBgpPeerSelector`],
//!   [`RouterConfigurationStaticRouteSelector`] and
//!   [`RouterConfigurationBfdPeerSelector`] for selecting the entries above.
//! * New [`SiloRouterConfigurations`], [`SiloRouterConfiguration`],
//!   [`SiloRouterConfigurationsUpdate`] and [`SiloRouterConfigurationEntry`]
//!   for the new `/v1/system/silos/{silo}/router-configurations` endpoints.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams,
    IdentityMetadataUpdateParams, Name, NameOrId, ObjectIdentity,
};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v1::early_networking::BfdMode;
use sled_agent_types_versions::v1::early_networking::ImportExportPolicy;
use sled_agent_types_versions::v1::early_networking::SwitchSlot;
use sled_agent_types_versions::v20::early_networking::MaxPathConfig;
use sled_agent_types_versions::v20::early_networking::RouterLifetimeConfig;
use std::net::IpAddr;
use uuid::Uuid;

/// A named collection of rack routing configuration
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq,
)]
pub struct RouterConfiguration {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The BGP configuration for this router configuration, if set
    pub bgp_config: Option<RouterConfigurationBgpConfig>,

    /// BGP peers in this router configuration
    pub bgp_peers: Vec<RouterConfigurationBgpPeer>,

    /// Static routes in this router configuration
    pub routes: Vec<StaticRoute>,

    /// BFD peers in this router configuration
    pub bfd_peers: Vec<BfdPeer>,
}

/// Parameters for creating a router configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterConfigurationCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Parameters for updating a router configuration
///
/// If a value is not specified, it will remain unchanged.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterConfigurationUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/// Select a router configuration by a name or id
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct RouterConfigurationSelector {
    /// A name or id to use when selecting a router configuration.
    pub configuration: NameOrId,
}

/// The BGP configuration associated with a router configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct RouterConfigurationBgpConfig {
    /// The autonomous system number
    pub asn: u32,

    /// Maximum number of equal-cost paths for BGP ECMP
    pub max_paths: MaxPathConfig,

    /// BGP announce set that defines the prefixes originated by this router
    /// configuration
    pub bgp_announce_set: NameOrId,
}

/// Parameters for setting the BGP configuration of a router configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterConfigurationBgpConfigSet {
    /// The autonomous system number
    pub asn: u32,

    /// Maximum number of equal-cost paths for BGP ECMP. Defaults to 1.
    #[serde(default)]
    pub max_paths: MaxPathConfig,

    /// BGP announce set that defines the prefixes originated by this router
    /// configuration
    pub bgp_announce_set: NameOrId,
}

/// The peer to establish a BGP session with
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BgpPeerKind {
    /// A session with a specific peer address on a given port
    Numbered {
        /// Address of the peer.
        addr: IpAddr,

        /// Name of the external port the peer is reachable on, such as
        /// `qsfp0`.
        port: Name,
    },
    /// An unnumbered session on a given port
    Unnumbered {
        /// Name of the external port the peer is reachable on, such as
        /// `qsfp0`.
        port: Name,
    },
}

/// A BGP peer within a router configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct RouterConfigurationBgpPeer {
    /// Name of this BGP peer entry
    pub name: Name,

    /// The peer to establish a BGP session with
    pub peer: BgpPeerKind,

    /// The remote autonomous system number of the peer
    pub remote_asn: u32,

    /// Import prefix filtering policy
    #[serde(default)]
    pub allowed_import: ImportExportPolicy,

    /// Export prefix filtering policy
    #[serde(default)]
    pub allowed_export: ImportExportPolicy,

    /// Hold time between keepalives in seconds
    pub hold_time: u32,

    /// Keepalive request interval in seconds
    pub keepalive: u32,

    /// TCP connection retry interval in seconds
    pub connect_retry: u32,

    /// Delay in seconds before sending open request after TCP session
    /// establishment
    pub delay_open: u32,

    /// Time to hold peer in idle state in seconds
    pub idle_hold_time: u32,

    /// Local preference for received routes
    pub local_pref: Option<u32>,

    /// BGP communities to include in updates
    #[serde(default)]
    pub communities: Vec<u32>,

    /// Multi-exit discriminator for updates sent to this peer
    pub multi_exit_discriminator: Option<u32>,

    /// Enforce that the first AS in paths matches the peer's AS
    pub enforce_first_as: bool,

    /// TCP-MD5 authentication key
    pub md5_auth_key: Option<String>,

    /// Minimum IP TTL for peer messages (GTSM)
    pub min_ttl: Option<u8>,

    /// VLAN ID associated with this peer
    pub vlan_id: Option<u16>,

    /// Router lifetime in seconds for unnumbered BGP peers. Defaults to 0
    /// (disabled).
    #[serde(default)]
    pub router_lifetime: RouterLifetimeConfig,
}

/// Select a BGP peer within a router configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct RouterConfigurationBgpPeerSelector {
    /// A name or id to use when selecting a router configuration.
    pub configuration: NameOrId,

    /// Name of the BGP peer entry.
    pub peer: Name,
}

/// A static route to a destination network through a gateway address
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct StaticRoute {
    /// Name of this static route entry
    pub name: Name,

    /// Route destination network
    pub dst: IpNet,

    /// Gateway address
    pub gw: IpAddr,

    /// Route RIB priority. Higher priority indicates precedence within and
    /// across protocols.
    pub rib_priority: Option<u8>,

    /// VLAN ID for 802.1Q tagged L2 segment the gateway is reachable over
    pub vlan_id: Option<u16>,
}

/// Select a static route within a router configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct RouterConfigurationStaticRouteSelector {
    /// A name or id to use when selecting a router configuration.
    pub configuration: NameOrId,

    /// Name of the static route entry.
    pub route: Name,
}

/// A BFD peer within a router configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BfdPeer {
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
    /// remote system)
    pub detection_threshold: u8,

    /// The minimum interval, in microseconds, between received BFD
    /// Control packets that this system requires
    pub required_rx: u64,

    /// The switch hosting this BFD session
    pub switch: SwitchSlot,
}

/// Select a BFD peer within a router configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct RouterConfigurationBfdPeerSelector {
    /// A name or id to use when selecting a router configuration.
    pub configuration: NameOrId,

    /// Name of the BFD peer entry.
    pub peer: Name,
}

/// A router configuration used by a silo, along with its priority within
/// that silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SiloRouterConfiguration {
    /// ID of the router configuration
    pub router_configuration_id: Uuid,

    /// Priority of the router configuration for a silo. Priorities are
    /// unique within the silo.
    pub priority: u16,
}

/// The set of router configurations used by a silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SiloRouterConfigurations {
    /// The router configurations used by the silo, in ascending priority
    /// order
    pub configurations: Vec<SiloRouterConfiguration>,
}

/// Assignment of a router configuration to a silo at a given priority
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SiloRouterConfigurationEntry {
    /// A name or id of the router configuration to assign
    pub router_configuration: NameOrId,

    /// Priority of the router configuration for a silo. Priorities must
    /// be unique within the silo.
    pub priority: u16,
}

/// Full replacement of the set of router configurations used by a silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SiloRouterConfigurationsUpdate {
    /// The complete new set of router configurations for the silo. Any
    /// currently assigned configuration not present here is unassigned.
    pub configurations: Vec<SiloRouterConfigurationEntry>,
}
