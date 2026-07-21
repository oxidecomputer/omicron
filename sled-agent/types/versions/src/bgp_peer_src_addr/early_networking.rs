// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! Changes in this version:
//!
//! * [`BgpPeerConfig`] gains a new [`BgpPeerConfig::src_addr`] field: an
//!   optional source address specifying which local IP address to bind when
//!   establishing outbound TCP connections to a BGP peer.

use crate::v1::early_networking as v1;
use crate::v20::early_networking as v20;
use crate::v30::early_networking as v30;
use crate::v42::early_networking as v42;
use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// A BGP peer configuration for a port.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
pub struct BgpPeerConfig {
    /// The autonomous system number of the router the peer belongs to.
    pub asn: u32,
    /// Switch port the peer is reachable on.
    pub port: String,
    /// Address of the peer.
    pub addr: v30::RouterPeerType,
    /// How long to keep a session alive without a keepalive in seconds.
    /// Defaults to 6.
    pub hold_time: Option<u64>,
    /// How long to keep a peer in idle after a state machine reset in seconds.
    pub idle_hold_time: Option<u64>,
    /// How long to delay sending open messages to a peer. In seconds.
    pub delay_open: Option<u64>,
    /// The interval in seconds between peer connection retry attempts.
    pub connect_retry: Option<u64>,
    /// The interval to send keepalive messages at.
    pub keepalive: Option<u64>,
    /// Require that a peer has a specified ASN.
    #[serde(default)]
    pub remote_asn: Option<u32>,
    /// Require messages from a peer have a minimum IP time to live field.
    #[serde(default)]
    pub min_ttl: Option<u8>,
    /// Use the given key for TCP-MD5 authentication with the peer.
    #[serde(default)]
    pub md5_auth_key: Option<String>,
    /// Apply the provided multi-exit discriminator (MED) updates sent to the peer.
    #[serde(default)]
    pub multi_exit_discriminator: Option<u32>,
    /// Include the provided communities in updates sent to the peer.
    #[serde(default)]
    pub communities: Vec<u32>,
    /// Apply a local preference to routes received from this peer.
    #[serde(default)]
    pub local_pref: Option<u32>,
    /// Enforce that the first AS in paths received from this peer is the peer's AS.
    #[serde(default)]
    pub enforce_first_as: bool,
    /// Define import policy for a peer.
    #[serde(default)]
    pub allowed_import: v1::ImportExportPolicy,
    /// Define export policy for a peer.
    #[serde(default)]
    pub allowed_export: v1::ImportExportPolicy,
    /// Associate a VLAN ID with a BGP peer session.
    #[serde(default)]
    pub vlan_id: Option<u16>,
    /// The local IP address to use as the source when establishing outbound
    /// TCP connections to this BGP peer. If `None`, the OS selects the source
    /// address.
    #[serde(default)]
    pub src_addr: Option<IpAddr>,
}

impl From<v30::BgpPeerConfig> for BgpPeerConfig {
    fn from(value: v30::BgpPeerConfig) -> Self {
        Self {
            asn: value.asn,
            port: value.port,
            addr: value.addr,
            hold_time: value.hold_time,
            idle_hold_time: value.idle_hold_time,
            delay_open: value.delay_open,
            connect_retry: value.connect_retry,
            keepalive: value.keepalive,
            remote_asn: value.remote_asn,
            min_ttl: value.min_ttl,
            md5_auth_key: value.md5_auth_key,
            multi_exit_discriminator: value.multi_exit_discriminator,
            communities: value.communities,
            local_pref: value.local_pref,
            enforce_first_as: value.enforce_first_as,
            allowed_import: value.allowed_import,
            allowed_export: value.allowed_export,
            vlan_id: value.vlan_id,
            src_addr: None,
        }
    }
}

impl From<BgpPeerConfig> for v30::BgpPeerConfig {
    fn from(value: BgpPeerConfig) -> Self {
        Self {
            asn: value.asn,
            port: value.port,
            addr: value.addr,
            hold_time: value.hold_time,
            idle_hold_time: value.idle_hold_time,
            delay_open: value.delay_open,
            connect_retry: value.connect_retry,
            keepalive: value.keepalive,
            remote_asn: value.remote_asn,
            min_ttl: value.min_ttl,
            md5_auth_key: value.md5_auth_key,
            multi_exit_discriminator: value.multi_exit_discriminator,
            communities: value.communities,
            local_pref: value.local_pref,
            enforce_first_as: value.enforce_first_as,
            allowed_import: value.allowed_import,
            allowed_export: value.allowed_export,
            vlan_id: value.vlan_id,
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
pub struct PortConfig {
    /// The set of routes associated with this port.
    pub routes: Vec<v1::RouteConfig>,
    /// This port's addresses and optional vlan IDs
    pub addresses: Vec<v30::UplinkAddressConfig>,
    /// Switch the port belongs to.
    pub switch: v1::SwitchSlot,
    /// Name of the port this config applies to.
    pub port: String,
    /// Port speed.
    pub uplink_port_speed: v1::LinkSpeed,
    /// Port forward error correction type.
    pub uplink_port_fec: Option<v1::LinkFec>,
    /// BGP peers on this port
    pub bgp_peers: Vec<BgpPeerConfig>,
    /// Whether or not to set autonegotiation
    #[serde(default)]
    pub autoneg: bool,
    /// LLDP configuration for this port
    pub lldp: Option<v1::LldpPortConfig>,
    /// TX-EQ configuration for this port
    pub tx_eq: Option<v1::TxEqConfig>,
}

impl PortConfig {
    /// Create a placeholder `PortConfig` for use in tests.
    pub fn empty_for_tests(port: &str) -> Self {
        Self {
            routes: Vec::new(),
            addresses: Vec::new(),
            switch: v1::SwitchSlot::Switch0,
            port: port.to_string(),
            uplink_port_speed: v1::LinkSpeed::Speed100G,
            uplink_port_fec: None,
            bgp_peers: Vec::new(),
            autoneg: false,
            lldp: None,
            tx_eq: None,
        }
    }
}

impl From<v30::PortConfig> for PortConfig {
    fn from(value: v30::PortConfig) -> Self {
        Self {
            routes: value.routes,
            addresses: value.addresses,
            switch: value.switch,
            port: value.port,
            uplink_port_speed: value.uplink_port_speed,
            uplink_port_fec: value.uplink_port_fec,
            bgp_peers: value.bgp_peers.into_iter().map(From::from).collect(),
            autoneg: value.autoneg,
            lldp: value.lldp,
            tx_eq: value.tx_eq,
        }
    }
}

impl From<PortConfig> for v30::PortConfig {
    fn from(value: PortConfig) -> Self {
        Self {
            routes: value.routes,
            addresses: value.addresses,
            switch: value.switch,
            port: value.port,
            uplink_port_speed: value.uplink_port_speed,
            uplink_port_fec: value.uplink_port_fec,
            bgp_peers: value.bgp_peers.into_iter().map(From::from).collect(),
            autoneg: value.autoneg,
            lldp: value.lldp,
            tx_eq: value.tx_eq,
        }
    }
}

/// Error returned when constructing a [`UplinkPorts`] from an empty list.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "a rack network config must contain at least one uplink port, \
     but the port list was empty"
)]
pub struct EmptyUplinkPortsError;

/// A non-empty list of uplink [`PortConfig`]s.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct UplinkPorts(Vec<PortConfig>);

impl UplinkPorts {
    /// Constructs an `UplinkPorts` from a list of ports, returning an error if
    /// the list is empty.
    pub fn new(ports: Vec<PortConfig>) -> Result<Self, EmptyUplinkPortsError> {
        if ports.is_empty() {
            return Err(EmptyUplinkPortsError);
        }
        Ok(Self(ports))
    }

    /// Returns the first port.
    pub fn first(&self) -> &PortConfig {
        &self.0[0]
    }

    /// Returns the number of ports, which is always at least one.
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns an iterator over the ports.
    pub fn iter(&self) -> std::slice::Iter<'_, PortConfig> {
        self.0.iter()
    }

    /// Returns the ports as a (non-empty) slice.
    pub fn as_slice(&self) -> &[PortConfig] {
        &self.0
    }

    /// Consumes `self`, returning the inner (non-empty) list of ports.
    pub fn into_vec(self) -> Vec<PortConfig> {
        self.0
    }
}

impl IntoIterator for UplinkPorts {
    type Item = PortConfig;
    type IntoIter = std::vec::IntoIter<PortConfig>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a UplinkPorts {
    type Item = &'a PortConfig;
    type IntoIter = std::slice::Iter<'a, PortConfig>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'de> Deserialize<'de> for UplinkPorts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ports = Vec::<PortConfig>::deserialize(deserializer)?;
        UplinkPorts::new(ports).map_err(|EmptyUplinkPortsError| {
            serde::de::Error::invalid_length(0, &"at least one uplink port")
        })
    }
}

impl JsonSchema for UplinkPorts {
    fn schema_name() -> String {
        "UplinkPorts".to_string()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Array.into()),
            array: Some(Box::new(schemars::schema::ArrayValidation {
                items: Some(generator.subschema_for::<PortConfig>().into()),
                min_items: Some(1),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

/// Initial network configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct RackNetworkConfig {
    pub rack_subnet: Ipv6Net,
    // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: IpAddr,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: IpAddr,
    /// Uplinks for connecting the rack to external networks
    pub ports: UplinkPorts,
    /// BGP configurations for connecting the rack to external networks
    pub bgp: Vec<v20::BgpConfig>,
    /// BFD configuration for connecting the rack to external networks
    #[serde(default)]
    pub bfd: Vec<v1::BfdPeerConfig>,
}

impl TryFrom<v42::RackNetworkConfig> for RackNetworkConfig {
    type Error = EmptyUplinkPortsError;

    fn try_from(old: v42::RackNetworkConfig) -> Result<Self, Self::Error> {
        let ports = UplinkPorts::new(
            old.ports.into_vec().into_iter().map(From::from).collect(),
        )?;
        Ok(Self {
            rack_subnet: old.rack_subnet,
            infra_ip_first: old.infra_ip_first,
            infra_ip_last: old.infra_ip_last,
            ports,
            bgp: old.bgp,
            bfd: old.bfd,
        })
    }
}

impl From<RackNetworkConfig> for v42::RackNetworkConfig {
    fn from(new: RackNetworkConfig) -> Self {
        Self {
            rack_subnet: new.rack_subnet,
            infra_ip_first: new.infra_ip_first,
            infra_ip_last: new.infra_ip_last,
            ports: v42::UplinkPorts::new(
                new.ports.into_vec().into_iter().map(From::from).collect(),
            )
            // Safety: we had at least one port coming in, so we have at
            // least one going out.
            .expect("non-empty UplinkPorts downgrade produced empty ports"),
            bgp: new.bgp,
            bfd: new.bfd,
        }
    }
}
