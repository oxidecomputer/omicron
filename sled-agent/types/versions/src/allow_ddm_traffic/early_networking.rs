// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! Changes in this version:
//!
//! * [`PortConfig`] gains an `allow_ddm_traffic` field, defaulting to `false`.
//! * Update types that transitively contain the newly-updated [`PortConfig`]:
//!     * [`UplinkPorts`]
//!     * [`RackNetworkConfig`]

use crate::v1::early_networking as v1;
use crate::v20::early_networking as v20;
use crate::v30::early_networking as v30;
use crate::v47::early_networking as v47;
use crate::v47::early_networking::EmptyUplinkPortsError;
use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

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
    pub bgp_peers: Vec<v47::BgpPeerConfig>,
    /// Whether or not to set autonegotiation
    #[serde(default)]
    pub autoneg: bool,
    /// LLDP configuration for this port
    pub lldp: Option<v1::LldpPortConfig>,
    /// TX-EQ configuration for this port
    pub tx_eq: Option<v1::TxEqConfig>,
    /// Whether or not to allow DDM traffic on this port
    #[serde(default)]
    pub allow_ddm_traffic: bool,
}

impl From<v47::PortConfig> for PortConfig {
    fn from(old: v47::PortConfig) -> Self {
        Self {
            routes: old.routes,
            addresses: old.addresses,
            switch: old.switch,
            port: old.port,
            uplink_port_speed: old.uplink_port_speed,
            uplink_port_fec: old.uplink_port_fec,
            bgp_peers: old.bgp_peers,
            autoneg: old.autoneg,
            lldp: old.lldp,
            tx_eq: old.tx_eq,
            allow_ddm_traffic: false,
        }
    }
}

impl From<PortConfig> for v47::PortConfig {
    fn from(new: PortConfig) -> Self {
        Self {
            routes: new.routes,
            addresses: new.addresses,
            switch: new.switch,
            port: new.port,
            uplink_port_speed: new.uplink_port_speed,
            uplink_port_fec: new.uplink_port_fec,
            bgp_peers: new.bgp_peers,
            autoneg: new.autoneg,
            lldp: new.lldp,
            tx_eq: new.tx_eq,
        }
    }
}

/// A non-empty list of uplink [`PortConfig`]s.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct UplinkPorts(pub(crate) Vec<PortConfig>);

impl UplinkPorts {
    /// Constructs an `UplinkPorts` from a list of ports, returning an error if
    /// the list is empty.
    pub fn new(ports: Vec<PortConfig>) -> Result<Self, EmptyUplinkPortsError> {
        if ports.is_empty() {
            return Err(EmptyUplinkPortsError);
        }
        Ok(Self(ports))
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

impl From<v47::UplinkPorts> for UplinkPorts {
    fn from(old: v47::UplinkPorts) -> Self {
        Self(old.0.into_iter().map(PortConfig::from).collect())
    }
}

impl From<UplinkPorts> for v47::UplinkPorts {
    fn from(new: UplinkPorts) -> Self {
        Self(new.0.into_iter().map(v47::PortConfig::from).collect())
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

impl From<v47::RackNetworkConfig> for RackNetworkConfig {
    fn from(old: v47::RackNetworkConfig) -> Self {
        Self {
            rack_subnet: old.rack_subnet,
            infra_ip_first: old.infra_ip_first,
            infra_ip_last: old.infra_ip_last,
            ports: old.ports.into(),
            bgp: old.bgp,
            bfd: old.bfd,
        }
    }
}

impl From<RackNetworkConfig> for v47::RackNetworkConfig {
    fn from(new: RackNetworkConfig) -> Self {
        Self {
            rack_subnet: new.rack_subnet,
            infra_ip_first: new.infra_ip_first,
            infra_ip_last: new.infra_ip_last,
            ports: new.ports.into(),
            bgp: new.bgp,
            bfd: new.bfd,
        }
    }
}
