// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types shared between Nexus and Sled Agent.

use crate::api::external::{self, Name};
use ipnetwork::Ipv4Network;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Display,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
};
use uuid::Uuid;

/// The type of network interface
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
    Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NetworkInterfaceKind {
    /// A vNIC attached to a guest instance
    Instance { id: Uuid },
    /// A vNIC associated with an internal service
    Service { id: Uuid },
}

/// Information required to construct a virtual network interface
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct NetworkInterface {
    pub id: Uuid,
    pub kind: NetworkInterfaceKind,
    pub name: external::Name,
    pub ip: IpAddr,
    pub mac: external::MacAddr,
    pub subnet: external::IpNet,
    pub vni: external::Vni,
    pub primary: bool,
    pub slot: u8,
}

/// An IP address and port range used for source NAT, i.e., making
/// outbound network connections from guests or services.
#[derive(
    Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct SourceNatConfig {
    /// The external address provided to the instance or service.
    pub ip: IpAddr,
    /// The first port used for source NAT, inclusive.
    pub first_port: u16,
    /// The last port used for source NAT, also inclusive.
    pub last_port: u16,
}

/// Initial network configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RackNetworkConfig {
    // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: Ipv4Addr,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: Ipv4Addr,
    /// Uplinks for connecting the rack to external networks
    pub uplinks: Vec<UplinkConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct UplinkConfig {
    /// Gateway address
    pub gateway_ip: Ipv4Addr,
    /// Switch to use for uplink
    pub switch: SwitchLocation,
    /// Switchport to use for external connectivity
    pub uplink_port: String,
    /// Speed for the Switchport
    pub uplink_port_speed: PortSpeed,
    /// Forward Error Correction setting for the uplink port
    pub uplink_port_fec: PortFec,
    /// IP Address and prefix (e.g., `192.168.0.1/16`) to apply to switchport
    /// (must be in infra_ip pool)
    pub uplink_cidr: Ipv4Network,
    /// VLAN id to use for uplink
    pub uplink_vid: Option<u16>,
}

/// Identifies switch physical location
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema, Hash, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum SwitchLocation {
    /// Switch in upper slot
    Switch0,
    /// Switch in lower slot
    Switch1,
}

impl Display for SwitchLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SwitchLocation::Switch0 => write!(f, "switch0"),
            SwitchLocation::Switch1 => write!(f, "switch1"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ParseSwitchLocationError(String);

impl FromStr for SwitchLocation {
    type Err = ParseSwitchLocationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "switch0" => Ok(Self::Switch0),
            "switch1" => Ok(Self::Switch1),
            _ => Err(ParseSwitchLocationError(format!(
                "not a valid location: {s}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExternalPortDiscovery {
    // Automatically discover ports via Dendrite
    Auto(HashMap<SwitchLocation, Ipv6Addr>),
    // Static configuration pairing switches with a collection of ports
    Static(HashMap<SwitchLocation, Vec<Name>>),
}

/// Switchport Speed options
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PortSpeed {
    #[serde(alias = "0G")]
    Speed0G,
    #[serde(alias = "1G")]
    Speed1G,
    #[serde(alias = "10G")]
    Speed10G,
    #[serde(alias = "25G")]
    Speed25G,
    #[serde(alias = "40G")]
    Speed40G,
    #[serde(alias = "50G")]
    Speed50G,
    #[serde(alias = "100G")]
    Speed100G,
    #[serde(alias = "200G")]
    Speed200G,
    #[serde(alias = "400G")]
    Speed400G,
}

/// Switchport FEC options
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PortFec {
    Firecode,
    None,
    Rs,
}
