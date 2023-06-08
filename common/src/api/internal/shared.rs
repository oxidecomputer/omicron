// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types shared between Nexus and Sled Agent.

use crate::api::external;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
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
    /// Gateway address
    pub gateway_ip: String,
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: String,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: String,
    /// Switchport to use for external connectivity
    pub uplink_port: String,
    /// Speed for the Switchport
    pub uplink_port_speed: PortSpeed,
    /// Forward Error Correction setting for the uplink port
    pub uplink_port_fec: PortFec,
    /// IP Address to apply to switchport (must be in infra_ip pool)
    pub uplink_ip: String,
}

/// Switchport Speed options
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub enum PortSpeed {
    #[serde(rename = "0G")]
    Speed0G,
    #[serde(rename = "1G")]
    Speed1G,
    #[serde(rename = "10G")]
    Speed10G,
    #[serde(rename = "25G")]
    Speed25G,
    #[serde(rename = "40G")]
    Speed40G,
    #[serde(rename = "50G")]
    Speed50G,
    #[serde(rename = "100G")]
    Speed100G,
    #[serde(rename = "200G")]
    Speed200G,
    #[serde(rename = "400G")]
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
