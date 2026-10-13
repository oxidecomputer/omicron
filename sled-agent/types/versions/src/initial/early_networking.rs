// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.

use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
pub struct BfdPeerConfig {
    pub local: Option<IpAddr>,
    pub remote: IpAddr,
    pub detection_threshold: u8,
    pub required_rx: u64,
    pub mode: BfdMode,
    pub switch: SwitchSlot,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
pub struct RouteConfig {
    /// The destination of the route.
    pub destination: IpNet,
    /// The nexthop/gateway address.
    pub nexthop: IpAddr,
    /// The VLAN id associated with this route.
    #[serde(default)]
    pub vlan_id: Option<u16>,
    /// The RIB priority (i.e. Admin Distance) associated with this route.
    #[serde(default)]
    pub rib_priority: Option<u8>,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema, Hash,
)]
pub struct UplinkAddressConfig {
    pub address: IpNet,
    /// The VLAN id (if any) associated with this address.
    #[serde(default)]
    pub vlan_id: Option<u16>,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Serialize,
    PartialEq,
    Eq,
    Hash,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
/// To what extent should this port participate in LLDP
pub enum LldpAdminStatus {
    #[default]
    Enabled,
    Disabled,
    RxOnly,
    TxOnly,
}

/// Per-port LLDP configuration settings.  Only the "status" setting is
/// mandatory.  All other fields have natural defaults or may be inherited from
/// the switch.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub struct LldpPortConfig {
    /// To what extent should this port participate in LLDP
    pub status: LldpAdminStatus,
    /// Chassis ID to advertise.  If this is set, it will be advertised as a
    /// LocallyAssigned ID type.  If this is not set, it will be
    /// inherited from the switch-level settings.
    pub chassis_id: Option<String>,
    /// Port ID to advertise.  If this is set, it will be advertised as a
    /// LocallyAssigned ID type.  If this is not set, it will be set to
    /// the port name. e.g., qsfp0/0.
    pub port_id: Option<String>,
    /// Port description to advertise.  If this is not set, no
    /// description will be advertised.
    pub port_description: Option<String>,
    /// System name to advertise.  If this is not set, it will be
    /// inherited from the switch-level settings.
    pub system_name: Option<String>,
    /// System description to advertise.  If this is not set, it will be
    /// inherited from the switch-level settings.
    pub system_description: Option<String>,
    /// Management IP addresses to advertise.  If this is not set, it will be
    /// inherited from the switch-level settings.
    pub management_addrs: Option<Vec<IpAddr>>,
}

/// Per-port tx-eq overrides.  This can be used to fine-tune the transceiver
/// equalization settings to improve signal integrity.
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub struct TxEqConfig {
    /// Pre-cursor tap1
    pub pre1: Option<i32>,
    /// Pre-cursor tap2
    pub pre2: Option<i32>,
    /// Main tap
    pub main: Option<i32>,
    /// Post-cursor tap2
    pub post2: Option<i32>,
    /// Post-cursor tap1
    pub post1: Option<i32>,
}

/// Identifies switch physical location
#[derive(
    Clone,
    Copy,
    Deserialize,
    Serialize,
    PartialEq,
    JsonSchema,
    Hash,
    Eq,
    PartialOrd,
    Ord,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub enum SwitchSlot {
    /// Switch in upper slot
    Switch0,
    /// Switch in lower slot
    Switch1,
}

/// The speed of a link.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Serialize,
    PartialEq,
    Eq,
    JsonSchema,
    Hash,
    daft::Diffable,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub enum LinkSpeed {
    /// Zero gigabits per second.
    #[serde(alias = "0G")]
    Speed0G,
    /// 1 gigabit per second.
    #[serde(alias = "1G")]
    Speed1G,
    /// 10 gigabits per second.
    #[serde(alias = "10G")]
    Speed10G,
    /// 25 gigabits per second.
    #[serde(alias = "25G")]
    Speed25G,
    /// 40 gigabits per second.
    #[serde(alias = "40G")]
    Speed40G,
    /// 50 gigabits per second.
    #[serde(alias = "50G")]
    Speed50G,
    /// 100 gigabits per second.
    #[serde(alias = "100G")]
    Speed100G,
    /// 200 gigabits per second.
    #[serde(alias = "200G")]
    Speed200G,
    /// 400 gigabits per second.
    #[serde(alias = "400G")]
    Speed400G,
}

/// The forward error correction mode of a link.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema, Hash,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub enum LinkFec {
    /// Firecode forward error correction.
    Firecode,
    /// No forward error correction.
    None,
    /// Reed-Solomon forward error correction.
    Rs,
}

/// BFD connection mode.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
)]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum BfdMode {
    SingleHop,
    MultiHop,
}

/// Define policy relating to the import and export of prefixes from a BGP
/// peer.
#[derive(
    Default,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    JsonSchema,
    Eq,
    PartialEq,
    Hash,
)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ImportExportPolicy {
    /// Do not perform any filtering.
    #[default]
    NoFiltering,
    Allow(Vec<oxnet::IpNet>),
}
