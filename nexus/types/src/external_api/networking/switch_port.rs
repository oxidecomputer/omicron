// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::net::IpAddr;

use api_identity::ObjectIdentity;
use omicron_common::api::external::{IdentityMetadata, IpNet, ObjectIdentity};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A switch port represents a physical external port on a rack switch.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPort {
    /// The id of the switch port.
    pub id: Uuid,

    /// The rack this switch port belongs to.
    pub rack_id: Uuid,

    /// The switch location of this switch port.
    pub switch_location: String,

    /// The name of this switch port.
    // TODO: possibly re-export and use the dpd_client::types::PortId here
    // https://github.com/oxidecomputer/omicron/issues/3059
    pub port_name: String,

    /// The primary settings group of this switch port. Will be `None` until
    /// this switch port is configured.
    pub port_settings_id: Option<Uuid>,
}

/// A switch port settings identity whose id may be used to view additional
/// details.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq,
)]
pub struct SwitchPortSettings {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

/// This structure contains all port settings information in one place. It's a
/// convenience data structure for getting a complete view of a particular
/// port's settings.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortSettingsView {
    /// The primary switch port settings handle.
    pub settings: SwitchPortSettings,

    /// Switch port settings included from other switch port settings groups.
    pub groups: Vec<SwitchPortSettingsGroups>,

    /// Layer 1 physical port settings.
    pub port: SwitchPortConfig,

    /// Layer 2 link settings.
    pub links: Vec<SwitchPortLinkConfig>,

    /// Link-layer discovery protocol (LLDP) settings.
    pub link_lldp: Vec<LldpServiceConfig>,

    /// Layer 3 interface settings.
    pub interfaces: Vec<SwitchInterfaceConfig>,

    /// Vlan interface settings.
    pub vlan_interfaces: Vec<SwitchVlanInterfaceConfig>,

    /// IP route settings.
    pub routes: Vec<SwitchPortRouteConfig>,

    /// BGP peer settings.
    pub bgp_peers: Vec<SwitchPortBgpPeerConfig>,

    /// Layer 3 IP address settings.
    pub addresses: Vec<SwitchPortAddressConfig>,
}

/// This structure maps a port settings object to a port settings groups. Port
/// settings objects may inherit settings from groups. This mapping defines the
/// relationship between settings objects and the groups they reference.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortSettingsGroups {
    /// The id of a port settings object referencing a port settings group.
    pub port_settings_id: Uuid,

    /// The id of a port settings group being referenced by a port settings
    /// object.
    pub port_settings_group_id: Uuid,
}

/// A port settings group is a named object that references a port settings
/// object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortSettingsGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The port settings that comprise this group.
    pub port_settings_id: Uuid,
}

/// The link geometry associated with a switch port.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SwitchPortGeometry {
    /// The port contains a single QSFP28 link with four lanes.
    Qsfp28x1,

    /// The port contains two QSFP28 links each with two lanes.
    Qsfp28x2,

    /// The port contains four SFP28 links each with one lane.
    Sfp28x4,
}

/// A physical port configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortConfig {
    /// The id of the port settings object this configuration belongs to.
    pub port_settings_id: Uuid,

    /// The physical link geometry of the port.
    pub geometry: SwitchPortGeometry,
}

/// A link configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortLinkConfig {
    /// The port settings this link configuration belongs to.
    pub port_settings_id: Uuid,

    /// The link-layer discovery protocol service configuration id for this
    /// link.
    pub lldp_service_config_id: Uuid,

    /// The name of this link.
    pub link_name: String,

    /// The maximum transmission unit for this link.
    pub mtu: u16,
}

/// A link layer discovery protocol (LLDP) service configuration.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct LldpServiceConfig {
    /// The id of this LLDP service instance.
    pub id: Uuid,

    /// The link-layer discovery protocol configuration for this service.
    pub lldp_config_id: Option<Uuid>,

    /// Whether or not the LLDP service is enabled.
    pub enabled: bool,
}

/// A link layer discovery protocol (LLDP) base configuration.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct LldpConfig {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The LLDP chassis identifier TLV.
    pub chassis_id: String,

    /// THE LLDP system name TLV.
    pub system_name: String,

    /// THE LLDP system description TLV.
    pub system_description: String,

    /// THE LLDP management IP TLV.
    pub management_ip: IpNet,
}

/// Describes the kind of an switch interface.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SwitchInterfaceKind {
    /// Primary interfaces are associated with physical links. There is exactly
    /// one primary interface per physical link.
    Primary,

    /// VLAN interfaces allow physical interfaces to be multiplexed onto
    /// multiple logical links, each distinguished by a 12-bit 802.1Q Ethernet
    /// tag.
    Vlan,

    /// Loopback interfaces are anchors for IP addresses that are not specific
    /// to any particular port.
    Loopback,
}

/// A switch port interface configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchInterfaceConfig {
    /// The port settings object this switch interface configuration belongs to.
    pub port_settings_id: Uuid,

    /// A unique identifier for this switch interface.
    pub id: Uuid,

    /// The name of this switch interface.
    // TODO: https://github.com/oxidecomputer/omicron/issues/3050
    // Use `Name` instead of `String` for `interface_name` type
    pub interface_name: String,

    /// Whether or not IPv6 is enabled on this interface.
    pub v6_enabled: bool,

    /// The switch interface kind.
    pub kind: SwitchInterfaceKind,
}

/// A switch port VLAN interface configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchVlanInterfaceConfig {
    /// The switch interface configuration this VLAN interface configuration
    /// belongs to.
    pub interface_config_id: Uuid,

    /// The virtual network id for this interface that is used for producing and
    /// consuming 802.1Q Ethernet tags. This field has a maximum value of 4095
    /// as 802.1Q tags are twelve bits.
    pub vlan_id: u16,
}

/// A route configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortRouteConfig {
    /// The port settings object this route configuration belongs to.
    pub port_settings_id: Uuid,

    /// The interface name this route configuration is assigned to.
    // TODO: https://github.com/oxidecomputer/omicron/issues/3050
    // Use `Name` instead of `String` for `interface_name` type
    pub interface_name: String,

    /// The route's destination network.
    pub dst: IpNet,

    /// The route's gateway address.
    pub gw: IpNet,

    /// The VLAN identifier for the route. Use this if the gateway is reachable
    /// over an 802.1Q tagged L2 segment.
    pub vlan_id: Option<u16>,
}

/// A BGP peer configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortBgpPeerConfig {
    /// The port settings object this BGP configuration belongs to.
    pub port_settings_id: Uuid,

    /// The id for the set of prefixes announced in this peer configuration.
    pub bgp_announce_set_id: Uuid,

    /// The id of the global BGP configuration referenced by this peer
    /// configuration.
    pub bgp_config_id: Uuid,

    /// The interface name used to establish a peer session.
    // TODO: https://github.com/oxidecomputer/omicron/issues/3050
    // Use `Name` instead of `String` for `interface_name` type
    pub interface_name: String,

    /// The address of the peer.
    pub addr: IpAddr,
}

/// A base BGP configuration.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpConfig {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The autonomous system number of this BGP configuration.
    pub asn: u32,

    /// Optional virtual routing and forwarding identifier for this BGP
    /// configuration.
    pub vrf: Option<String>,
}

/// Represents a BGP announce set by id. The id can be used with other API calls
/// to view and manage the announce set.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpAnnounceSet {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

/// A BGP announcement tied to an address lot block.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpAnnouncement {
    /// The id of the set this announcement is a part of.
    pub announce_set_id: Uuid,

    /// The address block the IP network being announced is drawn from.
    pub address_lot_block_id: Uuid,

    /// The IP network being announced.
    pub network: IpNet,
}

/// An IP address configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortAddressConfig {
    /// The port settings object this address configuration belongs to.
    pub port_settings_id: Uuid,

    /// The id of the address lot block this address is drawn from.
    pub address_lot_block_id: Uuid,

    /// The IP address and prefix.
    pub address: IpNet,

    /// The interface name this address belongs to.
    // TODO: https://github.com/oxidecomputer/omicron/issues/3050
    // Use `Name` instead of `String` for `interface_name` type
    pub interface_name: String,
}
