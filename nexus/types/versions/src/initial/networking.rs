// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the Nexus external API.
//!
//! This includes address lot, switch port, BGP, BFD, and routing configuration
//! types.

use api_identity::ObjectIdentity;
use omicron_common::api::external;
use omicron_common::api::external::{
    AddressLotKind, IdentityMetadata, IdentityMetadataCreateParams, Name,
    NameOrId, ObjectIdentity,
};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v1::early_networking::BfdMode;
use sled_agent_types_versions::v1::early_networking::ImportExportPolicy;
use sled_agent_types_versions::v1::early_networking::SwitchSlot;
use sled_agent_types_versions::v1::early_networking::TxEqConfig;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use uuid::Uuid;

// ADDRESS LOT

/// Select an address lot by an optional name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct AddressLotSelector {
    /// Name or id of the address lot to select
    pub address_lot: NameOrId,
}

/// Parameters for creating an address lot.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AddressLotCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The kind of address lot to create.
    pub kind: AddressLotKind,
    /// The blocks to add along with the new address lot.
    pub blocks: Vec<AddressLotBlockCreate>,
}

/// Parameters for creating an address lot block. First and last addresses are
/// inclusive.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AddressLotBlockCreate {
    /// The first address in the lot (inclusive).
    pub first_address: IpAddr,
    /// The last address in the lot (inclusive).
    pub last_address: IpAddr,
}

/// A loopback address is an address that is assigned to a rack switch but is
/// not associated with any particular port.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct LoopbackAddress {
    /// The id of the loopback address.
    pub id: Uuid,

    /// The address lot block this address came from.
    pub address_lot_block_id: Uuid,

    /// The id of the rack where this loopback address is assigned.
    pub rack_id: Uuid,

    /// Switch location where this loopback address is assigned.
    pub switch_location: String,

    /// The loopback IP address and prefix length.
    pub address: oxnet::IpNet,
}

/// Parameters for creating a loopback address on a particular rack switch.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct LoopbackAddressCreate {
    /// The name or id of the address lot this loopback address will pull an
    /// address from.
    pub address_lot: NameOrId,

    /// The rack containing the switch this loopback address will be configured on.
    pub rack_id: Uuid,

    // TODO: #3604 Consider using `SwitchSlot` type instead of `Name` for `LoopbackAddressCreate.switch_location`
    /// The location of the switch within the rack this loopback address will be
    /// configured on.
    pub switch_location: Name,

    /// The address to create.
    pub address: IpAddr,

    /// The subnet mask to use for the address.
    pub mask: u8,

    /// Address is an anycast address.
    ///
    /// This allows the address to be assigned to multiple locations simultaneously.
    pub anycast: bool,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct LoopbackAddressPath {
    /// The rack to use when selecting the loopback address.
    pub rack_id: Uuid,

    /// The switch location to use when selecting the loopback address.
    pub switch_slot: Name,

    /// The IP address and subnet mask to use when selecting the loopback
    /// address.
    pub address: IpAddr,

    /// The IP address and subnet mask to use when selecting the loopback
    /// address.
    pub subnet_mask: u8,
}

// SWITCH PORT SETTINGS

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
    pub port_name: Name,

    /// The primary settings group of this switch port. Will be `None` until
    /// this switch port is configured.
    pub port_settings_id: Option<Uuid>,
}

/// A switch port settings identity whose id may be used to view additional
/// details.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq,
)]
pub struct SwitchPortSettingsIdentity {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
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

/// Parameters for creating a port settings group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwtichPortSettingsGroupCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// Switch port settings to associate with the settings group being created.
    pub settings: SwitchPortSettingsCreate,
}

/// A physical port configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortConfig {
    /// The id of the port settings object this configuration belongs to.
    pub port_settings_id: Uuid,

    /// The physical link geometry of the port.
    pub geometry: SwitchPortGeometry,
}

/// Parameters for creating switch port settings. Switch port settings are the
/// central data structure for setting up external networking. Switch port
/// settings include link, interface, route, address and dynamic network
/// protocol configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchPortSettingsCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub port_config: SwitchPortConfigCreate,

    #[serde(default)]
    pub groups: Vec<NameOrId>,

    /// Link configurations.
    pub links: Vec<LinkConfigCreate>,

    /// Interface configurations.
    #[serde(default)]
    pub interfaces: Vec<SwitchInterfaceConfigCreate>,

    /// Route configurations.
    #[serde(default)]
    pub routes: Vec<RouteConfig>,

    /// BGP peer configurations.
    #[serde(default)]
    pub bgp_peers: Vec<BgpPeerConfig>,

    /// Address configurations.
    pub addresses: Vec<AddressConfig>,
}

impl SwitchPortSettingsCreate {
    pub fn new(identity: IdentityMetadataCreateParams) -> Self {
        Self {
            identity,
            port_config: SwitchPortConfigCreate {
                geometry: SwitchPortGeometry::Qsfp28x1,
            },
            groups: Vec::new(),
            links: Vec::new(),
            interfaces: Vec::new(),
            routes: Vec::new(),
            bgp_peers: Vec::new(),
            addresses: Vec::new(),
        }
    }
}

/// Physical switch port configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SwitchPortConfigCreate {
    /// Link geometry for the switch port.
    pub geometry: SwitchPortGeometry,
}

/// The link geometry associated with a switch port.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SwitchPortGeometry {
    /// The port contains a single QSFP28 link with four lanes.
    Qsfp28x1,

    /// The port contains two QSFP28 links each with two lanes.
    Qsfp28x2,

    /// The port contains four SFP28 links each with one lane.
    Sfp28x4,
}

/// Switch link configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct LinkConfigCreate {
    /// Link name. On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    /// Maximum transmission unit for the link.
    pub mtu: u16,

    /// The link-layer discovery protocol (LLDP) configuration for the link.
    pub lldp: LldpLinkConfigCreate,

    /// The requested forward-error correction method.  If this is not
    /// specified, the standard FEC for the underlying media will be applied
    /// if it can be determined.
    pub fec: Option<LinkFec>,

    /// The speed of the link.
    pub speed: LinkSpeed,

    /// Whether or not to set autonegotiation.
    pub autoneg: bool,

    /// Optional tx_eq settings.
    pub tx_eq: Option<TxEqConfig>,
}

/// The LLDP configuration associated with a port.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct LldpLinkConfigCreate {
    /// Whether or not LLDP is enabled.
    pub enabled: bool,

    /// The LLDP link name TLV.
    pub link_name: Option<String>,

    /// The LLDP link description TLV.
    pub link_description: Option<String>,

    /// The LLDP chassis identifier TLV.
    pub chassis_id: Option<String>,

    /// The LLDP system name TLV.
    pub system_name: Option<String>,

    /// The LLDP system description TLV.
    pub system_description: Option<String>,

    /// The LLDP management IP TLV.
    pub management_ip: Option<IpAddr>,
}

/// A link layer discovery protocol (LLDP) service configuration.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct LldpLinkConfig {
    /// The id of this LLDP service instance.
    pub id: Uuid,

    /// Whether or not the LLDP service is enabled.
    pub enabled: bool,

    /// The LLDP link name TLV.
    pub link_name: Option<String>,

    /// The LLDP link description TLV.
    pub link_description: Option<String>,

    /// The LLDP chassis identifier TLV.
    pub chassis_id: Option<String>,

    /// The LLDP system name TLV.
    pub system_name: Option<String>,

    /// The LLDP system description TLV.
    pub system_description: Option<String>,

    /// The LLDP management IP TLV.
    pub management_ip: Option<IpAddr>,
}

impl PartialEq<LldpLinkConfigCreate> for LldpLinkConfig {
    fn eq(&self, other: &LldpLinkConfigCreate) -> bool {
        self.enabled == other.enabled
            && self.link_name == other.link_name
            && self.link_description == other.link_description
            && self.chassis_id == other.chassis_id
            && self.system_name == other.system_name
            && self.system_description == other.system_description
            && self.management_ip == other.management_ip
    }
}

impl PartialEq<LldpLinkConfig> for LldpLinkConfigCreate {
    fn eq(&self, other: &LldpLinkConfig) -> bool {
        self.enabled == other.enabled
            && self.link_name == other.link_name
            && self.link_description == other.link_description
            && self.chassis_id == other.chassis_id
            && self.system_name == other.system_name
            && self.system_description == other.system_description
            && self.management_ip == other.management_ip
    }
}

/// A layer-3 switch interface configuration. When IPv6 is enabled, a link local
/// address will be created for the interface.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchInterfaceConfigCreate {
    /// Link name. On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    /// Whether or not IPv6 is enabled.
    pub v6_enabled: bool,

    /// What kind of switch interface this configuration represents.
    pub kind: SwitchInterfaceKind,
}

/// Indicates the kind for a switch interface.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SwitchInterfaceKind {
    /// Primary interfaces are associated with physical links. There is exactly
    /// one primary interface per physical link.
    Primary,

    /// VLAN interfaces allow physical interfaces to be multiplexed onto
    /// multiple logical links, each distinguished by a 12-bit 802.1Q Ethernet
    /// tag.
    Vlan(SwitchVlanInterface),

    /// Loopback interfaces are anchors for IP addresses that are not specific
    /// to any particular port.
    Loopback,
}

/// Describes the kind of an switch interface.
// This type is the same as `SwitchInterfaceKind` except that the `Vlan` variant
// doesn't contain any details about the VLAN ID. This type is removed in a
// future API revision.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SwitchInterfaceKindNoVlanDetails {
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
    pub interface_name: Name,

    /// Whether or not IPv6 is enabled on this interface.
    pub v6_enabled: bool,

    /// The switch interface kind.
    pub kind: SwitchInterfaceKindNoVlanDetails,
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

/// Configuration data associated with a switch VLAN interface. The VID
/// indicates a VLAN identifier. Must be between 1 and 4096.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchVlanInterface {
    /// The virtual network id (VID) that distinguishes this interface and is
    /// used for producing and consuming 802.1Q Ethernet tags. This field has a
    /// maximum value of 4095 as 802.1Q tags are twelve bits.
    pub vid: u16,
}

/// Route configuration data associated with a switch port configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouteConfig {
    /// Link name. On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    /// The set of routes assigned to a switch port.
    pub routes: Vec<Route>,
}

/// A route to a destination network through a gateway address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Route {
    /// The route destination.
    pub dst: IpNet,

    /// The route gateway.
    pub gw: IpAddr,

    /// VLAN id the gateway is reachable over.
    pub vid: Option<u16>,

    /// Route RIB priority. Higher priority indicates precedence within and across
    /// protocols.
    pub rib_priority: Option<u8>,
}

// BGP

/// Select a BGP config by a name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpConfigSelector {
    /// A name or id to use when selecting BGP config.
    pub name_or_id: NameOrId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpPeerConfig {
    /// Link that the peer is reachable on.
    /// On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    pub peers: Vec<BgpPeer>,
}

// BGP PEER (old shape: `addr` required, no `router_lifetime`)

/// A BGP peer configuration for an interface. Includes the set of announcements
/// that will be advertised to the peer identified by `addr`. The `bgp_config`
/// parameter is a reference to global BGP parameters. The `interface_name`
/// indicates what interface the peer should be contacted on.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpPeer {
    /// The global BGP configuration used for establishing a session with this
    /// peer.
    pub bgp_config: NameOrId,

    /// The name of interface to peer on. This is relative to the port
    /// configuration this BGP peer configuration is a part of. For example this
    /// value could be phy0 to refer to a primary physical interface. Or it
    /// could be vlan47 to refer to a VLAN interface.
    pub interface_name: Name,

    /// The address of the host to peer with.
    pub addr: IpAddr,

    /// How long to hold peer connections between keepalives (seconds).
    pub hold_time: u32,

    /// How long to hold a peer in idle before attempting a new session
    /// (seconds).
    pub idle_hold_time: u32,

    /// How long to delay sending an open request after establishing a TCP
    /// session (seconds).
    pub delay_open: u32,

    /// How long to to wait between TCP connection retries (seconds).
    pub connect_retry: u32,

    /// How often to send keepalive requests (seconds).
    pub keepalive: u32,

    /// Require that a peer has a specified ASN.
    pub remote_asn: Option<u32>,

    /// Require messages from a peer have a minimum IP time to live field.
    pub min_ttl: Option<u8>,

    /// Use the given key for TCP-MD5 authentication with the peer.
    pub md5_auth_key: Option<String>,

    /// Apply the provided multi-exit discriminator (MED) updates sent to the peer.
    pub multi_exit_discriminator: Option<u32>,

    /// Include the provided communities in updates sent to the peer.
    pub communities: Vec<u32>,

    /// Apply a local preference to routes received from this peer.
    pub local_pref: Option<u32>,

    /// Enforce that the first AS in paths received from this peer is the peer's AS.
    pub enforce_first_as: bool,

    /// Define import policy for a peer.
    pub allowed_import: ImportExportPolicy,

    /// Define export policy for a peer.
    pub allowed_export: ImportExportPolicy,

    /// Associate a VLAN ID with a peer.
    pub vlan_id: Option<u16>,
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
    pub network: oxnet::IpNet,
}

/// Parameters for creating a named set of BGP announcements.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpAnnounceSetCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The announcements in this set.
    pub announcement: Vec<BgpAnnouncementCreate>,
}

/// Select a BGP announce set by a name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpAnnounceSetSelector {
    /// Name or ID of the announce set
    pub announce_set: NameOrId,
}

/// List BGP announce set with an optional name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpAnnounceListSelector {
    /// Name or ID of the announce set
    pub announce_set: Option<NameOrId>,
}

/// Selector used for querying imported BGP routes.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpRouteSelector {
    /// The ASN to filter on. Required.
    pub asn: u32,
}

/// A BGP announcement tied to a particular address lot block.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpAnnouncementCreate {
    /// Address lot this announcement is drawn from.
    pub address_lot_block: NameOrId,

    /// The network being announced.
    pub network: IpNet,
}

/// Parameters for creating a BGP configuration. This includes and autonomous
/// system number (ASN) and a virtual routing and forwarding (VRF) identifier.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpConfigCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The autonomous system number of this BGP configuration.
    pub asn: u32,

    pub bgp_announce_set_id: NameOrId,

    /// Optional virtual routing and forwarding identifier for this BGP
    /// configuration.
    pub vrf: Option<Name>,

    // Dynamic BGP policy is not yet available so we skip adding it to the API
    /// A shaper program to apply to outgoing open and update messages.
    #[serde(skip)]
    pub shaper: Option<String>,
    /// A checker program to apply to incoming open and update messages.
    #[serde(skip)]
    pub checker: Option<String>,
}

/// Select a BGP status information by BGP config id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpStatusSelector {
    /// A name or id of the BGP configuration to get status for
    pub name_or_id: NameOrId,
}

// BGP MESSAGE HISTORY

/// Opaque object representing BGP message history for a given BGP peer. The
/// contents of this object are not yet stable.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BgpMessageHistory(pub(crate) mg_admin_client::types::MessageHistory);

impl JsonSchema for BgpMessageHistory {
    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        let obj = schemars::schema::Schema::Object(
            schemars::schema::SchemaObject::default(),
        );
        generator.definitions_mut().insert(Self::schema_name(), obj.clone());
        obj
    }

    fn schema_name() -> String {
        "BgpMessageHistory".to_owned()
    }
}

/// BGP message history for a particular switch.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SwitchBgpHistory {
    /// Switch this message history is associated with.
    pub switch: SwitchSlot,

    /// Message history indexed by peer address.
    pub history: HashMap<String, BgpMessageHistory>,
}

/// BGP message history for rack switches.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AggregateBgpMessageHistory {
    /// BGP history organized by switch.
    pub(crate) switch_histories: Vec<SwitchBgpHistory>,
}

// BFD

/// Information about a bidirectional forwarding detection (BFD) session.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BfdSessionEnable {
    /// Address the Oxide switch will listen on for BFD traffic. If `None` then
    /// the unspecified address (0.0.0.0 or ::) is used.
    pub local: Option<IpAddr>,

    /// Address of the remote peer to establish a BFD session with.
    pub remote: IpAddr,

    /// The negotiated Control packet transmission interval, multiplied by this
    /// variable, will be the Detection Time for this session (as seen by the
    /// remote system)
    pub detection_threshold: u8,

    /// The minimum interval, in microseconds, between received BFD
    /// Control packets that this system requires
    pub required_rx: u64,

    /// The switch to enable this session on. Must be `switch0` or `switch1`.
    pub switch: Name,

    /// Select either single-hop (RFC 5881) or multi-hop (RFC 5883)
    pub mode: BfdMode,
}

/// Information needed to disable a BFD session
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BfdSessionDisable {
    /// Address of the remote peer to disable a BFD session for.
    pub remote: IpAddr,

    /// The switch to enable this session on. Must be `switch0` or `switch1`.
    pub switch: Name,
}

/// A set of addresses associated with a port configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AddressConfig {
    /// Link to assign the addresses to.
    /// On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    /// The set of addresses assigned to the port configuration.
    pub addresses: Vec<Address>,
}

/// An address tied to an address lot.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Address {
    /// The address lot this address is drawn from.
    pub address_lot: NameOrId,

    /// The address and prefix length of this address.
    pub address: IpNet,

    /// Optional VLAN ID for this address
    pub vlan_id: Option<u16>,
}

/// Select a port settings object by an optional name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortSettingsSelector {
    /// An optional name or id to use when selecting port settings.
    pub port_settings: Option<NameOrId>,
}

/// Select a port settings info object by name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortSettingsInfoSelector {
    /// A name or id to use when selecting switch port settings info objects.
    pub port: NameOrId,
}

/// Select a switch port by name.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortPathSelector {
    /// A name to use when selecting switch ports.
    pub port: Name,
}

/// Select switch ports by rack id and location.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortSelector {
    /// A rack id to use when selecting switch ports.
    pub rack_id: Uuid,

    /// A switch location to use when selecting switch ports.
    pub switch_location: Name,
}

/// Select switch port interfaces by id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortPageSelector {
    /// An optional switch port id to use when listing switch ports.
    pub switch_port_id: Option<Uuid>,
}

/// Parameters for applying settings to switch ports.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortApplySettings {
    /// A name or id to use when applying switch port settings.
    pub port_settings: NameOrId,
}

/// Select an LLDP endpoint by rack/switch/port
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct LldpPortPathSelector {
    /// A rack id to use when selecting switch ports.
    pub rack_id: Uuid,

    /// A switch location to use when selecting switch ports.
    pub switch_slot: Name,

    /// A name to use when selecting switch ports.
    pub port: Name,
}

// BGP STATUS

/// The current status of a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpPeerStatus {
    /// IP address of the peer.
    pub addr: IpAddr,

    /// Local autonomous system number.
    pub local_asn: u32,

    /// Remote autonomous system number.
    pub remote_asn: u32,

    /// State of the peer.
    pub state: BgpPeerState,

    /// Time of last state change.
    pub state_duration_millis: u64,

    /// Switch with the peer session.
    pub switch: SwitchSlot,
}

/// The current state of a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BgpPeerState {
    /// Initial state. Refuse all incoming BGP connections. No resources
    /// allocated to peer.
    Idle,

    /// Waiting for the TCP connection to be completed.
    Connect,

    /// Trying to acquire peer by listening for and accepting a TCP connection.
    Active,

    /// Waiting for open message from peer.
    OpenSent,

    /// Waiting for keepalive or notification from peer.
    OpenConfirm,

    /// Synchronizing with peer.
    SessionSetup,

    /// Session established. Able to exchange update, notification and keepalive
    /// messages with peers.
    Established,
}

// BGP IMPORTED ROUTES (old IPv4-only type)

/// A route imported from a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpImportedRouteIpv4 {
    /// The destination network prefix.
    pub prefix: oxnet::Ipv4Net,

    /// The nexthop the prefix is reachable through.
    pub nexthop: Ipv4Addr,

    /// BGP identifier of the originating router.
    pub id: u32,

    /// Switch the route is imported into.
    pub switch: SwitchSlot,
}

// BGP EXPORTED (old HashMap-based type)

/// BGP exported routes indexed by peer address.
#[derive(
    Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Default,
)]
pub struct BgpExported {
    /// Exported routes indexed by peer address.
    pub exports: HashMap<String, Vec<oxnet::Ipv4Net>>,
}

// BGP CONFIG (old version without max_paths)

/// A base BGP configuration.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq,
)]
pub struct BgpConfig {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The autonomous system number of this BGP configuration.
    pub asn: u32,

    /// Optional virtual routing and forwarding identifier for this BGP
    /// configuration.
    pub vrf: Option<String>,
}

// SWITCH PORT SETTINGS (old response type with required BgpPeer.addr)

/// Switch port settings (old version with required BgpPeer.addr).
// TODO: several fields below embed `external::*` types directly from
// `omicron-common`, which means their serialized shape is not truly frozen.
// Once `omicron-common-versions` exists, replace these with version-local
// copies of the types to ensure the initial version's wire format is
// immutable.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchPortSettings {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// Switch port settings included from other switch port settings groups.
    pub groups: Vec<SwitchPortSettingsGroups>,

    /// Layer 1 physical port settings.
    pub port: SwitchPortConfig,

    /// Layer 2 link settings.
    pub links: Vec<SwitchPortLinkConfig>,

    /// Layer 3 interface settings.
    pub interfaces: Vec<SwitchInterfaceConfig>,

    /// Vlan interface settings.
    pub vlan_interfaces: Vec<SwitchVlanInterfaceConfig>,

    /// IP route settings.
    pub routes: Vec<external::SwitchPortRouteConfig>,

    /// BGP peer settings.
    pub bgp_peers: Vec<BgpPeer>,

    /// Layer 3 IP address settings.
    pub addresses: Vec<external::SwitchPortAddressView>,
}

/// The speed of a link.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LinkSpeed {
    /// Zero gigabits per second.
    Speed0G,
    /// 1 gigabit per second.
    Speed1G,
    /// 10 gigabits per second.
    Speed10G,
    /// 25 gigabits per second.
    Speed25G,
    /// 40 gigabits per second.
    Speed40G,
    /// 50 gigabits per second.
    Speed50G,
    /// 100 gigabits per second.
    Speed100G,
    /// 200 gigabits per second.
    Speed200G,
    /// 400 gigabits per second.
    Speed400G,
}

/// The forward error correction mode of a link.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LinkFec {
    /// Firecode forward error correction.
    Firecode,
    /// No forward error correction.
    None,
    /// Reed-Solomon forward error correction.
    Rs,
}

/// A link configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortLinkConfig {
    /// The port settings this link configuration belongs to.
    pub port_settings_id: Uuid,

    /// The name of this link.
    pub link_name: Name,

    /// The maximum transmission unit for this link.
    pub mtu: u16,

    /// The requested forward-error correction method.  If this is not
    /// specified, the standard FEC for the underlying media will be applied
    /// if it can be determined.
    pub fec: Option<LinkFec>,

    /// The configured speed of the link.
    pub speed: LinkSpeed,

    /// Whether or not the link has autonegotiation enabled.
    pub autoneg: bool,

    /// The link-layer discovery protocol service configuration for this
    /// link.
    pub lldp_link_config: Option<LldpLinkConfig>,

    /// The tx_eq configuration for this link.
    pub tx_eq_config: Option<TxEqConfig>,
}
