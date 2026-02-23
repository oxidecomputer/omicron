// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the Nexus external API.
//!
//! This includes address lot, switch port, BGP, BFD, and routing configuration
//! types.

use super::bfd::ExternalBfdMode;
use api_identity::ObjectIdentity;
use omicron_common::api::external;
use omicron_common::api::external::{
    AddressLotKind, IdentityMetadata, IdentityMetadataCreateParams,
    ImportExportPolicy, LinkFec, LinkSpeed, Name, NameOrId, ObjectIdentity,
    SwitchLocation,
};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
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

/// Parameters for creating a loopback address on a particular rack switch.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct LoopbackAddressCreate {
    /// The name or id of the address lot this loopback address will pull an
    /// address from.
    pub address_lot: NameOrId,

    /// The rack containing the switch this loopback address will be configured on.
    pub rack_id: Uuid,

    // TODO: #3604 Consider using `SwitchLocation` type instead of `Name` for `LoopbackAddressCreate.switch_location`
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
    pub switch_location: Name,

    /// The IP address and subnet mask to use when selecting the loopback
    /// address.
    pub address: IpAddr,

    /// The IP address and subnet mask to use when selecting the loopback
    /// address.
    pub subnet_mask: u8,
}

// SWITCH PORT SETTINGS

/// Parameters for creating a port settings group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwtichPortSettingsGroupCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// Switch port settings to associate with the settings group being created.
    pub settings: SwitchPortSettingsCreate,
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
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
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

/// Per-port tx-eq overrides.  This can be used to fine-tune the transceiver
/// equalization settings to improve signal integrity.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
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

impl From<omicron_common::api::internal::shared::TxEqConfig> for TxEqConfig {
    fn from(
        x: omicron_common::api::internal::shared::TxEqConfig,
    ) -> TxEqConfig {
        TxEqConfig {
            pre1: x.pre1,
            pre2: x.pre2,
            main: x.main,
            post2: x.post2,
            post1: x.post1,
        }
    }
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

impl PartialEq<LldpLinkConfigCreate>
    for omicron_common::api::external::LldpLinkConfig
{
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

impl PartialEq<omicron_common::api::external::LldpLinkConfig>
    for LldpLinkConfigCreate
{
    fn eq(
        &self,
        other: &omicron_common::api::external::LldpLinkConfig,
    ) -> bool {
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
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
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

/// Configuration data associated with a switch VLAN interface. The VID
/// indicates a VLAN identifier. Must be between 1 and 4096.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
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

// TODO: per RFD 619, these conversion impls between initial types and
// `omicron_common::api::external` types should live in the later version
// module that introduced the shape change (e.g. `bgp_unnumbered_peers`).
// They currently live here because `omicron-common-versions` does not yet
// exist; once it does, move these conversions out of the initial module.
impl From<BgpPeer> for external::BgpPeer {
    fn from(old: BgpPeer) -> external::BgpPeer {
        external::BgpPeer {
            bgp_config: old.bgp_config,
            interface_name: old.interface_name,
            addr: Some(old.addr),
            hold_time: old.hold_time,
            idle_hold_time: old.idle_hold_time,
            delay_open: old.delay_open,
            connect_retry: old.connect_retry,
            keepalive: old.keepalive,
            remote_asn: old.remote_asn,
            min_ttl: old.min_ttl,
            md5_auth_key: old.md5_auth_key,
            multi_exit_discriminator: old.multi_exit_discriminator,
            communities: old.communities,
            local_pref: old.local_pref,
            enforce_first_as: old.enforce_first_as,
            allowed_import: old.allowed_import,
            allowed_export: old.allowed_export,
            vlan_id: old.vlan_id,
            router_lifetime: 0,
        }
    }
}

impl TryFrom<external::BgpPeer> for BgpPeer {
    type Error = external::Error;

    fn try_from(new: external::BgpPeer) -> Result<Self, Self::Error> {
        let addr = new.addr.ok_or_else(|| {
            external::Error::invalid_request(
                "BGP peer has no address configured, but the API version \
                 in use requires an address. Update your client to use \
                 BGP unnumbered peers.",
            )
        })?;
        Ok(BgpPeer {
            bgp_config: new.bgp_config,
            interface_name: new.interface_name,
            addr,
            hold_time: new.hold_time,
            idle_hold_time: new.idle_hold_time,
            delay_open: new.delay_open,
            connect_retry: new.connect_retry,
            keepalive: new.keepalive,
            remote_asn: new.remote_asn,
            min_ttl: new.min_ttl,
            md5_auth_key: new.md5_auth_key,
            multi_exit_discriminator: new.multi_exit_discriminator,
            communities: new.communities,
            local_pref: new.local_pref,
            enforce_first_as: new.enforce_first_as,
            allowed_import: new.allowed_import,
            allowed_export: new.allowed_export,
            vlan_id: new.vlan_id,
        })
    }
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
    pub mode: ExternalBfdMode,
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
    pub switch_location: Name,

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
    pub switch: external::SwitchLocation,
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
    pub switch: SwitchLocation,
}

// TODO: these conversion impls between initial types and
// `omicron_common::api::external` types should live in the later version
// module that introduced the shape change. They currently live here because
// `omicron-common-versions` does not yet exist.
impl TryFrom<external::BgpImported> for BgpImportedRouteIpv4 {
    type Error = String;

    fn try_from(value: external::BgpImported) -> Result<Self, Self::Error> {
        let external::BgpImported { prefix, nexthop, id, switch } = value;

        let prefix = match prefix {
            oxnet::IpNet::V4(ipv4_net) => Ok(ipv4_net),
            oxnet::IpNet::V6(ipv6_net) => {
                Err(format!("prefix must be Ipv4Net but it is {ipv6_net}"))
            }
        }?;

        let nexthop = match nexthop {
            IpAddr::V4(ipv4_addr) => Ok(ipv4_addr),
            IpAddr::V6(ipv6_addr) => {
                Err(format!("nexthop must be Ipv4Addr but it is {ipv6_addr}"))
            }
        }?;

        Ok(Self { prefix, nexthop, id, switch })
    }
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

// TODO: see above comment on `TryFrom<external::BgpImported>`.
impl From<Vec<external::BgpExported>> for BgpExported {
    fn from(values: Vec<external::BgpExported>) -> Self {
        let mut out = Self::default();

        for export in values {
            let oxnet::IpNet::V4(net) = export.prefix else {
                continue;
            };
            match out.exports.entry(export.peer_id) {
                Entry::Occupied(mut occupied_entry) => {
                    occupied_entry.get_mut().push(net);
                }
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(vec![net]);
                }
            }
        }

        out
    }
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

// TODO: these conversion impls between initial types and
// `omicron_common::api::external` types should live in the later version
// module that introduced the shape change. They currently live here because
// `omicron-common-versions` does not yet exist.
impl From<external::BgpConfig> for BgpConfig {
    fn from(new: external::BgpConfig) -> Self {
        BgpConfig { identity: new.identity, asn: new.asn, vrf: new.vrf }
    }
}

impl From<BgpConfig> for external::BgpConfig {
    fn from(old: BgpConfig) -> external::BgpConfig {
        external::BgpConfig {
            identity: old.identity,
            asn: old.asn,
            vrf: old.vrf,
            max_paths: Default::default(),
        }
    }
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
    pub groups: Vec<external::SwitchPortSettingsGroups>,

    /// Layer 1 physical port settings.
    pub port: external::SwitchPortConfig,

    /// Layer 2 link settings.
    pub links: Vec<external::SwitchPortLinkConfig>,

    /// Layer 3 interface settings.
    pub interfaces: Vec<external::SwitchInterfaceConfig>,

    /// Vlan interface settings.
    pub vlan_interfaces: Vec<external::SwitchVlanInterfaceConfig>,

    /// IP route settings.
    pub routes: Vec<external::SwitchPortRouteConfig>,

    /// BGP peer settings.
    pub bgp_peers: Vec<BgpPeer>,

    /// Layer 3 IP address settings.
    pub addresses: Vec<external::SwitchPortAddressView>,
}

// TODO: this conversion impl should move out of the initial module once
// `omicron-common-versions` exists. See comment on `BgpPeer` above.
impl TryFrom<external::SwitchPortSettings> for SwitchPortSettings {
    type Error = external::Error;

    fn try_from(
        new: external::SwitchPortSettings,
    ) -> Result<Self, Self::Error> {
        Ok(SwitchPortSettings {
            identity: new.identity,
            groups: new.groups,
            port: new.port,
            links: new.links,
            interfaces: new.interfaces,
            vlan_interfaces: new.vlan_interfaces,
            routes: new.routes,
            bgp_peers: new
                .bgp_peers
                .into_iter()
                .map(BgpPeer::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            addresses: new.addresses,
        })
    }
}
