// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `STRONGER_UPLINK_ADDRESS_TYPES` version.
//!
//! * Change [`Address::address`] from [`IpNet`] to [`UplinkAddress`], which
//!   rejects various invalid addresses (localhost, multicast addresses, etc.).
//! * Define new versions of types that transitively include [`Address`]:
//!   * [`AddressConfig`]
//!   * [`SwitchPortSettingsCreate`]
//! * In [`LoopbackAddress`] and [`LoopbackAddressCreate`], replace the pair of
//!   fields `address` (`IpAddr`) and `prefix` (`u8`) with a single `address`
//!   field of type [`LoopbackAddressIpNet`]. This enforces valid prefix lengths
//!   and rejects invalid IPs.

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::networking::LinkConfigCreate;
use crate::v2025_11_20_00::networking::RouteConfig;
use crate::v2025_11_20_00::networking::SwitchInterfaceConfigCreate;
use crate::v2025_11_20_00::networking::SwitchPortConfigCreate;
use crate::v2026_03_06_01;
use crate::v2026_03_18_00;
use crate::v2026_03_18_00::networking::BgpPeerConfig;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use oxnet::IpNet;
use oxnet::IpNetPrefixError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v1::early_networking::SwitchSlot;
use sled_agent_types_versions::v30::early_networking::InvalidIpAddrError;
use sled_agent_types_versions::v30::early_networking::UplinkAddress;
use sled_agent_types_versions::v30::early_networking::UplinkIpNet;
use sled_agent_types_versions::v30::early_networking::UplinkIpNetError;
use uuid::Uuid;

/// An address tied to an address lot.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Address {
    /// The address lot this address is drawn from.
    pub address_lot: NameOrId,

    /// The address and prefix length of this address.
    pub address: UplinkAddress,

    /// Optional VLAN ID for this address
    pub vlan_id: Option<u16>,
}

impl TryFrom<v2025_11_20_00::networking::Address> for Address {
    type Error = UplinkIpNetError;

    fn try_from(
        value: v2025_11_20_00::networking::Address,
    ) -> Result<Self, Self::Error> {
        let address = match UplinkIpNet::try_from(value.address) {
            Ok(ip_net) => UplinkAddress::Static { ip_net },
            Err(err) => match err.err {
                // Prior API versions interpreted unspecified addresses as
                // "link-local"; convert this specific error to an addrconf
                // address, and return any other error as-is.
                InvalidIpAddrError::UnspecifiedAddress => {
                    UplinkAddress::AddrConf
                }
                InvalidIpAddrError::LoopbackAddress
                | InvalidIpAddrError::MulticastAddress
                | InvalidIpAddrError::Ipv4Broadcast
                | InvalidIpAddrError::Ipv6UnicastLinkLocal => return Err(err),
            },
        };

        Ok(Self {
            address_lot: value.address_lot,
            address,
            vlan_id: value.vlan_id,
        })
    }
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

impl TryFrom<v2025_11_20_00::networking::AddressConfig> for AddressConfig {
    type Error = UplinkIpNetError;

    fn try_from(
        value: v2025_11_20_00::networking::AddressConfig,
    ) -> Result<Self, Self::Error> {
        let addresses = value
            .addresses
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self { link_name: value.link_name, addresses })
    }
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

impl TryFrom<v2026_03_18_00::networking::SwitchPortSettingsCreate>
    for SwitchPortSettingsCreate
{
    type Error = UplinkIpNetError;

    fn try_from(
        value: v2026_03_18_00::networking::SwitchPortSettingsCreate,
    ) -> Result<Self, Self::Error> {
        let addresses = value
            .addresses
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            identity: value.identity,
            port_config: value.port_config,
            groups: value.groups,
            links: value.links,
            interfaces: value.interfaces,
            routes: value.routes,
            bgp_peers: value.bgp_peers,
            addresses,
        })
    }
}

/// IP address and subnet mask used for loopback addresses.
// This is a newtype wrapper around `UplinkIpNet`. We apply the same
// restrictions on valid IPs, but unlink `UplinkAddress`, we don't allow
// addrconf addresses.
#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    JsonSchema,
    Hash,
    PartialOrd,
    Ord,
)]
#[serde(from = "UplinkIpNet", into = "UplinkIpNet")]
pub struct LoopbackAddressIpNet(pub(crate) UplinkIpNet);

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

    /// The slot of the switch within the rack where this loopback address is
    /// assigned.
    pub switch_slot: SwitchSlot,

    /// The loopback IP address and prefix length.
    pub address: LoopbackAddressIpNet,
}

impl From<LoopbackAddress> for v2026_03_06_01::networking::LoopbackAddress {
    fn from(value: LoopbackAddress) -> Self {
        Self {
            id: value.id,
            address_lot_block_id: value.address_lot_block_id,
            rack_id: value.rack_id,
            switch_slot: value.switch_slot,
            address: value.address.0.into(),
        }
    }
}

/// Parameters for creating a loopback address on a particular rack switch.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct LoopbackAddressCreate {
    /// The name or id of the address lot this loopback address will pull an
    /// address from.
    pub address_lot: NameOrId,

    /// The rack containing the switch this loopback address will be configured
    /// on.
    pub rack_id: Uuid,

    /// The slot of the switch within the rack this loopback address will be
    /// configured on.
    pub switch_slot: SwitchSlot,

    /// The address to create and its subnet mask.
    pub address: LoopbackAddressIpNet,

    /// Address is an anycast address.
    ///
    /// This allows the address to be assigned to multiple locations
    /// simultaneously.
    pub anycast: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum LoopbackAddressConversionError {
    #[error(transparent)]
    IpNetPrefix(#[from] IpNetPrefixError),
    #[error(transparent)]
    UplinkIp(#[from] UplinkIpNetError),
}

impl TryFrom<v2026_03_06_01::networking::LoopbackAddressCreate>
    for LoopbackAddressCreate
{
    type Error = LoopbackAddressConversionError;

    fn try_from(
        value: v2026_03_06_01::networking::LoopbackAddressCreate,
    ) -> Result<Self, Self::Error> {
        let ip_net = IpNet::new(value.address, value.mask)?;
        let address = LoopbackAddressIpNet::try_from(ip_net)?;

        Ok(Self {
            address_lot: value.address_lot,
            rack_id: value.rack_id,
            switch_slot: value.switch_slot,
            address,
            anycast: value.anycast,
        })
    }
}
