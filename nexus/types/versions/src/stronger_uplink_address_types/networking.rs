// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `STRONGER_UPLINK_ADDRESS_TYPES` version.
//!
//! * Change the type of the [`Address::address`],
//!   [`LoopbackAddress::address`], and [`SwitchPortAddressView::address`]
//!   fields from [`IpNet`] to [`UplinkAddress`], which rejects various invalid
//!   addresses (multicast addresses, unspecified addresses, etc.).
//! * In [`LoopbackAddressCreate`], replace the pair of fields `address`
//!   (`IpAddr`) and `mask` (`u8`) with a single `address` field of type
//!   [`LoopbackAddressIpNet`]. This enforces valid prefix lengths and rejects
//!   invalid IPs.
//! * Define new versions of types that transitively include the above updated
//!   types:
//!   * [`AddressConfig`]
//!   * [`SwitchPortSettings`]
//!   * [`SwitchPortSettingsCreate`]

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::networking::LinkConfigCreate;
use crate::v2025_11_20_00::networking::RouteConfig;
use crate::v2025_11_20_00::networking::SwitchInterfaceConfigCreate;
use crate::v2025_11_20_00::networking::SwitchPortConfigCreate;
use crate::v2026_03_06_01;
use crate::v2026_04_16_00;
use crate::v2026_04_16_00::networking::BgpPeer;
use crate::v2026_04_16_00::networking::BgpPeerConfig;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadata;
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

    /// The address and prefix length of this address or a specification that
    /// this address should be an `addrconf` address.
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
                | InvalidIpAddrError::Ipv4MappedIpv6
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

impl TryFrom<v2026_04_16_00::networking::SwitchPortSettingsCreate>
    for SwitchPortSettingsCreate
{
    type Error = UplinkIpNetError;

    fn try_from(
        value: v2026_04_16_00::networking::SwitchPortSettingsCreate,
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
// restrictions on valid IPs, but unlike `UplinkAddress`, we don't allow
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
#[schemars(transparent)]
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

/// An IP address configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortAddressView {
    /// The port settings object this address configuration belongs to.
    pub port_settings_id: Uuid,

    /// The id of the address lot this address is drawn from.
    pub address_lot_id: Uuid,

    /// The name of the address lot this address is drawn from.
    pub address_lot_name: Name,

    /// The id of the address lot block this address is drawn from.
    pub address_lot_block_id: Uuid,

    /// The IP address and prefix, or indicator that this is an `addrconf`
    /// address.
    pub address: UplinkAddress,

    /// An optional VLAN ID
    pub vlan_id: Option<u16>,

    /// The interface name this address belongs to.
    pub interface_name: Name,
}

impl From<SwitchPortAddressView>
    for v2025_11_20_00::networking::SwitchPortAddressView
{
    fn from(value: SwitchPortAddressView) -> Self {
        Self {
            port_settings_id: value.port_settings_id,
            address_lot_id: value.address_lot_id,
            address_lot_name: value.address_lot_name,
            address_lot_block_id: value.address_lot_block_id,
            // Previous API version represented addrconf addresses as
            // UNSPECIFIED.
            address: value.address.ip_net_squashing_addrconf_to_unspecified(),
            vlan_id: value.vlan_id,
            interface_name: value.interface_name,
        }
    }
}

/// This structure contains all port settings information in one place. It's a
/// convenience data structure for getting a complete view of a particular
/// port's settings.
// TODO: several fields below embed `external::*` types directly from
// `omicron-common`, which means their serialized shape is not truly frozen.
// Once `omicron-common-versions` exists, replace these with version-local
// copies of the types to ensure the initial version's wire format is
// immutable.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
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
    pub addresses: Vec<SwitchPortAddressView>,
}

impl From<SwitchPortSettings>
    for v2026_04_16_00::networking::SwitchPortSettings
{
    fn from(value: SwitchPortSettings) -> Self {
        Self {
            identity: value.identity,
            groups: value.groups,
            port: value.port,
            links: value.links,
            interfaces: value.interfaces,
            vlan_interfaces: value.vlan_interfaces,
            routes: value.routes,
            bgp_peers: value.bgp_peers,
            addresses: value.addresses.into_iter().map(From::from).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::sync::LazyLock;

    static ADDRESS_LOT_NAME: LazyLock<Name> =
        LazyLock::new(|| "test-lot".parse().unwrap());
    static ADDRESS_LOT_NAME_OR_ID: LazyLock<NameOrId> =
        LazyLock::new(|| NameOrId::Name(ADDRESS_LOT_NAME.clone()));

    /// Build a minimal old-format `Address` for conversion tests.
    fn make_old_address(ip_net: IpNet) -> v2025_11_20_00::networking::Address {
        v2025_11_20_00::networking::Address {
            address_lot: ADDRESS_LOT_NAME_OR_ID.clone(),
            address: ip_net,
            vlan_id: None,
        }
    }

    /// Build a minimal old-format `LoopbackAddressCreate` for conversion tests.
    fn make_old_loopback_create(
        address: IpAddr,
        mask: u8,
    ) -> v2026_03_06_01::networking::LoopbackAddressCreate {
        v2026_03_06_01::networking::LoopbackAddressCreate {
            address_lot: ADDRESS_LOT_NAME_OR_ID.clone(),
            rack_id: Uuid::new_v4(),
            switch_slot: SwitchSlot::Switch0,
            address,
            mask,
            anycast: false,
        }
    }

    /// Build a minimal new-format `SwitchPortAddressView` for conversion tests.
    fn make_new_address_view(address: UplinkAddress) -> SwitchPortAddressView {
        SwitchPortAddressView {
            port_settings_id: Uuid::new_v4(),
            address_lot_id: Uuid::new_v4(),
            address_lot_name: ADDRESS_LOT_NAME.clone(),
            address_lot_block_id: Uuid::new_v4(),
            address,
            vlan_id: None,
            interface_name: "phy0".parse().unwrap(),
        }
    }

    #[test]
    fn test_address_conversion_accepts_valid_ips() {
        for ip_net in [
            "10.0.0.1/24".parse::<IpNet>().unwrap(),
            "fd00::1/64".parse().unwrap(),
        ] {
            let new = Address::try_from(make_old_address(ip_net)).unwrap();
            assert_eq!(
                new.address,
                UplinkAddress::Static { ip_net: ip_net.try_into().unwrap() },
            );
        }

        for ip_net in [
            "0.0.0.0/0".parse::<IpNet>().unwrap(),
            "0.0.0.0/32".parse().unwrap(),
            "::/0".parse().unwrap(),
            "::/128".parse().unwrap(),
        ] {
            let new = Address::try_from(make_old_address(ip_net)).unwrap();
            assert_eq!(new.address, UplinkAddress::AddrConf);
        }
    }

    #[test]
    fn test_address_conversion_preserves_fields() {
        let ip_net: IpNet = "10.0.0.1/24".parse().unwrap();
        let mut old = make_old_address(ip_net);
        old.vlan_id = Some(42);
        old.address_lot = NameOrId::Name("my-lot".parse().unwrap());
        let new = Address::try_from(old).unwrap();
        assert_eq!(new.vlan_id, Some(42));
        assert_eq!(new.address_lot, NameOrId::Name("my-lot".parse().unwrap()));
    }

    #[test]
    fn test_address_conversion_rejects_invalid_ips() {
        let cases = [
            (
                "IPv4 loopback",
                "127.0.0.1/8".parse::<IpNet>().unwrap(),
                InvalidIpAddrError::LoopbackAddress,
            ),
            (
                "IPv6 loopback",
                "::1/128".parse().unwrap(),
                InvalidIpAddrError::LoopbackAddress,
            ),
            (
                "multicast",
                "224.0.0.1/32".parse().unwrap(),
                InvalidIpAddrError::MulticastAddress,
            ),
            (
                "broadcast",
                "255.255.255.255/32".parse().unwrap(),
                InvalidIpAddrError::Ipv4Broadcast,
            ),
            (
                "IPv6 link-local",
                "fe80::1/64".parse().unwrap(),
                InvalidIpAddrError::Ipv6UnicastLinkLocal,
            ),
            (
                "IPv4-mapped IPv6",
                "::ffff:192.168.0.1/128".parse().unwrap(),
                InvalidIpAddrError::Ipv4MappedIpv6,
            ),
        ];

        for (label, ip_net, expected_err) in cases {
            let err = Address::try_from(make_old_address(ip_net))
                .expect_err(&format!("{label}: should have failed"));
            assert_eq!(err.err, expected_err);
        }
    }

    #[test]
    fn test_loopback_create_conversion_valid_addresses() {
        for (ip, prefix) in [
            ("203.0.113.1".parse().unwrap(), 24),
            ("fd00::1".parse().unwrap(), 64),
        ] {
            let old = make_old_loopback_create(ip, prefix);
            let new = LoopbackAddressCreate::try_from(old.clone()).unwrap();
            let expected = IpNet::new(ip, prefix).unwrap();
            assert_eq!(IpNet::from(new.address), expected);
        }
    }

    #[test]
    fn test_loopback_create_conversion_rejects_bad_prefixes() {
        for (ip, prefix) in [
            ("203.0.113.1".parse().unwrap(), 33),
            ("fd00::1".parse().unwrap(), 129),
        ] {
            let old = make_old_loopback_create(ip, prefix);
            let err = LoopbackAddressCreate::try_from(old)
                .expect_err("mask should fail");
            assert!(
                matches!(err, LoopbackAddressConversionError::IpNetPrefix(_)),
                "expected IpNetPrefix error, got: {err:?}"
            );
        }
    }

    #[test]
    fn test_loopback_create_conversion_rejects_invalid_ips() {
        let cases: Vec<(&str, IpAddr, u8)> = vec![
            ("loopback", IpAddr::V4(Ipv4Addr::LOCALHOST), 8),
            ("multicast", "224.0.0.1".parse().unwrap(), 32),
            ("broadcast", IpAddr::V4(Ipv4Addr::BROADCAST), 32),
            ("link-local", "fe80::1".parse().unwrap(), 64),
            ("unspecified v4", IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            ("unspecified v6", IpAddr::V6(Ipv6Addr::UNSPECIFIED), 128),
        ];

        for (label, ip, mask) in cases {
            let old = make_old_loopback_create(ip, mask);
            let err = LoopbackAddressCreate::try_from(old)
                .expect_err(&format!("{label}: should have failed"));
            assert!(
                matches!(err, LoopbackAddressConversionError::UplinkIp(_)),
                "{label}: expected UplinkIp error, got: {err:?}"
            );
        }
    }

    #[test]
    fn test_loopback_create_conversion_preserves_fields() {
        let old = make_old_loopback_create("10.0.0.1".parse().unwrap(), 24);
        let rack_id = old.rack_id;
        let new = LoopbackAddressCreate::try_from(old).unwrap();
        assert_eq!(new.rack_id, rack_id);
        assert_eq!(new.switch_slot, SwitchSlot::Switch0);
        assert!(!new.anycast);
        assert_eq!(new.address_lot, ADDRESS_LOT_NAME_OR_ID.clone(),);
    }

    #[test]
    fn test_address_view_reverse_static() {
        let ip_net: UplinkIpNet = "10.0.0.1/24".parse().unwrap();
        let new = make_new_address_view(UplinkAddress::Static { ip_net });
        let old: v2025_11_20_00::networking::SwitchPortAddressView = new.into();
        assert_eq!(old.address, IpNet::from(ip_net));
    }

    #[test]
    fn test_address_view_reverse_addrconf_becomes_unspecified() {
        let new = make_new_address_view(UplinkAddress::AddrConf);
        let old: v2025_11_20_00::networking::SwitchPortAddressView = new.into();
        let expected: IpNet = "::/128".parse().unwrap();
        assert_eq!(old.address, expected);
    }

    #[test]
    fn test_address_view_reverse_preserves_fields() {
        let ip_net: UplinkIpNet = "10.0.0.1/24".parse().unwrap();
        let mut new = make_new_address_view(UplinkAddress::Static { ip_net });
        new.vlan_id = Some(99);
        let port_settings_id = new.port_settings_id;
        let old: v2025_11_20_00::networking::SwitchPortAddressView = new.into();
        assert_eq!(old.port_settings_id, port_settings_id);
        assert_eq!(old.vlan_id, Some(99));
    }

    #[test]
    fn test_address_round_trip_static() {
        let ip_net: IpNet = "10.0.0.1/24".parse().unwrap();
        let old = make_old_address(ip_net);
        let new = Address::try_from(old).unwrap();
        // Simulate what the view path does: new address -> old address view
        let view = make_new_address_view(new.address);
        let old_view: v2025_11_20_00::networking::SwitchPortAddressView =
            view.into();
        assert_eq!(old_view.address, ip_net);
    }

    #[test]
    fn test_address_round_trip_unspecified_normalizes_to_ipv6() {
        // Old clients sending 0.0.0.0/0 get AddrConf, which round-trips back
        // as ::/128 (the canonical unspecified representation).
        let ip_net: IpNet = "0.0.0.0/0".parse().unwrap();
        let old = make_old_address(ip_net);
        let new = Address::try_from(old).unwrap();
        assert_eq!(new.address, UplinkAddress::AddrConf);

        let view = make_new_address_view(new.address);
        let old_view: v2025_11_20_00::networking::SwitchPortAddressView =
            view.into();
        let expected_canonical: IpNet = "::/128".parse().unwrap();
        assert_eq!(
            old_view.address, expected_canonical,
            "IPv4 UNSPECIFIED should normalize to ::/128 on round-trip"
        );
    }

    #[test]
    fn test_loopback_address_reverse() {
        let ip_net: UplinkIpNet = "203.0.113.99/24".parse().unwrap();
        let new = LoopbackAddress {
            id: Uuid::new_v4(),
            address_lot_block_id: Uuid::new_v4(),
            rack_id: Uuid::new_v4(),
            switch_slot: SwitchSlot::Switch0,
            address: LoopbackAddressIpNet(ip_net),
        };
        let id = new.id;
        let old: v2026_03_06_01::networking::LoopbackAddress = new.into();
        assert_eq!(old.id, id);
        assert_eq!(old.address, IpNet::from(ip_net));
    }
}
