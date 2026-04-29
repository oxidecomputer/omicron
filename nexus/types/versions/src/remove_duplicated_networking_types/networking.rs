// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `REMOVE_DUPLICATED_NETWORKING_TYPES` version.
//!
//! Changes in this version:
//!
//! * [`SwitchInterfaceConfig::kind`] is now [`SwitchInterfaceKind`] instead of
//!   a different type with the same name but less information.
//! * New [`SwitchPortSettings`] version:
//!     * Pick up the new [`SwitchInterfaceConfig`]
//!     * Remove the `vlan_interfaces` field; the information it contained is
//!       now embedded within [`SwitchPortSettings::interfaces`] (for any VLAN
//!       interfaces).

use crate::v2025_11_20_00::networking::SwitchInterfaceKind;
use crate::v2025_11_20_00::networking::SwitchInterfaceKindNoVlanDetails;
use crate::v2025_11_20_00::networking::SwitchPortConfig;
use crate::v2025_11_20_00::networking::SwitchPortLinkConfig;
use crate::v2025_11_20_00::networking::SwitchPortSettingsGroups;
use crate::v2025_11_20_00::networking::SwitchVlanInterface;
use crate::v2025_11_20_00::networking::SwitchVlanInterfaceConfig;
use crate::v2026_04_16_00::networking::BgpPeer;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::Name;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

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
    pub kind: SwitchInterfaceKind,
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
    pub groups: Vec<SwitchPortSettingsGroups>,

    /// Layer 1 physical port settings.
    pub port: SwitchPortConfig,

    /// Layer 2 link settings.
    pub links: Vec<SwitchPortLinkConfig>,

    /// Layer 3 interface settings.
    pub interfaces: Vec<SwitchInterfaceConfig>,

    /// IP route settings.
    pub routes: Vec<external::SwitchPortRouteConfig>,

    /// BGP peer settings.
    pub bgp_peers: Vec<BgpPeer>,

    /// Layer 3 IP address settings.
    pub addresses: Vec<external::SwitchPortAddressView>,
}

impl From<SwitchPortSettings>
    for crate::v2026_04_16_00::networking::SwitchPortSettings
{
    fn from(value: SwitchPortSettings) -> Self {
        let mut interfaces = Vec::with_capacity(value.interfaces.len());
        let mut vlan_interfaces = Vec::new();
        for iface in value.interfaces {
            let kind = match iface.kind {
                SwitchInterfaceKind::Primary => {
                    SwitchInterfaceKindNoVlanDetails::Primary
                }
                SwitchInterfaceKind::Vlan(SwitchVlanInterface { vid }) => {
                    vlan_interfaces.push(SwitchVlanInterfaceConfig {
                        interface_config_id: iface.id,
                        vlan_id: vid,
                    });
                    SwitchInterfaceKindNoVlanDetails::Vlan
                }
                SwitchInterfaceKind::Loopback => {
                    SwitchInterfaceKindNoVlanDetails::Loopback
                }
            };
            interfaces.push(
                crate::v2025_11_20_00::networking::SwitchInterfaceConfig {
                    port_settings_id: iface.port_settings_id,
                    id: iface.id,
                    interface_name: iface.interface_name,
                    v6_enabled: iface.v6_enabled,
                    kind,
                },
            );
        }

        Self {
            identity: value.identity,
            groups: value.groups,
            port: value.port,
            links: value.links,
            interfaces,
            vlan_interfaces,
            routes: value.routes,
            bgp_peers: value.bgp_peers,
            addresses: value.addresses,
        }
    }
}
