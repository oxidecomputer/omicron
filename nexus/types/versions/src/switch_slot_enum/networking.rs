// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::format_switch_slot_as_str;
use super::parse_str_as_switch_slot;
use crate::v2025_11_20_00;
use omicron_common::api::external::Error;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v1::early_networking::BfdMode;
use sled_agent_types_versions::v1::early_networking::SwitchSlot;
use std::net::IpAddr;
use uuid::Uuid;

// ADDRESS LOT

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
    pub address: oxnet::IpNet,
}

impl From<LoopbackAddress> for v2025_11_20_00::networking::LoopbackAddress {
    fn from(value: LoopbackAddress) -> Self {
        Self {
            id: value.id,
            address_lot_block_id: value.address_lot_block_id,
            rack_id: value.rack_id,
            switch_location: format_switch_slot_as_str(value.switch_slot)
                .to_owned(),
            address: value.address,
        }
    }
}

/// Parameters for creating a loopback address on a particular rack switch.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct LoopbackAddressCreate {
    /// The name or id of the address lot this loopback address will pull an
    /// address from.
    pub address_lot: NameOrId,

    /// The rack containing the switch this loopback address will be configured on.
    pub rack_id: Uuid,

    /// The slot of the switch within the rack this loopback address will be
    /// configured on.
    pub switch_slot: SwitchSlot,

    /// The address to create.
    pub address: IpAddr,

    /// The subnet mask to use for the address.
    pub mask: u8,

    /// Address is an anycast address.
    ///
    /// This allows the address to be assigned to multiple locations simultaneously.
    pub anycast: bool,
}

impl TryFrom<v2025_11_20_00::networking::LoopbackAddressCreate>
    for LoopbackAddressCreate
{
    type Error = Error;

    fn try_from(
        value: v2025_11_20_00::networking::LoopbackAddressCreate,
    ) -> Result<Self, Self::Error> {
        let switch_slot =
            parse_str_as_switch_slot(value.switch_location.as_str())?;
        Ok(Self {
            address_lot: value.address_lot,
            rack_id: value.rack_id,
            switch_slot,
            address: value.address,
            mask: value.mask,
            anycast: value.anycast,
        })
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct LoopbackAddressPath {
    /// The rack to use when selecting the loopback address.
    pub rack_id: Uuid,

    /// The slot of the switch within the rack to use when selecting the
    /// loopback address.
    pub switch_slot: SwitchSlot,

    /// The IP address and subnet mask to use when selecting the loopback
    /// address.
    pub address: IpAddr,

    /// The IP address and subnet mask to use when selecting the loopback
    /// address.
    pub subnet_mask: u8,
}

impl TryFrom<v2025_11_20_00::networking::LoopbackAddressPath>
    for LoopbackAddressPath
{
    type Error = Error;

    fn try_from(
        value: v2025_11_20_00::networking::LoopbackAddressPath,
    ) -> Result<Self, Self::Error> {
        let switch_slot = parse_str_as_switch_slot(value.switch_slot.as_str())?;
        Ok(Self {
            rack_id: value.rack_id,
            switch_slot,
            address: value.address,
            subnet_mask: value.subnet_mask,
        })
    }
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

    /// The slot of the switch within the rack to enable this session on.
    pub switch_slot: SwitchSlot,

    /// Select either single-hop (RFC 5881) or multi-hop (RFC 5883)
    pub mode: BfdMode,
}

impl TryFrom<v2025_11_20_00::networking::BfdSessionEnable>
    for BfdSessionEnable
{
    type Error = Error;

    fn try_from(
        value: v2025_11_20_00::networking::BfdSessionEnable,
    ) -> Result<Self, Self::Error> {
        let switch_slot = parse_str_as_switch_slot(value.switch.as_str())?;
        Ok(Self {
            local: value.local,
            remote: value.remote,
            detection_threshold: value.detection_threshold,
            required_rx: value.required_rx,
            switch_slot,
            mode: value.mode,
        })
    }
}

/// Information needed to disable a BFD session
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BfdSessionDisable {
    /// Address of the remote peer to disable a BFD session for.
    pub remote: IpAddr,

    /// The slot of the switch within the rack to disable this session on.
    pub switch_slot: SwitchSlot,
}

impl TryFrom<v2025_11_20_00::networking::BfdSessionDisable>
    for BfdSessionDisable
{
    type Error = Error;

    fn try_from(
        value: v2025_11_20_00::networking::BfdSessionDisable,
    ) -> Result<Self, Self::Error> {
        let switch_slot = parse_str_as_switch_slot(value.switch.as_str())?;
        Ok(Self { remote: value.remote, switch_slot })
    }
}

/// A switch port represents a physical external port on a rack switch.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPort {
    /// The id of the switch port.
    pub id: Uuid,

    /// The rack this switch port belongs to.
    pub rack_id: Uuid,

    /// The slot of the switch within the rack of this switch port.
    pub switch_slot: SwitchSlot,

    /// The name of this switch port.
    pub port_name: Name,

    /// The primary settings group of this switch port. Will be `None` until
    /// this switch port is configured.
    pub port_settings_id: Option<Uuid>,
}

impl From<SwitchPort> for v2025_11_20_00::networking::SwitchPort {
    fn from(value: SwitchPort) -> Self {
        Self {
            id: value.id,
            rack_id: value.rack_id,
            switch_location: format_switch_slot_as_str(value.switch_slot)
                .to_owned(),
            port_name: value.port_name,
            port_settings_id: value.port_settings_id,
        }
    }
}

/// Select switch ports by rack id and location.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortSelector {
    /// A rack id to use when selecting switch ports.
    pub rack_id: Uuid,

    /// The slot of the switch within the rack to use when selecting switch
    /// ports.
    pub switch_slot: SwitchSlot,
}

impl TryFrom<v2025_11_20_00::networking::SwitchPortSelector>
    for SwitchPortSelector
{
    type Error = Error;

    fn try_from(
        value: v2025_11_20_00::networking::SwitchPortSelector,
    ) -> Result<Self, Self::Error> {
        let switch_slot =
            parse_str_as_switch_slot(value.switch_location.as_str())?;
        Ok(Self { rack_id: value.rack_id, switch_slot })
    }
}

/// Select an LLDP endpoint by rack/switch/port
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct LldpPortPathSelector {
    /// A rack id to use when selecting switch ports.
    pub rack_id: Uuid,

    /// The slot of the switch within the rack to use when selecting switch
    /// ports.
    pub switch_slot: SwitchSlot,

    /// A name to use when selecting switch ports.
    pub port: Name,
}

impl TryFrom<v2025_11_20_00::networking::LldpPortPathSelector>
    for LldpPortPathSelector
{
    type Error = Error;

    fn try_from(
        value: v2025_11_20_00::networking::LldpPortPathSelector,
    ) -> Result<Self, Self::Error> {
        let switch_slot = parse_str_as_switch_slot(value.switch_slot.as_str())?;
        Ok(Self { rack_id: value.rack_id, switch_slot, port: value.port })
    }
}
