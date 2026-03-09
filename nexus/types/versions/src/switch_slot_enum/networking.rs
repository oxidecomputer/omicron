// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::Error;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::BfdMode;
use sled_agent_types::early_networking::SwitchSlot;
use std::net::IpAddr;
use uuid::Uuid;

use crate::v2025_11_20_00;

// ADDRESS LOT

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
    pub switch_location: SwitchSlot,

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
        let switch_location =
            parse_name_as_switch_slot(&value.switch_location)?;
        Ok(Self {
            address_lot: value.address_lot,
            rack_id: value.rack_id,
            switch_location,
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

    /// The switch location to use when selecting the loopback address.
    pub switch_location: SwitchSlot,

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
        let switch_location =
            parse_name_as_switch_slot(&value.switch_location)?;
        Ok(Self {
            rack_id: value.rack_id,
            switch_location,
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

    /// The switch to enable this session on.
    pub switch: SwitchSlot,

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
        let switch = parse_name_as_switch_slot(&value.switch)?;
        Ok(Self {
            local: value.local,
            remote: value.remote,
            detection_threshold: value.detection_threshold,
            required_rx: value.required_rx,
            switch,
            mode: value.mode,
        })
    }
}

/// Information needed to disable a BFD session
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BfdSessionDisable {
    /// Address of the remote peer to disable a BFD session for.
    pub remote: IpAddr,

    /// The switch to enable this session on.
    pub switch: SwitchSlot,
}

impl TryFrom<v2025_11_20_00::networking::BfdSessionDisable>
    for BfdSessionDisable
{
    type Error = Error;

    fn try_from(
        value: v2025_11_20_00::networking::BfdSessionDisable,
    ) -> Result<Self, Self::Error> {
        let switch = parse_name_as_switch_slot(&value.switch)?;
        Ok(Self { remote: value.remote, switch })
    }
}

/// Select switch ports by rack id and location.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortSelector {
    /// A rack id to use when selecting switch ports.
    pub rack_id: Uuid,

    /// A switch location to use when selecting switch ports.
    pub switch_location: SwitchSlot,
}

impl TryFrom<v2025_11_20_00::networking::SwitchPortSelector>
    for SwitchPortSelector
{
    type Error = Error;

    fn try_from(
        value: v2025_11_20_00::networking::SwitchPortSelector,
    ) -> Result<Self, Self::Error> {
        let switch_location =
            parse_name_as_switch_slot(&value.switch_location)?;
        Ok(Self { rack_id: value.rack_id, switch_location })
    }
}

/// Select an LLDP endpoint by rack/switch/port
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct LldpPortPathSelector {
    /// A rack id to use when selecting switch ports.
    pub rack_id: Uuid,

    /// A switch location to use when selecting switch ports.
    pub switch_location: SwitchSlot,

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
        let switch_location =
            parse_name_as_switch_slot(&value.switch_location)?;
        Ok(Self { rack_id: value.rack_id, switch_location, port: value.port })
    }
}

// Helper for converting old switch slot arguments (all `Name`s).
fn parse_name_as_switch_slot(name: &Name) -> Result<SwitchSlot, Error> {
    match name.as_str() {
        "switch0" => Ok(SwitchSlot::Switch0),
        "switch1" => Ok(SwitchSlot::Switch1),
        _ => Err(Error::invalid_request(format!(
            "invalid switch location `{name}` \
             (expected `switch0` or `switch1`)",
        ))),
    }
}
