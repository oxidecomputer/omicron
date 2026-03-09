// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::NameOrId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::BfdMode;
use sled_agent_types::early_networking::SwitchSlot;
use std::net::IpAddr;
use uuid::Uuid;

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

/// Information needed to disable a BFD session
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BfdSessionDisable {
    /// Address of the remote peer to disable a BFD session for.
    pub remote: IpAddr,

    /// The switch to enable this session on.
    pub switch: SwitchSlot,
}

/// Select switch ports by rack id and location.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortSelector {
    /// A rack id to use when selecting switch ports.
    pub rack_id: Uuid,

    /// A switch location to use when selecting switch ports.
    pub switch_location: SwitchSlot,
}
