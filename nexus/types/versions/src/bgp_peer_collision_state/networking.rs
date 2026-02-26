// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `BGP_PEER_COLLISION_STATE` version.
//!
//! This version adds `ConnectionCollision` to `BgpPeerState` but does not
//! include the `peer_id` field on `BgpPeerStatus`.

use omicron_common::api::external::SwitchLocation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// The current status of a BGP peer (with `ConnectionCollision` state,
/// without `peer_id`).
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
    pub switch: SwitchLocation,
}

impl From<crate::v2025_11_20_00::networking::BgpPeerStatus> for BgpPeerStatus {
    fn from(old: crate::v2025_11_20_00::networking::BgpPeerStatus) -> Self {
        BgpPeerStatus {
            addr: old.addr,
            local_asn: old.local_asn,
            remote_asn: old.remote_asn,
            state: old.state.into(),
            state_duration_millis: old.state_duration_millis,
            switch: old.switch,
        }
    }
}

impl From<BgpPeerStatus> for crate::v2025_11_20_00::networking::BgpPeerStatus {
    fn from(new: BgpPeerStatus) -> Self {
        Self {
            addr: new.addr,
            local_asn: new.local_asn,
            remote_asn: new.remote_asn,
            state: new.state.into(),
            state_duration_millis: new.state_duration_millis,
            switch: new.switch,
        }
    }
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

    /// Waiting for keepaliave or notification from peer.
    OpenConfirm,

    /// There is an ongoing Connection Collision that hasn't yet been resolved.
    /// Two connections are maintained until one connection receives an Open or
    /// is able to progress into Established.
    ConnectionCollision,

    /// Synchronizing with peer.
    SessionSetup,

    /// Session established. Able to exchange update, notification and keepalive
    /// messages with peers.
    Established,
}

impl From<crate::v2025_11_20_00::networking::BgpPeerState> for BgpPeerState {
    fn from(old: crate::v2025_11_20_00::networking::BgpPeerState) -> Self {
        use crate::v2025_11_20_00::networking as v1;

        match old {
            v1::BgpPeerState::Idle => Self::Idle,
            v1::BgpPeerState::Connect => Self::Connect,
            v1::BgpPeerState::Active => Self::Active,
            v1::BgpPeerState::OpenSent => Self::OpenSent,
            v1::BgpPeerState::OpenConfirm => Self::OpenConfirm,
            v1::BgpPeerState::SessionSetup => Self::SessionSetup,
            v1::BgpPeerState::Established => Self::Established,
        }
    }
}

impl From<BgpPeerState> for crate::v2025_11_20_00::networking::BgpPeerState {
    fn from(new: BgpPeerState) -> Self {
        match new {
            BgpPeerState::Idle => Self::Idle,
            BgpPeerState::Connect => Self::Connect,
            BgpPeerState::Active => Self::Active,
            BgpPeerState::OpenSent => Self::OpenSent,
            BgpPeerState::OpenConfirm => Self::OpenConfirm,
            BgpPeerState::ConnectionCollision | BgpPeerState::SessionSetup => {
                Self::SessionSetup
            }
            BgpPeerState::Established => Self::Established,
        }
    }
}
