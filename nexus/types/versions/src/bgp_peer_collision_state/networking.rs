// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `BGP_PEER_COLLISION_STATE` version.
//!
//! This version adds `ConnectionCollision` to `BgpPeerState` but does not
//! include the `peer_id` field on `BgpPeerStatus`.

use omicron_common::api::external;
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

/// The current state of a BGP peer (includes `ConnectionCollision`).
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

    /// There is an ongoing Connection Collision that hasn't yet been resolved.
    ConnectionCollision,

    /// Synchronizing with peer.
    SessionSetup,

    /// Session established. Able to exchange update, notification and keepalive
    /// messages with peers.
    Established,
}

impl From<external::BgpPeerStatus> for BgpPeerStatus {
    fn from(new: external::BgpPeerStatus) -> Self {
        BgpPeerStatus {
            addr: new.addr,
            local_asn: new.local_asn,
            remote_asn: new.remote_asn,
            state: match new.state {
                external::BgpPeerState::Idle => BgpPeerState::Idle,
                external::BgpPeerState::Connect => BgpPeerState::Connect,
                external::BgpPeerState::Active => BgpPeerState::Active,
                external::BgpPeerState::OpenSent => BgpPeerState::OpenSent,
                external::BgpPeerState::OpenConfirm => {
                    BgpPeerState::OpenConfirm
                }
                external::BgpPeerState::ConnectionCollision => {
                    BgpPeerState::ConnectionCollision
                }
                external::BgpPeerState::SessionSetup => {
                    BgpPeerState::SessionSetup
                }
                external::BgpPeerState::Established => {
                    BgpPeerState::Established
                }
            },
            state_duration_millis: new.state_duration_millis,
            switch: new.switch,
        }
    }
}
