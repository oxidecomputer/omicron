// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstore types for the Sled Agent API.

use std::collections::BTreeSet;
use std::net::SocketAddrV6;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::Baseboard;

/// Status of the local bootstore node.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BootstoreStatus {
    pub fsm_ledger_generation: u64,
    pub network_config_ledger_generation: Option<u64>,
    pub fsm_state: String,
    pub peers: BTreeSet<SocketAddrV6>,
    pub established_connections: Vec<EstablishedConnection>,
    pub accepted_connections: BTreeSet<SocketAddrV6>,
    pub negotiating_connections: BTreeSet<SocketAddrV6>,
}

impl From<bootstore::schemes::v0::Status> for BootstoreStatus {
    fn from(value: bootstore::schemes::v0::Status) -> Self {
        BootstoreStatus {
            fsm_ledger_generation: value.fsm_ledger_generation,
            network_config_ledger_generation: value
                .network_config_ledger_generation,
            fsm_state: value.fsm_state.to_string(),
            peers: value.peers,
            established_connections: value
                .connections
                .into_iter()
                .map(EstablishedConnection::from)
                .collect(),
            accepted_connections: value.accepted_connections,
            negotiating_connections: value.negotiating_connections,
        }
    }
}

/// An established connection to a bootstore peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct EstablishedConnection {
    pub baseboard: Baseboard,
    pub addr: SocketAddrV6,
}

impl From<(Baseboard, SocketAddrV6)> for EstablishedConnection {
    fn from(value: (Baseboard, SocketAddrV6)) -> Self {
        EstablishedConnection { baseboard: value.0, addr: value.1 }
    }
}
