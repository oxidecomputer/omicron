// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::Ipv6Addr;

use mg_admin_client::types::{
    DiscoveredRouter as MgDiscoveredRouter,
    PendingUnnumberedInterface as MgPendingUnnumberedInterface,
    RouterDiscoveryRuntimeState as MgRouterDiscoveryRuntimeState,
    UnnumberedInterface as MgUnnumberedInterface,
    UnnumberedManagerState as MgUnnumberedManagerState,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v1::early_networking::SwitchSlot;

fn nexus_interface_name(interface: String) -> String {
    if let Some(interface) = interface
        .strip_prefix("tfport")
        .and_then(|interface| interface.strip_suffix("_0"))
    {
        interface.to_string()
    } else {
        interface
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct UnnumberedInterfacePath {
    pub switch_slot: SwitchSlot,
    pub interface_name: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct UnnumberedManagerState {
    pub monitor_running: bool,
    pub pending_interfaces: Vec<PendingUnnumberedInterface>,
    pub active_interfaces: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SwitchUnnumberedManagerState {
    pub switch_slot: SwitchSlot,
    pub state: UnnumberedManagerState,
}

impl From<MgUnnumberedManagerState> for UnnumberedManagerState {
    fn from(value: MgUnnumberedManagerState) -> Self {
        let MgUnnumberedManagerState {
            monitor_running,
            pending_interfaces,
            active_interfaces,
        } = value;

        Self {
            monitor_running,
            pending_interfaces: pending_interfaces
                .into_iter()
                .map(Into::into)
                .collect(),
            active_interfaces: active_interfaces
                .into_iter()
                .map(nexus_interface_name)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct PendingUnnumberedInterface {
    /// Interface name
    pub interface: String,
    /// Configured router lifetime (seconds)
    pub router_lifetime: u16,
}

impl From<MgPendingUnnumberedInterface> for PendingUnnumberedInterface {
    fn from(value: MgPendingUnnumberedInterface) -> Self {
        let MgPendingUnnumberedInterface { interface, router_lifetime } = value;
        Self { interface: nexus_interface_name(interface), router_lifetime }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct RouterDiscoveryRuntimeState {
    /// ICMPv6 Router Advertisement transmit loop is running
    pub tx: bool,
    /// ICMPv6 Router Advertisement receive loop is running
    pub rx: bool,
}

impl From<MgRouterDiscoveryRuntimeState> for RouterDiscoveryRuntimeState {
    fn from(value: MgRouterDiscoveryRuntimeState) -> Self {
        let MgRouterDiscoveryRuntimeState { tx_running, rx_running } = value;
        Self { tx: tx_running, rx: rx_running }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct UnnumberedInterface {
    /// Interface name (e.g. "qsfp0")
    pub interface: String,
    /// IPv6 link-local address of this interface.
    pub local_address: Ipv6Addr,
    /// Router Lifetime advertised in ICMPv6 Router Advertisements sent on this
    /// interface.
    pub router_lifetime: u16,
    /// Information about discovered peer
    pub discovered_peer: Option<DiscoveredRouter>,
    /// State of rx/tx loops (None if interface not active in NDP)
    pub ndp_state: RouterDiscoveryRuntimeState,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SwitchUnnumberedInterface {
    pub switch_slot: SwitchSlot,
    pub interface: UnnumberedInterface,
}

impl From<MgUnnumberedInterface> for UnnumberedInterface {
    fn from(value: MgUnnumberedInterface) -> Self {
        let MgUnnumberedInterface {
            interface,
            local_address,
            scope_id: _,
            router_lifetime,
            discovered_peer,
            runtime_state,
        } = value;

        Self {
            interface: nexus_interface_name(interface),
            local_address,
            router_lifetime,
            discovered_peer: discovered_peer.map(Into::into),
            ndp_state: runtime_state.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DiscoveredRouter {
    /// Peer IPv6 address
    pub address: Ipv6Addr,
    /// When the peer was first discovered (ISO 8601 timestamp)
    pub discovered_at: String,
    /// When the most recent Router Advertisement was received (ISO 8601
    /// timestamp)
    pub last_advertisement: String,
    /// Router lifetime from RA (seconds)
    pub router_lifetime: u16,
    /// Reachable time from RA (milliseconds)
    pub reachable_time: u32,
    /// Retransmit timer from RA (milliseconds)
    pub retrans_timer: u32,
    /// Whether the peer entry has expired
    pub expired: bool,
    /// Time until expiry (human-readable), or None if already expired
    pub time_until_expiry: Option<String>,
}

impl From<MgDiscoveredRouter> for DiscoveredRouter {
    fn from(value: MgDiscoveredRouter) -> Self {
        let MgDiscoveredRouter {
            address,
            discovered_at,
            last_advertisement,
            router_lifetime,
            reachable_time,
            retrans_timer,
            expired,
            time_until_expiry,
        } = value;

        Self {
            address,
            discovered_at,
            last_advertisement,
            router_lifetime,
            reachable_time,
            retrans_timer,
            expired,
            time_until_expiry,
        }
    }
}
