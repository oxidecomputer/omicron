// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use std::time::Duration;

use mg_admin_client::types::{
    RouterDiscoveryRuntimeState as MgRouterDiscoveryRuntimeState,
    UnnumberedInterface as MgUnnumberedInterface,
    UnnumberedInterfaceStatus as MgUnnumberedInterfaceStatus,
    UnnumberedManagerState as MgUnnumberedManagerState,
};
use mg_api_types::unnumbered::DiscoveredRouter as MgDiscoveredRouter;
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
pub struct SwitchUnnumberedManagerState {
    pub switch_slot: SwitchSlot,
    pub state: UnnumberedManagerState,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct UnnumberedManagerState {
    pub monitor_running: bool,
    pub interfaces: BTreeMap<String, UnnumberedInterfaceStatus>,
}

impl From<MgUnnumberedManagerState> for UnnumberedManagerState {
    fn from(value: MgUnnumberedManagerState) -> Self {
        let MgUnnumberedManagerState { monitor_running, interfaces } = value;

        Self {
            monitor_running,
            interfaces: interfaces
                .into_iter()
                .map(|(interface, status)| {
                    (nexus_interface_name(interface), status.into())
                })
                .collect(),
        }
    }
}

/// Status of an interface configured for unnumbered operation.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnnumberedInterfaceStatus {
    /// Configured but not yet available on the system.
    Pending {
        /// Configured router lifetime (seconds)
        router_lifetime: u16,
    },
    /// Active for unnumbered operation.
    Active {
        /// Local IPv6 link-local address
        local_address: Ipv6Addr,
        /// IPv6 scope ID (interface index)
        scope_id: u32,
        /// Router lifetime advertised by this router (seconds)
        router_lifetime: u16,
        /// Information about the discovered peer. None if no peer has been
        /// discovered or the discovered entry has expired.
        discovered_peer: Option<DiscoveredRouter>,
        /// Runtime state for router discovery on this interface
        runtime_state: RouterDiscoveryRuntimeState,
    },
}

impl From<MgUnnumberedInterfaceStatus> for UnnumberedInterfaceStatus {
    fn from(value: MgUnnumberedInterfaceStatus) -> Self {
        match value {
            MgUnnumberedInterfaceStatus::Pending { router_lifetime } => {
                Self::Pending { router_lifetime }
            }
            MgUnnumberedInterfaceStatus::Active {
                local_address,
                scope_id,
                router_lifetime,
                discovered_peer,
                runtime_state,
            } => Self::Active {
                local_address,
                scope_id,
                router_lifetime,
                discovered_peer: discovered_peer.map(Into::into),
                runtime_state: runtime_state.into(),
            },
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

/// Information about a router discovered through router advertisements.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DiscoveredRouter {
    /// Router IPv6 address
    pub address: Ipv6Addr,
    /// Time elapsed since the router was first discovered
    pub time_since_discovered: Duration,
    /// Time elapsed since the most recent Router Advertisement was received
    pub time_since_last_rx: Duration,
    /// Effective reachable time governing expiry of this entry
    pub effective_reachable_time: Duration,
    /// Router lifetime from RA (seconds)
    pub router_lifetime: u16,
    /// Reachable time from RA (milliseconds)
    pub reachable_time: u32,
    /// Retransmit timer from RA (milliseconds)
    pub retrans_timer: u32,
}

impl From<MgDiscoveredRouter> for DiscoveredRouter {
    fn from(value: MgDiscoveredRouter) -> Self {
        let MgDiscoveredRouter {
            address,
            time_since_discovered,
            time_since_last_rx,
            effective_reachable_time,
            router_lifetime,
            reachable_time,
            retrans_timer,
        } = value;
        Self {
            address,
            time_since_discovered,
            time_since_last_rx,
            effective_reachable_time,
            router_lifetime,
            reachable_time,
            retrans_timer,
        }
    }
}
