// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! BGP unnumbered status networking types.

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv6Addr};
use std::time::Duration;

use mg_admin_client::types::{
    RouterDiscoveryRuntimeState as MgRouterDiscoveryRuntimeState,
    UnnumberedInterface as MgUnnumberedInterface,
    UnnumberedInterfaceStatus as MgUnnumberedInterfaceStatus,
    UnnumberedManagerState as MgUnnumberedManagerState,
};
use mg_api_types::unnumbered::DiscoveredRouter as MgDiscoveredRouter;
use omicron_common::tfport::TfportInterfaceName;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v1::early_networking::SwitchSlot;

fn nexus_interface_name(interface: String) -> String {
    match interface.parse::<TfportInterfaceName>() {
        Ok(interface_name) if interface_name.link_id() == 0 => {
            interface_name.port_name().to_owned()
        }
        Ok(_) | Err(_) => interface,
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct UnnumberedInterfacePath {
    pub switch_slot: SwitchSlot,
    pub interface_name: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[schemars(rename = "{T}SwitchResults")]
pub struct SwitchResults<T> {
    pub switch0: SwitchResult<T>,
    pub switch1: SwitchResult<T>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[schemars(rename = "{T}SwitchResult")]
pub enum SwitchResult<T> {
    Available { value: T },
    Unavailable { reason: SwitchUnavailableReason },
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SwitchUnavailableReason {
    MgdUnresolved,
    QueryFailed,
}

/// The current status of a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpPeerStatus {
    /// IP address of the peer.
    pub addr: IpAddr,

    /// Interface name.
    pub peer_id: String,

    /// Local autonomous system number.
    pub local_asn: u32,

    /// Remote autonomous system number.
    pub remote_asn: u32,

    /// State of the peer.
    pub state: crate::v2025_12_12_00::networking::BgpPeerState,

    /// Time of last state change.
    pub state_duration_millis: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BgpPeerStatuses(pub Vec<BgpPeerStatus>);

/// A route exported to a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpExported {
    /// Identifier for the BGP peer.
    pub peer_id: String,

    /// The destination network prefix.
    pub prefix: oxnet::IpNet,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BgpExportedRoutes(pub Vec<BgpExported>);

/// BGP message history indexed by peer address.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BgpMessageHistories(
    pub HashMap<String, crate::v2025_11_20_00::networking::BgpMessageHistory>,
);

/// A route imported from a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpImported {
    /// The destination network prefix.
    pub prefix: oxnet::IpNet,

    /// The nexthop the prefix is reachable through.
    pub nexthop: IpAddr,

    /// BGP identifier of the originating router.
    pub id: u32,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BgpImportedRoutes(pub Vec<BgpImported>);

impl From<SwitchResults<BgpPeerStatuses>>
    for Vec<crate::v2026_02_13_01::networking::BgpPeerStatus>
{
    fn from(results: SwitchResults<BgpPeerStatuses>) -> Self {
        let mut statuses = Vec::new();
        for (switch, result) in results {
            let SwitchResult::Available { value } = result else {
                continue;
            };
            statuses.extend(value.0.into_iter().map(|status| {
                crate::v2026_02_13_01::networking::BgpPeerStatus {
                    addr: status.addr,
                    peer_id: status.peer_id,
                    local_asn: status.local_asn,
                    remote_asn: status.remote_asn,
                    state: status.state,
                    state_duration_millis: status.state_duration_millis,
                    switch,
                }
            }));
        }
        statuses
    }
}

impl From<SwitchResults<BgpExportedRoutes>>
    for Vec<crate::v2026_02_13_01::networking::BgpExported>
{
    fn from(results: SwitchResults<BgpExportedRoutes>) -> Self {
        let mut routes = Vec::new();
        for (switch, result) in results {
            let SwitchResult::Available { value } = result else {
                continue;
            };
            routes.extend(value.0.into_iter().map(|route| {
                crate::v2026_02_13_01::networking::BgpExported {
                    peer_id: route.peer_id,
                    switch,
                    prefix: route.prefix,
                }
            }));
        }
        routes
    }
}

impl From<SwitchResults<BgpMessageHistories>>
    for crate::v2025_11_20_00::networking::AggregateBgpMessageHistory
{
    fn from(results: SwitchResults<BgpMessageHistories>) -> Self {
        let mut switch_histories = Vec::new();
        for (switch, result) in results {
            let SwitchResult::Available { value } = result else {
                continue;
            };
            switch_histories.push(
                crate::v2025_11_20_00::networking::SwitchBgpHistory {
                    switch,
                    history: value.0,
                },
            );
        }
        Self { switch_histories }
    }
}

impl From<SwitchResults<BgpImportedRoutes>>
    for Vec<crate::v2026_02_13_01::networking::BgpImported>
{
    fn from(results: SwitchResults<BgpImportedRoutes>) -> Self {
        let mut routes = Vec::new();
        for (switch, result) in results {
            let SwitchResult::Available { value } = result else {
                continue;
            };
            routes.extend(value.0.into_iter().map(|route| {
                crate::v2026_02_13_01::networking::BgpImported {
                    prefix: route.prefix,
                    nexthop: route.nexthop,
                    id: route.id,
                    switch,
                }
            }));
        }
        routes
    }
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
pub struct UnnumberedInterfaces(pub Vec<UnnumberedInterface>);

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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use sled_agent_types_versions::v1::early_networking::SwitchSlot;

    use super::*;

    #[test]
    fn converts_link_zero_tfport_name() {
        assert_eq!(nexus_interface_name("tfportqsfp0_0".into()), "qsfp0");
    }

    #[test]
    fn preserves_nonzero_and_malformed_tfport_names() {
        assert_eq!(
            nexus_interface_name("tfportqsfp0_1".into()),
            "tfportqsfp0_1"
        );
        assert_eq!(nexus_interface_name("tfport-bad".into()), "tfport-bad");
    }

    #[test]
    fn peer_status_conversion_restores_switch_slot_and_omits_query_failure() {
        let addr = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let results = SwitchResults {
            switch0: SwitchResult::Available {
                value: BgpPeerStatuses(vec![BgpPeerStatus {
                    addr,
                    peer_id: "tfportqsfp0_0".to_owned(),
                    local_asn: 64512,
                    remote_asn: 64513,
                    state: crate::v2025_12_12_00::networking::BgpPeerState::Established,
                    state_duration_millis: 100,
                }]),
            },
            switch1: SwitchResult::Unavailable {
                reason: SwitchUnavailableReason::QueryFailed,
            },
        };

        let statuses: Vec<crate::v2026_02_13_01::networking::BgpPeerStatus> =
            results.into();
        assert_eq!(
            statuses,
            vec![crate::v2026_02_13_01::networking::BgpPeerStatus {
                addr,
                peer_id: "tfportqsfp0_0".to_owned(),
                local_asn: 64512,
                remote_asn: 64513,
                state:
                    crate::v2025_12_12_00::networking::BgpPeerState::Established,
                state_duration_millis: 100,
                switch: SwitchSlot::Switch0,
            }]
        );

        let collision_state_statuses: Vec<
            crate::v2025_12_12_00::networking::BgpPeerStatus,
        > = statuses.into_iter().map(Into::into).collect();
        assert_eq!(collision_state_statuses.len(), 1);
        assert_eq!(collision_state_statuses[0].addr, addr);
        assert_eq!(collision_state_statuses[0].switch, SwitchSlot::Switch0);
    }

    #[test]
    fn exported_routes_conversion_restores_switch_slot_and_omits_unresolved() {
        let prefix = "192.0.2.0/24".parse().unwrap();
        let results = SwitchResults {
            switch0: SwitchResult::Unavailable {
                reason: SwitchUnavailableReason::MgdUnresolved,
            },
            switch1: SwitchResult::Available {
                value: BgpExportedRoutes(vec![BgpExported {
                    peer_id: "peer".to_owned(),
                    prefix,
                }]),
            },
        };

        let routes: Vec<crate::v2026_02_13_01::networking::BgpExported> =
            results.into();
        assert_eq!(
            routes,
            vec![crate::v2026_02_13_01::networking::BgpExported {
                peer_id: "peer".to_owned(),
                switch: SwitchSlot::Switch1,
                prefix,
            }]
        );

        let legacy: crate::v2025_11_20_00::networking::BgpExported =
            routes.into();
        assert_eq!(
            legacy.exports["peer"],
            vec!["192.0.2.0/24".parse().unwrap()]
        );
    }

    #[test]
    fn imported_routes_conversion_uses_switch_order() {
        let route = |id| BgpImported {
            prefix: "192.0.2.0/24".parse().unwrap(),
            nexthop: IpAddr::V4(Ipv4Addr::LOCALHOST),
            id,
        };
        let results = SwitchResults {
            switch0: SwitchResult::Available {
                value: BgpImportedRoutes(vec![route(0)]),
            },
            switch1: SwitchResult::Available {
                value: BgpImportedRoutes(vec![route(1)]),
            },
        };

        let routes: Vec<crate::v2026_02_13_01::networking::BgpImported> =
            results.into();
        assert_eq!(
            routes,
            vec![
                crate::v2026_02_13_01::networking::BgpImported {
                    prefix: "192.0.2.0/24".parse().unwrap(),
                    nexthop: IpAddr::V4(Ipv4Addr::LOCALHOST),
                    id: 0,
                    switch: SwitchSlot::Switch0,
                },
                crate::v2026_02_13_01::networking::BgpImported {
                    prefix: "192.0.2.0/24".parse().unwrap(),
                    nexthop: IpAddr::V4(Ipv4Addr::LOCALHOST),
                    id: 1,
                    switch: SwitchSlot::Switch1,
                },
            ]
        );

        let ipv4_routes: Vec<
            crate::v2025_11_20_00::networking::BgpImportedRouteIpv4,
        > = routes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(ipv4_routes.len(), 2);
        assert_eq!(ipv4_routes[0].switch, SwitchSlot::Switch0);
        assert_eq!(ipv4_routes[1].switch, SwitchSlot::Switch1);
    }

    #[test]
    fn message_history_conversion_omits_unavailable_switch() {
        let results = SwitchResults {
            switch0: SwitchResult::Available {
                value: BgpMessageHistories(HashMap::new()),
            },
            switch1: SwitchResult::Unavailable {
                reason: SwitchUnavailableReason::QueryFailed,
            },
        };

        let history: crate::v2025_11_20_00::networking::AggregateBgpMessageHistory =
            results.into();
        assert_eq!(history.switch_histories.len(), 1);
        assert_eq!(history.switch_histories[0].switch, SwitchSlot::Switch0);
    }

    #[test]
    fn unavailable_reasons_have_distinct_wire_representations() {
        let unresolved = SwitchResult::<BgpPeerStatuses>::Unavailable {
            reason: SwitchUnavailableReason::MgdUnresolved,
        };
        let failed = SwitchResult::<BgpPeerStatuses>::Unavailable {
            reason: SwitchUnavailableReason::QueryFailed,
        };

        assert_eq!(
            serde_json::to_value(unresolved).unwrap(),
            serde_json::json!({
                "status": "unavailable",
                "reason": "mgd_unresolved",
            })
        );
        assert_eq!(
            serde_json::to_value(failed).unwrap(),
            serde_json::json!({
                "status": "unavailable",
                "reason": "query_failed",
            })
        );
    }
}
