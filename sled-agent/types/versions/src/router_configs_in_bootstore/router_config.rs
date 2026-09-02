// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Router-configuration specs carried in the bootstore (RFD 662).
//!
//! Nexus renders each operator `RouterConfiguration` (plus the built-in
//! per-switch ones, when non-empty) into a [`RouterConfigSpec`] and stores
//! them in the [`SystemNetworkingConfig`] per switch. The scrimlet mgd
//! reconciler combines them with the uplink config from
//! `RackNetworkConfig` and sends one declarative apply to its switch's mgd.
//!
//! These are omicron-owned mirrors of maghemite's v13 `RouterSpec` tree:
//! the bootstore blob must never embed another repo's API types. Values
//! mgd needs but the control plane does not decide (listener groups, BGP
//! session resolution, jitter, the `[::]:179` listen address, tfport
//! interface naming) are filled in by the scrimlet reconciler when it
//! converts these to mg-api types.
//!
//! [`SystemNetworkingConfig`]: super::system_networking::SystemNetworkingConfig

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv6Addr};
use std::num::NonZeroU8;

use oxnet::{IpNet, Ipv4Net, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v1::early_networking::{BfdMode, SwitchSlot};

/// Complete desired state for one named router on one switch's mgd.
///
/// A spec named "default" configures the daemon-owned default router in
/// place; any other name creates/updates an operator router keyed by `id`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RouterConfigSpec {
    pub name: String,
    /// Scopes the router's persistent state and platform programming
    /// (ignored by mgd for the "default" router).
    pub id: Uuid,
    /// BGP configuration (None = no BGP for this router).
    pub bgp: Option<RouterConfigBgpSpec>,
    /// Complete set of IPv4 static routes.
    pub static4: Vec<RouterConfigStaticRoute4>,
    /// Complete set of IPv6 static routes.
    pub static6: Vec<RouterConfigStaticRoute6>,
    /// Complete set of BFD peers.
    pub bfd_peers: Vec<RouterConfigBfdPeer>,
}

impl RouterConfigSpec {
    pub fn is_empty(&self) -> bool {
        self.bgp.is_none()
            && self.static4.is_empty()
            && self.static6.is_empty()
            && self.bfd_peers.is_empty()
    }
}

/// BGP configuration for one router.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RouterConfigBgpSpec {
    pub asn: u32,
    /// Complete set of prefixes to originate.
    pub originate: Vec<IpNet>,
    /// Maximum number of equal-cost paths installed per destination.
    /// Absent means 1: a single best path.
    pub max_paths: Option<NonZeroU8>,
    /// Checker rhai code applied to ingress open and update messages.
    pub checker: Option<String>,
    /// Shaper rhai code applied to egress open and update messages.
    pub shaper: Option<String>,
    pub peers: Vec<RouterConfigBgpPeer>,
    pub unnumbered_peers: Vec<RouterConfigUnnumberedBgpPeer>,
}

/// A numbered BGP peer: the session runs to `addr` on the standard port.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RouterConfigBgpPeer {
    pub name: String,
    pub addr: IpAddr,
    #[serde(flatten)]
    pub parameters: RouterConfigBgpPeerParameters,
}

/// An unnumbered BGP peer: the session runs over a switch port's
/// link-local address, discovered via router advertisements.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RouterConfigUnnumberedBgpPeer {
    pub name: String,
    /// Switch port (e.g. "qsfp0"); the reconciler derives the tfport
    /// interface name from it.
    pub port: String,
    /// Lifetime (seconds) advertised in our router advertisements on this
    /// interface.
    pub router_lifetime: u16,
    #[serde(flatten)]
    pub parameters: RouterConfigBgpPeerParameters,
}

/// Session parameters shared by numbered and unnumbered BGP peers. These
/// are the knobs the control plane decides; daemon tunables (resolution,
/// jitter, collision resolution) are set by the scrimlet reconciler.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RouterConfigBgpPeerParameters {
    pub hold_time: u64,
    pub idle_hold_time: u64,
    pub delay_open: u64,
    pub connect_retry: u64,
    pub keepalive: u64,
    pub remote_asn: Option<u32>,
    pub min_ttl: Option<u8>,
    pub md5_auth_key: Option<String>,
    pub multi_exit_discriminator: Option<u32>,
    pub communities: Vec<u32>,
    pub local_pref: Option<u32>,
    pub enforce_first_as: bool,
    pub vlan_id: Option<u16>,
    /// Prefixes allowed in from this peer (None = no filtering). Applies
    /// to both address families; each family keeps its matching subset.
    pub allowed_import: Option<Vec<IpNet>>,
    /// Prefixes allowed out to this peer (None = no filtering).
    pub allowed_export: Option<Vec<IpNet>>,
}

/// An IPv4 static route (the nexthop may be IPv6: v4-over-v6 routing).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RouterConfigStaticRoute4 {
    pub prefix: Ipv4Net,
    pub nexthop: IpAddr,
    pub vlan_id: Option<u16>,
    pub rib_priority: u8,
}

/// An IPv6 static route.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RouterConfigStaticRoute6 {
    pub prefix: Ipv6Net,
    pub nexthop: Ipv6Addr,
    pub vlan_id: Option<u16>,
    pub rib_priority: u8,
}

/// A BFD peer attached to a router.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RouterConfigBfdPeer {
    pub peer: IpAddr,
    pub listen: IpAddr,
    /// Minimum interval (microseconds) between packets we require from
    /// the peer.
    pub required_rx: u64,
    /// Missed packets before the session is declared down.
    pub detection_threshold: NonZeroU8,
    pub mode: BfdMode,
}

/// One entry of a port's prioritized tunnel-router list (RFD 662). Mirrors
/// `omicron_common::api::internal::shared::RouterListEntry` for the
/// on-disk blob (the name differs so the two types keep distinct schema
/// names in the sled-agent OpenAPI document).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RouterConfigListEntry {
    /// Lower value wins length ties during boundary lookup.
    pub priority: u16,
    /// `None` selects the default router.
    pub router_id: Option<Uuid>,
}

/// Priority given to the default router in a defaulted router list.
pub const DEFAULT_ROUTER_LIST_PRIORITY: u16 = 1000;

/// The router list used when no control-plane list has been configured:
/// just the default router. Boot-critical service ports (boundary NTP)
/// need working egress before nexus ever writes a list.
pub fn default_router_list() -> Vec<RouterConfigListEntry> {
    vec![RouterConfigListEntry {
        priority: DEFAULT_ROUTER_LIST_PRIORITY,
        router_id: None,
    }]
}

/// Operator router specs per switch. Absent switches mean an empty spec
/// list: the reconciler still sends an apply so removed routers are torn
/// down.
pub type SwitchRouterConfigs = BTreeMap<SwitchSlot, Vec<RouterConfigSpec>>;
