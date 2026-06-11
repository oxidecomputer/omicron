// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2026 Oxide Computer Company

//! Multicast networking types for the sled-agent API.
//!
//! These types support overlay-to-underlay multicast mapping and
//! multicast forwarding configuration via OPTE. The underlay address
//! space is ff04::/64, a subset of admin-local scope per
//! [RFC 7346](https://www.rfc-editor.org/rfc/rfc7346).

use std::net::IpAddr;
use std::net::Ipv6Addr;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Mapping from an overlay multicast group to an underlay multicast
/// address.
///
/// The underlay address must be within the underlay multicast subnet
/// (ff04::/64). This invariant is enforced by mapping in Nexus, not
/// validated at this layer.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Mcast2PhysMapping {
    /// Overlay multicast group address.
    pub group: IpAddr,
    /// Underlay IPv6 multicast address (ff04::/64).
    pub underlay: Ipv6Addr,
}

/// Clear a mapping from an overlay multicast group to an underlay
/// multicast address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ClearMcast2Phys {
    /// Overlay multicast group address.
    pub group: IpAddr,
    /// Underlay IPv6 multicast address. See [`Mcast2PhysMapping::underlay`].
    pub underlay: Ipv6Addr,
}

/// Forwarding entry for an underlay multicast address, specifying
/// which next hops should receive replicated packets.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct McastForwardingEntry {
    /// Underlay IPv6 multicast address. See [`Mcast2PhysMapping::underlay`].
    pub underlay: Ipv6Addr,
    /// Next hops with replication and source filter configuration.
    pub next_hops: Vec<McastForwardingNextHop>,
}

/// Clear all forwarding entries for an underlay multicast address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ClearMcastForwarding {
    /// Underlay IPv6 multicast address. See [`Mcast2PhysMapping::underlay`].
    pub underlay: Ipv6Addr,
}

/// A forwarding next hop with replication mode and aggregated
/// source filter.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct McastForwardingNextHop {
    /// Unicast IPv6 address of the destination sled.
    pub next_hop: Ipv6Addr,
    /// Replication mode for this next hop.
    pub replication: McastReplication,
    /// Aggregated source filter for this destination.
    pub filter: McastSourceFilter,
}

/// Replication mode for multicast forwarding.
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum McastReplication {
    /// Replicate to front panel ports (egress to external networks).
    External,
    /// Replicate to sled underlay ports.
    Underlay,
    /// Replicate to both external and underlay ports.
    Both,
}

/// Source filter for multicast forwarding.
#[derive(
    Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct McastSourceFilter {
    /// Filter mode.
    pub mode: McastFilterMode,
    /// Source addresses to include or exclude.
    pub sources: Vec<IpAddr>,
}

/// Filter mode for multicast source filtering.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum McastFilterMode {
    /// Accept only packets from listed sources (SSM).
    Include,
    /// Accept packets from all sources except those listed.
    /// With an empty sources list this is any-source multicast (ASM).
    #[default]
    Exclude,
}

/// Declarative multicast group subscription for an OPTE port.
///
/// Represents a single group membership with optional source filtering.
/// Empty `sources` means any-source multicast (ASM) and non-empty means
/// source-specific multicast (SSM).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct MulticastGroupCfg {
    /// The multicast group IP address (IPv4 or IPv6).
    pub group_ip: IpAddr,
    /// Source addresses for source-filtered multicast.
    pub sources: Vec<IpAddr>,
}
