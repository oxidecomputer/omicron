// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast M2P and forwarding types for version `MCAST_UPSTREAM_TYPES`.

use std::net::{IpAddr, Ipv6Addr};

use mg_api_types::mrib::{MulticastAddr, UnderlayMulticastIpv6};
use omicron_common::api::external;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v64;
use crate::v64::multicast::McastForwardingNextHop;

/// Mapping from an overlay multicast group to an underlay multicast address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Mcast2PhysMapping {
    /// Overlay multicast group address.
    pub group: MulticastAddr,
    /// Underlay IPv6 multicast address within ff04::/64.
    pub underlay: UnderlayMulticastIpv6,
}

/// Clear a mapping from an overlay multicast group to an underlay
/// multicast address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ClearMcast2Phys {
    /// Overlay multicast group address.
    pub group: MulticastAddr,
    /// Underlay IPv6 multicast address within ff04::/64.
    pub underlay: UnderlayMulticastIpv6,
}

/// Forwarding entry for an underlay multicast address, specifying
/// which next hops should receive replicated packets.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct McastForwardingEntry {
    /// Underlay IPv6 multicast address within ff04::/64.
    pub underlay: UnderlayMulticastIpv6,
    /// Next hops with replication and source filter configuration.
    pub next_hops: Vec<McastForwardingNextHop>,
}

/// Clear all forwarding entries for an underlay multicast address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ClearMcastForwarding {
    /// Underlay IPv6 multicast address within ff04::/64.
    pub underlay: UnderlayMulticastIpv6,
}

fn group_from_v64(group: IpAddr) -> Result<MulticastAddr, external::Error> {
    MulticastAddr::new(group)
        .map_err(|e| external::Error::invalid_request(e.to_string()))
}

fn underlay_from_v64(
    underlay: Ipv6Addr,
) -> Result<UnderlayMulticastIpv6, external::Error> {
    UnderlayMulticastIpv6::new(underlay)
        .map_err(|e| external::Error::invalid_request(e.to_string()))
}

impl TryFrom<v64::multicast::Mcast2PhysMapping> for Mcast2PhysMapping {
    type Error = external::Error;

    fn try_from(
        old: v64::multicast::Mcast2PhysMapping,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            group: group_from_v64(old.group)?,
            underlay: underlay_from_v64(old.underlay)?,
        })
    }
}

impl From<Mcast2PhysMapping> for v64::multicast::Mcast2PhysMapping {
    fn from(new: Mcast2PhysMapping) -> Self {
        Self { group: new.group.ip(), underlay: new.underlay.ip() }
    }
}

impl TryFrom<v64::multicast::ClearMcast2Phys> for ClearMcast2Phys {
    type Error = external::Error;

    fn try_from(
        old: v64::multicast::ClearMcast2Phys,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            group: group_from_v64(old.group)?,
            underlay: underlay_from_v64(old.underlay)?,
        })
    }
}

impl TryFrom<v64::multicast::McastForwardingEntry> for McastForwardingEntry {
    type Error = external::Error;

    fn try_from(
        old: v64::multicast::McastForwardingEntry,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            underlay: underlay_from_v64(old.underlay)?,
            next_hops: old.next_hops,
        })
    }
}

impl From<McastForwardingEntry> for v64::multicast::McastForwardingEntry {
    fn from(new: McastForwardingEntry) -> Self {
        Self { underlay: new.underlay.ip(), next_hops: new.next_hops }
    }
}

impl TryFrom<v64::multicast::ClearMcastForwarding> for ClearMcastForwarding {
    type Error = external::Error;

    fn try_from(
        old: v64::multicast::ClearMcastForwarding,
    ) -> Result<Self, Self::Error> {
        Ok(Self { underlay: underlay_from_v64(old.underlay)? })
    }
}
