// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus types for external APIs related to multirack.
//!
//! Right now these are mostly just copies of the discovered rack and sled types
//! defined in the Nexus inventory types; that's where we're getting the data
//! that this API returns, so it makes sense that the types are pretty much the
//! same.
//!
//! Why not simply use the same type? Well, we want to use some subtly different
//! field types in the external API types vs the internal inventory types, and
//! using a separate type here may also prove useful in the future when it comes
//! to API versioning.
//!
//! The different type name also helps convey that we're filtering out unadopted
//! racks and sleds in the API response: even though the fields are basically
//! the same, we won't simply be returning the full contents of the discovered
//! rack field in the inventory collection.

use std::net::Ipv6Addr;

use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use sled_agent_types_versions::latest::multirack::SledAgentStartState;

/// The external Nexus API type returned for queries of unadopted racks.
///
/// As of this writing, these fields are created pretty directly from the
/// corresponding `DiscoveredRack` type in the inventory code, though it is
/// entirely possible that could change in the future; we might add more
/// internal fields to the inventory types that are used for other purposes,
/// while this external API type remains the same.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct UnadoptedRack {
    pub rack_id: Uuid,
    pub rack_subnet: Ipv6Net,
    pub join_service_ip: Ipv6Addr,

    pub unadopted_sleds: Vec<UnadoptedSled>,
}

/// The external Nexus API type nested inside of [`UnadoptedRack`] to convey
/// information on individual discovered sleds in that rack.
///
/// As of this writing, these fields are created pretty directly from the
/// corresponding `DiscoveredSled` type in the inventory code, though it is
/// entirely possible that could change in the future; we might add more
/// internal fields to the inventory types that are used for other purposes,
/// while this external API type remains the same.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct UnadoptedSled {
    pub sled_id: Uuid,

    pub part_number: String,
    pub serial_number: String,
    pub sled_subnet: Ipv6Net,

    pub sled_agent_start_state: SledAgentStartState,
}
