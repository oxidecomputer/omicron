// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Floating IP types for version BGP_PEER_COLLISION_STATE.
//!
//! This version has FloatingIpCreate without the `ip_version` field.

use omicron_common::api::external::{IdentityMetadataCreateParams, NameOrId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use crate::v2025112000;

/// Parameters for creating a new floating IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// An IP address to reserve for use as a floating IP. This field is
    /// optional: when not set, an address will be automatically chosen from
    /// `pool`. If set, then the IP must be available in the resolved `pool`.
    pub ip: Option<IpAddr>,
    /// The parent IP pool that a floating IP is pulled from. If unset, the
    /// default pool is selected.
    pub pool: Option<NameOrId>,
}

impl From<v2025112000::floating_ip::FloatingIpCreate> for FloatingIpCreate {
    fn from(
        old: v2025112000::floating_ip::FloatingIpCreate,
    ) -> FloatingIpCreate {
        FloatingIpCreate { identity: old.identity, ip: old.ip, pool: old.pool }
    }
}
