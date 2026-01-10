// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Floating IP types for version SILO_PROJECT_IP_VERSION_AND_POOL_TYPE.
//!
//! This version has floating IP creation with pool only (no ip_version).

use omicron_common::api::external::{IdentityMetadataCreateParams, NameOrId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use crate::v2025112000;

/// Parameters for creating a new floating IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// An IP address to reserve for use as a floating IP.
    pub ip: Option<IpAddr>,
    /// The parent IP pool that a floating IP is pulled from.
    pub pool: Option<NameOrId>,
}

impl From<v2025112000::floating_ip::FloatingIpCreate> for FloatingIpCreate {
    fn from(
        old: v2025112000::floating_ip::FloatingIpCreate,
    ) -> FloatingIpCreate {
        FloatingIpCreate { identity: old.identity, ip: old.ip, pool: old.pool }
    }
}
