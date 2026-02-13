// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Floating IP types for version DUAL_STACK_NICS.
//!
//! This version adds `ip_version` to floating IP creation for pool selection.

use omicron_common::api::external::{
    IdentityMetadataCreateParams, IpVersion, NameOrId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use crate::v2026_01_01_00;

/// Parameters for creating a new floating IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// An IP address to reserve for use as a floating IP.
    pub ip: Option<IpAddr>,
    /// The parent IP pool that a floating IP is pulled from.
    pub pool: Option<NameOrId>,
    /// The IP version preference for address allocation.
    pub ip_version: Option<IpVersion>,
}

impl From<v2026_01_01_00::floating_ip::FloatingIpCreate> for FloatingIpCreate {
    fn from(
        old: v2026_01_01_00::floating_ip::FloatingIpCreate,
    ) -> FloatingIpCreate {
        FloatingIpCreate {
            identity: old.identity,
            ip: old.ip,
            pool: old.pool,
            ip_version: old.ip_version,
        }
    }
}
