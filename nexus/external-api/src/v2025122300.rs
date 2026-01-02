// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2025122300 to 2026010100.
//!
//! Version 2025122300 types (before `ip_version` and `pool_type` were added
//! to `SiloIpPool` responses).
//!
//! Key differences from newer API versions:
//! - [`SiloIpPool`] doesn't have `ip_version` or `pool_type` fields.
//!   Newer versions include these fields to indicate the IP version
//!   and pool type (unicast or multicast) of the pool.
//!
//! Affected endpoints:
//! - `GET /v1/ip-pools` (project_ip_pool_list)
//! - `GET /v1/ip-pools/{pool}` (project_ip_pool_view)
//! - `GET /v1/system/silos/{silo}/ip-pools` (silo_ip_pool_list)
//!
//! [`SiloIpPool`]: self::SiloIpPool

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use nexus_types::external_api::views;
use omicron_common::api::external::IdentityMetadata;

/// An IP pool in the context of a silo (pre-2026010100 API version).
///
/// This version does not include `ip_version` or `pool_type` fields.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloIpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    /// There can be at most one default for a given silo.
    pub is_default: bool,
}

impl From<views::SiloIpPool> for SiloIpPool {
    fn from(new: views::SiloIpPool) -> SiloIpPool {
        SiloIpPool { identity: new.identity, is_default: new.is_default }
    }
}
