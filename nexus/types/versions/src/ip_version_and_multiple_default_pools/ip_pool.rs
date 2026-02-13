// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP pool types for version IP_VERSION_AND_MULTIPLE_DEFAULT_POOLS.
//!
//! This version's SiloIpPool does not include ip_version or pool_type fields.

use omicron_common::api::external::IdentityMetadata;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
