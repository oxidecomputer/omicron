// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP pool types for version POOL_SELECTION_ENUMS.
//!
//! This version introduces `PoolSelector`, a tagged enum for type-safe pool
//! selection that makes invalid states unrepresentable.

use omicron_common::api::external::{IpVersion, NameOrId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Specify which IP pool to allocate from.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PoolSelector {
    /// Use the specified pool by name or ID.
    Explicit {
        /// The pool to allocate from.
        pool: NameOrId,
    },
    /// Use the default pool for the silo.
    Auto {
        /// IP version to use when multiple default pools exist.
        /// Required if both IPv4 and IPv6 default pools are configured.
        #[serde(default)]
        ip_version: Option<IpVersion>,
    },
}

impl Default for PoolSelector {
    fn default() -> Self {
        PoolSelector::Auto { ip_version: None }
    }
}

// Conversion from v2026_01_03_00's flat pool/ip_version fields.
impl TryFrom<(Option<NameOrId>, Option<IpVersion>)> for PoolSelector {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        (pool, ip_version): (Option<NameOrId>, Option<IpVersion>),
    ) -> Result<Self, Self::Error> {
        match (pool, ip_version) {
            // Named pool specified -> ip_version must not be set
            (Some(pool), None) => Ok(PoolSelector::Explicit { pool }),
            // Named pool & ip_version is an invalid combination
            (Some(_), Some(_)) => {
                Err(omicron_common::api::external::Error::invalid_request(
                    "cannot specify both `pool` and `ip_version`; \
                     `ip_version` is only used when allocating from the default pool",
                ))
            }
            // Default pool with optional ip_version preference
            (None, ip_version) => Ok(PoolSelector::Auto { ip_version }),
        }
    }
}

/// Parameters for updating an IP Pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolSiloUpdate {
    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    ///
    /// A silo can have at most one default pool per combination of pool type
    /// (unicast or multicast) and IP version (IPv4 or IPv6), allowing up to 4
    /// default pools total. When a pool is made default, an existing default
    /// of the same type and version will remain linked but will no longer be
    /// the default.
    pub is_default: bool,
}

impl From<crate::v2025_11_20_00::ip_pool::IpPoolSiloUpdate>
    for IpPoolSiloUpdate
{
    fn from(old: crate::v2025_11_20_00::ip_pool::IpPoolSiloUpdate) -> Self {
        IpPoolSiloUpdate { is_default: old.is_default }
    }
}

/// Parameters for linking an IP pool to a silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolLinkSilo {
    pub silo: NameOrId,
    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    ///
    /// A silo can have at most one default pool per combination of pool type
    /// (unicast or multicast) and IP version (IPv4 or IPv6), allowing up to 4
    /// default pools total.
    pub is_default: bool,
}

impl From<crate::v2025_11_20_00::ip_pool::IpPoolLinkSilo> for IpPoolLinkSilo {
    fn from(old: crate::v2025_11_20_00::ip_pool::IpPoolLinkSilo) -> Self {
        IpPoolLinkSilo { silo: old.silo, is_default: old.is_default }
    }
}
