// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Floating IP types for version POOL_SELECTION_ENUMS.
//!
//! This version introduces `AddressSelector`, a tagged enum for type-safe
//! IP address allocation that makes invalid states unrepresentable.

use omicron_common::api::external::{IdentityMetadataCreateParams, NameOrId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use super::ip_pool::PoolSelector;
use crate::v2025121200;
use crate::v2026010100;
use crate::v2026010300;

/// Specify how to allocate a floating IP address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AddressSelector {
    /// Reserve a specific IP address.
    Explicit {
        /// The IP address to reserve. Must be available in the pool.
        ip: IpAddr,
        /// The pool containing this address. If not specified, the default
        /// pool for the address's IP version is used.
        pool: Option<NameOrId>,
    },
    /// Automatically allocate an IP address from a specified pool.
    Auto {
        /// Pool selection.
        ///
        /// If omitted, this field uses the silo's default pool. If the
        /// silo has default pools for both IPv4 and IPv6, the request will
        /// fail unless `ip_version` is specified in the pool selector.
        #[serde(default)]
        pool_selector: PoolSelector,
    },
}

impl Default for AddressSelector {
    fn default() -> Self {
        AddressSelector::Auto { pool_selector: PoolSelector::default() }
    }
}

/// Parameters for creating a new floating IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// IP address allocation method.
    #[serde(default)]
    pub address_selector: AddressSelector,
}

impl TryFrom<v2026010300::floating_ip::FloatingIpCreate> for FloatingIpCreate {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        old: v2026010300::floating_ip::FloatingIpCreate,
    ) -> Result<Self, Self::Error> {
        let address_selector = match (old.ip, old.pool, old.ip_version) {
            // Explicit IP address provided -> ip_version must not be set
            (Some(ip), pool, None) => AddressSelector::Explicit { ip, pool },
            // Explicit IP and ip_version is an invalid combination
            (Some(_), _, Some(_)) => {
                return Err(
                    omicron_common::api::external::Error::invalid_request(
                        "cannot specify both `ip` and `ip_version`; \
                     the IP version is determined by the explicit IP address",
                    ),
                );
            }
            // No explicit IP, but named pool specified -> ip_version must not be set
            (None, Some(pool), None) => AddressSelector::Auto {
                pool_selector: PoolSelector::Explicit { pool },
            },
            // Named pool and ip_version is an invalid combination
            (None, Some(_), Some(_)) => {
                return Err(
                    omicron_common::api::external::Error::invalid_request(
                        "cannot specify both `pool` and `ip_version`; \
                     `ip_version` is only used when allocating from the default pool",
                    ),
                );
            }
            // Allocate from default pool with optional IP version preference
            (None, None, ip_version) => AddressSelector::Auto {
                pool_selector: PoolSelector::Auto { ip_version },
            },
        };
        Ok(FloatingIpCreate { identity: old.identity, address_selector })
    }
}

// Direct conversion from v2026010100 (chains through v2026010300)
impl TryFrom<v2026010100::floating_ip::FloatingIpCreate> for FloatingIpCreate {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        old: v2026010100::floating_ip::FloatingIpCreate,
    ) -> Result<Self, Self::Error> {
        let intermediate: v2026010300::floating_ip::FloatingIpCreate =
            old.into();
        intermediate.try_into()
    }
}

// Direct conversion from v2025121200 (chains through v2026010100 → v2026010300)
impl TryFrom<v2025121200::floating_ip::FloatingIpCreate> for FloatingIpCreate {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        old: v2025121200::floating_ip::FloatingIpCreate,
    ) -> Result<Self, Self::Error> {
        let v2026010100: v2026010100::floating_ip::FloatingIpCreate =
            old.into();
        v2026010100.try_into()
    }
}
