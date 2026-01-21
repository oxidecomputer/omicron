// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2026011501 to 2026011600.
//!
//! ## AddressSelector -> AddressAllocator Rename
//!
//! [`FloatingIpCreate`] prior to v2026011600 has an `address_selector` field
//! with type [`AddressSelector`]. This is a bit of a misnomer in our current
//! scheme, where "selector" implies filtering/fetching from existing resources.
//! Newer versions use `address_allocator` field with type [`AddressAllocator`],
//! which better describes the action of reserving/assigning a floating IP
//! address from a pool.
//!
//! Affected endpoints:
//! - `POST /v1/floating-ips` (floating_ip_create)
//!
//! [`FloatingIpCreate`]: self::FloatingIpCreate
//! [`AddressSelector`]: self::AddressSelector
//! [`AddressAllocator`]: nexus_types::external_api::params::AddressAllocator

use nexus_types::external_api::params;
use omicron_common::api::external::{IdentityMetadataCreateParams, NameOrId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

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
        pool_selector: params::PoolSelector,
    },
}

impl Default for AddressSelector {
    fn default() -> Self {
        AddressSelector::Auto { pool_selector: params::PoolSelector::default() }
    }
}

impl From<AddressSelector> for params::AddressAllocator {
    fn from(value: AddressSelector) -> Self {
        match value {
            AddressSelector::Explicit { ip, pool } => {
                params::AddressAllocator::Explicit { ip, pool }
            }
            AddressSelector::Auto { pool_selector } => {
                params::AddressAllocator::Auto { pool_selector }
            }
        }
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

impl From<FloatingIpCreate> for params::FloatingIpCreate {
    fn from(value: FloatingIpCreate) -> Self {
        Self {
            identity: value.identity,
            address_allocator: value.address_selector.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        identity_strategy, optional_name_or_id_strategy, pool_selector_strategy,
    };
    use proptest::prelude::*;
    use std::net::IpAddr;
    use test_strategy::proptest;

    fn floating_ip_create_strategy() -> impl Strategy<Value = FloatingIpCreate>
    {
        let address_selector = prop_oneof![
            (any::<IpAddr>(), optional_name_or_id_strategy())
                .prop_map(|(ip, pool)| AddressSelector::Explicit { ip, pool }),
            pool_selector_strategy().prop_map(|pool_selector| {
                AddressSelector::Auto { pool_selector }
            }),
        ];

        (identity_strategy(), address_selector).prop_map(
            |(identity, address_selector)| FloatingIpCreate {
                identity,
                address_selector,
            },
        )
    }

    /// Verifies that conversion to params::FloatingIpCreate preserves identity
    /// and correctly maps AddressSelector to AddressAllocator.
    #[proptest]
    fn floating_ip_create_converts_correctly(
        #[strategy(floating_ip_create_strategy())] expected: FloatingIpCreate,
    ) {
        use proptest::test_runner::TestCaseError;
        let actual: params::FloatingIpCreate = expected.clone().into();

        prop_assert_eq!(expected.identity.name, actual.identity.name);
        prop_assert_eq!(
            expected.identity.description,
            actual.identity.description
        );

        match expected.address_selector {
            AddressSelector::Explicit {
                ip: expected_ip,
                pool: expected_pool,
            } => {
                let params::AddressAllocator::Explicit {
                    ip: actual_ip,
                    pool: actual_pool,
                } = actual.address_allocator
                else {
                    return Err(TestCaseError::fail(
                        "expected Explicit variant",
                    ));
                };
                prop_assert_eq!(expected_ip, actual_ip);
                prop_assert_eq!(expected_pool, actual_pool);
            }
            AddressSelector::Auto { pool_selector: expected_pool_selector } => {
                let params::AddressAllocator::Auto {
                    pool_selector: actual_pool_selector,
                } = actual.address_allocator
                else {
                    return Err(TestCaseError::fail("expected Auto variant"));
                };
                prop_assert_eq!(expected_pool_selector, actual_pool_selector);
            }
        }
    }
}
