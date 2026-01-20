// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026011600 (`RENAME_ADDRESS_SELECTOR_TO_ADDRESS_ALLOCATOR`)
//! that changed in version 2026012000 (`ADDRESS_ALLOCATOR_OPTIONAL_IP`).
//!
//! ## Explicit Allocation Changes
//!
//! [`AddressAllocator::Explicit`] requires the `ip` field.
//! `ADDRESS_ALLOCATOR_OPTIONAL_IP` makes `ip` optional, allowing allocation
//! from a pool without specifying an IP.
//!
//! Affected endpoints:
//! - `POST /v1/floating-ips` (floating_ip_create)
//!
//! [`AddressAllocator::Explicit`]: self::AddressAllocator::Explicit

use std::net::IpAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use nexus_types::external_api::params;
use omicron_common::api::external::{IdentityMetadataCreateParams, NameOrId};

/// Specify how to allocate a floating IP address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AddressAllocator {
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

impl Default for AddressAllocator {
    fn default() -> Self {
        AddressAllocator::Auto {
            pool_selector: params::PoolSelector::default(),
        }
    }
}

impl From<AddressAllocator> for params::AddressAllocator {
    fn from(value: AddressAllocator) -> Self {
        match value {
            AddressAllocator::Explicit { ip, pool } => {
                params::AddressAllocator::Explicit(params::ExplicitAllocation {
                    ip: Some(ip),
                    pool,
                })
            }
            AddressAllocator::Auto { pool_selector } => {
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
    pub address_allocator: AddressAllocator,
}

impl From<FloatingIpCreate> for params::FloatingIpCreate {
    fn from(value: FloatingIpCreate) -> Self {
        Self {
            identity: value.identity,
            address_allocator: value.address_allocator.into(),
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

    fn address_allocator_strategy() -> impl Strategy<Value = AddressAllocator> {
        prop_oneof![
            (any::<IpAddr>(), optional_name_or_id_strategy())
                .prop_map(|(ip, pool)| AddressAllocator::Explicit { ip, pool }),
            pool_selector_strategy().prop_map(|pool_selector| {
                AddressAllocator::Auto { pool_selector }
            }),
        ]
    }

    fn floating_ip_create_strategy() -> impl Strategy<Value = FloatingIpCreate>
    {
        (identity_strategy(), address_allocator_strategy()).prop_map(
            |(identity, address_allocator)| FloatingIpCreate {
                identity,
                address_allocator,
            },
        )
    }

    /// Verifies that conversion to params::FloatingIpCreate preserves identity
    /// and correctly maps AddressAllocator with required ip to AddressAllocator
    /// with optional ip.
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

        match expected.address_allocator {
            AddressAllocator::Explicit {
                ip: expected_ip,
                pool: expected_pool,
            } => {
                let params::AddressAllocator::Explicit(explicit) =
                    actual.address_allocator
                else {
                    return Err(TestCaseError::fail(
                        "expected Explicit variant",
                    ));
                };
                prop_assert_eq!(Some(expected_ip), explicit.ip);
                prop_assert_eq!(expected_pool, explicit.pool);
            }
            AddressAllocator::Auto {
                pool_selector: expected_pool_selector,
            } => {
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

    /// Verifies that JSON serialized from this version can be deserialized into
    /// the latest params version.
    #[proptest]
    fn wire_format_compatible_with_latest(
        #[strategy(address_allocator_strategy())] allocator: AddressAllocator,
    ) {
        let json = serde_json::to_string(&allocator).unwrap();
        let parsed: params::AddressAllocator =
            serde_json::from_str(&json).unwrap();

        match allocator {
            AddressAllocator::Explicit { ip, pool } => {
                let params::AddressAllocator::Explicit(explicit) = parsed
                else {
                    panic!("Expected Explicit variant");
                };
                assert_eq!(Some(ip), explicit.ip);
                assert_eq!(pool, explicit.pool);
            }
            AddressAllocator::Auto { pool_selector } => {
                let params::AddressAllocator::Auto { pool_selector: actual } =
                    parsed
                else {
                    panic!("Expected Auto variant");
                };
                assert_eq!(pool_selector, actual);
            }
        }
    }

    /// Verifies explicit JSON wire format with required ip.
    #[test]
    fn explicit_json_wire_format() {
        let json =
            r#"{"type": "explicit", "ip": "10.0.0.1", "pool": "my-pool"}"#;

        // Must parse into this version
        let v2026011600: AddressAllocator = serde_json::from_str(json).unwrap();
        assert!(matches!(v2026011600, AddressAllocator::Explicit { .. }));

        // Must also parse into latest params
        let latest: params::AddressAllocator =
            serde_json::from_str(json).unwrap();
        let params::AddressAllocator::Explicit(explicit) = latest else {
            panic!("Expected Explicit variant");
        };
        assert_eq!(explicit.ip, Some("10.0.0.1".parse().unwrap()));
        assert_eq!(
            explicit.pool,
            Some(NameOrId::Name("my-pool".parse().unwrap()))
        );
    }
}
