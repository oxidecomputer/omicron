// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026011600 (`RENAME_ADDRESS_SELECTOR_TO_ADDRESS_ALLOCATOR`)
//! that changed in version 2026012000 (`FLOATING_IP_ALLOCATOR_UPDATE`).
//!
//! These types are also valid through version 2026011601 (`EXTERNAL_SUBNET_ATTACHMENT`),
//! which did not modify them.
//!
//! ## AddressAllocator Changes
//!
//! This version's [`AddressAllocator::Explicit`] has both `ip` and optional `pool` fields.
//! `FLOATING_IP_ALLOCATOR_UPDATE` simplifies `Explicit` to only require `ip` (pool is
//! inferred from the address), with pool-based allocation moving to
//! `Auto(PoolSelector::Explicit { pool })`.
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
            // Pool field is ignored since the IP uniquely identifies the pool
            AddressAllocator::Explicit { ip, pool: _ } => {
                params::AddressAllocator::Explicit { ip }
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
    use omicron_common::api::external::IpVersion;
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
    /// and correctly maps AddressAllocator.
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
            AddressAllocator::Explicit { ip: expected_ip, .. } => {
                match actual.address_allocator {
                    params::AddressAllocator::Explicit { ip } => {
                        prop_assert_eq!(expected_ip, ip);
                    }
                    _ => {
                        return Err(TestCaseError::fail(
                            "expected Explicit variant",
                        ));
                    }
                }
            }
            AddressAllocator::Auto {
                pool_selector: expected_pool_selector,
            } => match actual.address_allocator {
                params::AddressAllocator::Auto {
                    pool_selector: actual_pool_selector,
                } => {
                    prop_assert_eq!(
                        expected_pool_selector,
                        actual_pool_selector
                    );
                }
                _ => {
                    return Err(TestCaseError::fail("expected Auto variant"));
                }
            },
        }
    }

    /// Verifies explicit JSON wire format.
    #[test]
    fn explicit_json_wire_format() {
        let json = r#"{"type": "explicit", "ip": "10.0.0.1"}"#;

        let parsed: AddressAllocator = serde_json::from_str(json).unwrap();
        match parsed {
            AddressAllocator::Explicit { ip, .. } => {
                let expected: IpAddr = "10.0.0.1".parse().unwrap();
                assert_eq!(ip, expected);
            }
            _ => panic!("Expected Explicit variant"),
        }
    }

    /// Verifies auto JSON wire format with explicit pool selector.
    #[test]
    fn auto_explicit_pool_json_wire_format() {
        let json = r#"{"type": "auto", "pool_selector": {"type": "explicit", "pool": "my-pool"}}"#;

        let parsed: AddressAllocator = serde_json::from_str(json).unwrap();
        match parsed {
            AddressAllocator::Auto { pool_selector } => {
                assert!(matches!(
                    pool_selector,
                    params::PoolSelector::Explicit { .. }
                ));
            }
            _ => panic!("Expected Auto variant"),
        }
    }

    /// Verifies auto JSON wire format with auto pool selector.
    #[test]
    fn auto_auto_pool_json_wire_format() {
        let json = r#"{"type": "auto", "pool_selector": {"type": "auto", "ip_version": "v4"}}"#;

        let parsed: AddressAllocator = serde_json::from_str(json).unwrap();
        match parsed {
            AddressAllocator::Auto { pool_selector } => match pool_selector {
                params::PoolSelector::Auto { ip_version } => {
                    assert_eq!(ip_version, Some(IpVersion::V4));
                }
                _ => panic!("Expected Auto pool selector"),
            },
            _ => panic!("Expected Auto variant"),
        }
    }

    /// Verifies auto JSON wire format with default pool selector.
    #[test]
    fn auto_default_json_wire_format() {
        let json = r#"{"type": "auto"}"#;

        let parsed: AddressAllocator = serde_json::from_str(json).unwrap();
        match parsed {
            AddressAllocator::Auto { pool_selector } => match pool_selector {
                params::PoolSelector::Auto { ip_version } => {
                    assert_eq!(ip_version, None);
                }
                _ => panic!("Expected Auto pool selector"),
            },
            _ => panic!("Expected Auto variant"),
        }
    }
}
