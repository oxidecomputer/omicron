// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026010500 (`POOL_SELECTION_ENUMS`) that changed in
//! version 2026011600 (`RENAME_ADDRESS_SELECTOR_TO_ADDRESS_ALLOCATOR`).
//!
//! ## AddressAllocator Rename
//!
//! [`FloatingIpCreate`] has an `address_selector` field with type
//! [`AddressSelector`]. The "selector" naming is a misnomer in our current
//! scheme, where "selector" implies filtering/fetching from existing resources.
//! `RENAME_ADDRESS_SELECTOR_TO_ADDRESS_ALLOCATOR` renames these to
//! `address_allocator` and [`AddressAllocator`], which better describes the
//! action of reserving/assigning a floating IP from a pool.
//!
//! Affected endpoints:
//! - `POST /v1/floating-ips` (floating_ip_create)
//!
//! [`FloatingIpCreate`]: self::FloatingIpCreate
//! [`AddressSelector`]: self::AddressSelector
//! [`AddressAllocator`]: crate::v2026011601::AddressAllocator

use std::net::IpAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2026011601;
use nexus_types::external_api::params;
use omicron_common::api::external::{IdentityMetadataCreateParams, NameOrId};

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

impl From<AddressSelector> for v2026011601::AddressAllocator {
    fn from(value: AddressSelector) -> Self {
        match value {
            AddressSelector::Explicit { ip, pool } => {
                v2026011601::AddressAllocator::Explicit { ip, pool }
            }
            AddressSelector::Auto { pool_selector } => {
                v2026011601::AddressAllocator::Auto { pool_selector }
            }
        }
    }
}

impl From<AddressSelector> for params::AddressAllocator {
    fn from(value: AddressSelector) -> Self {
        v2026011601::AddressAllocator::from(value).into()
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

impl From<FloatingIpCreate> for v2026011601::FloatingIpCreate {
    fn from(value: FloatingIpCreate) -> Self {
        Self {
            identity: value.identity,
            address_allocator: value.address_selector.into(),
        }
    }
}

impl From<FloatingIpCreate> for params::FloatingIpCreate {
    fn from(value: FloatingIpCreate) -> Self {
        v2026011601::FloatingIpCreate::from(value).into()
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

    fn address_selector_strategy() -> impl Strategy<Value = AddressSelector> {
        prop_oneof![
            (any::<IpAddr>(), optional_name_or_id_strategy())
                .prop_map(|(ip, pool)| AddressSelector::Explicit { ip, pool }),
            pool_selector_strategy().prop_map(|pool_selector| {
                AddressSelector::Auto { pool_selector }
            }),
        ]
    }

    fn floating_ip_create_strategy() -> impl Strategy<Value = FloatingIpCreate>
    {
        (identity_strategy(), address_selector_strategy()).prop_map(
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
            AddressSelector::Explicit { ip: expected_ip, .. } => {
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
            AddressSelector::Auto { pool_selector: expected_pool_selector } => {
                match actual.address_allocator {
                    params::AddressAllocator::Auto {
                        pool_selector: actual_pool_selector,
                    } => {
                        prop_assert_eq!(
                            expected_pool_selector,
                            actual_pool_selector
                        );
                    }
                    _ => {
                        return Err(TestCaseError::fail(
                            "expected Auto variant",
                        ));
                    }
                }
            }
        }
    }

    /// Verifies explicit JSON wire format parses into versioned types.
    /// Conversion to latest params is tested by floating_ip_create_converts_correctly.
    #[test]
    fn explicit_json_wire_format() {
        let json =
            r#"{"type": "explicit", "ip": "10.0.0.1", "pool": "my-pool"}"#;

        // Must parse into this version (AddressSelector)
        let v2026011501: AddressSelector = serde_json::from_str(json).unwrap();
        assert!(matches!(v2026011501, AddressSelector::Explicit { .. }));

        // Must also parse into v2026011601 (AddressAllocator)
        let v2026011601: v2026011601::AddressAllocator =
            serde_json::from_str(json).unwrap();
        assert!(matches!(
            v2026011601,
            v2026011601::AddressAllocator::Explicit { .. }
        ));
    }

    /// Verifies auto JSON wire format with explicit pool selector.
    #[test]
    fn auto_explicit_pool_json_wire_format() {
        let json = r#"{"type": "auto", "pool_selector": {"type": "explicit", "pool": "my-pool"}}"#;

        let parsed: AddressSelector = serde_json::from_str(json).unwrap();
        match parsed {
            AddressSelector::Auto { pool_selector } => {
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

        let parsed: AddressSelector = serde_json::from_str(json).unwrap();
        match parsed {
            AddressSelector::Auto { pool_selector } => match pool_selector {
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

        let parsed: AddressSelector = serde_json::from_str(json).unwrap();
        match parsed {
            AddressSelector::Auto { pool_selector } => match pool_selector {
                params::PoolSelector::Auto { ip_version } => {
                    assert_eq!(ip_version, None);
                }
                _ => panic!("Expected Auto pool selector"),
            },
            _ => panic!("Expected Auto variant"),
        }
    }
}
