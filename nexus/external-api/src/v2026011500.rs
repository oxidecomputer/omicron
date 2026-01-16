// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2026011500 to 2026011600.
//!
//! ## AddressSelector -> AddressAllocator Rename
//!
//! [`FloatingIpCreate`] prior to v2026011600 has an `address_selector` field
//! with type [`AddressSelector`]. This is a bit of a misnomer in our current
//! scheme, where "selector" implies filtering/fetching from existing resources.
//! Newer versions use `address_allocator` field with type `AddressAllocator`,
//! which better describes the action of reserving/assigning a floating IP
//! address from a pool.
//!
//! Affected endpoints:
//! - `POST /v1/floating-ips` (floating_ip_create)
//!
//! [`FloatingIpCreate`]: self::FloatingIpCreate
//! [`AddressSelector`]: self::AddressSelector

use nexus_types::external_api::params;
use omicron_common::api::external::{IdentityMetadataCreateParams, NameOrId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Specify how to allocate a floating IP address.
///
/// This is the old name for what is now called `AddressAllocator`.
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
///
/// This version uses `address_selector` field instead of `address_allocator`
/// in newer versions.
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
    use omicron_common::api::external::{IpVersion, Name};
    use proptest::prelude::*;
    use test_strategy::proptest;
    use uuid::Uuid;

    // =========================================================================
    // Proptest strategies
    // =========================================================================
    //
    // TODO: Add proptests for the conversions in all of the other version
    // modules, and extract common strategies into a separate test-utils module.
    //
    // Alternatively, consider adding `#[cfg_attr(any(test, feature = "testing"),
    // derive(test_strategy::Arbitrary))]` to types like `PoolSelector`,
    // `NameOrId`, and `Name` in their defining crates. This pattern is used
    // elsewhere in omicron (e.g., `Generation` in omicron-common, `BaseboardId`
    // in sled-hardware/types) with a `testing` feature that enables `proptest`
    // and `test-strategy` dependencies.

    /// Strategy for generating valid `Name` values.
    ///
    /// Per RFD 4, names must be 1-63 characters and match the regex
    /// `[a-z]([-a-z0-9]*[a-z0-9])?`. We use `{0,61}` instead of `*` to
    /// bound generation length and avoid filtering out long strings.
    fn name_strategy() -> impl Strategy<Value = Name> {
        "[a-z]([-a-z0-9]{0,61}[a-z0-9])?"
            .prop_filter_map("valid name", |s| s.parse::<Name>().ok())
    }

    fn name_or_id_strategy() -> impl Strategy<Value = NameOrId> {
        prop_oneof![
            name_strategy().prop_map(NameOrId::Name),
            any::<u128>().prop_map(|n| NameOrId::Id(Uuid::from_u128(n))),
        ]
    }

    fn ip_version_strategy() -> impl Strategy<Value = IpVersion> {
        prop_oneof![Just(IpVersion::V4), Just(IpVersion::V6)]
    }

    fn pool_selector_strategy() -> impl Strategy<Value = params::PoolSelector> {
        prop_oneof![
            name_or_id_strategy()
                .prop_map(|pool| params::PoolSelector::Explicit { pool }),
            proptest::option::of(ip_version_strategy()).prop_map(
                |ip_version| params::PoolSelector::Auto { ip_version }
            ),
        ]
    }

    fn address_selector_strategy() -> impl Strategy<Value = AddressSelector> {
        prop_oneof![
            (any::<IpAddr>(), proptest::option::of(name_or_id_strategy()))
                .prop_map(|(ip, pool)| AddressSelector::Explicit { ip, pool }),
            pool_selector_strategy().prop_map(|pool_selector| {
                AddressSelector::Auto { pool_selector }
            }),
        ]
    }

    fn identity_strategy() -> impl Strategy<Value = IdentityMetadataCreateParams>
    {
        // Description is limited to 512 characters in the database.
        (name_strategy(), ".{0,512}").prop_map(|(name, description)| {
            IdentityMetadataCreateParams { name, description }
        })
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

    // =========================================================================
    // Property tests
    // =========================================================================

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
