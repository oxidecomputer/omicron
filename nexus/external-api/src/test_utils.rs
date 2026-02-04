// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common proptest strategies for versioned API type tests.
//!
//! This module provides reusable strategies for generating test data
//! used in property-based tests across version modules.

use nexus_types::external_api::params;
use omicron_common::api::external::{
    IdentityMetadataCreateParams, IpVersion, Name, NameOrId,
};
use proptest::prelude::*;
use std::net::IpAddr;
use uuid::Uuid;

/// Strategy for generating valid `Name` values.
///
/// Per RFD 4, names must be 1-63 characters and match the regex
/// `[a-z]([-a-z0-9]*[a-z0-9])?`. We use `{0,61}` instead of `*` to
/// bound generation length and avoid filtering out long strings.
pub fn name_strategy() -> impl Strategy<Value = Name> {
    "[a-z]([-a-z0-9]{0,61}[a-z0-9])?"
        .prop_filter_map("valid name", |s| s.parse::<Name>().ok())
}

/// Strategy for generating `NameOrId` values.
pub fn name_or_id_strategy() -> impl Strategy<Value = NameOrId> {
    prop_oneof![
        name_strategy().prop_map(NameOrId::Name),
        any::<u128>().prop_map(|n| NameOrId::Id(Uuid::from_u128(n))),
    ]
}

/// Strategy for generating `IpVersion` values.
pub fn ip_version_strategy() -> impl Strategy<Value = IpVersion> {
    prop_oneof![Just(IpVersion::V4), Just(IpVersion::V6)]
}

/// Strategy for generating `PoolSelector` values.
///
/// Generates one of:
/// - `Explicit { pool }`: Use a specific pool identified by name or ID
/// - `Auto { ip_version }`: Use the silo's default pool, optionally filtered
///   by IP version (IPv4/IPv6)
pub fn pool_selector_strategy() -> impl Strategy<Value = params::PoolSelector> {
    prop_oneof![
        name_or_id_strategy()
            .prop_map(|pool| params::PoolSelector::Explicit { pool }),
        proptest::option::of(ip_version_strategy())
            .prop_map(|ip_version| params::PoolSelector::Auto { ip_version }),
    ]
}

/// Strategy for generating `IdentityMetadataCreateParams` values.
///
/// Description is limited to 512 characters in the database.
pub fn identity_strategy() -> impl Strategy<Value = IdentityMetadataCreateParams>
{
    (name_strategy(), ".{0,512}").prop_map(|(name, description)| {
        IdentityMetadataCreateParams { name, description }
    })
}

/// Strategy for generating optional IP addresses.
pub fn optional_ip_strategy() -> impl Strategy<Value = Option<IpAddr>> {
    proptest::option::of(any::<IpAddr>())
}

/// Strategy for generating optional `NameOrId` values.
pub fn optional_name_or_id_strategy() -> impl Strategy<Value = Option<NameOrId>>
{
    proptest::option::of(name_or_id_strategy())
}
