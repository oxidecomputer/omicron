// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2025121200 (BGP_PEER_COLLISION_STATE) that changed in
//! subsequent versions.
//!
//! Types before `ip_version` preference was added for default IP pool selection.
//!
//! ## IP Pool Selection Types
//!
//! Valid until 2025122300 (IP_VERSION_AND_MULTIPLE_DEFAULT_POOLS).
//!
//! [`FloatingIpCreate`] and [`EphemeralIpCreate`] don't have the `ip_version`
//! field. Newer versions allow specifying IPv4/IPv6 preference when allocating
//! from default pools.
//!
//! ## Multicast Types
//!
//! Valid until 2026010800 (MULTICAST_IMPLICIT_LIFECYCLE_UPDATES).
//!
//! Multicast types are re-exported from `v2025122300`.
//!
//! [`FloatingIpCreate`]: self::FloatingIpCreate
//! [`EphemeralIpCreate`]: self::EphemeralIpCreate

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use nexus_types::external_api::params;
use omicron_common::api::external;

use crate::v2026010300;

// Re-export multicast types from v2025122300.
// They're identical for both versions (both use NameOrId, explicit
// create/update, no source_ips per member)
pub use super::v2025122300::{
    InstanceMulticastGroupPath, InstanceUpdate, MulticastGroup,
    MulticastGroupByIpPath, MulticastGroupCreate, MulticastGroupMember,
    MulticastGroupMemberAdd, MulticastGroupMemberPath, MulticastGroupPath,
    MulticastGroupUpdate,
};

/// Parameters for creating an ephemeral IP address for an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct EphemeralIpCreate {
    /// Name or ID of the IP pool used to allocate an address.
    /// If unspecified, the default IP pool will be used.
    pub pool: Option<external::NameOrId>,
}

// Converts directly to params::EphemeralIpCreate using PoolSelector
impl From<EphemeralIpCreate> for params::EphemeralIpCreate {
    fn from(old: EphemeralIpCreate) -> params::EphemeralIpCreate {
        let pool_selector = match old.pool {
            Some(pool) => params::PoolSelector::Explicit { pool },
            None => params::PoolSelector::Auto { ip_version: None },
        };
        params::EphemeralIpCreate { pool_selector }
    }
}

/// Parameters for creating a new floating IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: external::IdentityMetadataCreateParams,
    /// An IP address to reserve for use as a floating IP. This field is
    /// optional: when not set, an address will be automatically chosen from
    /// `pool`. If set, then the IP must be available in the resolved `pool`.
    pub ip: Option<std::net::IpAddr>,
    /// The parent IP pool that a floating IP is pulled from. If unset, the
    /// default pool is selected.
    pub pool: Option<external::NameOrId>,
}

// Converts to v2026010300::FloatingIpCreate (adds ip_version: None)
impl From<FloatingIpCreate> for v2026010300::FloatingIpCreate {
    fn from(old: FloatingIpCreate) -> v2026010300::FloatingIpCreate {
        v2026010300::FloatingIpCreate {
            identity: old.identity,
            ip: old.ip,
            pool: old.pool,
            ip_version: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        identity_strategy, optional_ip_strategy, optional_name_or_id_strategy,
    };
    use proptest::prelude::*;
    use test_strategy::proptest;

    fn floating_ip_create_strategy() -> impl Strategy<Value = FloatingIpCreate>
    {
        (
            identity_strategy(),
            optional_ip_strategy(),
            optional_name_or_id_strategy(),
        )
            .prop_map(|(identity, ip, pool)| FloatingIpCreate {
                identity,
                ip,
                pool,
            })
    }

    /// Verifies that the conversion from v2025121200::FloatingIpCreate to
    /// v2026010300::FloatingIpCreate preserves all existing fields, and that
    /// the ip_version field is set to None.
    #[proptest]
    fn floating_ip_create_converts_correctly(
        #[strategy(floating_ip_create_strategy())] input: FloatingIpCreate,
    ) {
        let output: v2026010300::FloatingIpCreate = input.clone().into();

        prop_assert_eq!(input.identity.name, output.identity.name);
        prop_assert_eq!(
            input.identity.description,
            output.identity.description
        );
        prop_assert_eq!(input.ip, output.ip);
        prop_assert_eq!(input.pool, output.pool);
        prop_assert_eq!(output.ip_version, None);
    }
}
