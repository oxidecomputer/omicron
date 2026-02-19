// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2025121200 to 2026010100.
//!
//! Version 2025121200 types (before `ip_version` preference was added for
//! default IP pool selection).
//!
//! ## IP Pool Selection Changes
//!
//! Key differences from newer API versions:
//! - [`FloatingIpCreate`], [`EphemeralIpCreate`], and [`ExternalIpCreate`]
//!   don't have the `ip_version` field. Newer versions allow specifying
//!   IPv4/IPv6 preference when allocating from default pools.
//! - When multiple default pools of different IP versions exist for a silo,
//!   older clients cannot resolve the conflict. Newer API versions
//!   require the `ip_version` field in this scenario.
//!
//! Affected endpoints:
//! - `POST /v1/floating-ips` (floating_ip_create)
//! - `POST /v1/instances/{instance}/external-ips/ephemeral` (instance_ephemeral_ip_attach)
//! - `POST /v1/instances` (instance_create)
//!
//! ## Multicast Types
//!
//! Multicast types are re-exported from `v2025122300`.
//! Both versions use `NameOrId` for group references and have the same
//! explicit create/update semantics.
//!
//! [`FloatingIpCreate`]: self::FloatingIpCreate
//! [`EphemeralIpCreate`]: self::EphemeralIpCreate
//! [`ExternalIpCreate`]: self::ExternalIpCreate

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use nexus_types::external_api::params;
use omicron_common::api::external;

use crate::{v2026010100, v2026010300, v2026013000};

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

/// The type of IP address to attach to an instance during creation.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalIpCreate {
    /// An IP address providing both inbound and outbound access.
    /// The address is automatically assigned from the provided IP pool
    /// or the default IP pool if not specified.
    Ephemeral {
        /// Name or ID of the IP pool to use. If unspecified, the
        /// default IP pool will be used.
        pool: Option<external::NameOrId>,
    },
    /// A floating IP address.
    Floating {
        /// The name or ID of the floating IP address to attach.
        floating_ip: external::NameOrId,
    },
}

// Converts to v2026010300::ExternalIpCreate (adds ip_version: None)
impl From<ExternalIpCreate> for v2026010300::ExternalIpCreate {
    fn from(old: ExternalIpCreate) -> v2026010300::ExternalIpCreate {
        match old {
            ExternalIpCreate::Ephemeral { pool } => {
                v2026010300::ExternalIpCreate::Ephemeral {
                    pool,
                    ip_version: None,
                }
            }
            ExternalIpCreate::Floating { floating_ip } => {
                v2026010300::ExternalIpCreate::Floating { floating_ip }
            }
        }
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

/// Create-time parameters for an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: external::IdentityMetadataCreateParams,
    /// The number of vCPUs to be allocated to the instance
    pub ncpus: external::InstanceCpuCount,
    /// The amount of RAM (in bytes) to be allocated to the instance
    pub memory: external::ByteCount,
    /// The hostname to be assigned to the instance
    pub hostname: external::Hostname,
    /// User data for instance initialization systems (such as cloud-init).
    #[serde(default, with = "params::UserData")]
    pub user_data: Vec<u8>,
    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces: v2026010100::InstanceNetworkInterfaceAttachment,
    /// The external IP addresses provided to this instance.
    #[serde(default)]
    pub external_ips: Vec<ExternalIpCreate>,
    /// The multicast groups this instance should join.
    #[serde(default)]
    pub multicast_groups: Vec<external::NameOrId>,
    /// A list of disks to be attached to the instance.
    #[serde(default)]
    pub disks: Vec<v2026013000::InstanceDiskAttachment>,
    /// The disk the instance is configured to boot from.
    #[serde(default)]
    pub boot_disk: Option<v2026013000::InstanceDiskAttachment>,
    /// An allowlist of SSH public keys to be transferred to the instance.
    pub ssh_public_keys: Option<Vec<external::NameOrId>>,
    /// Should this instance be started upon creation; true by default.
    #[serde(default = "params::bool_true")]
    pub start: bool,
    /// The auto-restart policy for this instance.
    #[serde(default)]
    pub auto_restart_policy: Option<external::InstanceAutoRestartPolicy>,
    /// Anti-Affinity groups which this instance should be added.
    #[serde(default)]
    pub anti_affinity_groups: Vec<external::NameOrId>,
    /// The CPU platform to be used for this instance.
    #[serde(default)]
    pub cpu_platform: Option<external::InstanceCpuPlatform>,
}

impl From<InstanceCreate> for v2026010100::InstanceCreate {
    fn from(old: InstanceCreate) -> v2026010100::InstanceCreate {
        v2026010100::InstanceCreate {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            network_interfaces: old.network_interfaces,
            external_ips: old
                .external_ips
                .into_iter()
                .map(Into::into)
                .collect(),
            multicast_groups: old.multicast_groups,
            disks: old.disks,
            boot_disk: old.boot_disk,
            ssh_public_keys: old.ssh_public_keys,
            start: old.start,
            auto_restart_policy: old.auto_restart_policy,
            anti_affinity_groups: old.anti_affinity_groups,
            cpu_platform: old.cpu_platform,
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
