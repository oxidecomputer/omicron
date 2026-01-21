// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026010500 (POOL_SELECTION_ENUMS) that changed in
//! version 2026011600 (RENAME_ADDRESS_SELECTOR_TO_ADDRESS_ALLOCATOR).
//!
//! ## Pool Selection Changes
//!
//! Valid until 2026011600 (RENAME_ADDRESS_SELECTOR_TO_ADDRESS_ALLOCATOR).
//!
//! [`FloatingIpCreate`] uses [`AddressSelector`] enum (renamed to `AddressAllocator`
//! in newer versions). This provides explicit/auto allocation selection.
//!
//! ## Multicast Changes
//!
//! Valid until 2026010800 (MULTICAST_IMPLICIT_LIFECYCLE_UPDATES).
//!
//! [`InstanceCreate`] uses `Vec<NameOrId>` for `multicast_groups`. Newer
//! versions use `Vec<MulticastGroupJoinSpec>` which supports implicit group
//! lifecycle (groups are created automatically when referenced).
//!
//! Affected endpoints:
//! - `POST /v1/floating-ips` (floating_ip_create)
//! - `POST /v1/instances` (instance_create)
//!
//! [`FloatingIpCreate`]: self::FloatingIpCreate
//! [`AddressSelector`]: self::AddressSelector
//! [`InstanceCreate`]: self::InstanceCreate
//! [`MulticastGroupJoinSpec`]: nexus_types::external_api::params::MulticastGroupJoinSpec

use std::net::IpAddr;

use crate::v2026011600;
use nexus_types::external_api::params;
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, NameOrId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// v2026010500 (POOL_SELECTION_ENUMS) uses the current `InstanceNetworkInterfaceAttachment`
// with `DefaultIpv4`, `DefaultIpv6`, `DefaultDualStack` variants.
//
// Only the multicast_groups field differs (Vec<NameOrId> vs Vec<MulticastGroupJoinSpec>).
pub use params::InstanceNetworkInterfaceAttachment;

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
        v2026011600::AddressAllocator::from(value).into()
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
        v2026011600::FloatingIpCreate::from(value).into()
    }
}

/// Create-time parameters for an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The number of vCPUs to be allocated to the instance
    pub ncpus: InstanceCpuCount,
    /// The amount of RAM (in bytes) to be allocated to the instance
    pub memory: ByteCount,
    /// The hostname to be assigned to the instance
    pub hostname: Hostname,

    /// User data for instance initialization systems (such as cloud-init).
    /// Must be a Base64-encoded string, as specified in RFC 4648 § 4 (+ and /
    /// characters with padding). Maximum 32 KiB unencoded data.
    #[serde(default, with = "params::UserData")]
    pub user_data: Vec<u8>,

    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces: InstanceNetworkInterfaceAttachment,

    /// The external IP addresses provided to this instance.
    ///
    /// By default, all instances have outbound connectivity, but no inbound
    /// connectivity. These external addresses can be used to provide a fixed,
    /// known IP address for making inbound connections to the instance.
    #[serde(default)]
    pub external_ips: Vec<params::ExternalIpCreate>,

    /// Multicast groups this instance should join at creation.
    ///
    /// Groups can be specified by name, UUID, or IP address. Non-existent
    /// groups are created automatically.
    #[serde(default)]
    pub multicast_groups: Vec<NameOrId>,

    /// A list of disks to be attached to the instance.
    ///
    /// Disk attachments of type "create" will be created, while those of type
    /// "attach" must already exist.
    ///
    /// The order of this list does not guarantee a boot order for the instance.
    /// Use the boot_disk attribute to specify a boot disk. When boot_disk is
    /// specified it will count against the disk attachment limit.
    #[serde(default)]
    pub disks: Vec<params::InstanceDiskAttachment>,

    /// The disk the instance is configured to boot from.
    ///
    /// This disk can either be attached if it already exists or created along
    /// with the instance.
    ///
    /// Specifying a boot disk is optional but recommended to ensure predictable
    /// boot behavior. The boot disk can be set during instance creation or
    /// later if the instance is stopped. The boot disk counts against the disk
    /// attachment limit.
    ///
    /// An instance that does not have a boot disk set will use the boot
    /// options specified in its UEFI settings, which are controlled by both the
    /// instance's UEFI firmware and the guest operating system. Boot options
    /// can change as disks are attached and detached, which may result in an
    /// instance that only boots to the EFI shell until a boot disk is set.
    #[serde(default)]
    pub boot_disk: Option<params::InstanceDiskAttachment>,

    /// An allowlist of SSH public keys to be transferred to the instance via
    /// cloud-init during instance creation.
    ///
    /// If not provided, all SSH public keys from the user's profile will be sent.
    /// If an empty list is provided, no public keys will be transmitted to the
    /// instance.
    pub ssh_public_keys: Option<Vec<NameOrId>>,

    /// Should this instance be started upon creation; true by default.
    #[serde(default = "params::bool_true")]
    pub start: bool,

    /// The auto-restart policy for this instance.
    ///
    /// This policy determines whether the instance should be automatically
    /// restarted by the control plane on failure. If this is `null`, no
    /// auto-restart policy will be explicitly configured for this instance, and
    /// the control plane will select the default policy when determining
    /// whether the instance can be automatically restarted.
    ///
    /// Currently, the global default auto-restart policy is "best-effort", so
    /// instances with `null` auto-restart policies will be automatically
    /// restarted. However, in the future, the default policy may be
    /// configurable through other mechanisms, such as on a per-project basis.
    /// In that case, any configured default policy will be used if this is
    /// `null`.
    #[serde(default)]
    pub auto_restart_policy: Option<InstanceAutoRestartPolicy>,

    /// Anti-Affinity groups which this instance should be added.
    #[serde(default)]
    pub anti_affinity_groups: Vec<NameOrId>,

    /// The CPU platform to be used for this instance. If this is `null`, the
    /// instance requires no particular CPU platform; when it is started the
    /// instance will have the most general CPU platform supported by the sled
    /// it is initially placed on.
    #[serde(default)]
    pub cpu_platform: Option<InstanceCpuPlatform>,
}

impl From<InstanceCreate> for params::InstanceCreate {
    fn from(value: InstanceCreate) -> Self {
        Self {
            identity: value.identity,
            ncpus: value.ncpus,
            memory: value.memory,
            hostname: value.hostname,
            user_data: value.user_data,
            network_interfaces: value.network_interfaces,
            external_ips: value.external_ips,
            multicast_groups: value
                .multicast_groups
                .into_iter()
                .map(|g| params::MulticastGroupJoinSpec {
                    group: g.into(),
                    source_ips: None,
                    ip_version: None,
                })
                .collect(),
            disks: value.disks,
            boot_disk: value.boot_disk,
            ssh_public_keys: value.ssh_public_keys,
            start: value.start,
            auto_restart_policy: value.auto_restart_policy,
            anti_affinity_groups: value.anti_affinity_groups,
            cpu_platform: value.cpu_platform,
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
            AddressSelector::Explicit { ip: expected_ip, .. } => {
                let params::AddressAllocator::Explicit { ip: actual_ip } =
                    actual.address_allocator
                else {
                    return Err(TestCaseError::fail(
                        "expected Explicit variant",
                    ));
                };
                prop_assert_eq!(expected_ip, actual_ip);
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
