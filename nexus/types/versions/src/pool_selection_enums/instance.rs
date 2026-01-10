// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version POOL_SELECTION_ENUMS.
//!
//! This version changes IP pool selection to use `PoolSelector` enums
//! instead of flat pool/ip_version fields.

use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, NameOrId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ip_pool::PoolSelector;
use crate::v2025121200;
use crate::v2026010100;
use crate::v2026010300;

use crate::v2025112000::instance::{UserData, bool_true};
use crate::v2025120300::instance::InstanceDiskAttachment;
use crate::v2026010300::instance::InstanceNetworkInterfaceAttachment;

/// Parameters for creating an external IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalIpCreate {
    /// An IP address providing both inbound and outbound access. The address is
    /// automatically assigned from a pool.
    Ephemeral {
        /// Pool to allocate from.
        #[serde(default)]
        pool_selector: PoolSelector,
    },
    /// An IP address providing both inbound and outbound access. The address is
    /// an existing floating IP object assigned to the current project.
    ///
    /// The floating IP must not be in use by another instance or service.
    Floating { floating_ip: NameOrId },
}

impl TryFrom<v2026010300::instance::ExternalIpCreate> for ExternalIpCreate {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        old: v2026010300::instance::ExternalIpCreate,
    ) -> Result<Self, Self::Error> {
        match old {
            v2026010300::instance::ExternalIpCreate::Ephemeral {
                pool,
                ip_version,
            } => {
                let pool_selector = (pool, ip_version).try_into()?;
                Ok(ExternalIpCreate::Ephemeral { pool_selector })
            }
            v2026010300::instance::ExternalIpCreate::Floating {
                floating_ip,
            } => Ok(ExternalIpCreate::Floating { floating_ip }),
        }
    }
}

/// Parameters for creating an ephemeral IP address for an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct EphemeralIpCreate {
    /// Pool to allocate from.
    #[serde(default)]
    pub pool_selector: PoolSelector,
}

impl TryFrom<v2026010300::instance::EphemeralIpCreate> for EphemeralIpCreate {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        old: v2026010300::instance::EphemeralIpCreate,
    ) -> Result<Self, Self::Error> {
        let pool_selector = (old.pool, old.ip_version).try_into()?;
        Ok(EphemeralIpCreate { pool_selector })
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
    #[serde(default, with = "UserData")]
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
    pub external_ips: Vec<ExternalIpCreate>,

    /// The multicast groups this instance should join.
    ///
    /// The instance will be automatically added as a member of the specified
    /// multicast groups during creation, enabling it to send and receive
    /// multicast traffic for those groups.
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
    pub disks: Vec<InstanceDiskAttachment>,

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
    pub boot_disk: Option<InstanceDiskAttachment>,

    /// An allowlist of SSH public keys to be transferred to the instance via
    /// cloud-init during instance creation.
    ///
    /// If not provided, all SSH public keys from the user's profile will be sent.
    /// If an empty list is provided, no public keys will be transmitted to the
    /// instance.
    pub ssh_public_keys: Option<Vec<NameOrId>>,

    /// Should this instance be started upon creation; true by default.
    #[serde(default = "bool_true")]
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

impl TryFrom<v2026010300::instance::InstanceCreate> for InstanceCreate {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        old: v2026010300::instance::InstanceCreate,
    ) -> Result<Self, Self::Error> {
        let external_ips: Vec<ExternalIpCreate> = old
            .external_ips
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(InstanceCreate {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            network_interfaces: old.network_interfaces,
            external_ips,
            multicast_groups: old.multicast_groups,
            disks: old.disks,
            boot_disk: old.boot_disk,
            ssh_public_keys: old.ssh_public_keys,
            start: old.start,
            auto_restart_policy: old.auto_restart_policy,
            anti_affinity_groups: old.anti_affinity_groups,
            cpu_platform: old.cpu_platform,
        })
    }
}

// Direct conversion from v2026010100 (chains through v2026010300)
impl TryFrom<v2026010100::instance::InstanceCreate> for InstanceCreate {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        old: v2026010100::instance::InstanceCreate,
    ) -> Result<Self, Self::Error> {
        let intermediate: v2026010300::instance::InstanceCreate =
            old.try_into()?;
        intermediate.try_into()
    }
}

// Direct conversion from v2025121200 (chains through v2026010100 → v2026010300)
impl TryFrom<v2025121200::instance::InstanceCreate> for InstanceCreate {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        old: v2025121200::instance::InstanceCreate,
    ) -> Result<Self, Self::Error> {
        let v2026010100: v2026010100::instance::InstanceCreate = old.into();
        v2026010100.try_into()
    }
}

// Note: Direct conversion from v2026010100 is not needed since
// v2026010100::instance::EphemeralIpCreate is a re-export of v2026010300's type.

// Direct conversion for EphemeralIpCreate from v2025121200
impl TryFrom<v2025121200::instance::EphemeralIpCreate> for EphemeralIpCreate {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        old: v2025121200::instance::EphemeralIpCreate,
    ) -> Result<Self, Self::Error> {
        let v2026010100: v2026010100::instance::EphemeralIpCreate = old.into();
        v2026010100.try_into()
    }
}
