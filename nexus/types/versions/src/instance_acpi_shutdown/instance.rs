// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadata, IdentityMetadataCreateParams,
    NameOrId, Nullable, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2025_11_20_00::instance::{
    InstanceAutoRestartPolicy, InstanceAutoRestartStatus, InstanceCpuCount,
    InstanceRuntimeState,
};
use crate::v2026_01_03_00::instance::InstanceNetworkInterfaceAttachment;
use crate::v2026_01_05_00::instance::ExternalIpCreate;
use crate::v2026_01_08_00::multicast::MulticastGroupJoinSpec;
use crate::v2026_06_05_00::instance::InstanceDiskAttachment;
use crate::v2026_06_08_00;
use crate::v2026_06_08_00::instance::InstanceCpuPlatform;

/// View of an Instance
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Instance {
    // TODO is flattening here the intent in RFD 4?
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// ID for the project containing this instance
    pub project_id: Uuid,

    /// Number of CPUs allocated for this instance
    pub ncpus: InstanceCpuCount,
    /// Memory allocated for this instance
    pub memory: ByteCount,
    /// RFC1035-compliant hostname for the instance
    pub hostname: String,

    /// The ID of the disk used to boot this instance, if a specific one is assigned
    pub boot_disk_id: Option<Uuid>,

    #[serde(flatten)]
    pub runtime: InstanceRuntimeState,

    #[serde(flatten)]
    pub auto_restart_status: InstanceAutoRestartStatus,

    /// The CPU platform for this instance. If this is `null`, the instance
    /// requires no particular CPU platform.
    pub cpu_platform: Option<InstanceCpuPlatform>,

    /// When true, this instance has opted in to jumbo frames (8500 byte MTU)
    /// on its primary network interface. The effective MTU also depends on
    /// the fleet-wide jumbo-frames opt-in; if that is disabled, the primary
    /// interface uses the default MTU regardless of this value. Changes only
    /// take effect on the next instance restart.
    pub enable_jumbo_frames: bool,

    // TODO doc, type
    pub shutdown_policy: Option<()>,
}

impl From<v2026_06_08_00::instance::Instance> for Instance {
    fn from(value: v2026_06_08_00::instance::Instance) -> Self {
        Instance {
            identity: value.identity,
            project_id: value.project_id,
            ncpus: value.ncpus,
            memory: value.memory,
            hostname: value.hostname,
            boot_disk_id: value.boot_disk_id,
            runtime: value.runtime,
            auto_restart_status: value.auto_restart_status,
            cpu_platform: value.cpu_platform.map(Into::into),
            enable_jumbo_frames: value.enable_jumbo_frames,
            shutdown_policy: None,
        }
    }
}

impl TryFrom<Instance> for v2026_06_08_00::instance::Instance {
    type Error = dropshot::HttpError;

    fn try_from(value: Instance) -> Result<Self, Self::Error> {
        if let Some(policy) = value.shutdown_policy
        /* TODO(lif): && policy isn't the existing behavior of force shutdown always */
        {
            return Err(dropshot::HttpError::for_internal_error(
                "Instance with shutdown_policy cannot be represented in \
                API version prior to shutdown_policy support"
                    .to_string(),
            ));
        }
        Ok(v2026_06_08_00::instance::Instance {
            identity: value.identity,
            project_id: value.project_id,
            ncpus: value.ncpus,
            memory: value.memory,
            hostname: value.hostname,
            boot_disk_id: value.boot_disk_id,
            runtime: value.runtime,
            auto_restart_status: value.auto_restart_status,
            cpu_platform: value.cpu_platform,
            enable_jumbo_frames: value.enable_jumbo_frames,
        })
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
    /// Multicast groups this instance should join at creation.
    ///
    /// Groups can be specified by name, UUID, or IP address. Non-existent
    /// groups are created automatically.
    #[serde(default)]
    pub multicast_groups: Vec<MulticastGroupJoinSpec>,
    /// A list of disks to be attached to the instance.
    ///
    /// Disk attachments of type "create" will be created, while those of type
    /// "attach" must already exist.
    ///
    /// The order of this list does not guarantee a boot order for the
    /// instance. Use the boot_disk attribute to specify a boot disk. When
    /// boot_disk is specified it will count against the disk attachment limit.
    #[serde(default)]
    pub disks: Vec<InstanceDiskAttachment>,
    /// The disk the instance is configured to boot from.
    ///
    /// This disk can either be attached if it already exists or created along
    /// with the instance.
    ///
    /// Specifying a boot disk is optional but recommended to ensure
    /// predictable boot behavior. The boot disk can be set during instance
    /// creation or later if the instance is stopped. The boot disk counts
    /// against the disk attachment limit.
    ///
    /// An instance that does not have a boot disk set will use the boot
    /// options specified in its UEFI settings, which are controlled by both
    /// the instance's UEFI firmware and the guest operating system. Boot
    /// options can change as disks are attached and detached, which may
    /// result in an instance that only boots to the EFI shell until a boot
    /// disk is set.
    #[serde(default)]
    pub boot_disk: Option<InstanceDiskAttachment>,
    /// An allowlist of SSH public keys to be transferred to the instance via
    /// cloud-init during instance creation.
    ///
    /// If not provided, all SSH public keys from the user's profile will be
    /// sent. If an empty list is provided, no public keys will be transmitted
    /// to the instance.
    pub ssh_public_keys: Option<Vec<NameOrId>>,
    /// Should this instance be started upon creation; true by default.
    #[serde(default = "bool_true")]
    pub start: bool,
    /// The auto-restart policy for this instance.
    ///
    /// This policy determines whether the instance should be automatically
    /// restarted by the control plane on failure. If this is `null`, no
    /// auto-restart policy will be explicitly configured for this instance,
    /// and the control plane will select the default policy when determining
    /// whether the instance can be automatically restarted.
    ///
    /// Currently, the global default auto-restart policy is "best-effort",
    /// so instances with `null` auto-restart policies will be automatically
    /// restarted. However, in the future, the default policy may be
    /// configurable through other mechanisms, such as on a per-project
    /// basis. In that case, any configured default policy will be used if
    /// this is `null`.
    #[serde(default)]
    pub auto_restart_policy: Option<InstanceAutoRestartPolicy>,
    /// Anti-affinity groups to which this instance should be added.
    #[serde(default)]
    pub anti_affinity_groups: Vec<NameOrId>,
    /// The CPU platform to be used for this instance. If this is `null`, the
    /// instance requires no particular CPU platform; when it is started the
    /// instance will have the most general CPU platform supported by the sled
    /// it is initially placed on.
    #[serde(default)]
    pub cpu_platform: Option<InstanceCpuPlatform>,
    /// Enable jumbo frames (8500 byte MTU) on the instance's primary network
    /// interface. Requires the fleet-wide jumbo-frames opt-in to be enabled
    /// by an operator; otherwise this field must be `false`. Changes only take
    /// effect on the next instance restart.
    #[serde(default)]
    pub enable_jumbo_frames: bool,
    // TODO: doc, type
    #[serde(default)]
    pub shutdown_policy: Option<()>,
}

impl From<v2026_06_08_00::instance::InstanceCreate> for InstanceCreate {
    fn from(old: v2026_06_08_00::instance::InstanceCreate) -> Self {
        InstanceCreate {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            network_interfaces: old.network_interfaces,
            external_ips: old.external_ips,
            multicast_groups: old.multicast_groups,
            disks: old.disks,
            boot_disk: old.boot_disk,
            ssh_public_keys: old.ssh_public_keys,
            start: old.start,
            auto_restart_policy: old.auto_restart_policy,
            anti_affinity_groups: old.anti_affinity_groups,
            cpu_platform: old.cpu_platform,
            enable_jumbo_frames: old.enable_jumbo_frames,
            shutdown_policy: None,
        }
    }
}

/// Parameters of an `Instance` that can be reconfigured after creation.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceUpdate {
    /// The number of vCPUs to be allocated to the instance
    pub ncpus: InstanceCpuCount,

    /// The amount of RAM (in bytes) to be allocated to the instance
    pub memory: ByteCount,

    /// The disk the instance is configured to boot from.
    ///
    /// Setting a boot disk is optional but recommended to ensure predictable
    /// boot behavior. The boot disk can be set during instance creation or
    /// later if the instance is stopped. The boot disk counts against the
    /// disk attachment limit.
    ///
    /// An instance that does not have a boot disk set will use the boot
    /// options specified in its UEFI settings, which are controlled by both
    /// the instance's UEFI firmware and the guest operating system. Boot
    /// options can change as disks are attached and detached, which may
    /// result in an instance that only boots to the EFI shell until a boot
    /// disk is set.
    pub boot_disk: Nullable<NameOrId>,

    /// The auto-restart policy for this instance.
    ///
    /// This policy determines whether the instance should be automatically
    /// restarted by the control plane on failure. If this is `null`, any
    /// explicitly configured auto-restart policy will be unset, and the
    /// control plane will select the default policy when determining whether
    /// the instance can be automatically restarted.
    ///
    /// Currently, the global default auto-restart policy is "best-effort",
    /// so instances with `null` auto-restart policies will be automatically
    /// restarted. However, in the future, the default policy may be
    /// configurable through other mechanisms, such as on a per-project
    /// basis. In that case, any configured default policy will be used if
    /// this is `null`.
    pub auto_restart_policy: Nullable<InstanceAutoRestartPolicy>,

    /// The CPU platform to be used for this instance. If this is `null`,
    /// the instance requires no particular CPU platform; when it is started
    /// the instance will have the most general CPU platform supported by
    /// the sled it is initially placed on.
    pub cpu_platform: Nullable<InstanceCpuPlatform>,

    /// Multicast groups this instance should join.
    ///
    /// When specified, this replaces the instance's current multicast group
    /// membership with the new set of groups. The instance will leave any
    /// groups not listed here and join any new groups that are specified.
    ///
    /// Each entry can specify the group by name, UUID, or IP address, along with
    /// optional source IP filtering for SSM (Source-Specific Multicast). When
    /// a group doesn't exist, it will be implicitly created using the default
    /// multicast pool (or you can specify `ip_version` to disambiguate if needed).
    ///
    /// If not provided, the instance's multicast group membership will not
    /// be changed.
    #[serde(default)]
    pub multicast_groups: Option<Vec<MulticastGroupJoinSpec>>,

    /// Update the per-instance jumbo-frames opt-in. Setting this to `true`
    /// requires the fleet-wide jumbo-frames opt-in to be enabled. Changes only
    /// take effect on the next instance restart.
    pub enable_jumbo_frames: bool,

    // TODO doc, type
    pub shutdown_policy: Option<()>,
}

impl From<v2026_06_08_00::instance::InstanceUpdate> for InstanceUpdate {
    fn from(old: v2026_06_08_00::instance::InstanceUpdate) -> Self {
        Self {
            ncpus: old.ncpus,
            memory: old.memory,
            boot_disk: old.boot_disk,
            auto_restart_policy: old.auto_restart_policy,
            cpu_platform: old.cpu_platform,
            multicast_groups: old.multicast_groups,
            enable_jumbo_frames: old.enable_jumbo_frames,
            shutdown_policy: None,
        }
    }
}
