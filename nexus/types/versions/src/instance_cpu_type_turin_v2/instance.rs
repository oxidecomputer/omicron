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

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::instance::{
    InstanceAutoRestartPolicy, InstanceAutoRestartStatus, InstanceCpuCount,
    InstanceRuntimeState, UserData, bool_true,
};
use crate::v2026_01_03_00::instance::InstanceNetworkInterfaceAttachment;
use crate::v2026_01_05_00::instance::ExternalIpCreate;
use crate::v2026_01_08_00::multicast::MulticastGroupJoinSpec;
use crate::v2026_06_05_00;
use crate::v2026_06_05_00::instance::InstanceDiskAttachment;

/// A required CPU platform for an instance.
///
/// When an instance specifies a required CPU platform:
///
/// - The system may expose (to the VM) new CPU features that are only present
///   on that platform (or on newer platforms of the same lineage that also
///   support those features).
/// - The instance must run on hosts that have CPUs that support all the
///   features of the supplied platform.
///
/// That is, the instance is restricted to hosts that have the CPUs which
/// support all features of the required platform, but in exchange the CPU
/// features exposed by the platform are available for the guest to use. Note
/// that this may prevent an instance from starting (if the hosts that could run
/// it are full but there is capacity on other incompatible hosts).
///
/// If an instance does not specify a required CPU platform, then when
/// it starts, the control plane selects a host for the instance and then
/// supplies the guest with the "minimum" CPU platform supported by that host.
/// This maximizes the number of hosts that can run the VM if it later needs to
/// migrate to another host.
///
/// In all cases, the CPU features presented by a given CPU platform are a
/// subset of what the corresponding hardware may actually support; features
/// which cannot be used from a virtual environment or do not have full
/// hypervisor support may be masked off.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Eq, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum InstanceCpuPlatform {
    /// An AMD Milan-like CPU platform.
    AmdMilan,

    /// An AMD Turin-like CPU platform. Prefer `amd_turin_v2` over this; this
    /// CPU platform is retained for instances that specifically requested it
    /// before `amd_turin_v2` was added.
    ///
    /// This initial version of the Turin CPU platform includes no cache
    /// or TLB information in CPUID leaf `8000_0006`. While this was
    /// intentional, Oxide later discovered some guest software interprets the
    /// zeroed leaves as reporting cache sizes of 0 bytes and behaves
    /// incorrectly and unpredictably as a result (see
    /// [Propolis#1152](https://github.com/oxidecomputer/propolis/issues/1152)).
    // Note that there is only Turin, not Turin Dense - feature-wise there are
    // collapsed together as the guest-visible platform is the same.
    // If the two must be distinguished for instance placement, we'll want to
    // track whatever the motivating constraint is more explicitly. CPU
    // families, and especially the vendor code names, don't necessarily promise
    // details about specific processor packaging choices.
    AmdTurin,

    /// An AMD Turin-like CPU platform.
    ///
    /// This version of the Turin CPU platform includes cache and TLB
    /// information in CPUID leaf `8000_0006`, similar to the cache information
    /// included in the initial Milan-like CPU platform.
    AmdTurinV2,
}

impl From<v2025_11_20_00::instance::InstanceCpuPlatform>
    for InstanceCpuPlatform
{
    fn from(old: v2025_11_20_00::instance::InstanceCpuPlatform) -> Self {
        match old {
            v2025_11_20_00::instance::InstanceCpuPlatform::AmdMilan => {
                InstanceCpuPlatform::AmdMilan
            }
            v2025_11_20_00::instance::InstanceCpuPlatform::AmdTurin => {
                InstanceCpuPlatform::AmdTurin
            }
        }
    }
}

impl TryFrom<InstanceCpuPlatform>
    for v2025_11_20_00::instance::InstanceCpuPlatform
{
    type Error = dropshot::HttpError;

    fn try_from(new: InstanceCpuPlatform) -> Result<Self, Self::Error> {
        match new {
            InstanceCpuPlatform::AmdMilan => {
                Ok(v2025_11_20_00::instance::InstanceCpuPlatform::AmdMilan)
            }
            InstanceCpuPlatform::AmdTurin => {
                Ok(v2025_11_20_00::instance::InstanceCpuPlatform::AmdTurin)
            }
            InstanceCpuPlatform::AmdTurinV2 => {
                Err(dropshot::HttpError::for_client_error(
                    Some(String::from("Not Acceptable")),
                    dropshot::ClientErrorStatusCode::NOT_ACCEPTABLE,
                    String::from(
                        "CPU platform amd_turin_v2 not supported for client version",
                    ),
                ))
            }
        }
    }
}

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
}

impl From<v2026_06_05_00::instance::Instance> for Instance {
    fn from(value: v2026_06_05_00::instance::Instance) -> Self {
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
        }
    }
}

impl TryFrom<Instance> for v2026_06_05_00::instance::Instance {
    type Error = dropshot::HttpError;

    fn try_from(value: Instance) -> Result<Self, Self::Error> {
        let converted_cpu = match value.cpu_platform {
            Some(platform) => Some(platform.try_into()?),
            None => None,
        };
        Ok(v2026_06_05_00::instance::Instance {
            identity: value.identity,
            project_id: value.project_id,
            ncpus: value.ncpus,
            memory: value.memory,
            hostname: value.hostname,
            boot_disk_id: value.boot_disk_id,
            runtime: value.runtime,
            auto_restart_status: value.auto_restart_status,
            cpu_platform: converted_cpu,
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
    /// Enable jumbo frames (8500 byte MTU) on the instance's primary OPTE
    /// interface. Requires the fleet-wide jumbo-frames opt-in to be enabled
    /// by an operator; otherwise this field must be `false`. Changes only take
    /// effect on the next instance restart.
    #[serde(default)]
    pub enable_jumbo_frames: bool,
}

impl From<v2026_06_05_00::instance::InstanceCreate> for InstanceCreate {
    fn from(old: v2026_06_05_00::instance::InstanceCreate) -> Self {
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
            cpu_platform: old.cpu_platform.map(Into::into),
            enable_jumbo_frames: old.enable_jumbo_frames,
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
}

impl From<v2026_06_05_00::instance::InstanceUpdate> for InstanceUpdate {
    fn from(old: v2026_06_05_00::instance::InstanceUpdate) -> Self {
        Self {
            ncpus: old.ncpus,
            memory: old.memory,
            boot_disk: old.boot_disk,
            auto_restart_policy: old.auto_restart_policy,
            cpu_platform: Nullable(old.cpu_platform.0.map(Into::into)),
            multicast_groups: old.multicast_groups,
            enable_jumbo_frames: old.enable_jumbo_frames,
        }
    }
}
