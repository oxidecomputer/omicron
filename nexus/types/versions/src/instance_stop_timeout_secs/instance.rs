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
    InstanceRuntimeState, UserData, bool_true,
};
use crate::v2026_01_03_00::instance::InstanceNetworkInterfaceAttachment;
use crate::v2026_01_05_00::instance::ExternalIpCreate;
use crate::v2026_01_08_00::multicast::MulticastGroupJoinSpec;
use crate::v2026_06_05_00::instance::InstanceDiskAttachment;
use crate::v2026_06_08_00::instance::InstanceCpuPlatform;
use crate::v2026_11_11_00;
use crate::v2026_11_11_00::instance::{
    AcpiTimeoutSecs, InstanceShutdownPolicy,
};

/// How long the guest is given to shut itself down before it is forcibly
/// powered off, after an ACPI power button press.
///
/// A timeout period of zero seconds results in no power button signal, instead
/// stopping the instance immediately. This is analogous to simply pulling power
/// from the instance; no notification is sent to the guest.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(try_from = "u64", into = "u64")]
pub struct StopTimeoutSecs(#[schemars(range(min = 0, max = 900))] u64);

// schemars(range(max)) above is required to be a *literal*. this is the same
// trick as taken with `AcpiTimeoutSecs` in the previous API version. as with
// there, if you have changed `MAX` and caused this assert to fire, change the
// schemars() bounds above, and fix the constant here to match.
const _: () = const { assert!(StopTimeoutSecs::MAX == 900) };

impl StopTimeoutSecs {
    const MAX: u64 = 900;

    pub const ZERO: StopTimeoutSecs = const {
        match StopTimeoutSecs::from_u64(0) {
            Ok(timeout) => timeout,
            Err(_e) => {
                panic!("zero wasn't a valid stop timeout?");
            }
        }
    };
}

impl StopTimeoutSecs {
    // const glue to convert a u64 to StopTimeoutSecs, entirely so `ZERO` above
    // can also be const. This is used in `TryFrom<u64>` below so that there's
    // a normal `TryFrom` to use elsewhere, and to give a slightly better error
    // message.
    const fn from_u64(time: u64) -> Result<Self, &'static str> {
        if time > Self::MAX {
            return Err("shutdown_timeout_secs is out of range");
        }

        Ok(Self(time))
    }
}

impl From<StopTimeoutSecs> for u64 {
    fn from(value: StopTimeoutSecs) -> Self {
        value.0
    }
}

impl TryFrom<u64> for StopTimeoutSecs {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::from_u64(value).map_err(|err| {
            anyhow::anyhow!(
                "{err}. must be between 0 and {} (got {value})",
                Self::MAX
            )
        })
    }
}

impl From<StopTimeoutSecs> for InstanceShutdownPolicy {
    fn from(value: StopTimeoutSecs) -> Self {
        match value.0 {
            0 => InstanceShutdownPolicy::HardOff,
            n => {
                // `stop_timeout_secs_and_shutdown_policy_are_equivalent`
                // exhaustively covers that these are identically expressive.
                let timeout_secs = AcpiTimeoutSecs::try_from(n).expect(
                    "instance_stop_timeout_secs tests this cannot fail",
                );
                InstanceShutdownPolicy::PowerButton { timeout_secs }
            }
        }
    }
}

impl From<InstanceShutdownPolicy> for StopTimeoutSecs {
    fn from(value: InstanceShutdownPolicy) -> Self {
        let timeout = match value {
            InstanceShutdownPolicy::HardOff => 0,
            InstanceShutdownPolicy::PowerButton { timeout_secs } => {
                u64::from(timeout_secs)
            }
        };

        // `stop_timeout_secs_and_shutdown_policy_are_equivalent`
        // exhaustively covers that these are identically expressive.
        StopTimeoutSecs::try_from(timeout)
            .expect("instance_stop_timeout_secs tests this cannot fail")
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

    /// How long the guest is given to shut itself down before it is forcibly
    /// powered off, after an ACPI power button press.
    ///
    /// If the timeout period is configured for zero seconds, the instance is
    /// stopped as if power was immediately pulled; no notification is sent to
    /// the guest.
    pub stop_timeout_secs: StopTimeoutSecs,
}

impl From<Instance> for v2026_11_11_00::instance::Instance {
    fn from(value: Instance) -> Self {
        Self {
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
            shutdown_policy: value.stop_timeout_secs.into(),
        }
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
    /// How long the guest is given to shut itself down before it is forcibly
    /// powered off, after an ACPI power button press.
    ///
    /// If the timeout period is configured for zero seconds, the instance is
    /// stopped as if power was immediately pulled; no notification is sent to
    /// the guest.
    ///
    /// This timeout period applies when the instance is stopped for any reason,
    /// including when waiting for a guest to stop as part of a call to the
    /// `instance_reboot` API.
    ///
    /// If not provided, the timeout period will be set to zero, meaning instance
    /// stop will be immediate with no notification.
    #[serde(default)]
    pub stop_timeout_secs: StopTimeoutSecs,
}

impl From<v2026_11_11_00::instance::InstanceCreate> for InstanceCreate {
    fn from(old: v2026_11_11_00::instance::InstanceCreate) -> Self {
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
            stop_timeout_secs: old.shutdown_policy.into(),
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

    /// How long the guest is given to shut itself down before it is forcibly
    /// powered off, after an ACPI power button press.
    ///
    /// If the timeout period is configured for zero seconds, the instance is
    /// stopped as if power was immediately pulled; no notification is sent to
    /// the guest.
    ///
    /// This timeout applies starting from the next instance stop after the
    /// update completes; if a shutdown is in progress when the instance is
    /// updated, that in-progress shutdown's timeout (if any) is not altered.
    pub stop_timeout_secs: StopTimeoutSecs,
}

impl From<v2026_11_11_00::instance::InstanceUpdate> for InstanceUpdate {
    fn from(old: v2026_11_11_00::instance::InstanceUpdate) -> Self {
        let stop_timeout_secs = match old.shutdown_policy.0 {
            // From the old API docs:
            // > If not provided, the current default policy will be selected
            // > (hard power of without an ACPI soft shutdown)
            None => StopTimeoutSecs::ZERO,
            Some(policy) => StopTimeoutSecs::from(policy),
        };
        Self {
            ncpus: old.ncpus,
            memory: old.memory,
            boot_disk: old.boot_disk,
            auto_restart_policy: old.auto_restart_policy,
            cpu_platform: old.cpu_platform,
            multicast_groups: old.multicast_groups,
            enable_jumbo_frames: old.enable_jumbo_frames,
            stop_timeout_secs,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn stop_timeout_secs_and_shutdown_policy_are_equivalent() {
        assert_eq!(AcpiTimeoutSecs::MAX, StopTimeoutSecs::MAX);

        fn as_shutdown_policy(
            i: u64,
        ) -> anyhow::Result<InstanceShutdownPolicy> {
            match i {
                0 => Ok(InstanceShutdownPolicy::HardOff),
                other => Ok(InstanceShutdownPolicy::PowerButton {
                    timeout_secs: AcpiTimeoutSecs::try_from(other)?,
                }),
            }
        }

        for i in 0..(StopTimeoutSecs::MAX + 100) {
            let as_shutdown_policy = as_shutdown_policy(i);
            let as_timeout = StopTimeoutSecs::try_from(i);

            match (as_shutdown_policy, as_timeout) {
                (Ok(InstanceShutdownPolicy::HardOff), Ok(timeout)) => {
                    assert_eq!(u64::from(timeout), 0);
                }
                (
                    Ok(InstanceShutdownPolicy::PowerButton {
                        timeout_secs: policy_timeout,
                    }),
                    Ok(timeout),
                ) => {
                    assert_eq!(u64::from(policy_timeout), u64::from(timeout));
                }
                (Err(_), Err(_)) => {
                    // If some timeout seconds is unreasonable as the older
                    // InstanceShutdownPolicy, it is also an unreasonable
                    // StopTimeoutSecs.
                }
                (as_shutdown_policy, as_timeout) => {
                    panic!(
                        "{} seconds was interpreted incompatibly:
  InstanceShutdownPolicy: {:?}
  StopTimeoutSecs: {:?}",
                        i, as_shutdown_policy, as_timeout,
                    );
                }
            }
        }
    }
}
