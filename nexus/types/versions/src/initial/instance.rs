// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version INITIAL.

use std::net::IpAddr;

use api_identity::ObjectIdentity;
use chrono::{DateTime, Utc};
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadata, IdentityMetadataCreateParams, Name,
    NameOrId, ObjectIdentity,
};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use super::disk::DiskCreate;

#[inline]
pub fn bool_true() -> bool {
    true
}

pub const MAX_USER_DATA_BYTES: usize = 32 * 1024; // 32 KiB

pub struct UserData;
impl UserData {
    pub fn serialize<S>(
        data: &Vec<u8>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use base64::prelude::*;
        BASE64_STANDARD.encode(data).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use base64::prelude::*;
        match BASE64_STANDARD.decode(<String>::deserialize(deserializer)?) {
            Ok(buf) => {
                if buf.len() > MAX_USER_DATA_BYTES {
                    Err(<D::Error as serde::de::Error>::invalid_length(
                        buf.len(),
                        &"less than 32 KiB",
                    ))
                } else {
                    Ok(buf)
                }
            }
            Err(_) => Err(<D::Error as serde::de::Error>::invalid_value(
                serde::de::Unexpected::Other("invalid base64 string"),
                &"a valid base64 string",
            )),
        }
    }
}

impl JsonSchema for UserData {
    fn schema_name() -> String {
        "String".to_string()
    }

    fn json_schema(
        _: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            format: Some("byte".to_string()),
            ..Default::default()
        }
        .into()
    }

    fn is_referenceable() -> bool {
        false
    }
}

/// Create-time parameters for an `InstanceNetworkInterface`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceNetworkInterfaceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The VPC in which to create the interface.
    pub vpc_name: Name,
    /// The VPC Subnet in which to create the interface.
    pub subnet_name: Name,
    /// The IP address for the interface. One will be auto-assigned if not provided.
    pub ip: Option<IpAddr>,
    /// A set of additional networks that this interface may send and
    /// receive traffic on.
    #[serde(default)]
    pub transit_ips: Vec<IpNet>,
}

/// Describes an attachment of an `InstanceNetworkInterface` to an `Instance`,
/// at the time the instance is created.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "params", rename_all = "snake_case")]
pub enum InstanceNetworkInterfaceAttachment {
    /// Create one or more `InstanceNetworkInterface`s for the `Instance`.
    ///
    /// If more than one interface is provided, then the first will be
    /// designated the primary interface for the instance.
    Create(Vec<InstanceNetworkInterfaceCreate>),

    /// The default networking configuration for an instance is to create a
    /// single primary interface with an automatically-assigned IP address. The
    /// IP will be pulled from the Project's default VPC / VPC Subnet.
    #[default]
    Default,

    /// No network interfaces at all will be created for the instance.
    None,
}

/// Parameters for creating an external IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalIpCreate {
    /// An IP address providing both inbound and outbound access. The address is
    /// automatically assigned from the provided IP pool or the default IP pool
    /// if not specified.
    Ephemeral { pool: Option<NameOrId> },
    /// An IP address providing both inbound and outbound access. The address is
    /// an existing floating IP object assigned to the current project.
    ///
    /// The floating IP must not be in use by another instance or service.
    Floating { floating_ip: NameOrId },
}

/// During instance creation, attach this disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceDiskAttach {
    /// A disk name to attach
    pub name: Name,
}

/// Describe the instance's disks at creation time
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InstanceDiskAttachment {
    /// During instance creation, create and attach disks
    Create(DiskCreate),

    /// During instance creation, attach this disk
    Attach(InstanceDiskAttach),
}

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
/// hypervisor support may be masked off. See RFD 314 for specific CPU features
/// in a CPU platform
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Eq, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum InstanceCpuPlatform {
    /// An AMD Milan-like CPU platform.
    AmdMilan,

    /// An AMD Turin-like CPU platform.
    // Note that there is only Turin, not Turin Dense - feature-wise there are
    // collapsed together as the guest-visible platform is the same.
    // If the two must be distinguished for instance placement, we'll want to
    // track whatever the motivating constraint is more explicitly. CPU
    // families, and especially the vendor code names, don't necessarily promise
    // details about specific processor packaging choices.
    AmdTurin,
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InstanceSelector {
    /// Name or ID of the project, only required if `instance` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the instance
    pub instance: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalInstanceSelector {
    /// Name or ID of the project, only required if `instance` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the instance
    pub instance: Option<NameOrId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InstanceNetworkInterfaceSelector {
    /// Name or ID of the project, only required if `instance` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the instance, only required if `network_interface` is provided as a `Name`
    pub instance: Option<NameOrId>,
    /// Name or ID of the network interface
    pub network_interface: NameOrId,
}

/// Parameters for updating an `InstanceNetworkInterface`
///
/// Note that modifying IP addresses for an interface is not yet supported, a
/// new interface must be created instead.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceNetworkInterfaceUpdate {
    #[serde(flatten)]
    pub identity: omicron_common::api::external::IdentityMetadataUpdateParams,

    /// Make a secondary interface the instance's primary interface.
    ///
    /// If applied to a secondary interface, that interface will become the
    /// primary on the next reboot of the instance. Note that this may have
    /// implications for routing between instances, as the new primary interface
    /// will be on a distinct subnet from the previous primary interface.
    ///
    /// Note that this can only be used to select a new primary interface for an
    /// instance. Requests to change the primary interface into a secondary will
    /// return an error.
    // TODO-completeness TODO-doc When we get there, this should note that a
    // change in the primary interface will result in changes to the DNS records
    // for the instance, though not the name.
    #[serde(default)]
    pub primary: bool,

    /// A set of additional networks that this interface may send and receive traffic on
    #[serde(default)]
    pub transit_ips: Vec<IpNet>,
}

/// Parameters for creating an ephemeral IP address for an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub struct EphemeralIpCreate {
    /// Name or ID of the IP pool used to allocate an address. If unspecified,
    /// the default IP pool will be used.
    pub pool: Option<NameOrId>,
}

/// Parameters for detaching an external IP from an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalIpDetach {
    Ephemeral,
    Floating { floating_ip: NameOrId },
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
    /// later if the instance is stopped. The boot disk counts against the disk
    /// attachment limit.
    ///
    /// An instance that does not have a boot disk set will use the boot
    /// options specified in its UEFI settings, which are controlled by both the
    /// instance's UEFI firmware and the guest operating system. Boot options
    /// can change as disks are attached and detached, which may result in an
    /// instance that only boots to the EFI shell until a boot disk is set.
    pub boot_disk: omicron_common::api::external::Nullable<NameOrId>,

    /// The auto-restart policy for this instance.
    ///
    /// This policy determines whether the instance should be automatically
    /// restarted by the control plane on failure. If this is `null`, any
    /// explicitly configured auto-restart policy will be unset, and
    /// the control plane will select the default policy when determining
    /// whether the instance can be automatically restarted.
    ///
    /// Currently, the global default auto-restart policy is "best-effort", so
    /// instances with `null` auto-restart policies will be automatically
    /// restarted. However, in the future, the default policy may be
    /// configurable through other mechanisms, such as on a per-project basis.
    /// In that case, any configured default policy will be used if this is
    /// `null`.
    pub auto_restart_policy:
        omicron_common::api::external::Nullable<InstanceAutoRestartPolicy>,

    /// The CPU platform to be used for this instance. If this is `null`, the
    /// instance requires no particular CPU platform; when it is started the
    /// instance will have the most general CPU platform supported by the sled
    /// it is initially placed on.
    pub cpu_platform:
        omicron_common::api::external::Nullable<InstanceCpuPlatform>,

    /// Multicast groups this instance should join.
    ///
    /// When specified, this replaces the instance's current multicast group
    /// membership with the new set of groups. The instance will leave any
    /// groups not listed here and join any new groups that are specified.
    ///
    /// If not provided (None), the instance's multicast group membership
    /// will not be changed.
    #[serde(default)]
    pub multicast_groups: Option<Vec<NameOrId>>,
}

/// Forwarded to a propolis server to request the contents of an Instance's serial console.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct InstanceSerialConsoleRequest {
    /// Name or ID of the project, only required if `instance` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Character index in the serial buffer from which to read, counting the bytes output since
    /// instance start. If this is not provided, `most_recent` must be provided, and if this *is*
    /// provided, `most_recent` must *not* be provided.
    pub from_start: Option<u64>,
    /// Character index in the serial buffer from which to read, counting *backward* from the most
    /// recently buffered data retrieved from the instance. (See note on `from_start` about mutual
    /// exclusivity)
    pub most_recent: Option<u64>,
    /// Maximum number of bytes of buffered serial console contents to return. If the requested
    /// range runs to the end of the available buffer, the data returned will be shorter than
    /// `max_bytes`.
    pub max_bytes: Option<u64>,
}

/// Forwarded to a propolis server to request the contents of an Instance's serial console.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct InstanceSerialConsoleStreamRequest {
    /// Name or ID of the project, only required if `instance` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Character index in the serial buffer from which to read, counting *backward* from the most
    /// recently buffered data retrieved from the instance.
    pub most_recent: Option<u64>,
}

/// Contents of an Instance's serial console buffer.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceSerialConsoleData {
    /// The bytes starting from the requested offset up to either the end of the buffer or the
    /// request's `max_bytes`. Provided as a u8 array rather than a string, as it may not be UTF-8.
    pub data: Vec<u8>,
    /// The absolute offset since boot (suitable for use as `byte_offset` in a subsequent request)
    /// of the last byte returned in `data`.
    pub last_byte_offset: u64,
}

/// Running state of an Instance (primarily: booted or stopped)
///
/// This typically reflects whether it's starting, running, stopping, or stopped,
/// but also includes states related to the Instance's lifecycle
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
// TODO-polish: RFD 315
pub enum InstanceState {
    /// The instance is being created.
    Creating,
    /// The instance is currently starting up.
    Starting,
    /// The instance is currently running.
    Running,
    /// The instance has been requested to stop and a transition to "Stopped" is imminent.
    Stopping,
    /// The instance is currently stopped.
    Stopped,
    /// The instance is in the process of rebooting - it will remain
    /// in the "rebooting" state until the VM is starting once more.
    Rebooting,
    /// The instance is in the process of migrating - it will remain
    /// in the "migrating" state until the migration process is complete
    /// and the destination propolis is ready to continue execution.
    Migrating,
    /// The instance is attempting to recover from a failure.
    Repairing,
    /// The instance has encountered a failure.
    Failed,
    /// The instance has been deleted.
    Destroyed,
}

/// The number of CPUs in an Instance
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
pub struct InstanceCpuCount(pub u16);

/// The state of an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeState {
    pub run_state: InstanceState,
    pub time_run_state_updated: DateTime<Utc>,
    /// The timestamp of the most recent time this instance was automatically
    /// restarted by the control plane.
    ///
    /// If this is not present, then this instance has not been automatically
    /// restarted.
    pub time_last_auto_restarted: Option<DateTime<Utc>>,
}

/// Status of control-plane driven automatic failure recovery for this instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceAutoRestartStatus {
    /// `true` if this instance's auto-restart policy will permit the control
    /// plane to automatically restart it if it enters the `Failed` state.
    //
    // Rename this field, as the struct is `#[serde(flatten)]`ed into the
    // `Instance` type, and we would like the field to be prefixed with
    // `auto_restart`.
    #[serde(rename = "auto_restart_enabled")]
    pub enabled: bool,

    /// The auto-restart policy configured for this instance, or `null` if no
    /// explicit policy has been configured.
    ///
    /// This policy determines whether the instance should be automatically
    /// restarted by the control plane on failure. If this is `null`, the
    /// control plane will use the default policy when determining whether or
    /// not to automatically restart this instance, which may or may not allow
    /// it to be restarted. The value of the `auto_restart_enabled` field
    /// indicates whether the instance will be auto-restarted, based on its
    /// current policy or the default if it has no configured policy.
    //
    // Rename this field, as the struct is `#[serde(flatten)]`ed into the
    // `Instance` type, and we would like the field to be prefixed with
    // `auto_restart`.
    #[serde(rename = "auto_restart_policy")]
    pub policy: Option<InstanceAutoRestartPolicy>,

    /// The time at which the auto-restart cooldown period for this instance
    /// completes, permitting it to be automatically restarted again. If the
    /// instance enters the `Failed` state, it will not be restarted until after
    /// this time.
    ///
    /// If this is not present, then either the instance has never been
    /// automatically restarted, or the cooldown period has already expired,
    /// allowing the instance to be restarted immediately if it fails.
    //
    // Rename this field, as the struct is `#[serde(flatten)]`ed into the
    // `Instance` type, and we would like the field to be prefixed with
    // `auto_restart`.
    #[serde(rename = "auto_restart_cooldown_expiration")]
    pub cooldown_expiration: Option<DateTime<Utc>>,
}

/// A policy determining when an instance should be automatically restarted by
/// the control plane.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Eq, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum InstanceAutoRestartPolicy {
    /// The instance should not be automatically restarted by the control plane
    /// if it fails.
    Never,
    /// If this instance is running and unexpectedly fails (e.g. due to a host
    /// software crash or unexpected host reboot), the control plane will make a
    /// best-effort attempt to restart it. The control plane may choose not to
    /// restart the instance to preserve the overall availability of the system.
    BestEffort,
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
}
