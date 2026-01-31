// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026013001 (`READ_ONLY_DISKS`) that changed
//! in version 2026013100 (`READ_ONLY_DISKS_NULLABLE`).
//!
//! This is one of the silliest API versions so far (if not *the* silliest),
//! since all the Rust structs are completely identical. However, making the API
//! accept [`DiskSource::Snapshot`] and [`DiskSource::Image`] messages _without_
//! a `read_only` field requires adding a `#[serde(default)]` attribute, which
//! changes the generated OpenAPI document, and the easiest way to generate a
//! new OpenAPI document is just to...copy and paste all the types again. Yay.

use nexus_types::external_api::params;
use omicron_common::api::external;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Create-time parameters for a `Disk`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskCreate {
    /// The common identifying metadata for the disk
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The source for this `Disk`'s blocks
    pub disk_backend: DiskBackend,

    /// The total size of the Disk (in bytes)
    pub size: ByteCount,
}

impl From<DiskCreate> for params::DiskCreate {
    fn from(old: DiskCreate) -> Self {
        let DiskCreate { identity, disk_backend, size } = old;
        params::DiskCreate { identity, disk_backend: disk_backend.into(), size }
    }
}

/// The source of a `Disk`'s blocks
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DiskBackend {
    Local {},

    Distributed {
        /// The initial source for this disk
        disk_source: DiskSource,
    },
}

impl From<DiskBackend> for params::DiskBackend {
    fn from(old: DiskBackend) -> Self {
        match old {
            DiskBackend::Local {} => params::DiskBackend::Local {},
            DiskBackend::Distributed { disk_source } => {
                params::DiskBackend::Distributed {
                    disk_source: disk_source.into(),
                }
            }
        }
    }
}

/// Different sources for a Distributed Disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DiskSource {
    /// Create a blank disk
    Blank {
        /// size of blocks for this Disk. valid values are: 512, 2048, or 4096
        block_size: params::BlockSize,
    },

    /// Create a disk from a disk snapshot
    Snapshot {
        snapshot_id: Uuid,
        /// If `true`, the disk created from this snapshot will be read-only.
        read_only: bool,
    },

    /// Create a disk from an image
    Image {
        image_id: Uuid,
        /// If `true`, the disk created from this image will be read-only.
        read_only: bool,
    },

    /// Create a blank disk that will accept bulk writes or pull blocks from an
    /// external source.
    ImportingBlocks { block_size: params::BlockSize },
}

impl From<DiskSource> for params::DiskSource {
    fn from(old: DiskSource) -> Self {
        // This is the funny part: you'll note that all these types are
        // ~*EXACTLY THE SAME*~. I love API versioning!
        match old {
            DiskSource::Blank { block_size } => Self::Blank { block_size },
            DiskSource::Snapshot { snapshot_id, read_only } => {
                Self::Snapshot { snapshot_id, read_only }
            }
            DiskSource::Image { image_id, read_only } => {
                Self::Image { image_id, read_only }
            }
            DiskSource::ImportingBlocks { block_size } => {
                Self::ImportingBlocks { block_size }
            }
        }
    }
}

/// Describe the instance's disks at creation time
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InstanceDiskAttachment {
    /// During instance creation, create and attach disks
    Create(DiskCreate),

    /// During instance creation, attach this disk
    Attach(params::InstanceDiskAttach),
}

impl From<InstanceDiskAttachment> for params::InstanceDiskAttachment {
    fn from(old: InstanceDiskAttachment) -> params::InstanceDiskAttachment {
        match old {
            InstanceDiskAttachment::Create(create) => {
                params::InstanceDiskAttachment::Create(create.into())
            }

            InstanceDiskAttachment::Attach(attach) => {
                params::InstanceDiskAttachment::Attach(attach)
            }
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
    /// Must be a Base64-encoded string, as specified in RFC 4648 § 4 (+ and /
    /// characters with padding). Maximum 32 KiB unencoded data.
    // While serde happily accepts #[serde(with = "<mod>")] as a shorthand for
    // specifying `serialize_with` and `deserialize_with`, schemars requires the
    // argument to `with` to be a type rather than merely a path prefix (i.e. a
    // mod or type). It's admittedly a bit tricky for schemars to address;
    // unlike `serialize` or `deserialize`, `JsonSchema` requires several
    // functions working together. It's unfortunate that schemars has this
    // built-in incompatibility, exacerbated by its glacial rate of progress
    // and immunity to offers of help.
    #[serde(default, with = "params::UserData")]
    pub user_data: Vec<u8>,

    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces: params::InstanceNetworkInterfaceAttachment,

    /// The external IP addresses provided to this instance.
    ///
    /// By default, all instances have outbound connectivity, but no inbound
    /// connectivity. These external addresses can be used to provide a fixed,
    /// known IP address for making inbound connections to the instance.
    // Delegates through v2025121200 → params::ExternalIpCreate
    #[serde(default)]
    pub external_ips: Vec<params::ExternalIpCreate>,

    /// Multicast groups this instance should join at creation.
    ///
    /// Groups can be specified by name, UUID, or IP address. Non-existent
    /// groups are created automatically.
    #[serde(default)]
    pub multicast_groups: Vec<params::MulticastGroupJoinSpec>,

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
    pub ssh_public_keys: Option<Vec<external::NameOrId>>,

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
    pub auto_restart_policy: Option<external::InstanceAutoRestartPolicy>,

    /// Anti-Affinity groups which this instance should be added.
    #[serde(default)]
    pub anti_affinity_groups: Vec<external::NameOrId>,

    /// The CPU platform to be used for this instance. If this is `null`, the
    /// instance requires no particular CPU platform; when it is started the
    /// instance will have the most general CPU platform supported by the sled
    /// it is initially placed on.
    #[serde(default)]
    pub cpu_platform: Option<external::InstanceCpuPlatform>,
}

impl From<InstanceCreate> for params::InstanceCreate {
    fn from(old: InstanceCreate) -> params::InstanceCreate {
        params::InstanceCreate {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            network_interfaces: old.network_interfaces,
            external_ips: old.external_ips,
            multicast_groups: old.multicast_groups,
            disks: old.disks.into_iter().map(Into::into).collect(),
            boot_disk: old.boot_disk.map(Into::into),
            ssh_public_keys: old.ssh_public_keys,
            start: old.start,
            auto_restart_policy: old.auto_restart_policy,
            anti_affinity_groups: old.anti_affinity_groups,
            cpu_platform: old.cpu_platform,
        }
    }
}
