// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version BGP_PEER_COLLISION_STATE.
//!
//! This version has IP creation types without the `ip_version` field.

use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, NameOrId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2025112000;
use crate::v2025120300;
use crate::v2026010100;

use crate::v2025112000::instance::{UserData, bool_true};
use crate::v2025120300::instance::InstanceDiskAttachment;

/// Parameters for creating an ephemeral IP address for an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct EphemeralIpCreate {
    /// Name or ID of the IP pool used to allocate an address.
    /// If unspecified, the default IP pool will be used.
    pub pool: Option<NameOrId>,
}

impl From<v2025112000::instance::EphemeralIpCreate> for EphemeralIpCreate {
    fn from(
        old: v2025112000::instance::EphemeralIpCreate,
    ) -> EphemeralIpCreate {
        EphemeralIpCreate { pool: old.pool }
    }
}

impl From<EphemeralIpCreate> for v2026010100::instance::EphemeralIpCreate {
    fn from(
        old: EphemeralIpCreate,
    ) -> v2026010100::instance::EphemeralIpCreate {
        v2026010100::instance::EphemeralIpCreate {
            pool: old.pool,
            ip_version: None,
        }
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
        pool: Option<NameOrId>,
    },
    /// A floating IP address.
    Floating {
        /// The name or ID of the floating IP address to attach.
        floating_ip: NameOrId,
    },
}

impl From<v2025112000::instance::ExternalIpCreate> for ExternalIpCreate {
    fn from(old: v2025112000::instance::ExternalIpCreate) -> Self {
        match old {
            v2025112000::instance::ExternalIpCreate::Ephemeral { pool } => {
                ExternalIpCreate::Ephemeral { pool }
            }
            v2025112000::instance::ExternalIpCreate::Floating {
                floating_ip,
            } => ExternalIpCreate::Floating { floating_ip },
        }
    }
}

impl From<ExternalIpCreate> for v2026010100::instance::ExternalIpCreate {
    fn from(old: ExternalIpCreate) -> v2026010100::instance::ExternalIpCreate {
        match old {
            ExternalIpCreate::Ephemeral { pool } => {
                v2026010100::instance::ExternalIpCreate::Ephemeral {
                    pool,
                    ip_version: None,
                }
            }
            ExternalIpCreate::Floating { floating_ip } => {
                v2026010100::instance::ExternalIpCreate::Floating {
                    floating_ip,
                }
            }
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
    #[serde(default, with = "UserData")]
    pub user_data: Vec<u8>,
    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces:
        v2026010100::instance::InstanceNetworkInterfaceAttachment,
    /// The external IP addresses provided to this instance.
    #[serde(default)]
    pub external_ips: Vec<ExternalIpCreate>,
    /// The multicast groups this instance should join.
    #[serde(default)]
    pub multicast_groups: Vec<NameOrId>,
    /// A list of disks to be attached to the instance.
    #[serde(default)]
    pub disks: Vec<InstanceDiskAttachment>,
    /// The disk the instance is configured to boot from.
    #[serde(default)]
    pub boot_disk: Option<InstanceDiskAttachment>,
    /// An allowlist of SSH public keys to be transferred to the instance.
    pub ssh_public_keys: Option<Vec<NameOrId>>,
    /// Should this instance be started upon creation; true by default.
    #[serde(default = "bool_true")]
    pub start: bool,
    /// The auto-restart policy for this instance.
    #[serde(default)]
    pub auto_restart_policy: Option<InstanceAutoRestartPolicy>,
    /// Anti-Affinity groups which this instance should be added.
    #[serde(default)]
    pub anti_affinity_groups: Vec<NameOrId>,
    /// The CPU platform to be used for this instance.
    #[serde(default)]
    pub cpu_platform: Option<InstanceCpuPlatform>,
}

impl From<v2025120300::instance::InstanceCreate> for InstanceCreate {
    fn from(old: v2025120300::instance::InstanceCreate) -> Self {
        InstanceCreate {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            network_interfaces: old.network_interfaces.into(),
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

// Direct conversion from initial version (chains through LOCAL_STORAGE)
impl From<v2025112000::instance::InstanceCreate> for InstanceCreate {
    fn from(old: v2025112000::instance::InstanceCreate) -> Self {
        let intermediate: v2025120300::instance::InstanceCreate = old.into();
        intermediate.into()
    }
}

impl From<InstanceCreate> for v2026010100::instance::InstanceCreate {
    fn from(old: InstanceCreate) -> v2026010100::instance::InstanceCreate {
        v2026010100::instance::InstanceCreate {
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

// Conversions from v2025112000 to v2026010100 for InstanceNetworkInterfaceAttachment
// and InstanceNetworkInterfaceCreate are defined in v2026010100::instance.
