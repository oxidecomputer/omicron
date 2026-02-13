// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version SILO_PROJECT_IP_VERSION_AND_POOL_TYPE.
//!
//! This version has old-style network interfaces with single IP + transit_ips.

use omicron_common::address::IpVersion;
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, Name,
    NameOrId,
};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::instance::{UserData, bool_true};
use crate::v2025_12_03_00;
use crate::v2025_12_03_00::instance::InstanceDiskAttachment;

/// Describes an attachment of an `InstanceNetworkInterface` to an `Instance`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "params", rename_all = "snake_case")]
pub enum InstanceNetworkInterfaceAttachment {
    /// Create one or more `InstanceNetworkInterface`s for the `Instance`.
    Create(Vec<InstanceNetworkInterfaceCreate>),

    /// The default networking configuration for an instance is to create a
    /// single primary interface with an automatically-assigned IP address.
    #[default]
    Default,

    /// No network interfaces at all will be created for the instance.
    None,
}

impl From<v2025_11_20_00::instance::InstanceNetworkInterfaceAttachment>
    for InstanceNetworkInterfaceAttachment
{
    fn from(
        old: v2025_11_20_00::instance::InstanceNetworkInterfaceAttachment,
    ) -> Self {
        match old {
            v2025_11_20_00::instance::InstanceNetworkInterfaceAttachment::Create(
                nics,
            ) => InstanceNetworkInterfaceAttachment::Create(
                nics.into_iter().map(Into::into).collect(),
            ),
            v2025_11_20_00::instance::InstanceNetworkInterfaceAttachment::Default => {
                InstanceNetworkInterfaceAttachment::Default
            }
            v2025_11_20_00::instance::InstanceNetworkInterfaceAttachment::None => {
                InstanceNetworkInterfaceAttachment::None
            }
        }
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

impl From<v2025_11_20_00::instance::InstanceNetworkInterfaceCreate>
    for InstanceNetworkInterfaceCreate
{
    fn from(
        old: v2025_11_20_00::instance::InstanceNetworkInterfaceCreate,
    ) -> Self {
        InstanceNetworkInterfaceCreate {
            identity: old.identity,
            vpc_name: old.vpc_name,
            subnet_name: old.subnet_name,
            ip: old.ip,
            transit_ips: old.transit_ips,
        }
    }
}

/// The type of IP address to attach to an instance during creation.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalIpCreate {
    /// An IP address providing both inbound and outbound access. The address is
    /// automatically assigned from the provided IP pool or the default IP pool
    /// if not specified.
    Ephemeral {
        /// Name or ID of the IP pool to use.
        pool: Option<NameOrId>,
        /// IP version to use when allocating from the default pool. Only used
        /// when `pool` is not specified. Required if multiple default pools of
        /// different IP versions exist. Allocation fails if no pool of the
        /// requested version is available.
        #[serde(default)]
        ip_version: Option<IpVersion>,
    },
    /// A floating IP address.
    Floating { floating_ip: NameOrId },
}

impl From<v2025_11_20_00::instance::ExternalIpCreate> for ExternalIpCreate {
    fn from(
        old: v2025_11_20_00::instance::ExternalIpCreate,
    ) -> ExternalIpCreate {
        match old {
            v2025_11_20_00::instance::ExternalIpCreate::Ephemeral { pool } => {
                ExternalIpCreate::Ephemeral { pool, ip_version: None }
            }
            v2025_11_20_00::instance::ExternalIpCreate::Floating {
                floating_ip,
            } => ExternalIpCreate::Floating { floating_ip },
        }
    }
}

/// Parameters for creating an ephemeral IP address for an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct EphemeralIpCreate {
    /// Name or ID of the IP pool used to allocate an address.
    pub pool: Option<NameOrId>,
    /// IP version to use when allocating from the default pool. Only used
    /// when `pool` is not specified.
    #[serde(default)]
    pub ip_version: Option<IpVersion>,
}

impl From<v2025_11_20_00::instance::EphemeralIpCreate> for EphemeralIpCreate {
    fn from(
        old: v2025_11_20_00::instance::EphemeralIpCreate,
    ) -> EphemeralIpCreate {
        EphemeralIpCreate { pool: old.pool, ip_version: None }
    }
}

/// Create-time parameters for an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ncpus: InstanceCpuCount,
    pub memory: ByteCount,
    pub hostname: Hostname,
    #[serde(default, with = "UserData")]
    pub user_data: Vec<u8>,
    #[serde(default)]
    pub network_interfaces: InstanceNetworkInterfaceAttachment,
    #[serde(default)]
    pub external_ips: Vec<ExternalIpCreate>,
    #[serde(default)]
    pub multicast_groups: Vec<NameOrId>,
    #[serde(default)]
    pub disks: Vec<InstanceDiskAttachment>,
    #[serde(default)]
    pub boot_disk: Option<InstanceDiskAttachment>,
    pub ssh_public_keys: Option<Vec<NameOrId>>,
    #[serde(default = "bool_true")]
    pub start: bool,
    #[serde(default)]
    pub auto_restart_policy: Option<InstanceAutoRestartPolicy>,
    #[serde(default)]
    pub anti_affinity_groups: Vec<NameOrId>,
    #[serde(default)]
    pub cpu_platform: Option<InstanceCpuPlatform>,
}

impl From<v2025_12_03_00::instance::InstanceCreate> for InstanceCreate {
    fn from(old: v2025_12_03_00::instance::InstanceCreate) -> Self {
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
