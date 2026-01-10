// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version SILO_PROJECT_IP_VERSION_AND_POOL_TYPE.
//!
//! This version has old-style network interfaces with single IP + transit_ips.

use api_identity::ObjectIdentity;
use omicron_common::api::external;
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadata, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform,
    IpVersion, MacAddr, NameOrId, ObjectIdentity, PrivateIpStack,
    PrivateIpv4Stack, PrivateIpv6Stack,
};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

use crate::v2025120300;
use crate::v2025121200;

use crate::v2025112000::instance::{UserData, bool_true};
use crate::v2025120300::instance::InstanceDiskAttachment;

// Re-export network interface types from the initial version where they were
// first defined. These types are unchanged in this version.
pub use crate::v2025112000::instance::{
    InstanceNetworkInterfaceAttachment, InstanceNetworkInterfaceCreate,
};

/// An `InstanceNetworkInterface` represents a virtual network interface device
/// attached to an instance.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InstanceNetworkInterface {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The Instance to which the interface belongs.
    pub instance_id: Uuid,

    /// The VPC to which the interface belongs.
    pub vpc_id: Uuid,

    /// The subnet to which the interface belongs.
    pub subnet_id: Uuid,

    /// The MAC address assigned to this interface.
    pub mac: MacAddr,

    /// The IP address assigned to this interface.
    pub ip: IpAddr,

    /// True if this interface is the primary for the instance to which it's
    /// attached.
    pub primary: bool,

    /// A set of additional networks that this interface may send and
    /// receive traffic on.
    #[serde(default)]
    pub transit_ips: Vec<IpNet>,
}

impl TryFrom<InstanceNetworkInterface> for external::InstanceNetworkInterface {
    type Error = external::Error;

    fn try_from(value: InstanceNetworkInterface) -> Result<Self, Self::Error> {
        let ip_stack = match value.ip {
            IpAddr::V4(ip) => {
                let transit_ips = value
                    .transit_ips
                    .into_iter()
                    .map(|ipnet| match ipnet {
                        IpNet::V4(v4) => Ok(v4),
                        IpNet::V6(_) => Err(external::Error::invalid_request(
                            "A network interface cannot have an IPv4 \
                                address and IPv6 transit IPs",
                        )),
                    })
                    .collect::<Result<_, _>>()?;
                PrivateIpStack::V4(PrivateIpv4Stack { ip, transit_ips })
            }
            IpAddr::V6(ip) => {
                let transit_ips = value
                    .transit_ips
                    .into_iter()
                    .map(|ipnet| match ipnet {
                        IpNet::V6(v6) => Ok(v6),
                        IpNet::V4(_) => Err(external::Error::invalid_request(
                            "A network interface cannot have an IPv6 \
                                address and IPv4 transit IPs",
                        )),
                    })
                    .collect::<Result<_, _>>()?;
                PrivateIpStack::V6(PrivateIpv6Stack { ip, transit_ips })
            }
        };
        Ok(external::InstanceNetworkInterface {
            identity: value.identity,
            instance_id: value.instance_id,
            vpc_id: value.vpc_id,
            subnet_id: value.subnet_id,
            mac: value.mac,
            primary: value.primary,
            ip_stack,
        })
    }
}

impl TryFrom<external::InstanceNetworkInterface> for InstanceNetworkInterface {
    type Error = external::Error;

    fn try_from(
        value: external::InstanceNetworkInterface,
    ) -> Result<Self, Self::Error> {
        let (ip, transit_ips) = match value.ip_stack {
            PrivateIpStack::V4(v4) => (
                v4.ip.into(),
                v4.transit_ips.into_iter().map(Into::into).collect(),
            ),
            PrivateIpStack::V6(v6) => (
                v6.ip.into(),
                v6.transit_ips.into_iter().map(Into::into).collect(),
            ),
            PrivateIpStack::DualStack { v4, v6 } => {
                return Err(external::Error::invalid_request(format!(
                    "The network interface with ID '{}' is \
                        a dual-stack NIC, with IPv4 address '{}' \
                        and IPv6 address '{}'. However, the version \
                        of the client being used is unable to fully \
                        represent both IPv4 and IPv6 addresses. \
                        Update your client and retry the request.",
                    value.identity.id, v4.ip, v6.ip,
                )));
            }
        };
        Ok(Self {
            identity: value.identity,
            instance_id: value.instance_id,
            vpc_id: value.vpc_id,
            subnet_id: value.subnet_id,
            mac: value.mac,
            ip,
            primary: value.primary,
            transit_ips,
        })
    }
}

// --- External IP Types (with flat pool/ip_version fields) ---

/// The type of IP address to attach to an instance during creation.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalIpCreate {
    /// An IP address providing both inbound and outbound access.
    Ephemeral {
        /// Name or ID of the IP pool to use. If unspecified, the
        /// default IP pool will be used.
        pool: Option<NameOrId>,
        /// The IP version preference for address allocation.
        ip_version: Option<IpVersion>,
    },
    /// A floating IP address.
    Floating {
        /// The name or ID of the floating IP address to attach.
        floating_ip: NameOrId,
    },
}

/// Parameters for creating an ephemeral IP address for an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct EphemeralIpCreate {
    /// Name or ID of the IP pool used to allocate an address.
    pub pool: Option<NameOrId>,
    /// The IP version preference for address allocation.
    pub ip_version: Option<IpVersion>,
}

// --- Instance Create ---

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

impl From<v2025120300::instance::InstanceCreate> for InstanceCreate {
    fn from(old: v2025120300::instance::InstanceCreate) -> Self {
        // Chain conversion through v2025121200 for external_ips.
        // v2025120300 uses v2025112000's ExternalIpCreate (without ip_version).
        // v2025121200 converts to this version's ExternalIpCreate (with ip_version).
        let external_ips: Vec<ExternalIpCreate> = old
            .external_ips
            .into_iter()
            .map(|eip| {
                let intermediate: v2025121200::instance::ExternalIpCreate =
                    eip.into();
                intermediate.into()
            })
            .collect();

        InstanceCreate {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            // network_interfaces is the same type (v2025112000)
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
        }
    }
}

impl From<v2025121200::instance::EphemeralIpCreate> for EphemeralIpCreate {
    fn from(
        old: v2025121200::instance::EphemeralIpCreate,
    ) -> EphemeralIpCreate {
        EphemeralIpCreate { pool: old.pool, ip_version: None }
    }
}

impl From<v2025121200::instance::ExternalIpCreate> for ExternalIpCreate {
    fn from(old: v2025121200::instance::ExternalIpCreate) -> ExternalIpCreate {
        match old {
            v2025121200::instance::ExternalIpCreate::Ephemeral { pool } => {
                ExternalIpCreate::Ephemeral { pool, ip_version: None }
            }
            v2025121200::instance::ExternalIpCreate::Floating {
                floating_ip,
            } => ExternalIpCreate::Floating { floating_ip },
        }
    }
}

impl From<v2025121200::instance::InstanceCreate> for InstanceCreate {
    fn from(old: v2025121200::instance::InstanceCreate) -> InstanceCreate {
        InstanceCreate {
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
