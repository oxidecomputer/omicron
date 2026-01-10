// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version DUAL_STACK_NICS.
//!
//! This version introduces dual-stack network interface support and adds
//! `ip_version` to IP pool selection.

use omicron_common::address::ConcreteIp;
use omicron_common::api::external::{
    ByteCount, Error, Hostname, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, Name,
    NameOrId,
};
use oxnet::{Ipv4Net, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, Ipv6Addr};

use crate::v2025112000;
use crate::v2026010100;

use crate::v2025112000::instance::{UserData, bool_true};
use crate::v2025120300::instance::InstanceDiskAttachment;

// --- IP Assignment Types ---

// Shadow type for JsonSchema generation
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
enum IpAssignmentShadow<T: ConcreteIp> {
    #[default]
    Auto,
    Explicit(T),
}

trait IpAssignmentSchema {
    fn ip_assignment_schema_name() -> String;
}

impl IpAssignmentSchema for Ipv4Addr {
    fn ip_assignment_schema_name() -> String {
        String::from("Ipv4Assignment")
    }
}

impl IpAssignmentSchema for Ipv6Addr {
    fn ip_assignment_schema_name() -> String {
        String::from("Ipv6Assignment")
    }
}

/// How a VPC-private IP address is assigned to a network interface.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum IpAssignment<T: ConcreteIp> {
    /// Automatically assign an IP address from the VPC Subnet.
    #[default]
    Auto,
    /// Explicitly assign a specific address, if available.
    Explicit(T),
}

impl<T> JsonSchema for IpAssignment<T>
where
    T: ConcreteIp + IpAssignmentSchema,
{
    fn schema_name() -> String {
        <T as IpAssignmentSchema>::ip_assignment_schema_name()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        IpAssignmentShadow::<T>::json_schema(generator)
    }
}

impl<T: ConcreteIp> From<T> for IpAssignment<T> {
    fn from(ip: T) -> Self {
        Self::Explicit(ip)
    }
}

/// How to assign an IPv4 address.
pub type Ipv4Assignment = IpAssignment<Ipv4Addr>;

/// How to assign an IPv6 address.
pub type Ipv6Assignment = IpAssignment<Ipv6Addr>;

// --- Private IP Stack Types ---

/// Configuration for a network interface's IPv4 addressing.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct PrivateIpv4StackCreate {
    /// The VPC-private address to assign to the interface.
    pub ip: Ipv4Assignment,
    /// Additional IP networks the interface can send / receive on.
    #[serde(default)]
    pub transit_ips: Vec<Ipv4Net>,
}

/// Configuration for a network interface's IPv6 addressing.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct PrivateIpv6StackCreate {
    /// The VPC-private address to assign to the interface.
    pub ip: Ipv6Assignment,
    /// Additional IP networks the interface can send / receive on.
    #[serde(default)]
    pub transit_ips: Vec<Ipv6Net>,
}

/// Create parameters for a network interface's IP stack.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum PrivateIpStackCreate {
    /// The interface has only an IPv4 stack.
    V4(PrivateIpv4StackCreate),
    /// The interface has only an IPv6 stack.
    V6(PrivateIpv6StackCreate),
    /// The interface has both an IPv4 and IPv6 stack.
    DualStack { v4: PrivateIpv4StackCreate, v6: PrivateIpv6StackCreate },
}

impl PrivateIpStackCreate {
    /// Construct an IP configuration with only an automatic IPv4 address.
    pub fn auto_ipv4() -> Self {
        PrivateIpStackCreate::V4(PrivateIpv4StackCreate::default())
    }

    /// Construct an IP configuration with both IPv4 / IPv6 addresses.
    pub fn auto_dual_stack() -> Self {
        PrivateIpStackCreate::DualStack {
            v4: PrivateIpv4StackCreate::default(),
            v6: PrivateIpv6StackCreate::default(),
        }
    }
}

// --- Network Interface Types ---

/// Describes an attachment of an `InstanceNetworkInterface` to an `Instance`,
/// at the time the instance is created.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "params", rename_all = "snake_case")]
#[derive(Default)]
pub enum InstanceNetworkInterfaceAttachment {
    /// Create one or more `InstanceNetworkInterface`s for the `Instance`.
    Create(Vec<InstanceNetworkInterfaceCreate>),

    /// Create a single primary interface with an automatically-assigned IPv4
    /// address.
    DefaultIpv4,

    /// Create a single primary interface with an automatically-assigned IPv6
    /// address.
    DefaultIpv6,

    /// Create a single primary interface with automatically-assigned IPv4 and
    /// IPv6 addresses.
    #[default]
    DefaultDualStack,

    /// No network interfaces at all will be created for the instance.
    None,
}

impl TryFrom<v2025112000::instance::InstanceNetworkInterfaceAttachment>
    for InstanceNetworkInterfaceAttachment
{
    type Error = Error;

    fn try_from(
        value: v2025112000::instance::InstanceNetworkInterfaceAttachment,
    ) -> Result<Self, Self::Error> {
        match value {
            v2025112000::instance::InstanceNetworkInterfaceAttachment::Create(
                nics,
            ) => nics
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map(Self::Create),
            v2025112000::instance::InstanceNetworkInterfaceAttachment::Default => {
                Ok(Self::DefaultIpv4)
            }
            v2025112000::instance::InstanceNetworkInterfaceAttachment::None => {
                Ok(Self::None)
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
    /// The IP stack configuration for this interface.
    #[serde(default = "PrivateIpStackCreate::auto_dual_stack")]
    pub ip_config: PrivateIpStackCreate,
}

impl TryFrom<v2025112000::instance::InstanceNetworkInterfaceCreate>
    for InstanceNetworkInterfaceCreate
{
    type Error = Error;

    fn try_from(
        value: v2025112000::instance::InstanceNetworkInterfaceCreate,
    ) -> Result<Self, Self::Error> {
        use oxnet::IpNet;

        let mut ipv4_transit_ips = Vec::new();
        let mut ipv6_transit_ips = Vec::new();
        for net in value.transit_ips {
            match net {
                IpNet::V4(ipv4) => ipv4_transit_ips.push(ipv4),
                IpNet::V6(ipv6) => ipv6_transit_ips.push(ipv6),
            }
        }
        if !ipv4_transit_ips.is_empty() && !ipv6_transit_ips.is_empty() {
            return Err(Error::invalid_request(
                "Cannot specify both IPv4 and IPv6 transit IPs",
            ));
        }
        let ip_config = match value.ip {
            None => {
                if !ipv4_transit_ips.is_empty() {
                    PrivateIpStackCreate::V4(PrivateIpv4StackCreate {
                        ip: IpAssignment::Auto,
                        transit_ips: ipv4_transit_ips,
                    })
                } else if !ipv6_transit_ips.is_empty() {
                    PrivateIpStackCreate::V6(PrivateIpv6StackCreate {
                        ip: IpAssignment::Auto,
                        transit_ips: ipv6_transit_ips,
                    })
                } else {
                    PrivateIpStackCreate::auto_ipv4()
                }
            }
            Some(std::net::IpAddr::V4(ipv4)) => {
                if !ipv6_transit_ips.is_empty() {
                    return Err(Error::invalid_request(
                        "Cannot specify IPv6 transit IPs with an IPv4 address",
                    ));
                }
                PrivateIpStackCreate::V4(PrivateIpv4StackCreate {
                    ip: IpAssignment::Explicit(ipv4),
                    transit_ips: ipv4_transit_ips,
                })
            }
            Some(std::net::IpAddr::V6(ipv6)) => {
                if !ipv4_transit_ips.is_empty() {
                    return Err(Error::invalid_request(
                        "Cannot specify IPv4 transit IPs with an IPv6 address",
                    ));
                }
                PrivateIpStackCreate::V6(PrivateIpv6StackCreate {
                    ip: IpAssignment::Explicit(ipv6),
                    transit_ips: ipv6_transit_ips,
                })
            }
        };
        Ok(Self {
            identity: value.identity,
            vpc_name: value.vpc_name,
            subnet_name: value.subnet_name,
            ip_config,
        })
    }
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
    // ExternalIpCreate is unchanged from v2026010100.
    #[serde(default)]
    pub external_ips: Vec<v2026010100::instance::ExternalIpCreate>,
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

impl TryFrom<v2026010100::instance::InstanceCreate> for InstanceCreate {
    type Error = Error;

    fn try_from(
        old: v2026010100::instance::InstanceCreate,
    ) -> Result<Self, Self::Error> {
        let network_interfaces = old.network_interfaces.try_into()?;
        Ok(InstanceCreate {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            network_interfaces,
            // ExternalIpCreate is unchanged from v2026010100.
            external_ips: old.external_ips,
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
