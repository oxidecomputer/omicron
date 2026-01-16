// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2026011300 to 2026011500.
//!
//! ## Network Interface Changes
//!
//! [`InstanceNetworkInterfaceCreate`] uses `subnet_name: Name` for a single
//! subnet. Newer versions use `subnets: Vec<NetworkInterfaceSubnetConfig>`
//! to support multiple subnets with optional direct attachment.
//!
//! Affected endpoints:
//! - `POST /v1/instances` (instance_create)
//! - `POST /v1/network-interfaces` (instance_network_interface_create)
//!
//! [`InstanceNetworkInterfaceCreate`]: self::InstanceNetworkInterfaceCreate

use nexus_types::external_api::params;
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, Name,
    NameOrId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Create-time parameters for an `InstanceNetworkInterface`
///
/// This version uses a single `subnet_name` field. Newer versions use
/// `subnets: Vec<NetworkInterfaceSubnetConfig>` for multiple subnets.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceNetworkInterfaceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The VPC in which to create the interface.
    pub vpc_name: Name,
    /// The VPC Subnet in which to create the interface.
    pub subnet_name: Name,
    /// The IP stack configuration for this interface.
    ///
    /// If not provided, a default configuration will be used, which creates a
    /// dual-stack IPv4 / IPv6 interface.
    #[serde(default = "params::PrivateIpStackCreate::auto_dual_stack")]
    pub ip_config: params::PrivateIpStackCreate,
}

impl From<InstanceNetworkInterfaceCreate>
    for params::InstanceNetworkInterfaceCreate
{
    fn from(value: InstanceNetworkInterfaceCreate) -> Self {
        Self {
            identity: value.identity,
            vpc_name: value.vpc_name,
            subnets: vec![params::NetworkInterfaceSubnetConfig {
                subnet: value.subnet_name.into(),
                attached: false,
            }],
            ip_config: value.ip_config,
        }
    }
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

    /// Create a single primary interface with an automatically-assigned IPv4
    /// address.
    ///
    /// The IP will be pulled from the Project's default VPC / VPC Subnet.
    DefaultIpv4,

    /// Create a single primary interface with an automatically-assigned IPv6
    /// address.
    ///
    /// The IP will be pulled from the Project's default VPC / VPC Subnet.
    DefaultIpv6,

    /// Create a single primary interface with automatically-assigned IPv4 and
    /// IPv6 addresses.
    ///
    /// The IPs will be pulled from the Project's default VPC / VPC Subnet.
    #[default]
    DefaultDualStack,

    /// No network interfaces at all will be created for the instance.
    None,
}

impl From<InstanceNetworkInterfaceAttachment>
    for params::InstanceNetworkInterfaceAttachment
{
    fn from(value: InstanceNetworkInterfaceAttachment) -> Self {
        match value {
            InstanceNetworkInterfaceAttachment::Create(nics) => {
                Self::Create(nics.into_iter().map(Into::into).collect())
            }
            InstanceNetworkInterfaceAttachment::DefaultIpv4 => {
                Self::DefaultIpv4
            }
            InstanceNetworkInterfaceAttachment::DefaultIpv6 => {
                Self::DefaultIpv6
            }
            InstanceNetworkInterfaceAttachment::DefaultDualStack => {
                Self::DefaultDualStack
            }
            InstanceNetworkInterfaceAttachment::None => Self::None,
        }
    }
}

/// Create-time parameters for an `Instance`
///
/// This version uses the old `InstanceNetworkInterfaceAttachment` with
/// `subnet_name: Name`. Newer versions use `subnets: Vec<NetworkInterfaceSubnetConfig>`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The number of vCPUs to be allocated to the instance.
    pub ncpus: InstanceCpuCount,
    /// The amount of RAM (in bytes) to be allocated to the instance.
    pub memory: ByteCount,
    /// The hostname to be assigned to the instance.
    pub hostname: Hostname,

    /// User data for instance initialization systems (such as cloud-init).
    #[serde(default, with = "params::UserData")]
    pub user_data: Vec<u8>,

    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces: InstanceNetworkInterfaceAttachment,

    /// The external IP addresses provided to this instance.
    #[serde(default)]
    pub external_ips: Vec<params::ExternalIpCreate>,

    /// Multicast groups this instance should join at creation.
    #[serde(default)]
    pub multicast_groups: Vec<params::MulticastGroupJoinSpec>,

    /// A list of disks to be attached to the instance.
    #[serde(default)]
    pub disks: Vec<params::InstanceDiskAttachment>,

    /// The disk the instance is configured to boot from.
    #[serde(default)]
    pub boot_disk: Option<params::InstanceDiskAttachment>,

    /// An allowlist of SSH public keys to be transferred to the instance.
    pub ssh_public_keys: Option<Vec<NameOrId>>,

    /// Should this instance be started upon creation; true by default.
    #[serde(default = "params::bool_true")]
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

impl From<InstanceCreate> for params::InstanceCreate {
    fn from(value: InstanceCreate) -> Self {
        Self {
            identity: value.identity,
            ncpus: value.ncpus,
            memory: value.memory,
            hostname: value.hostname,
            user_data: value.user_data,
            network_interfaces: value.network_interfaces.into(),
            external_ips: value.external_ips,
            multicast_groups: value.multicast_groups,
            disks: value.disks,
            boot_disk: value.boot_disk,
            ssh_public_keys: value.ssh_public_keys,
            start: value.start,
            auto_restart_policy: value.auto_restart_policy,
            anti_affinity_groups: value.anti_affinity_groups,
            cpu_platform: value.cpu_platform,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// InstanceNetworkInterfaceCreate: subnet_name → single unattached subnet
        #[test]
        fn nic_create_converts_subnet_name_to_single_unattached(
            // Names must start with a letter, can contain letters/numbers,
            // but cannot end with a hyphen
            subnet_name in "[a-z]([a-z0-9]*[a-z0-9])?"
        ) {
            let name: Name = subnet_name.parse().unwrap();
            let old = InstanceNetworkInterfaceCreate {
                identity: IdentityMetadataCreateParams {
                    name: "nic0".parse().unwrap(),
                    description: "test".to_string(),
                },
                vpc_name: "default".parse().unwrap(),
                subnet_name: name.clone(),
                ip_config: params::PrivateIpStackCreate::auto_dual_stack(),
            };
            let result: params::InstanceNetworkInterfaceCreate = old.into();

            prop_assert!(result.subnets.len() == 1);
            prop_assert!(!result.subnets[0].attached);
            prop_assert!(matches!(
                &result.subnets[0].subnet,
                NameOrId::Name(n) if n == &name
            ));
        }

        /// InstanceNetworkInterfaceAttachment::Create preserves NIC count
        #[test]
        fn attachment_create_preserves_nic_count(count in 1usize..5) {
            let nics: Vec<_> = (0..count)
                .map(|i| InstanceNetworkInterfaceCreate {
                    identity: IdentityMetadataCreateParams {
                        name: format!("nic{i}").parse().unwrap(),
                        description: "test".to_string(),
                    },
                    vpc_name: "default".parse().unwrap(),
                    subnet_name: format!("subnet{i}").parse().unwrap(),
                    ip_config: params::PrivateIpStackCreate::auto_dual_stack(),
                })
                .collect();
            let old = InstanceNetworkInterfaceAttachment::Create(nics);
            let result: params::InstanceNetworkInterfaceAttachment = old.into();

            match result {
                params::InstanceNetworkInterfaceAttachment::Create(converted) => {
                    prop_assert!(converted.len() == count);
                }
                _ => panic!("expected Create variant"),
            }
        }
    }
}
