// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Params define the request bodies of API endpoints for creating or updating resources.
 */

use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, IdentityMetadataUpdateParams,
    InstanceCpuCount, Ipv4Net, Ipv6Net, Name,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

/*
 * ORGANIZATIONS
 */

/**
 * Create-time parameters for an [`Organization`](crate::external_api::views::Organization)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct OrganizationCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/**
 * Updateable properties of an [`Organization`](crate::external_api::views::Organization)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct OrganizationUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/*
 * PROJECTS
 */

/**
 * Create-time parameters for a [`Project`](crate::external_api::views::Project)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/**
 * Updateable properties of a [`Project`](crate::external_api::views::Project)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/*
 * NETWORK INTERFACES
 */

/**
 * Create-time parameters for a
 * [`NetworkInterface`](omicron_common::api::external::NetworkInterface)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NetworkInterfaceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ip: Option<IpAddr>,
}

/*
 * INSTANCES
 */

/// Describes an attachment of a `NetworkInterface` to an `Instance`, at the
/// time the instance is created.
// NOTE: VPC's are an organizing concept for networking resources, not for
// instances. It's true that all networking resources for an instance must
// belong to a single VPC, but we don't consider instances to be "scoped" to a
// VPC in the same way that they are scoped to projects, for example.
//
// This is slightly different than some other cloud providers, such as AWS,
// which use VPCs as both a networking concept, and a container more similar to
// our concept of a project. One example for why this is useful is that "moving"
// an instance to a new VPC can be done by detaching any interfaces in the
// original VPC and attaching interfaces in the new VPC.
//
// This type then requires the VPC identifiers, exactly because instances are
// _not_ scoped to a VPC, and so the VPC and/or VPC Subnet names are not present
// in the path of endpoints handling instance operations.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "params")]
pub enum InstanceNetworkInterfaceAttachment {
    // TODO-completeness: Add variant for attaching to existing interface
    /// Create a new `NetworkInterface` for the `Instance`
    Create(InstanceCreateNetworkInterface),
    /// Default networking setup, which creates a single interface with an
    /// auto-assigned IP address from project's "default" VPC and "default" VPC
    /// Subnet.
    Default,
    /// No network interfaces at all will be created for the instance.
    None,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreateNetworkInterface {
    /// The name of the VPC in which to create the interface
    pub vpc_name: Name,

    /// The parameters used to create each interface.
    pub interface_params: Vec<InstanceCreateNetworkInterfaceParams>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreateNetworkInterfaceParams {
    /// The name of the VPC Subnet in which to create the instance
    pub vpc_subnet_name: Name,

    /// Create-time parameters for the interface.
    pub params: NetworkInterfaceCreate,
}

impl Default for InstanceNetworkInterfaceAttachment {
    fn default() -> Self {
        Self::Default
    }
}

/**
 * Create-time parameters for an [`Instance`](omicron_common::api::external::Instance)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ncpus: InstanceCpuCount,
    pub memory: ByteCount,
    pub hostname: String, /* TODO-cleanup different type? */
    /// The network interface to create or attach for this instance. If not
    /// provided, a default interface will be created.
    #[serde(default)]
    pub network_interface: InstanceNetworkInterfaceAttachment,
}

/**
 * Migration parameters for an [`Instance`](omicron_common::api::external::Instance)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrate {
    pub dst_sled_uuid: Uuid,
}

/*
 * VPCS
 */

/**
 * Create-time parameters for a [`Vpc`](crate::external_api::views::Vpc)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The IPv6 prefix for this VPC.
    ///
    /// All IPv6 subnets created from this VPC must be taken from this range,
    /// which sould be a Unique Local Address in the range `fd00::/48`. The
    /// default VPC Subnet will have the first `/64` range from this prefix.
    pub ipv6_prefix: Option<Ipv6Net>,

    pub dns_name: Name,
}

/**
 * Updateable properties of a [`Vpc`](crate::external_api::views::Vpc)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    pub dns_name: Option<Name>,
}

/**
 * Create-time parameters for a [`VpcSubnet`](crate::external_api::views::VpcSubnet)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnetCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The IPv4 address range for this subnet.
    ///
    /// It must be allocated from an RFC 1918 private address range, and must
    /// not overlap with any other existing subnet in the VPC.
    pub ipv4_block: Ipv4Net,

    /// The IPv6 address range for this subnet.
    ///
    /// It must be allocated from the RFC 4193 Unique Local Address range, with
    /// the prefix equal to the parent VPC's prefix. A random `/64` block will
    /// be assigned if one is not provided. It must not overlap with any
    /// existing subnet in the VPC.
    pub ipv6_block: Option<Ipv6Net>,
}

/**
 * Updateable properties of a [`VpcSubnet`](crate::external_api::views::VpcSubnet)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnetUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    // TODO-correctness: These need to be removed. Changing these is effectively
    // creating a new resource, so we should require explicit
    // deletion/recreation by the client.
    pub ipv4_block: Option<Ipv4Net>,
    pub ipv6_block: Option<Ipv6Net>,
}

/*
 * VPC ROUTERS
 */

/// Create-time parameters for a [`VpcRouter`](omicron_common::api::external::VpcRouter)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of a [`VpcRouter`](omicron_common::api::external::VpcRouter)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/*
 * DISKS
 */

/**
 * Create-time parameters for a [`Disk`](omicron_common::api::external::Disk)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskCreate {
    /** common identifying metadata */
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /** id for snapshot from which the Disk should be created, if any */
    pub snapshot_id: Option<Uuid>, /* TODO should be a name? */
    /** size of the Disk */
    pub size: ByteCount,
}

const BLOCK_SIZE: u32 = 1_u32 << 12;
const EXTENT_SIZE: u32 = 1_u32 << 20;

impl DiskCreate {
    pub fn block_size(&self) -> ByteCount {
        ByteCount::from(BLOCK_SIZE)
    }

    pub fn blocks_per_extent(&self) -> i64 {
        EXTENT_SIZE as i64 / BLOCK_SIZE as i64
    }

    pub fn extent_count(&self) -> i64 {
        let extent_size = EXTENT_SIZE as i64;
        let size = self.size.to_bytes() as i64;
        size / extent_size
            + ((size % extent_size) + extent_size - 1) / extent_size
    }
}

/**
 * Parameters for the [`Disk`](omicron_common::api::external::Disk) to be
 * attached or detached to an instance
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskIdentifier {
    pub disk: Name,
}

/*
 * BUILT-IN USERS
 *
 * These cannot be created via the external API, but we use the same interfaces
 * for creating them internally as we use for types that can be created in the
 * external API.
 */

/**
 * Create-time parameters for a [`UserBuiltin`](crate::db::model::UserBuiltin)
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UserBuiltinCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::convert::TryFrom;

    fn new_disk_create_params(size: ByteCount) -> DiskCreate {
        DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from("myobject".to_string()).unwrap(),
                description: "desc".to_string(),
            },
            snapshot_id: None,
            size,
        }
    }

    #[test]
    fn test_extent_count() {
        let params = new_disk_create_params(ByteCount::try_from(0u64).unwrap());
        assert_eq!(0, params.extent_count());

        let params = new_disk_create_params(ByteCount::try_from(1u64).unwrap());
        assert_eq!(1, params.extent_count());
        let params = new_disk_create_params(
            ByteCount::try_from(EXTENT_SIZE - 1).unwrap(),
        );
        assert_eq!(1, params.extent_count());
        let params =
            new_disk_create_params(ByteCount::try_from(EXTENT_SIZE).unwrap());
        assert_eq!(1, params.extent_count());

        let params = new_disk_create_params(
            ByteCount::try_from(EXTENT_SIZE + 1).unwrap(),
        );
        assert_eq!(2, params.extent_count());

        // Mostly just checking we don't blow up on an unwrap here.
        let params =
            new_disk_create_params(ByteCount::try_from(i64::MAX).unwrap());
        assert!(
            params.size.to_bytes()
                <= (params.extent_count() as u64)
                    * (params.blocks_per_extent() as u64)
                    * params.block_size().to_bytes()
        );
    }
}
