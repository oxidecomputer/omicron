// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Params define the request bodies of API endpoints for creating or updating resources.

use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, IdentityMetadataUpdateParams,
    InstanceCpuCount, Ipv4Net, Ipv6Net, Name,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::IpAddr;
use uuid::Uuid;

// Silos

/// Create-time parameters for a [`Silo`](crate::external_api::views::Silo)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub discoverable: bool,
}

// ORGANIZATIONS

/// Create-time parameters for an [`Organization`](crate::external_api::views::Organization)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct OrganizationCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of an [`Organization`](crate::external_api::views::Organization)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct OrganizationUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// PROJECTS

/// Create-time parameters for a [`Project`](crate::external_api::views::Project)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of a [`Project`](crate::external_api::views::Project)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// NETWORK INTERFACES

/// Create-time parameters for a
/// [`NetworkInterface`](omicron_common::api::external::NetworkInterface)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NetworkInterfaceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The VPC in which to create the interface.
    pub vpc_name: Name,
    /// The VPC Subnet in which to create the interface.
    pub subnet_name: Name,
    /// The IP address for the interface. One will be auto-assigned if not provided.
    pub ip: Option<IpAddr>,
}

// INSTANCES

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
    /// Create one or more `NetworkInterface`s for the `Instance`
    Create(Vec<NetworkInterfaceCreate>),

    /// Default networking setup, which creates a single interface with an
    /// auto-assigned IP address from project's "default" VPC and "default" VPC
    /// Subnet.
    Default,

    /// No network interfaces at all will be created for the instance.
    None,
}

impl Default for InstanceNetworkInterfaceAttachment {
    fn default() -> Self {
        Self::Default
    }
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

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceDiskAttach {
    /// A disk name to attach
    pub name: Name,
}

/// Create-time parameters for an [`Instance`](omicron_common::api::external::Instance)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ncpus: InstanceCpuCount,
    pub memory: ByteCount,
    pub hostname: String, // TODO-cleanup different type?

    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces: InstanceNetworkInterfaceAttachment,

    /// The disks to be created or attached for this instance.
    #[serde(default)]
    pub disks: Vec<InstanceDiskAttachment>,
}

/// Migration parameters for an [`Instance`](omicron_common::api::external::Instance)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrate {
    pub dst_sled_uuid: Uuid,
}

// VPCS

/// Create-time parameters for a [`Vpc`](crate::external_api::views::Vpc)
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

/// Updateable properties of a [`Vpc`](crate::external_api::views::Vpc)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    pub dns_name: Option<Name>,
}

/// Create-time parameters for a [`VpcSubnet`](crate::external_api::views::VpcSubnet)
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

/// Updateable properties of a [`VpcSubnet`](crate::external_api::views::VpcSubnet)
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

// VPC ROUTERS

/// Create-time parameters for a [`VpcRouter`](crate::external_api::views::VpcRouter)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of a [`VpcRouter`](crate::external_api::views::VpcRouter)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// DISKS

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "u32")] // invoke the try_from validation routine below
pub struct BlockSize(pub u32);

impl TryFrom<u32> for BlockSize {
    type Error = anyhow::Error;
    fn try_from(x: u32) -> Result<BlockSize, Self::Error> {
        if ![512, 2048, 4096].contains(&x) {
            anyhow::bail!("invalid block size {}", x);
        }

        Ok(BlockSize(x))
    }
}

impl Into<ByteCount> for BlockSize {
    fn into(self) -> ByteCount {
        ByteCount::from(self.0)
    }
}

impl From<BlockSize> for u64 {
    fn from(bs: BlockSize) -> u64 {
        bs.0 as u64
    }
}

impl JsonSchema for BlockSize {
    fn schema_name() -> String {
        "BlockSize".to_string()
    }

    fn json_schema(
        _gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                id: None,
                title: Some("disk block size in bytes".to_string()),
                description: None,
                default: None,
                deprecated: false,
                read_only: false,
                write_only: false,
                examples: vec![],
            })),
            instance_type: Some(schemars::schema::SingleOrVec::Single(
                Box::new(schemars::schema::InstanceType::Integer),
            )),
            format: None,
            enum_values: Some(vec![
                serde_json::json!(512),
                serde_json::json!(2048),
                serde_json::json!(4096),
            ]),
            const_value: None,
            subschemas: None,
            number: None,
            string: None,
            array: None,
            object: None,
            reference: None,
            extensions: BTreeMap::new(),
        })
    }
}

/// Different sources for a disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum DiskSource {
    /// Create a blank disk
    Blank {
        /// size of blocks for this Disk. valid values are: 512, 2048, or 4096
        block_size: BlockSize,
    },
    /// Create a disk from a disk snapshot
    Snapshot { snapshot_id: Uuid },
    /// Create a disk from a project image
    Image { image_id: Uuid },
    /// Create a disk from a global image
    GlobalImage { image_id: Uuid },
}

/// Create-time parameters for a [`Disk`](omicron_common::api::external::Disk)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// initial source for this disk
    pub disk_source: DiskSource,
    /// total size of the Disk in bytes
    pub size: ByteCount,
}

const EXTENT_SIZE: u32 = 1_u32 << 20;

impl DiskCreate {
    pub fn extent_size(&self) -> i64 {
        EXTENT_SIZE as i64
    }

    pub fn extent_count(&self) -> i64 {
        let extent_size = EXTENT_SIZE as i64;
        let size = self.size.to_bytes() as i64;
        size / extent_size
            + ((size % extent_size) + extent_size - 1) / extent_size
    }
}

/// Parameters for the [`Disk`](omicron_common::api::external::Disk) to be
/// attached or detached to an instance
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskIdentifier {
    pub name: Name,
}

/// Parameters for the
/// [`NetworkInterface`](omicron_common::api::external::NetworkInterface) to be
/// attached or detached to an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NetworkInterfaceIdentifier {
    pub interface_name: Name,
}

// IMAGES

/// The source of the underlying image.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum ImageSource {
    Url(String),
    Snapshot(Uuid),
}

/// Create-time parameters for an
/// [`Image`](omicron_common::api::external::Image)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ImageCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// block size in bytes
    pub block_size: BlockSize,

    /// The source of the image's contents.
    pub source: ImageSource,
}

// SNAPSHOTS

/// Create-time parameters for a [`Snapshot`](omicron_common::api::external::Snapshot)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SnapshotCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The name of the disk to be snapshotted
    pub disk: Name,
}

// BUILT-IN USERS
//
// These cannot be created via the external API, but we use the same interfaces
// for creating them internally as we use for types that can be created in the
// external API.

/// Create-time parameters for a [`UserBuiltin`](crate::db::model::UserBuiltin)
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
            disk_source: DiskSource::Blank {
                block_size: BlockSize::try_from(4096).unwrap(),
            },
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
        let _params =
            new_disk_create_params(ByteCount::try_from(i64::MAX).unwrap());

        // Note that i64::MAX bytes is an invalid disk size as it's not
        // divisible by 4096.
        let max_disk_size = i64::MAX - (i64::MAX % 4096);
        let params =
            new_disk_create_params(ByteCount::try_from(max_disk_size).unwrap());
        let block_size: u64 = 4096;
        let blocks_per_extent: u64 = params.extent_size() as u64 / block_size;
        assert_eq!(params.extent_count() as u64, 8796093022208_u64);

        // Assert that the regions allocated will fit this disk
        assert!(
            params.size.to_bytes() as u64
                <= (params.extent_count() as u64)
                    * blocks_per_extent
                    * block_size
        );
    }
}
