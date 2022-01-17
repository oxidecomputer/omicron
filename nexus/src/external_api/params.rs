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
use std::convert::TryFrom;
use uuid::Uuid;

/*
 * ORGANIZATIONS
 */

/**
 * Create-time parameters for an [`Organization`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/**
 * Updateable properties of an [`Organization`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/*
 * PROJECTS
 */

/**
 * Create-time parameters for a [`Project`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/**
 * Updateable properties of a [`Project`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/*
 * NETWORK INTERFACES
 */

/**
 * Create-time parameters for a [`NetworkInterface`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NetworkInterfaceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/*
 * INSTANCES
 */

/**
 * Create-time parameters for an [`Instance`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ncpus: InstanceCpuCount,
    pub memory: ByteCount,
    pub hostname: String, /* TODO-cleanup different type? */
}

/*
 * VPCS
 */

/**
 * Create-time parameters for a [`Vpc`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub dns_name: Name,
}

/**
 * Updateable properties of a [`Vpc`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    pub dns_name: Option<Name>,
}

/**
 * Create-time parameters for a [`VpcSubnet`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcSubnetCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ipv4_block: Option<Ipv4Net>,
    pub ipv6_block: Option<Ipv6Net>,
}

/**
 * Updateable properties of a [`VpcSubnet`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcSubnetUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    pub ipv4_block: Option<Ipv4Net>,
    pub ipv6_block: Option<Ipv6Net>,
}

/*
 * VPC ROUTERS
 */

/// Create-time parameters for a [`VpcRouter`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcRouterCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of a [`VpcRouter`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcRouterUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/*
 * DISKS
 */

/**
 * Create-time parameters for a [`Disk`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiskCreate {
    /** common identifying metadata */
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /** id for snapshot from which the Disk should be created, if any */
    pub snapshot_id: Option<Uuid>, /* TODO should be a name? */
    /** size of the Disk */
    pub size: ByteCount,
}

const BLOCK_SIZE: u32 = 512_u32;
const EXTENT_SIZE: u32 = 1_u32 << 20;

impl DiskCreate {
    pub fn block_size(&self) -> ByteCount {
        ByteCount::from(BLOCK_SIZE)
    }

    pub fn extent_size(&self) -> ByteCount {
        ByteCount::from(EXTENT_SIZE)
    }

    pub fn extent_count(&self) -> i64 {
        let extent_size = self.extent_size().to_bytes();
        i64::try_from((self.size.to_bytes() + extent_size - 1) / extent_size).unwrap()
    }
}

/**
 * Parameters for the [`Disk`] to be attached or detached to an instance
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
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
 * Create-time parameters for a [`UserBuiltin`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserBuiltinCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

#[cfg(test)]
mod test {
    use super::*;

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
        let params = new_disk_create_params(ByteCount::try_from(EXTENT_SIZE - 1).unwrap());
        assert_eq!(1, params.extent_count());
        let params = new_disk_create_params(ByteCount::try_from(EXTENT_SIZE).unwrap());
        assert_eq!(1, params.extent_count());

        let params = new_disk_create_params(ByteCount::try_from(EXTENT_SIZE + 1).unwrap());
        assert_eq!(2, params.extent_count());

        // Mostly just checking we don't blow up on an unwrap here.
        let params = new_disk_create_params(ByteCount::try_from(i64::MAX).unwrap());
        assert!(params.size.to_bytes() < (params.extent_count() as u64) * params.extent_size().to_bytes());
    }
}

