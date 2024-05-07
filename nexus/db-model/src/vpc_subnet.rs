// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::Generation;
use super::{Ipv4Net, Ipv6Net, Name};
use crate::collection::DatastoreCollectionConfig;
use crate::schema::network_interface;
use crate::schema::vpc_subnet;
use crate::NetworkInterface;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;
use uuid::Uuid;

#[derive(
    Queryable,
    Insertable,
    Clone,
    Debug,
    Selectable,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = vpc_subnet)]
pub struct VpcSubnet {
    #[diesel(embed)]
    pub identity: VpcSubnetIdentity,

    pub vpc_id: Uuid,
    pub rcgen: Generation,
    pub ipv4_block: Ipv4Net,
    pub ipv6_block: Ipv6Net,
}

impl VpcSubnet {
    /// Create a new VPC Subnet.
    ///
    /// NOTE: This assumes that the IP address ranges provided in `params` are
    /// valid for the VPC, and does not do any further validation.
    pub fn new(
        subnet_id: Uuid,
        vpc_id: Uuid,
        identity: external::IdentityMetadataCreateParams,
        ipv4_block: oxnet::Ipv4Net,
        ipv6_block: oxnet::Ipv6Net,
    ) -> Self {
        let identity = VpcSubnetIdentity::new(subnet_id, identity);
        Self {
            identity,
            vpc_id,
            rcgen: Generation::new(),
            ipv4_block: Ipv4Net(ipv4_block),
            ipv6_block: Ipv6Net(ipv6_block),
        }
    }

    /// Verify that the provided IP address is contained in the VPC Subnet.
    ///
    /// This checks:
    ///
    /// - The subnet has an allocated block of the same version as the address
    /// - The allocated block contains the address.
    /// - The address is not reserved.
    pub fn check_requestable_addr(
        &self,
        addr: IpAddr,
    ) -> Result<(), external::Error> {
        match addr {
            IpAddr::V4(addr) => self.ipv4_block.check_requestable_addr(addr),
            IpAddr::V6(addr) => self.ipv6_block.check_requestable_addr(addr),
        }
        .map_err(|e| external::Error::invalid_request(e.to_string()))
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum RequestAddressError {
    #[error("{} is outside subnet {}", .0, .1)]
    OutsideSubnet(IpAddr, ipnetwork::IpNetwork),
    #[error(
        "The first {} addresses of a subnet are reserved",
        NUM_INITIAL_RESERVED_IP_ADDRESSES
    )]
    Reserved,
    #[error("Cannot request a broadcast address")]
    Broadcast,
}

impl From<VpcSubnet> for views::VpcSubnet {
    fn from(subnet: VpcSubnet) -> Self {
        Self {
            identity: subnet.identity(),
            vpc_id: subnet.vpc_id,
            ipv4_block: subnet.ipv4_block.0,
            ipv6_block: subnet.ipv6_block.0,
        }
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = vpc_subnet)]
pub struct VpcSubnetUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::VpcSubnetUpdate> for VpcSubnetUpdate {
    fn from(params: params::VpcSubnetUpdate) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}

impl DatastoreCollectionConfig<NetworkInterface> for VpcSubnet {
    type CollectionId = Uuid;
    type GenerationNumberColumn = vpc_subnet::dsl::rcgen;
    type CollectionTimeDeletedColumn = vpc_subnet::dsl::time_deleted;
    type CollectionIdColumn = network_interface::dsl::subnet_id;
}
