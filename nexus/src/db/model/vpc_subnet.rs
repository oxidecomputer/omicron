// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Ipv4Net, Ipv6Net, Name};
use crate::db::{identity::Resource, schema::vpc_subnet};
use crate::external_api::params;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_types::external_api::views;
use omicron_common::api::external;
use std::net::IpAddr;
use uuid::Uuid;

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = vpc_subnet)]
pub struct VpcSubnet {
    #[diesel(embed)]
    pub identity: VpcSubnetIdentity,

    pub vpc_id: Uuid,
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
        ipv4_block: external::Ipv4Net,
        ipv6_block: external::Ipv6Net,
    ) -> Self {
        let identity = VpcSubnetIdentity::new(subnet_id, identity);
        Self {
            identity,
            vpc_id,
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
        let subnet = match addr {
            IpAddr::V4(addr) => {
                if self.ipv4_block.check_requestable_addr(addr) {
                    return Ok(());
                }
                ipnetwork::IpNetwork::V4(self.ipv4_block.0 .0)
            }
            IpAddr::V6(addr) => {
                if self.ipv6_block.check_requestable_addr(addr) {
                    return Ok(());
                }
                ipnetwork::IpNetwork::V6(self.ipv6_block.0 .0)
            }
        };
        Err(external::Error::invalid_request(&format!(
            "Address '{}' not in subnet '{}' or is reserved for rack services",
            addr, subnet,
        )))
    }
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
