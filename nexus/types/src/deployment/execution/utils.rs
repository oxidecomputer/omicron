// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::{IpAddr, SocketAddrV6};

use nexus_sled_agent_shared::inventory::SledRole;
use omicron_common::address::{Ipv6Subnet, SLED_PREFIX};
use omicron_uuid_kinds::SledUuid;

use crate::deployment::{
    blueprint_zone_type, Blueprint, BlueprintZoneFilter, BlueprintZoneType,
};

/// The minimal information needed to represent a sled in the context of
/// blueprint execution.
#[derive(Debug, Clone)]
pub struct Sled {
    id: SledUuid,
    sled_agent_address: SocketAddrV6,
    role: SledRole,
}

impl Sled {
    pub fn new(
        id: SledUuid,
        sled_agent_address: SocketAddrV6,
        role: SledRole,
    ) -> Sled {
        Sled { id, sled_agent_address, role }
    }

    pub fn id(&self) -> SledUuid {
        self.id
    }

    pub fn sled_agent_address(&self) -> SocketAddrV6 {
        self.sled_agent_address
    }

    pub fn subnet(&self) -> Ipv6Subnet<SLED_PREFIX> {
        Ipv6Subnet::<SLED_PREFIX>::new(*self.sled_agent_address.ip())
    }

    pub fn role(&self) -> SledRole {
        self.role
    }

    pub fn is_scrimlet(&self) -> bool {
        self.role == SledRole::Scrimlet
    }
}

/// Return the Nexus external addresses according to the given blueprint
pub fn blueprint_nexus_external_ips(blueprint: &Blueprint) -> Vec<IpAddr> {
    blueprint
        .all_omicron_zones(BlueprintZoneFilter::ShouldBeExternallyReachable)
        .filter_map(|(_, z)| match z.zone_type {
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                external_ip,
                ..
            }) => Some(external_ip.ip),
            _ => None,
        })
        .collect()
}
