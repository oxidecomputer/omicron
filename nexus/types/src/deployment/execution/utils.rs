// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::{IpAddr, SocketAddrV6};

use nexus_sled_agent_shared::inventory::SledRole;
use omicron_common::address::{Ipv6Subnet, SLED_PREFIX};
use omicron_uuid_kinds::SledUuid;

use crate::{
    deployment::{
        Blueprint, BlueprintZoneDisposition, BlueprintZoneType,
        blueprint_zone_type,
    },
    external_api::views::SledPolicy,
};

/// The minimal information needed to represent a sled in the context of
/// blueprint execution.
#[derive(Debug, Clone)]
pub struct Sled {
    id: SledUuid,
    policy: SledPolicy,
    sled_agent_address: SocketAddrV6,
    repo_depot_port: u16,
    role: SledRole,
}

impl Sled {
    pub fn new(
        id: SledUuid,
        policy: SledPolicy,
        sled_agent_address: SocketAddrV6,
        repo_depot_port: u16,
        role: SledRole,
    ) -> Sled {
        Sled { id, policy, sled_agent_address, repo_depot_port, role }
    }

    pub fn id(&self) -> SledUuid {
        self.id
    }

    pub fn policy(&self) -> SledPolicy {
        self.policy
    }

    pub fn sled_agent_address(&self) -> SocketAddrV6 {
        self.sled_agent_address
    }

    pub fn repo_depot_address(&self) -> SocketAddrV6 {
        SocketAddrV6::new(
            *self.sled_agent_address().ip(),
            self.repo_depot_port,
            0,
            0,
        )
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
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(_, z)| match z.zone_type {
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                external_ip,
                ..
            }) => Some(external_ip.ip),
            _ => None,
        })
        .collect()
}
