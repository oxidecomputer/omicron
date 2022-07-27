// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ServiceKind;
use crate::ipv6;
use crate::schema::service;
use db_macros::Asset;
use internal_dns_client::names::{ServiceName, AAAA, SRV};
use nexus_types::identity::Asset;
use omicron_common::address::{
    DENDRITE_PORT, DNS_SERVER_PORT, NEXUS_INTERNAL_PORT, OXIMETER_PORT,
};
use std::net::{Ipv6Addr, SocketAddrV6};
use uuid::Uuid;

/// Representation of services which may run on Sleds.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset, PartialEq)]
#[diesel(table_name = service)]
pub struct Service {
    #[diesel(embed)]
    identity: ServiceIdentity,

    pub sled_id: Uuid,
    pub ip: ipv6::Ipv6Addr,
    pub kind: ServiceKind,
}

impl Service {
    pub fn new(
        id: Uuid,
        sled_id: Uuid,
        addr: Ipv6Addr,
        kind: ServiceKind,
    ) -> Self {
        Self {
            identity: ServiceIdentity::new(id),
            sled_id,
            ip: addr.into(),
            kind,
        }
    }

    pub fn aaaa(&self) -> AAAA {
        AAAA::Zone(self.id())
    }

    pub fn srv(&self) -> SRV {
        match self.kind {
            ServiceKind::InternalDNS => SRV::Service(ServiceName::InternalDNS),
            ServiceKind::Nexus => SRV::Service(ServiceName::Nexus),
            ServiceKind::Oximeter => SRV::Service(ServiceName::Oximeter),
            ServiceKind::Dendrite => SRV::Service(ServiceName::Dendrite),
        }
    }

    pub fn address(&self) -> SocketAddrV6 {
        let port = match self.kind {
            ServiceKind::InternalDNS => DNS_SERVER_PORT,
            ServiceKind::Nexus => NEXUS_INTERNAL_PORT,
            ServiceKind::Oximeter => OXIMETER_PORT,
            ServiceKind::Dendrite => DENDRITE_PORT,
        };
        SocketAddrV6::new(Ipv6Addr::from(self.ip), port, 0, 0)
    }
}
