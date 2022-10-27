// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ServiceKind;
use crate::ipv6;
use crate::schema::service;
use crate::SqlU16;
use db_macros::Asset;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use uuid::Uuid;

/// Representation of services which may run on Sleds.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = service)]
pub struct Service {
    #[diesel(embed)]
    identity: ServiceIdentity,

    pub sled_id: Uuid,
    pub ip: Option<ipv6::Ipv6Addr>,
    pub port: Option<SqlU16>,
    pub kind: ServiceKind,
}

impl Service {
    pub fn new(
        id: Uuid,
        sled_id: Uuid,
        addr: Option<Ipv6Addr>,
        port: Option<u16>,
        kind: ServiceKind,
    ) -> Self {
        Self {
            identity: ServiceIdentity::new(id),
            sled_id,
            ip: addr.map(|x| x.into()),
            port: port.map(|x| x.into()),
            kind,
        }
    }

    pub fn address(&self) -> Option<SocketAddrV6> {
        if let Some(ip) = self.ip {
            if let Some(port) = self.port {
                return Some(std::net::SocketAddrV6::new(*ip, *port, 0, 0));
            }
        }

        None
    }
}
