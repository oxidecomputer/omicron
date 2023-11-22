// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ServiceKind;
use crate::ipv6;
use crate::schema::service;
use crate::SqlU16;
use db_macros::Asset;
use std::net::SocketAddrV6;
use uuid::Uuid;

/// Representation of services which may run on Sleds.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = service)]
pub struct Service {
    #[diesel(embed)]
    identity: ServiceIdentity,

    pub sled_id: Uuid,
    pub zone_id: Option<Uuid>,
    pub ip: ipv6::Ipv6Addr,
    pub port: SqlU16,
    pub kind: ServiceKind,
}

impl Service {
    pub fn new(
        id: Uuid,
        sled_id: Uuid,
        zone_id: Option<Uuid>,
        addr: SocketAddrV6,
        kind: ServiceKind,
    ) -> Self {
        Self {
            identity: ServiceIdentity::new(id),
            sled_id,
            zone_id,
            ip: addr.ip().into(),
            port: addr.port().into(),
            kind,
        }
    }
}
