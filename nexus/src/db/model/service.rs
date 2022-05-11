// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ServiceKind, SqlU16};
use crate::db::ipv6;
use crate::db::schema::service;
use db_macros::Asset;
use std::net::SocketAddrV6;
use uuid::Uuid;

#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = service)]
pub struct Service {
    #[diesel(embed)]
    identity: ServiceIdentity,

    // Sled to which this Zpool belongs.
    pub sled_id: Uuid,

    // ServiceAddress (Sled Agent).
    pub ip: ipv6::Ipv6Addr,
    pub port: SqlU16,

    kind: ServiceKind,
}

impl Service {
    pub fn new(
        id: Uuid,
        sled_id: Uuid,
        addr: SocketAddrV6,
        kind: ServiceKind,
    ) -> Self {
        Self {
            identity: ServiceIdentity::new(id),
            sled_id,
            ip: addr.ip().into(),
            port: addr.port().into(),
            kind,
        }
    }
}
