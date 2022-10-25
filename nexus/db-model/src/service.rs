// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ServiceKind;
use crate::ipv6;
use crate::schema::service;
use db_macros::Asset;
use std::net::Ipv6Addr;
use uuid::Uuid;

/// Representation of services which may run on Sleds.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = service)]
pub struct Service {
    #[diesel(embed)]
    identity: ServiceIdentity,

    pub sled_id: Uuid,
    pub ip: Option<ipv6::Ipv6Addr>,
    pub kind: ServiceKind,
}

impl Service {
    pub fn new(
        id: Uuid,
        sled_id: Uuid,
        addr: Option<Ipv6Addr>,
        kind: ServiceKind,
    ) -> Self {
        Self {
            identity: ServiceIdentity::new(id),
            sled_id,
            ip: addr.map(|x| x.into()),
            kind,
        }
    }
}
