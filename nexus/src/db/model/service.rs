// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::SqlU16;
use crate::db::schema::service;
use crate::db::ipv6;
use db_macros::Asset;
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
}

