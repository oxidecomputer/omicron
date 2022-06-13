// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, SqlU16};
use crate::db::collection_insert::DatastoreCollection;
use crate::db::ipv6;
use crate::db::schema::{service, sled, zpool};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use uuid::Uuid;

/// Database representation of a Sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = sled)]
pub struct Sled {
    #[diesel(embed)]
    identity: SledIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    // ServiceAddress (Sled Agent).
    pub ip: ipv6::Ipv6Addr,
    pub port: SqlU16,

    /// The last IP address provided to an Oxide service on this sled
    pub last_used_address: ipv6::Ipv6Addr,
}

impl Sled {
    pub fn new(id: Uuid, addr: SocketAddrV6) -> Self {
        let last_used_address = {
            let mut segments = addr.ip().segments();
            segments[7] += omicron_common::address::RSS_RESERVED_ADDRESSES;
            ipv6::Ipv6Addr::from(Ipv6Addr::from(segments))
        };
        Self {
            identity: SledIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            ip: ipv6::Ipv6Addr::from(addr.ip()),
            port: addr.port().into(),
            last_used_address,
        }
    }

    pub fn ip(&self) -> Ipv6Addr {
        self.ip.into()
    }

    pub fn address(&self) -> SocketAddrV6 {
        self.address_with_port(self.port.into())
    }

    pub fn address_with_port(&self, port: u16) -> SocketAddrV6 {
        SocketAddrV6::new(self.ip(), port, 0, 0)
    }
}

impl DatastoreCollection<super::Zpool> for Sled {
    type CollectionId = Uuid;
    type GenerationNumberColumn = sled::dsl::rcgen;
    type CollectionTimeDeletedColumn = sled::dsl::time_deleted;
    type CollectionIdColumn = zpool::dsl::sled_id;
}

impl DatastoreCollection<super::ServiceInstance> for Sled {
    type CollectionId = Uuid;
    type GenerationNumberColumn = sled::dsl::rcgen;
    type CollectionTimeDeletedColumn = sled::dsl::time_deleted;
    type CollectionIdColumn = service::dsl::sled_id;
}
