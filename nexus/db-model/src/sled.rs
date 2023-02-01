// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, SqlU16};
use crate::collection::DatastoreCollectionConfig;
use crate::ipv6;
use crate::schema::{physical_disk, service, sled, zpool};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_types::{external_api::views, identity::Asset};
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

    pub rack_id: Uuid,

    is_scrimlet: bool,
    serial_number: String,
    part_number: String,
    revision: i64,

    // ServiceAddress (Sled Agent).
    pub ip: ipv6::Ipv6Addr,
    pub port: SqlU16,

    /// The last IP address provided to an Oxide service on this sled
    pub last_used_address: ipv6::Ipv6Addr,
}

impl Sled {
    pub fn new(
        id: Uuid,
        addr: SocketAddrV6,
        is_scrimlet: bool,
        serial_number: String,
        part_number: String,
        revision: i64,
        rack_id: Uuid,
    ) -> Self {
        let last_used_address = {
            let mut segments = addr.ip().segments();
            segments[7] += omicron_common::address::RSS_RESERVED_ADDRESSES;
            ipv6::Ipv6Addr::from(Ipv6Addr::from(segments))
        };
        Self {
            identity: SledIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            rack_id,
            is_scrimlet,
            serial_number,
            part_number,
            revision,
            ip: ipv6::Ipv6Addr::from(addr.ip()),
            port: addr.port().into(),
            last_used_address,
        }
    }

    pub fn is_scrimlet(&self) -> bool {
        self.is_scrimlet
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

impl From<Sled> for views::Sled {
    fn from(sled: Sled) -> Self {
        Self {
            identity: sled.identity(),
            service_address: sled.address(),
            rack_id: sled.rack_id,
            baseboard: views::Baseboard {
                serial: sled.serial_number,
                part: sled.part_number,
                revision: sled.revision,
            },
        }
    }
}

impl DatastoreCollectionConfig<super::PhysicalDisk> for Sled {
    type CollectionId = Uuid;
    type GenerationNumberColumn = sled::dsl::rcgen;
    type CollectionTimeDeletedColumn = sled::dsl::time_deleted;
    type CollectionIdColumn = physical_disk::dsl::id;
}

// TODO: Can we remove this? We have one for physical disks now.
impl DatastoreCollectionConfig<super::Zpool> for Sled {
    type CollectionId = Uuid;
    type GenerationNumberColumn = sled::dsl::rcgen;
    type CollectionTimeDeletedColumn = sled::dsl::time_deleted;
    type CollectionIdColumn = zpool::dsl::sled_id;
}

impl DatastoreCollectionConfig<super::Service> for Sled {
    type CollectionId = Uuid;
    type GenerationNumberColumn = sled::dsl::rcgen;
    type CollectionTimeDeletedColumn = sled::dsl::time_deleted;
    type CollectionIdColumn = service::dsl::sled_id;
}
