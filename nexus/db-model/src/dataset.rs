// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{DatasetKind, Generation, Region, SqlU16};
use crate::collection::DatastoreCollectionConfig;
use crate::ipv6;
use crate::schema::{dataset, region};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use serde::{Deserialize, Serialize};
use std::net::{Ipv6Addr, SocketAddrV6};
use uuid::Uuid;

/// Database representation of a Dataset.
///
/// A dataset represents a portion of a Zpool, which is then made
/// available to a service on the Sled.
#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Asset,
    Deserialize,
    Serialize,
    PartialEq,
)]
#[diesel(table_name = dataset)]
pub struct Dataset {
    #[diesel(embed)]
    identity: DatasetIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    pub pool_id: Uuid,

    ip: Option<ipv6::Ipv6Addr>,
    port: Option<SqlU16>,

    pub kind: DatasetKind,
    pub size_used: Option<i64>,
    zone_name: Option<String>,
}

impl Dataset {
    pub fn new(
        id: Uuid,
        pool_id: Uuid,
        addr: Option<SocketAddrV6>,
        kind: DatasetKind,
        zone_name: Option<String>,
    ) -> Self {
        let size_used = match kind {
            DatasetKind::Crucible => Some(0),
            _ => None,
        };
        Self {
            identity: DatasetIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            pool_id,
            ip: addr.map(|addr| addr.ip().into()),
            port: addr.map(|addr| addr.port().into()),
            kind,
            size_used,
            zone_name,
        }
    }

    pub fn address(&self) -> Option<SocketAddrV6> {
        self.address_with_port(self.port?.into())
    }

    pub fn address_with_port(&self, port: u16) -> Option<SocketAddrV6> {
        Some(SocketAddrV6::new(Ipv6Addr::from(self.ip?), port, 0, 0))
    }
}

// Datasets contain regions
impl DatastoreCollectionConfig<Region> for Dataset {
    type CollectionId = Uuid;
    type GenerationNumberColumn = dataset::dsl::rcgen;
    type CollectionTimeDeletedColumn = dataset::dsl::time_deleted;
    type CollectionIdColumn = region::dsl::dataset_id;
}
