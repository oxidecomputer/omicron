// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{DatasetKind, Generation, Region, SqlU16};
use crate::collection::DatastoreCollection;
use crate::schema::{dataset, region};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
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

    ip: ipnetwork::IpNetwork,
    port: SqlU16,

    kind: DatasetKind,
    pub size_used: Option<i64>,
}

impl Dataset {
    pub fn new(
        id: Uuid,
        pool_id: Uuid,
        addr: SocketAddr,
        kind: DatasetKind,
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
            ip: addr.ip().into(),
            port: addr.port().into(),
            kind,
            size_used,
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.address_with_port(self.port.into())
    }

    pub fn address_with_port(&self, port: u16) -> SocketAddr {
        SocketAddr::new(self.ip.ip(), port)
    }
}

// Datasets contain regions
impl DatastoreCollection<Region> for Dataset {
    type CollectionId = Uuid;
    type GenerationNumberColumn = dataset::dsl::rcgen;
    type CollectionTimeDeletedColumn = dataset::dsl::time_deleted;
    type CollectionIdColumn = region::dsl::dataset_id;
}
