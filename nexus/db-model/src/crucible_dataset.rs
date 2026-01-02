// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, Region, SqlU16};
use crate::collection::DatastoreCollectionConfig;
use crate::ipv6;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_db_schema::schema::{crucible_dataset, region};
use omicron_uuid_kinds::*;
use serde::{Deserialize, Serialize};
use std::net::{Ipv6Addr, SocketAddrV6};
use uuid::Uuid;

/// Database representation of a Crucible dataset's live information.
///
/// This includes the socket address of the Crucible agent that owns this
/// dataset and the amount of space used.
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
#[diesel(table_name = crucible_dataset)]
#[asset(uuid_kind = DatasetKind)]
pub struct CrucibleDataset {
    #[diesel(embed)]
    identity: CrucibleDatasetIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    pub pool_id: DbTypedUuid<ZpoolKind>,

    ip: ipv6::Ipv6Addr,
    port: SqlU16,

    pub size_used: i64,

    /// Do not consider this dataset as a candidate during region allocation
    #[serde(default)]
    no_provision: bool,
}

impl CrucibleDataset {
    pub fn new(
        id: omicron_uuid_kinds::DatasetUuid,
        pool_id: ZpoolUuid,
        addr: SocketAddrV6,
    ) -> Self {
        Self {
            identity: CrucibleDatasetIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            pool_id: pool_id.into(),
            ip: addr.ip().into(),
            port: addr.port().into(),
            size_used: 0,
            no_provision: false,
        }
    }

    pub fn time_deleted(&self) -> Option<DateTime<Utc>> {
        self.time_deleted
    }

    pub fn address(&self) -> SocketAddrV6 {
        self.address_with_port(self.port.into())
    }

    pub fn address_with_port(&self, port: u16) -> SocketAddrV6 {
        SocketAddrV6::new(Ipv6Addr::from(self.ip), port, 0, 0)
    }

    pub fn no_provision(&self) -> bool {
        self.no_provision
    }

    pub fn pool_id(&self) -> ZpoolUuid {
        self.pool_id.into()
    }
}

// Datasets contain regions
impl DatastoreCollectionConfig<Region> for CrucibleDataset {
    type CollectionId = Uuid;
    type GenerationNumberColumn = crucible_dataset::dsl::rcgen;
    type CollectionTimeDeletedColumn = crucible_dataset::dsl::time_deleted;
    type CollectionIdColumn = region::dsl::dataset_id;
}
