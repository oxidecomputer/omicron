// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, Region, SqlU16};
use crate::collection::DatastoreCollectionConfig;
use crate::ipv6;
use crate::schema::{crucible_dataset, region};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use serde::{Deserialize, Serialize};
use std::net::{Ipv6Addr, SocketAddrV6};
use uuid::Uuid;

/// Database representation of a Crucible dataset's live information.
///
/// This includes the socket address of the Crucible downstairs that owns this
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

    pub pool_id: Uuid,

    ip: ipv6::Ipv6Addr,
    port: SqlU16,

    pub size_used: i64,
}

impl CrucibleDataset {
    pub fn new(
        id: omicron_uuid_kinds::DatasetUuid,
        pool_id: Uuid,
        addr: SocketAddrV6,
    ) -> Self {
        Self {
            identity: CrucibleDatasetIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            pool_id,
            ip: addr.ip().into(),
            port: addr.port().into(),
            size_used: 0,
        }
    }

    pub fn address(&self) -> SocketAddrV6 {
        self.address_with_port(self.port.into())
    }

    pub fn address_with_port(&self, port: u16) -> SocketAddrV6 {
        SocketAddrV6::new(Ipv6Addr::from(self.ip), port, 0, 0)
    }
}

/*
 * TODO-john
impl From<BlueprintDatasetConfig> for CrucibleDataset {
    fn from(bp: BlueprintDatasetConfig) -> Self {
        let kind = DatasetKind::from(&bp.kind);
        let zone_name = bp.kind.zone_name().map(|s| s.to_string());
        // Only Crucible uses this "size_used" field.
        let size_used = match bp.kind {
            ApiDatasetKind::Crucible => Some(0),
            ApiDatasetKind::Cockroach
            | ApiDatasetKind::Clickhouse
            | ApiDatasetKind::ClickhouseKeeper
            | ApiDatasetKind::ClickhouseServer
            | ApiDatasetKind::ExternalDns
            | ApiDatasetKind::InternalDns
            | ApiDatasetKind::TransientZone { .. }
            | ApiDatasetKind::TransientZoneRoot
            | ApiDatasetKind::Debug
            | ApiDatasetKind::Update => None,
        };
        let addr = bp.address;
        Self {
            identity: DatasetIdentity::new(bp.id),
            time_deleted: None,
            rcgen: Generation::new(),
            pool_id: bp.pool.id().into_untyped_uuid(),
            kind,
            ip: addr.map(|addr| addr.ip().into()),
            port: addr.map(|addr| addr.port().into()),
            size_used,
            zone_name,
            quota: bp.quota.map(ByteCount::from),
            reservation: bp.reservation.map(ByteCount::from),
            compression: Some(bp.compression.to_string()),
        }
    }
}
*/

// Datasets contain regions
impl DatastoreCollectionConfig<Region> for CrucibleDataset {
    type CollectionId = Uuid;
    type GenerationNumberColumn = crucible_dataset::dsl::rcgen;
    type CollectionTimeDeletedColumn = crucible_dataset::dsl::time_deleted;
    type CollectionIdColumn = region::dsl::dataset_id;
}
