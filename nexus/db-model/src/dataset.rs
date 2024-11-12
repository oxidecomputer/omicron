// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ByteCount, DatasetKind, Generation, Region, SqlU16};
use crate::collection::DatastoreCollectionConfig;
use crate::ipv6;
use crate::schema::{dataset, region};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_types::deployment::BlueprintDatasetConfig;
use omicron_common::api::external::Error;
use omicron_common::api::internal::shared::DatasetKind as ApiDatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::ZpoolUuid;
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

    quota: Option<ByteCount>,
    reservation: Option<ByteCount>,
    // This is the stringified form of
    // "omicron_common::disk::CompressionAlgorithm".
    //
    // It can't serialize to the database without forcing omicron_common to
    // depend on Diesel -- we could create a newtype, but "to_string" and
    // "parse" cover this usage similarly.
    compression: Option<String>,
}

impl Dataset {
    pub fn new(
        id: Uuid,
        pool_id: Uuid,
        addr: Option<SocketAddrV6>,
        api_kind: ApiDatasetKind,
    ) -> Self {
        let kind = DatasetKind::from(&api_kind);
        let (size_used, zone_name) = match api_kind {
            ApiDatasetKind::Crucible => (Some(0), None),
            ApiDatasetKind::TransientZone { name } => (None, Some(name)),
            _ => (None, None),
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
            quota: None,
            reservation: None,
            compression: None,
        }
    }

    pub fn address(&self) -> Option<SocketAddrV6> {
        self.address_with_port(self.port?.into())
    }

    pub fn address_with_port(&self, port: u16) -> Option<SocketAddrV6> {
        Some(SocketAddrV6::new(Ipv6Addr::from(self.ip?), port, 0, 0))
    }
}

impl From<BlueprintDatasetConfig> for Dataset {
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
            identity: DatasetIdentity::new(bp.id.into_untyped_uuid()),
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

impl TryFrom<Dataset> for omicron_common::disk::DatasetConfig {
    type Error = Error;

    fn try_from(dataset: Dataset) -> Result<Self, Self::Error> {
        let compression = if let Some(c) = dataset.compression {
            c.parse().map_err(|e: anyhow::Error| {
                Error::internal_error(&e.to_string())
            })?
        } else {
            omicron_common::disk::CompressionAlgorithm::Off
        };

        Ok(Self {
            id: DatasetUuid::from_untyped_uuid(dataset.identity.id),
            name: omicron_common::disk::DatasetName::new(
                omicron_common::zpool_name::ZpoolName::new_external(
                    ZpoolUuid::from_untyped_uuid(dataset.pool_id),
                ),
                dataset.kind.try_into_api(dataset.zone_name)?,
            ),
            inner: omicron_common::disk::SharedDatasetConfig {
                quota: dataset.quota.map(|q| q.into()),
                reservation: dataset.reservation.map(|r| r.into()),
                compression,
            },
        })
    }
}

// Datasets contain regions
impl DatastoreCollectionConfig<Region> for Dataset {
    type CollectionId = Uuid;
    type GenerationNumberColumn = dataset::dsl::rcgen;
    type CollectionTimeDeletedColumn = dataset::dsl::time_deleted;
    type CollectionIdColumn = region::dsl::dataset_id;
}
