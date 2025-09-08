// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::CrucibleDataset;
use super::Generation;
use super::LocalStorageDataset;
use crate::ByteCount;
use crate::collection::DatastoreCollectionConfig;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_db_schema::schema::crucible_dataset;
use nexus_db_schema::schema::local_storage_dataset;
use nexus_db_schema::schema::zpool;
use omicron_uuid_kinds::PhysicalDiskKind;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolKind;
use omicron_uuid_kinds::ZpoolUuid;

/// Database representation of a Pool.
///
/// A zpool represents a ZFS storage pool, allocated on a single
/// physical sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = zpool)]
#[asset(uuid_kind = ZpoolKind)]
pub struct Zpool {
    #[diesel(embed)]
    identity: ZpoolIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    // Sled to which this Zpool belongs.
    pub sled_id: DbTypedUuid<SledKind>,

    // The physical disk to which this Zpool is attached.
    pub physical_disk_id: DbTypedUuid<PhysicalDiskKind>,

    /// Multiple datasets are created per pool, and are used for all persistent
    /// data, both customer data (in the form of Crucible regions and local
    /// storage) and non-customer data (zone root datasets, delegated zone
    /// datasets, debug logs, core files, and more). To prevent Crucible regions
    /// from taking all the dataset space, reserve space that customer data
    /// related allocation is not allowed to use.
    ///
    /// This value is consulted during customer data related allocation queries,
    /// and can change at runtime. A pool could become "overprovisioned" if this
    /// value increases over the total storage minus how much storage customer
    /// data currently occupy, though this won't immediately cause any problems
    /// and can be identified and fixed via omdb commands.
    control_plane_storage_buffer: ByteCount,
}

impl Zpool {
    pub fn new(
        id: ZpoolUuid,
        sled_id: SledUuid,
        physical_disk_id: PhysicalDiskUuid,
        control_plane_storage_buffer: ByteCount,
    ) -> Self {
        Self {
            identity: ZpoolIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            sled_id: sled_id.into(),
            physical_disk_id: physical_disk_id.into(),
            control_plane_storage_buffer,
        }
    }

    pub fn time_deleted(&self) -> Option<DateTime<Utc>> {
        self.time_deleted
    }

    pub fn control_plane_storage_buffer(&self) -> ByteCount {
        self.control_plane_storage_buffer
    }

    pub fn sled_id(&self) -> SledUuid {
        self.sled_id.into()
    }

    pub fn physical_disk_id(&self) -> PhysicalDiskUuid {
        self.physical_disk_id.into()
    }
}

impl DatastoreCollectionConfig<CrucibleDataset> for Zpool {
    type CollectionId = DbTypedUuid<ZpoolKind>;
    type GenerationNumberColumn = zpool::dsl::rcgen;
    type CollectionTimeDeletedColumn = zpool::dsl::time_deleted;
    type CollectionIdColumn = crucible_dataset::dsl::pool_id;
}

impl DatastoreCollectionConfig<LocalStorageDataset> for Zpool {
    type CollectionId = DbTypedUuid<ZpoolKind>;
    type GenerationNumberColumn = zpool::dsl::rcgen;
    type CollectionTimeDeletedColumn = zpool::dsl::time_deleted;
    type CollectionIdColumn = local_storage_dataset::dsl::pool_id;
}
