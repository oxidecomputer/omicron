// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Dataset, Generation};
use crate::collection::DatastoreCollectionConfig;
use crate::schema::{dataset, zpool};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use uuid::Uuid;

/// Database representation of a Pool.
///
/// A zpool represents a ZFS storage pool, allocated on a single
/// physical sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = zpool)]
pub struct Zpool {
    #[diesel(embed)]
    identity: ZpoolIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    // Sled to which this Zpool belongs.
    pub sled_id: Uuid,

    // The physical disk to which this Zpool is attached.
    pub physical_disk_id: Uuid,
}

impl Zpool {
    pub fn new(id: Uuid, sled_id: Uuid, physical_disk_id: Uuid) -> Self {
        Self {
            identity: ZpoolIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            sled_id,
            physical_disk_id,
        }
    }
}

impl DatastoreCollectionConfig<Dataset> for Zpool {
    type CollectionId = Uuid;
    type GenerationNumberColumn = zpool::dsl::rcgen;
    type CollectionTimeDeletedColumn = zpool::dsl::time_deleted;
    type CollectionIdColumn = dataset::dsl::pool_id;
}
