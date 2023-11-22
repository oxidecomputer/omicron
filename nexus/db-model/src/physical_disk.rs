// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, PhysicalDiskKind};
use crate::collection::DatastoreCollectionConfig;
use crate::schema::{physical_disk, zpool};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_types::{external_api::views, identity::Asset};
use uuid::Uuid;

/// Physical disk attached to sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = physical_disk)]
pub struct PhysicalDisk {
    #[diesel(embed)]
    identity: PhysicalDiskIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub variant: PhysicalDiskKind,
    pub sled_id: Uuid,
}

impl PhysicalDisk {
    pub fn new(
        vendor: String,
        serial: String,
        model: String,
        variant: PhysicalDiskKind,
        sled_id: Uuid,
    ) -> Self {
        Self {
            identity: PhysicalDiskIdentity::new(Uuid::new_v4()),
            time_deleted: None,
            rcgen: Generation::new(),
            vendor,
            serial,
            model,
            variant,
            sled_id,
        }
    }

    pub fn uuid(&self) -> Uuid {
        self.identity.id
    }

    // This is slightly gross, but:
    // the `authz_resource` macro really expects that the "primary_key"
    // for an object can be acquired by "id()".
    //
    // The PhysicalDisk object does actually have a separate convenience
    // UUID, but may be looked by up vendor/serial/model too.
    pub fn id(&self) -> (String, String, String) {
        (self.vendor.clone(), self.serial.clone(), self.model.clone())
    }

    pub fn time_deleted(&self) -> Option<DateTime<Utc>> {
        self.time_deleted
    }
}

impl From<PhysicalDisk> for views::PhysicalDisk {
    fn from(disk: PhysicalDisk) -> Self {
        Self {
            identity: disk.identity(),
            sled_id: Some(disk.sled_id),
            vendor: disk.vendor,
            serial: disk.serial,
            model: disk.model,
            form_factor: disk.variant.into(),
        }
    }
}

impl DatastoreCollectionConfig<super::Zpool> for PhysicalDisk {
    type CollectionId = Uuid;
    type GenerationNumberColumn = physical_disk::dsl::rcgen;
    type CollectionTimeDeletedColumn = physical_disk::dsl::time_deleted;
    type CollectionIdColumn = zpool::dsl::sled_id;
}
