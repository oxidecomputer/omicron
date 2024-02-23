// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, PhysicalDiskKind, PhysicalDiskState};
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
    pub state: PhysicalDiskState,
}

impl PhysicalDisk {
    pub fn new(
        vendor: String,
        serial: String,
        model: String,
        variant: PhysicalDiskKind,
        sled_id: Uuid,
    ) -> Self {
        // NOTE: We may want to be more restrictive when parsing the vendor,
        // serial, and model values, so that we can supply a separator
        // distinguishing them.
        //
        // Theoretically, we could have the following problem:
        //
        // - A Disk vendor "Foo" makes a disk with serial "Bar", and model "Rev1".
        //   - This becomes: "FooBarRev1".
        // - A Disk vendor "FooBar" makes a disk with serial "Rev", and model "1".
        //   - This becomes: "FooBarRev1", and conflicts.
        let interpolated_name = format!("{vendor}{serial}{model}");
        let disk_id = Uuid::new_v5(
            &crate::HARDWARE_UUID_NAMESPACE,
            interpolated_name.as_bytes(),
        );
        println!("Physical Disk ID: {disk_id}, from {interpolated_name}");
        Self {
            identity: PhysicalDiskIdentity::new(disk_id),
            time_deleted: None,
            rcgen: Generation::new(),
            vendor,
            serial,
            model,
            variant,
            sled_id,
            state: PhysicalDiskState::Active,
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
            state: disk.state.into(),
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
