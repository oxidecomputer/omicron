// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    Generation, PhysicalDiskKind, PhysicalDiskPolicy, PhysicalDiskState,
};
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
    pub disk_policy: PhysicalDiskPolicy,
    pub disk_state: PhysicalDiskState,
}

impl PhysicalDisk {
    /// Creates a new in-service, active disk
    pub fn new(
        id: Uuid,
        vendor: String,
        serial: String,
        model: String,
        variant: PhysicalDiskKind,
        sled_id: Uuid,
    ) -> Self {
        Self {
            identity: PhysicalDiskIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            vendor,
            serial,
            model,
            variant,
            sled_id,
            disk_policy: PhysicalDiskPolicy::InService,
            disk_state: PhysicalDiskState::Active,
        }
    }

    pub fn id(&self) -> Uuid {
        self.identity.id
    }

    pub fn time_deleted(&self) -> Option<DateTime<Utc>> {
        self.time_deleted
    }
}

impl From<PhysicalDisk> for views::PhysicalDisk {
    fn from(disk: PhysicalDisk) -> Self {
        Self {
            identity: disk.identity(),
            policy: disk.disk_policy.into(),
            state: disk.disk_state.into(),
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
