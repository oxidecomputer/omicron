// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::PhysicalDiskKind;
use crate::schema::physical_disk;
use uuid::Uuid;

/// Physical disk attached to sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable)]
#[diesel(table_name = physical_disk)]
pub struct PhysicalDisk {
    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub variant: PhysicalDiskKind,
    pub sled_id: Uuid,
    pub total_size: i64,
}

impl PhysicalDisk {
    pub fn new(
        vendor: String,
        serial: String,
        model: String,
        variant: PhysicalDiskKind,
        sled_id: Uuid,
        total_size: i64,
    ) -> Self {
        Self { vendor, serial, model, variant, sled_id, total_size }
    }
}
