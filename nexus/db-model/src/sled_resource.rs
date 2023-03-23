// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{ByteCount, SledResourceKind, SqlU32};
use crate::schema::sled_resource;
use uuid::Uuid;

/// Describes sled resource usage by services
#[derive(Clone, Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = sled_resource)]
pub struct SledResource {
    pub id: Uuid,
    pub kind: SledResourceKind,

    pub hardware_threads: SqlU32,
    pub physical_ram: ByteCount,
}

impl SledResource {
    pub fn new(
        id: Uuid,
        kind: SledResourceKind,
        hardware_threads: u32,
        physical_ram: ByteCount,
    ) -> Self {
        Self {
            id,
            kind,
            hardware_threads: SqlU32::new(hardware_threads),
            physical_ram,
        }
    }
}
