// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::sled_resource;
use crate::{ByteCount, SledResourceKind, SqlU32};
use uuid::Uuid;

#[derive(Clone, Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = sled_resource)]
pub struct Resources {
    pub zpool_id: Option<Uuid>,
    pub hardware_threads: SqlU32,
    pub rss_ram: ByteCount,
    pub reservoir_ram: ByteCount,
}

impl Resources {
    pub fn new(
        threads: u32,
        rss_ram: ByteCount,
        reservoir_ram: ByteCount,
    ) -> Self {
        Self {
            zpool_id: None,
            hardware_threads: SqlU32(threads),
            rss_ram,
            reservoir_ram,
        }
    }
}

/// Describes sled resource usage by services
#[derive(Clone, Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = sled_resource)]
pub struct SledResource {
    pub id: Uuid,
    pub sled_id: Uuid,
    pub kind: SledResourceKind,

    #[diesel(embed)]
    pub resources: Resources,
}

impl SledResource {
    pub fn new(
        id: Uuid,
        sled_id: Uuid,
        kind: SledResourceKind,
        resources: Resources,
    ) -> Self {
        Self { id, sled_id, kind, resources }
    }
}
