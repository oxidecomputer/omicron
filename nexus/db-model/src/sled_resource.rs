// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::sled_resource;
use crate::typed_uuid::DbTypedUuid;
use crate::{ByteCount, SledResourceKind, SqlU32};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceKind;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
use uuid::Uuid;

type DbInstanceUuid = DbTypedUuid<InstanceKind>;
type DbSledUuid = DbTypedUuid<SledKind>;

#[derive(Clone, Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = sled_resource)]
pub struct Resources {
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
        Self { hardware_threads: SqlU32(threads), rss_ram, reservoir_ram }
    }
}

/// Describes sled resource usage by services
#[derive(Clone, Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = sled_resource)]
pub struct SledResource {
    pub id: Uuid,
    pub sled_id: DbSledUuid,

    #[diesel(embed)]
    pub resources: Resources,

    pub kind: SledResourceKind,
    pub instance_id: Option<DbInstanceUuid>,
}

impl SledResource {
    pub fn new_for_vmm(
        id: PropolisUuid,
        instance_id: InstanceUuid,
        sled_id: SledUuid,
        resources: Resources,
    ) -> Self {
        Self {
            id: id.into_untyped_uuid(),
            instance_id: Some(instance_id.into()),
            sled_id: sled_id.into(),
            kind: SledResourceKind::Instance,
            resources,
        }
    }
}
