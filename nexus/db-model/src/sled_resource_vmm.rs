// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::typed_uuid::DbTypedUuid;
use crate::{ByteCount, SqlU32};
use nexus_db_schema::schema::sled_resource_vmm;
use omicron_uuid_kinds::InstanceKind;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisKind;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;

type DbInstanceUuid = DbTypedUuid<InstanceKind>;
type DbPropolisUuid = DbTypedUuid<PropolisKind>;
type DbSledUuid = DbTypedUuid<SledKind>;

#[derive(Clone, Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = sled_resource_vmm)]
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

/// Describes sled resource usage by a VMM
#[derive(Clone, Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = sled_resource_vmm)]
pub struct SledResourceVmm {
    pub id: DbPropolisUuid,
    pub sled_id: DbSledUuid,

    #[diesel(embed)]
    pub resources: Resources,

    pub instance_id: Option<DbInstanceUuid>,
}

impl SledResourceVmm {
    pub fn new(
        id: PropolisUuid,
        instance_id: InstanceUuid,
        sled_id: SledUuid,
        resources: Resources,
    ) -> Self {
        Self {
            id: id.into(),
            instance_id: Some(instance_id.into()),
            sled_id: sled_id.into(),
            resources,
        }
    }

    pub fn id(&self) -> PropolisUuid {
        self.id.into()
    }

    pub fn sled_id(&self) -> SledUuid {
        self.sled_id.into()
    }

    pub fn instance_id(&self) -> Option<InstanceUuid> {
        self.instance_id.map(|x| x.into())
    }
}
