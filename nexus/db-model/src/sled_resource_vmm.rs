// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ByteCount;
use crate::SqlU32;
use crate::impl_enum_type;
use crate::typed_uuid::DbTypedUuid;
use nexus_db_schema::schema::sled_resource_vmm;
use omicron_uuid_kinds::InstanceKind;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisKind;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    /// The various states that a sled_resource_vmm record can be in:
    ///
    /// - `tombstoned` means that the VMM is not currently running the instance,
    ///   but the propolis process may still exist and/or the zone has not been
    ///   destroyed yet, meaning it is still consuming sled resources.
    ///
    /// - `active` means that the VMM is currently running the instance.
    ///    A sled resource reservation is in the `active` state if either
    ///    of the following are true:
    ///    1. It was created in order to start a previously `Stopped`
    ///       instance.
    ///    2. A migration into that VMM has completed successfully.
    ///
    /// - `target` means that the VMM is a migration destination for the
    ///   `active` VMM
    SledResourceVmmStateEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum SledResourceVmmState;

    // Enum values
    Tombstoned => b"tombstoned"
    Active => b"active"
    Target => b"target"
);

impl fmt::Display for SledResourceVmmState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SledResourceVmmState::Tombstoned => write!(f, "tombstoned"),
            SledResourceVmmState::Active => write!(f, "active"),
            SledResourceVmmState::Target => write!(f, "target"),
        }
    }
}

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

    pub state: SledResourceVmmState,
}

impl SledResourceVmm {
    pub fn new(
        id: PropolisUuid,
        instance_id: InstanceUuid,
        sled_id: SledUuid,
        resources: Resources,
        state: SledResourceVmmState,
    ) -> Self {
        Self {
            id: id.into(),
            instance_id: Some(instance_id.into()),
            sled_id: sled_id.into(),
            resources,
            state,
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
