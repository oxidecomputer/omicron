// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DbTypedUuid;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_schema::schema::rendezvous_local_storage_dataset;
use omicron_uuid_kinds::BlueprintKind;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ZpoolKind;
use omicron_uuid_kinds::ZpoolUuid;

#[derive(Queryable, Insertable, Debug, Clone, Selectable, PartialEq)]
#[diesel(table_name = rendezvous_local_storage_dataset)]
pub struct RendezvousLocalStorageDataset {
    id: DbTypedUuid<DatasetKind>,

    time_created: DateTime<Utc>,
    time_tombstoned: Option<DateTime<Utc>>,

    blueprint_id_when_created: DbTypedUuid<BlueprintKind>,
    blueprint_id_when_tombstoned: Option<DbTypedUuid<BlueprintKind>>,

    pub pool_id: DbTypedUuid<ZpoolKind>,

    pub size_used: i64,

    /// Do not consider this dataset as a candidate during local storage
    /// allocation
    no_provision: bool,
}

impl RendezvousLocalStorageDataset {
    pub fn new(
        id: DatasetUuid,
        pool_id: ZpoolUuid,
        blueprint_id_when_created: BlueprintUuid,
    ) -> Self {
        Self {
            id: id.into(),
            time_created: Utc::now(),
            time_tombstoned: None,
            blueprint_id_when_created: blueprint_id_when_created.into(),
            blueprint_id_when_tombstoned: None,
            pool_id: pool_id.into(),
            size_used: 0,
            no_provision: false,
        }
    }

    pub fn id(&self) -> DatasetUuid {
        self.id.into()
    }

    pub fn pool_id(&self) -> ZpoolUuid {
        self.pool_id.into()
    }

    pub fn is_tombstoned(&self) -> bool {
        self.time_tombstoned.is_some()
    }
}
