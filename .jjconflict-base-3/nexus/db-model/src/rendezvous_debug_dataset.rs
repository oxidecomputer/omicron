// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::rendezvous_debug_dataset;
use omicron_uuid_kinds::BlueprintKind;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ZpoolKind;
use omicron_uuid_kinds::ZpoolUuid;
use serde::{Deserialize, Serialize};

/// Database representation of a Debug Dataset available for use.
#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Deserialize,
    Serialize,
    PartialEq,
)]
#[diesel(table_name = rendezvous_debug_dataset)]
pub struct RendezvousDebugDataset {
    id: DbTypedUuid<DatasetKind>,
    time_created: DateTime<Utc>,
    time_tombstoned: Option<DateTime<Utc>>,
    pool_id: DbTypedUuid<ZpoolKind>,
    blueprint_id_when_created: DbTypedUuid<BlueprintKind>,
    blueprint_id_when_tombstoned: Option<DbTypedUuid<BlueprintKind>>,
}

impl RendezvousDebugDataset {
    pub fn new(
        id: DatasetUuid,
        pool_id: ZpoolUuid,
        blueprint_id: BlueprintUuid,
    ) -> Self {
        Self {
            id: id.into(),
            time_created: Utc::now(),
            time_tombstoned: None,
            pool_id: pool_id.into(),
            blueprint_id_when_created: blueprint_id.into(),
            blueprint_id_when_tombstoned: None,
        }
    }

    pub fn id(&self) -> DatasetUuid {
        self.id.into()
    }

    pub fn pool_id(&self) -> ZpoolUuid {
        self.pool_id.into()
    }

    pub fn blueprint_id_when_created(&self) -> BlueprintUuid {
        self.blueprint_id_when_created.into()
    }

    pub fn blueprint_id_when_tombstoned(&self) -> Option<BlueprintUuid> {
        self.blueprint_id_when_tombstoned.map(From::from)
    }

    pub fn is_tombstoned(&self) -> bool {
        // A CHECK constraint in the schema guarantees both the `*_tombstoned`
        // fields are set or not set; document that check here with an
        // assertion.
        debug_assert_eq!(
            self.time_tombstoned.is_some(),
            self.blueprint_id_when_tombstoned.is_some()
        );
        self.time_tombstoned.is_some()
    }
}
