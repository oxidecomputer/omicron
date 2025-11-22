// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DbTypedUuid;
use crate::ByteCount;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_schema::schema::local_storage_dataset_allocation;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ExternalZpoolKind;
use omicron_uuid_kinds::ExternalZpoolUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
use serde::Deserialize;
use serde::Serialize;

/// A slice of a the local storage dataset present on each zpool, allocated for
/// use with a local storage type Disk. This is a child dataset of that local
/// storage dataset, itself containing a child zvol that will be delegated to a
/// Propolis zone.
#[derive(
    Queryable, Insertable, Debug, Clone, Selectable, Serialize, Deserialize,
)]
#[diesel(table_name = local_storage_dataset_allocation)]
pub struct LocalStorageDatasetAllocation {
    id: DbTypedUuid<DatasetKind>,

    pub time_created: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    local_storage_dataset_id: DbTypedUuid<DatasetKind>,
    pool_id: DbTypedUuid<ExternalZpoolKind>,
    sled_id: DbTypedUuid<SledKind>,

    /// Size of this dataset, which is enough to contain the child zvol plus
    /// some overhead.
    pub dataset_size: ByteCount,
}

impl LocalStorageDatasetAllocation {
    pub fn id(&self) -> DatasetUuid {
        self.id.into()
    }

    pub fn local_storage_dataset_id(&self) -> DatasetUuid {
        self.local_storage_dataset_id.into()
    }

    pub fn pool_id(&self) -> ExternalZpoolUuid {
        self.pool_id.into()
    }

    pub fn sled_id(&self) -> SledUuid {
        self.sled_id.into()
    }
}
