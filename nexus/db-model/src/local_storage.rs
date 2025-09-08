// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DbTypedUuid;
use super::Generation;
use chrono::DateTime;
use chrono::Utc;
use db_macros::Asset;
use nexus_db_schema::schema::local_storage_dataset;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ZpoolKind;
use omicron_uuid_kinds::ZpoolUuid;

#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset, PartialEq)]
#[diesel(table_name = local_storage_dataset)]
#[asset(uuid_kind = DatasetKind)]
pub struct LocalStorageDataset {
    #[diesel(embed)]
    identity: LocalStorageDatasetIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    pub pool_id: DbTypedUuid<ZpoolKind>,

    pub size_used: i64,

    /// Do not consider this dataset as a candidate during local storage
    /// allocation
    no_provision: bool,
}

impl LocalStorageDataset {
    pub fn new(id: DatasetUuid, pool_id: ZpoolUuid) -> Self {
        Self {
            identity: LocalStorageDatasetIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            pool_id: pool_id.into(),
            size_used: 0,
            no_provision: false,
        }
    }

    pub fn pool_id(&self) -> ZpoolUuid {
        self.pool_id.into()
    }
}
