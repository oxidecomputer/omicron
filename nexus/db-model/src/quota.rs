// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ByteCount;
use crate::DbTypedUuid;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::silo_quotas;
use nexus_types::external_api::silo;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloKind;
use omicron_uuid_kinds::SiloUuid;
use serde::{Deserialize, Serialize};

#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Serialize,
    Deserialize,
    AsChangeset,
)]
#[diesel(table_name = silo_quotas)]
pub struct SiloQuotas {
    pub silo_id: DbTypedUuid<SiloKind>,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,

    /// The number of CPUs that this silo is allowed to use
    pub cpus: i64,

    /// The amount of memory (in bytes) that this silo is allowed to use
    #[diesel(column_name = memory_bytes)]
    pub memory: ByteCount,

    /// The amount of storage (in bytes) that this silo is allowed to use
    #[diesel(column_name = storage_bytes)]
    pub storage: ByteCount,
}

impl SiloQuotas {
    pub fn silo_id(&self) -> SiloUuid {
        self.silo_id.into()
    }

    pub fn new(
        silo_id: SiloUuid,
        cpus: i64,
        memory: ByteCount,
        storage: ByteCount,
    ) -> Self {
        Self {
            silo_id: silo_id.into(),
            time_created: Utc::now(),
            time_modified: Utc::now(),
            cpus,
            memory,
            storage,
        }
    }

    pub fn arbitrarily_high_default(silo_id: SiloUuid) -> Self {
        let count = silo::SiloQuotasCreate::arbitrarily_high_default();
        Self::new(
            silo_id,
            count.cpus,
            count.memory.into(),
            count.storage.into(),
        )
    }
}

impl From<SiloQuotas> for silo::SiloQuotas {
    fn from(silo_quotas: SiloQuotas) -> Self {
        Self {
            silo_id: silo_quotas.silo_id.into_untyped_uuid(),
            limits: silo::VirtualResourceCounts {
                cpus: silo_quotas.cpus,
                memory: silo_quotas.memory.into(),
                storage: silo_quotas.storage.into(),
            },
        }
    }
}

// Describes a set of updates for the [`SiloQuotas`] model.
#[derive(AsChangeset)]
#[diesel(table_name = silo_quotas)]
pub struct SiloQuotasUpdate {
    pub cpus: Option<i64>,
    #[diesel(column_name = memory_bytes)]
    pub memory: Option<ByteCount>,
    #[diesel(column_name = storage_bytes)]
    pub storage: Option<ByteCount>,
    pub time_modified: DateTime<Utc>,
}

impl From<silo::SiloQuotasUpdate> for SiloQuotasUpdate {
    fn from(params: silo::SiloQuotasUpdate) -> Self {
        Self {
            cpus: params.cpus,
            memory: params.memory.map(|f| f.into()),
            storage: params.storage.map(|f| f.into()),
            time_modified: Utc::now(),
        }
    }
}
