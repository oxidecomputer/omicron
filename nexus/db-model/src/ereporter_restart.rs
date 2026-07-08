// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::DbTypedUuid;
use crate::EreporterType;
use crate::SpMgsSlot;
use crate::SpType;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_schema::schema::ereporter_restart;
use omicron_uuid_kinds::EreporterRestartKind;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::RackKind;
use omicron_uuid_kinds::RackUuid;

#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = ereporter_restart)]
pub struct EreporterRestart {
    pub id: DbTypedUuid<EreporterRestartKind>,
    /// The timestamp at which the first ereport received from this restart ID
    /// was collected.
    ///
    /// This is set when inserting a tranche of ereports with a restart ID that
    /// does not already exist in the database. Once a restart entry is
    /// created, this field will not change.
    // XXX(eliza): perhaps we should adjust this timestamp *backwards* if we are
    // inserting a tranche of ereports from an already known restart ID that
    // have an earlier collection time than this value? Think about this...
    pub time_first_seen: DateTime<Utc>,
    pub reporter: EreporterType,
    pub slot_type: SpType,
    pub slot: Option<SpMgsSlot>,
    pub rack_id: DbTypedUuid<RackKind>,
    /// The time at which the most recently received ereport from this restart
    /// ID was collected.
    ///
    /// As new ereports are inserted into the database, this field is updated to
    /// the collection time of those ereports, if their collection time is more
    /// recent than the current value. This value will only ever move forwards
    /// in time as new ereports are collected.
    pub time_latest_ereport_received: DateTime<Utc>,
}

impl EreporterRestart {
    pub fn slot_number(&self) -> Option<u16> {
        self.slot.map(|slot| (*slot).0)
    }

    pub fn id(&self) -> &EreporterRestartUuid {
        &self.id.0
    }

    pub fn rack_id(&self) -> &RackUuid {
        &self.rack_id.0
    }
}

impl iddqd::IdOrdItem for EreporterRestart {
    type Key<'a> = &'a EreporterRestartUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id()
    }

    iddqd::id_upcast!();
}
