// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::fm::AlertRequest;
use crate::fm::DiagnosisEngine;
use crate::fm::Ereport;
use crate::inventory::SpType;
use chrono::{DateTime, Utc};
use iddqd::{IdOrdItem, IdOrdMap};
use omicron_uuid_kinds::{
    CaseUuid, CollectionUuid, OmicronZoneUuid, SitrepUuid,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Case {
    pub id: CaseUuid,
    pub created_sitrep_id: SitrepUuid,
    pub time_created: DateTime<Utc>,

    pub closed_sitrep_id: Option<SitrepUuid>,
    pub time_closed: Option<DateTime<Utc>>,

    pub de: DiagnosisEngine,

    pub ereports: IdOrdMap<Arc<Ereport>>,

    pub alerts_requested: IdOrdMap<AlertRequest>,

    pub impacted_sp_slots: IdOrdMap<ImpactedSpSlot>,

    pub comment: String,
}

impl Case {
    pub fn is_open(&self) -> bool {
        self.time_closed.is_none()
    }
}

impl IdOrdItem for Case {
    type Key<'a> = &'a CaseUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ImpactedSpSlot {
    pub sp_type: SpType,
    pub slot: u8,
    pub created_sitrep_id: SitrepUuid,
    pub comment: String,
}

impl IdOrdItem for ImpactedSpSlot {
    type Key<'a> = (&'a SpType, &'a u8);
    fn key(&self) -> Self::Key<'_> {
        (&self.sp_type, &self.slot)
    }

    iddqd::id_upcast!();
}
