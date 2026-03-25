// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management support bundle requests and per-variant data selection
//! models.

use crate::DbTypedUuid;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{
    fm_support_bundle_data_ereports, fm_support_bundle_data_host_info,
    fm_support_bundle_data_reconfigurator,
    fm_support_bundle_data_sled_cubby_info, fm_support_bundle_data_sp_dumps,
    fm_support_bundle_request,
};
use nexus_types::fm;
use nexus_types::fm::ereport::EreportFilters;
use nexus_types::support_bundle::SledSelection;
use omicron_uuid_kinds::{
    CaseKind, GenericUuid, SitrepKind, SledUuid, SupportBundleKind,
};

// --- SupportBundleRequest (parent row) ---

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_support_bundle_request)]
pub struct SupportBundleRequest {
    pub id: DbTypedUuid<SupportBundleKind>,
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub requested_sitrep_id: DbTypedUuid<SitrepKind>,
    pub case_id: DbTypedUuid<CaseKind>,
}

impl SupportBundleRequest {
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        case_id: impl Into<DbTypedUuid<CaseKind>>,
        req: fm::case::SupportBundleRequest,
    ) -> Self {
        let fm::case::SupportBundleRequest {
            id,
            requested_sitrep_id,
            data_selection: _,
        } = req;
        SupportBundleRequest {
            id: id.into(),
            sitrep_id: sitrep_id.into(),
            requested_sitrep_id: requested_sitrep_id.into(),
            case_id: case_id.into(),
        }
    }
}

// --- Per-variant data selection tables ---
//
// Ordered to match `BundleData` / `BundleDataCategory`.

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_support_bundle_data_reconfigurator)]
pub struct SbdataReconfigurator {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_support_bundle_data_host_info)]
pub struct SbdataHostInfo {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
    pub all_sleds: bool,
    pub sled_ids: Vec<uuid::Uuid>,
}

impl SbdataHostInfo {
    pub fn new(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        request_id: impl Into<DbTypedUuid<SupportBundleKind>>,
        sleds: &SledSelection,
    ) -> Self {
        let (all_sleds, sled_ids) = match sleds {
            SledSelection::All => (true, Vec::new()),
            SledSelection::Specific(ids) => {
                (false, ids.iter().map(|id| id.into_untyped_uuid()).collect())
            }
        };
        SbdataHostInfo {
            sitrep_id: sitrep_id.into(),
            request_id: request_id.into(),
            all_sleds,
            sled_ids,
        }
    }

    /// Reconstruct the `SledSelection` from the DB row.
    pub fn into_sled_selection(self) -> SledSelection {
        if self.all_sleds {
            SledSelection::All
        } else {
            SledSelection::Specific(
                self.sled_ids
                    .into_iter()
                    .map(SledUuid::from_untyped_uuid)
                    .collect(),
            )
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_support_bundle_data_sled_cubby_info)]
pub struct SbdataSledCubbyInfo {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_support_bundle_data_sp_dumps)]
pub struct SbdataSpDumps {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_support_bundle_data_ereports)]
pub struct SbdataEreports {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub only_serials: Vec<String>,
    pub only_classes: Vec<String>,
}

impl SbdataEreports {
    pub fn new(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        request_id: impl Into<DbTypedUuid<SupportBundleKind>>,
        filters: &EreportFilters,
    ) -> Self {
        SbdataEreports {
            sitrep_id: sitrep_id.into(),
            request_id: request_id.into(),
            start_time: filters.start_time(),
            end_time: filters.end_time(),
            only_serials: filters.only_serials().to_vec(),
            only_classes: filters.only_classes().to_vec(),
        }
    }

    /// Reconstruct the `EreportFilters` from the DB row.
    ///
    /// Returns an error if the stored start/end times are inconsistent
    /// (e.g. start > end due to corrupt data).
    pub fn into_ereport_filters(
        self,
    ) -> Result<EreportFilters, omicron_common::api::external::Error> {
        let mut filters = EreportFilters::new();
        if let Some(t) = self.start_time {
            filters = filters.with_start_time(t)?;
        }
        if let Some(t) = self.end_time {
            filters = filters.with_end_time(t)?;
        }
        Ok(filters
            .with_serials(self.only_serials)
            .with_classes(self.only_classes))
    }
}
