// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management support bundle requests and data selection models.

use crate::DbTypedUuid;
use crate::impl_enum_type;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{
    fm_sb_req_data_selection, fm_support_bundle_request,
};
use nexus_types::fm;
use nexus_types::fm::ereport::EreportFilters;
use nexus_types::support_bundle as support_bundle_types;
use nexus_types::support_bundle::{
    BundleData, BundleDataSelection, SledSelection,
};
use omicron_uuid_kinds::{
    CaseKind, GenericUuid, SitrepKind, SledUuid, SupportBundleKind,
};
use serde::{Deserialize, Serialize};

impl_enum_type!(
    BundleDataCategoryEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Serialize,
        Deserialize,
        AsExpression,
        FromSqlRow,
    )]
    pub enum BundleDataCategory;

    Reconfigurator => b"reconfigurator"
    HostInfo => b"host_info"
    SledCubbyInfo => b"sled_cubby_info"
    SpDumps => b"sp_dumps"
    Ereports => b"ereports"
);

impl From<&support_bundle_types::BundleData> for BundleDataCategory {
    fn from(data: &support_bundle_types::BundleData) -> Self {
        match data {
            support_bundle_types::BundleData::Reconfigurator => {
                BundleDataCategory::Reconfigurator
            }
            support_bundle_types::BundleData::HostInfo(_) => {
                BundleDataCategory::HostInfo
            }
            support_bundle_types::BundleData::SledCubbyInfo => {
                BundleDataCategory::SledCubbyInfo
            }
            support_bundle_types::BundleData::SpDumps => {
                BundleDataCategory::SpDumps
            }
            support_bundle_types::BundleData::Ereports(_) => {
                BundleDataCategory::Ereports
            }
        }
    }
}

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

// --- Data selection rows ---

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_sb_req_data_selection)]
pub struct SbReqDataSelection {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
    pub category: BundleDataCategory,
    // HostInfo fields
    pub all_sleds: Option<bool>,
    pub sled_ids: Option<Vec<uuid::Uuid>>,
    // Ereports fields
    pub ereport_start_time: Option<DateTime<Utc>>,
    pub ereport_end_time: Option<DateTime<Utc>>,
    pub ereport_only_serials: Option<Vec<String>>,
    pub ereport_only_classes: Option<Vec<String>>,
}

impl SbReqDataSelection {
    /// Create rows from a `BundleDataSelection` for a given request.
    pub fn from_data_selection(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>> + Copy,
        request_id: impl Into<DbTypedUuid<SupportBundleKind>> + Copy,
        selection: &BundleDataSelection,
    ) -> Vec<Self> {
        selection
            .iter()
            .map(|data| Self::from_bundle_data(sitrep_id, request_id, data))
            .collect()
    }

    fn from_bundle_data(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        request_id: impl Into<DbTypedUuid<SupportBundleKind>>,
        data: &BundleData,
    ) -> Self {
        let sitrep_id = sitrep_id.into();
        let request_id = request_id.into();
        let base = Self {
            sitrep_id,
            request_id,
            category: BundleDataCategory::from(data),
            all_sleds: None,
            sled_ids: None,
            ereport_start_time: None,
            ereport_end_time: None,
            ereport_only_serials: None,
            ereport_only_classes: None,
        };
        match data {
            BundleData::Reconfigurator
            | BundleData::SledCubbyInfo
            | BundleData::SpDumps => base,
            BundleData::HostInfo(sled_selection) => match sled_selection {
                SledSelection::All => Self {
                    all_sleds: Some(true),
                    sled_ids: Some(vec![]),
                    ..base
                },
                SledSelection::Specific(set) => Self {
                    all_sleds: Some(false),
                    sled_ids: Some(
                        set.iter().map(|id| id.into_untyped_uuid()).collect(),
                    ),
                    ..base
                },
            },
            BundleData::Ereports(filters) => Self {
                ereport_start_time: filters.start_time(),
                ereport_end_time: filters.end_time(),
                ereport_only_serials: Some(filters.only_serials().to_vec()),
                ereport_only_classes: Some(filters.only_classes().to_vec()),
                ..base
            },
        }
    }

    /// Convert a set of data selection rows back into a `BundleDataSelection`.
    /// Empty input produces the default selection (collect everything).
    pub fn into_data_selection(rows: Vec<Self>) -> BundleDataSelection {
        rows.into_iter().map(Self::into_bundle_data).collect()
    }

    fn into_bundle_data(self) -> BundleData {
        match self.category {
            BundleDataCategory::Reconfigurator => BundleData::Reconfigurator,
            BundleDataCategory::SledCubbyInfo => BundleData::SledCubbyInfo,
            BundleDataCategory::SpDumps => BundleData::SpDumps,
            BundleDataCategory::HostInfo => {
                if self.all_sleds.expect(
                    "CHECK constraint guarantees all_sleds IS NOT NULL \
                     for host_info rows",
                ) {
                    BundleData::HostInfo(SledSelection::All)
                } else {
                    let ids = self.sled_ids.expect(
                        "CHECK constraint guarantees sled_ids IS NOT NULL \
                         for host_info rows",
                    );
                    BundleData::HostInfo(SledSelection::Specific(
                        ids.into_iter()
                            .map(SledUuid::from_untyped_uuid)
                            .collect(),
                    ))
                }
            }
            BundleDataCategory::Ereports => {
                let mut filters = EreportFilters::new();
                if let Some(t) = self.ereport_start_time {
                    filters = filters.with_start_time(t).expect(
                        "CHECK constraint guarantees start_time <= end_time",
                    );
                }
                if let Some(t) = self.ereport_end_time {
                    filters = filters.with_end_time(t).expect(
                        "CHECK constraint guarantees start_time <= end_time",
                    );
                }
                filters =
                    filters.with_serials(self.ereport_only_serials.expect(
                        "CHECK constraint guarantees ereport_only_serials \
                         IS NOT NULL for ereports rows",
                    ));
                filters =
                    filters.with_classes(self.ereport_only_classes.expect(
                        "CHECK constraint guarantees ereport_only_classes \
                         IS NOT NULL for ereports rows",
                    ));
                BundleData::Ereports(filters)
            }
        }
    }
}
