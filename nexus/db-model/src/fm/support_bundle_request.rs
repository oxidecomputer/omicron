// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management support bundle requests and data selection models.

use crate::DbTypedUuid;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{
    fm_support_bundle_request,
    fm_support_bundle_request_data_selection_ereports,
    fm_support_bundle_request_data_selection_flags,
    fm_support_bundle_request_data_selection_host_info,
};
use nexus_types::fm;
use nexus_types::fm::ereport::EreportFilters;
use nexus_types::support_bundle::{
    BundleData, BundleDataCategory, SledSelection,
};
use omicron_uuid_kinds::{
    CaseKind, GenericUuid, SitrepKind, SledUuid, SupportBundleKind,
};

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

/// Flags table row — tracks which payload-less data categories are selected.
/// Always inserted alongside the parent bundle request.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_support_bundle_request_data_selection_flags)]
pub struct DataSelectionFlags {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
    pub include_reconfigurator: bool,
    pub include_sled_cubby_info: bool,
    pub include_sp_dumps: bool,
}

impl DataSelectionFlags {
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        request_id: impl Into<DbTypedUuid<SupportBundleKind>>,
        data_selection: &nexus_types::support_bundle::BundleDataSelection,
    ) -> Self {
        DataSelectionFlags {
            sitrep_id: sitrep_id.into(),
            request_id: request_id.into(),
            include_reconfigurator: data_selection
                .contains(BundleDataCategory::Reconfigurator),
            include_sled_cubby_info: data_selection
                .contains(BundleDataCategory::SledCubbyInfo),
            include_sp_dumps: data_selection
                .contains(BundleDataCategory::SpDumps),
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_support_bundle_request_data_selection_host_info)]
pub struct HostInfo {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
    pub all_sleds: bool,
    pub sled_ids: Vec<uuid::Uuid>,
}

impl HostInfo {
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        request_id: impl Into<DbTypedUuid<SupportBundleKind>>,
        sleds: SledSelection,
    ) -> Self {
        let (all_sleds, sled_ids) = match sleds {
            SledSelection::All => (true, Vec::new()),
            SledSelection::Specific(ids) => (
                false,
                ids.into_iter().map(|id| id.into_untyped_uuid()).collect(),
            ),
        };
        HostInfo {
            sitrep_id: sitrep_id.into(),
            request_id: request_id.into(),
            all_sleds,
            sled_ids,
        }
    }
}

impl From<HostInfo> for BundleData {
    fn from(row: HostInfo) -> Self {
        let HostInfo { sitrep_id: _, request_id: _, all_sleds, sled_ids } = row;
        let selection = if all_sleds {
            SledSelection::All
        } else {
            SledSelection::Specific(
                sled_ids.into_iter().map(SledUuid::from_untyped_uuid).collect(),
            )
        };
        BundleData::HostInfo(selection)
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_support_bundle_request_data_selection_ereports)]
pub struct Ereports {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub only_serials: Vec<String>,
    pub only_classes: Vec<String>,
}

impl Ereports {
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        request_id: impl Into<DbTypedUuid<SupportBundleKind>>,
        filters: EreportFilters,
    ) -> Self {
        Ereports {
            sitrep_id: sitrep_id.into(),
            request_id: request_id.into(),
            start_time: filters.start_time(),
            end_time: filters.end_time(),
            only_serials: filters.only_serials().to_vec(),
            only_classes: filters.only_classes().to_vec(),
        }
    }
}

impl TryFrom<Ereports> for BundleData {
    type Error = omicron_common::api::external::Error;

    fn try_from(row: Ereports) -> Result<Self, Self::Error> {
        let Ereports {
            sitrep_id: _,
            request_id: _,
            start_time,
            end_time,
            only_serials,
            only_classes,
        } = row;
        let mut filters = EreportFilters::new();
        if let Some(t) = start_time {
            filters = filters.with_start_time(t)?;
        }
        if let Some(t) = end_time {
            filters = filters.with_end_time(t)?;
        }
        Ok(BundleData::Ereports(
            filters.with_serials(only_serials).with_classes(only_classes),
        ))
    }
}

/// Joined query result: flags + optional host_info + optional ereports.
/// All fields use `#[diesel(embed)]` so no `table_name` is needed.
#[derive(Queryable, Selectable)]
pub struct BundleDataSelection {
    #[diesel(embed)]
    pub flags: DataSelectionFlags,
    #[diesel(embed)]
    pub host_info: Option<HostInfo>,
    #[diesel(embed)]
    pub ereports: Option<Ereports>,
}

impl TryFrom<BundleDataSelection>
    for nexus_types::support_bundle::BundleDataSelection
{
    type Error = omicron_common::api::external::Error;

    fn try_from(row: BundleDataSelection) -> Result<Self, Self::Error> {
        let mut selection =
            nexus_types::support_bundle::BundleDataSelection::new();
        if row.flags.include_reconfigurator {
            selection.insert(BundleData::Reconfigurator);
        }
        if row.flags.include_sled_cubby_info {
            selection.insert(BundleData::SledCubbyInfo);
        }
        if row.flags.include_sp_dumps {
            selection.insert(BundleData::SpDumps);
        }
        if let Some(host_info) = row.host_info {
            selection.insert(host_info.into());
        }
        if let Some(ereports) = row.ereports {
            selection.insert(ereports.try_into()?);
        }
        Ok(selection)
    }
}
