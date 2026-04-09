// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::typed_uuid::DbTypedUuid;
use nexus_db_schema::schema::support_bundle;
use nexus_db_schema::schema::{
    support_bundle_data_selection_ereports,
    support_bundle_data_selection_flags,
    support_bundle_data_selection_host_info,
};

use chrono::{DateTime, Utc};
use nexus_types::external_api::support_bundle as support_bundle_types;
use nexus_types::fm::ereport::{EreportFilters, EreportFiltersParams};
use nexus_types::internal_api::views as internal_views;
use nexus_types::support_bundle::BundleData;
use nexus_types::support_bundle::SledSelection;
use omicron_uuid_kinds::CaseKind;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::SupportBundleKind;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolKind;
use omicron_uuid_kinds::ZpoolUuid;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    SupportBundleStateEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum SupportBundleState;

    // Enum values
    Collecting => b"collecting"
    Active => b"active"
    Destroying => b"destroying"
    Failing => b"failing"
    Failed => b"failed"
);

impl SupportBundleState {
    /// Returns the list of valid prior states.
    ///
    /// This is used to confirm that state updates are performed legally,
    /// and defines the possible state transitions.
    pub fn valid_old_states(&self) -> Vec<SupportBundleState> {
        use SupportBundleState::*;

        match self {
            Collecting => vec![],
            Active => vec![Collecting],
            // The "Destroying" state is terminal.
            Destroying => vec![Active, Collecting, Failing],
            Failing => vec![Collecting, Active],
            // The "Failed" state is terminal.
            Failed => vec![Active, Collecting, Failing],
        }
    }
}

impl From<SupportBundleState> for support_bundle_types::SupportBundleState {
    fn from(state: SupportBundleState) -> Self {
        use SupportBundleState::*;

        match state {
            Collecting => support_bundle_types::SupportBundleState::Collecting,
            Active => support_bundle_types::SupportBundleState::Active,
            Destroying => support_bundle_types::SupportBundleState::Destroying,
            // The distinction between "failing" and "failed" should not be
            // visible to end-users. This is internal book-keeping to decide
            // whether or not the bundle record can be safely deleted.
            //
            // Either way, it should be possible to delete the bundle.
            // If a user requests that we delete a bundle in these states:
            // - "Failing" bundles will become "Destroying"
            // - "Failed" bundles can be deleted immediately
            Failing => support_bundle_types::SupportBundleState::Failed,
            Failed => support_bundle_types::SupportBundleState::Failed,
        }
    }
}

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
#[diesel(table_name = support_bundle)]
pub struct SupportBundle {
    pub id: DbTypedUuid<SupportBundleKind>,
    pub time_created: DateTime<Utc>,
    pub reason_for_creation: String,
    pub reason_for_failure: Option<String>,
    pub state: SupportBundleState,
    pub zpool_id: DbTypedUuid<ZpoolKind>,
    pub dataset_id: DbTypedUuid<DatasetKind>,
    pub assigned_nexus: Option<DbTypedUuid<OmicronZoneKind>>,
    pub user_comment: Option<String>,
    pub fm_case_id: Option<DbTypedUuid<CaseKind>>,
}

impl SupportBundle {
    pub fn new(
        id: SupportBundleUuid,
        reason_for_creation: String,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        nexus_id: OmicronZoneUuid,
        user_comment: Option<String>,
        fm_case_id: Option<CaseUuid>,
    ) -> Self {
        Self {
            id: id.into(),
            time_created: Utc::now(),
            reason_for_creation,
            reason_for_failure: None,
            state: SupportBundleState::Collecting,
            zpool_id: zpool_id.into(),
            dataset_id: dataset_id.into(),
            assigned_nexus: Some(nexus_id.into()),
            user_comment,
            fm_case_id: fm_case_id.map(Into::into),
        }
    }

    pub fn id(&self) -> SupportBundleUuid {
        self.id.into()
    }
}

impl From<SupportBundle> for support_bundle_types::SupportBundleInfo {
    fn from(bundle: SupportBundle) -> Self {
        Self {
            id: bundle.id.into(),
            time_created: bundle.time_created,
            reason_for_creation: bundle.reason_for_creation,
            reason_for_failure: bundle.reason_for_failure,
            user_comment: bundle.user_comment,
            state: bundle.state.into(),
        }
    }
}

impl From<SupportBundle> for internal_views::SupportBundleInfo {
    fn from(bundle: SupportBundle) -> Self {
        let fm_case_id = bundle.fm_case_id.map(|id| id.into());
        let base: support_bundle_types::SupportBundleInfo = bundle.into();
        Self { base, fm_case_id }
    }
}

// --- Data selection tables owned by support_bundle ---

/// Flags table row — tracks which payload-less data categories are selected.
/// Always inserted alongside the parent bundle.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = support_bundle_data_selection_flags)]
pub struct DataSelectionFlags {
    pub bundle_id: DbTypedUuid<SupportBundleKind>,
    pub include_reconfigurator: bool,
    pub include_sled_cubby_info: bool,
    pub include_sp_dumps: bool,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = support_bundle_data_selection_host_info)]
pub struct HostInfo {
    pub bundle_id: DbTypedUuid<SupportBundleKind>,
    pub all_sleds: bool,
    pub sled_ids: Vec<uuid::Uuid>,
}

impl HostInfo {
    pub fn new(
        bundle_id: impl Into<DbTypedUuid<SupportBundleKind>>,
        sleds: SledSelection,
    ) -> Self {
        let (all_sleds, sled_ids) = match sleds {
            SledSelection::All => (true, Vec::new()),
            SledSelection::Specific(ids) => (
                false,
                ids.into_iter().map(|id| id.into_untyped_uuid()).collect(),
            ),
        };
        HostInfo { bundle_id: bundle_id.into(), all_sleds, sled_ids }
    }
}

impl From<HostInfo> for BundleData {
    fn from(row: HostInfo) -> Self {
        let HostInfo { bundle_id: _, all_sleds, sled_ids } = row;
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
#[diesel(table_name = support_bundle_data_selection_ereports)]
pub struct Ereports {
    pub bundle_id: DbTypedUuid<SupportBundleKind>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub only_serials: Vec<String>,
    pub only_classes: Vec<String>,
}

impl Ereports {
    pub fn new(
        bundle_id: impl Into<DbTypedUuid<SupportBundleKind>>,
        filters: EreportFilters,
    ) -> Self {
        Ereports {
            bundle_id: bundle_id.into(),
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
            bundle_id: _,
            start_time,
            end_time,
            only_serials,
            only_classes,
        } = row;
        EreportFiltersParams {
            start_time,
            end_time,
            only_serials,
            only_classes,
        }
        .try_into()
        .map(BundleData::Ereports)
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
