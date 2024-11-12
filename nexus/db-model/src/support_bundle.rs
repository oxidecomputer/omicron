// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::schema::support_bundle;
use crate::typed_uuid::DbTypedUuid;

use chrono::{DateTime, Utc};
use nexus_types::external_api::shared::SupportBundleInfo as SupportBundleView;
use nexus_types::external_api::shared::SupportBundleState as SupportBundleStateView;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SupportBundleKind;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolKind;
use omicron_uuid_kinds::ZpoolUuid;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "support_bundle_state", schema = "public"))]
    pub struct SupportBundleStateEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = SupportBundleStateEnum)]
    pub enum SupportBundleState;

    // Enum values
    Collecting => b"collecting"
    Collected => b"collected"
    Cancelling => b"cancelling"
    Failed => b"failed"
    Active => b"active"
);

impl SupportBundleState {
    pub fn might_have_dataset_storage(&self) -> bool {
        use SupportBundleState::*;

        match self {
            Collecting => false,
            Collected => true,
            Cancelling => true,
            Failed => false,
            Active => true,
        }
    }
}

impl From<SupportBundleState> for SupportBundleStateView {
    fn from(state: SupportBundleState) -> Self {
        use SupportBundleState::*;

        match state {
            Collecting => SupportBundleStateView::Collecting,
            // The distinction between "collected" and "collecting" is an
            // internal detail that doesn't need to be exposed through the API.
            Collected => SupportBundleStateView::Collecting,
            Cancelling => SupportBundleStateView::Cancelling,
            Failed => SupportBundleStateView::Failed,
            Active => SupportBundleStateView::Active,
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
}

impl SupportBundle {
    pub fn new(
        reason_for_creation: &'static str,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        Self {
            id: SupportBundleUuid::new_v4().into(),
            time_created: Utc::now(),
            reason_for_creation: reason_for_creation.to_string(),
            reason_for_failure: None,
            state: SupportBundleState::Collecting,
            zpool_id: zpool_id.into(),
            dataset_id: dataset_id.into(),
            assigned_nexus: Some(nexus_id.into()),
        }
    }
}

impl From<SupportBundle> for SupportBundleView {
    fn from(bundle: SupportBundle) -> Self {
        Self {
            id: bundle.id.into(),
            time_created: bundle.time_created,
            reason_for_creation: bundle.reason_for_creation,
            reason_for_failure: bundle.reason_for_failure,
            state: bundle.state.into(),
        }
    }
}
