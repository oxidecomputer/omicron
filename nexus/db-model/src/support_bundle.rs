// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::typed_uuid::DbTypedUuid;
use nexus_db_schema::schema::support_bundle;

use chrono::{DateTime, Utc};
use nexus_types::external_api::shared::SupportBundleInfo as SupportBundleView;
use nexus_types::external_api::shared::SupportBundleState as SupportBundleStateView;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::OmicronZoneUuid;
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

impl From<SupportBundleState> for SupportBundleStateView {
    fn from(state: SupportBundleState) -> Self {
        use SupportBundleState::*;

        match state {
            Collecting => SupportBundleStateView::Collecting,
            Active => SupportBundleStateView::Active,
            Destroying => SupportBundleStateView::Destroying,
            // The distinction between "failing" and "failed" should not be
            // visible to end-users. This is internal book-keeping to decide
            // whether or not the bundle record can be safely deleted.
            //
            // Either way, it should be possible to delete the bundle.
            // If a user requests that we delete a bundle in these states:
            // - "Failing" bundles will become "Destroying"
            // - "Failed" bundles can be deleted immediately
            Failing => SupportBundleStateView::Failed,
            Failed => SupportBundleStateView::Failed,
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
}

impl SupportBundle {
    pub fn new(
        reason_for_creation: &'static str,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        nexus_id: OmicronZoneUuid,
        user_comment: Option<String>,
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
            user_comment,
        }
    }

    pub fn id(&self) -> SupportBundleUuid {
        self.id.into()
    }
}

impl From<SupportBundle> for SupportBundleView {
    fn from(bundle: SupportBundle) -> Self {
        Self {
            id: bundle.id.into_untyped_uuid(),
            time_created: bundle.time_created,
            reason_for_creation: bundle.reason_for_creation,
            reason_for_failure: bundle.reason_for_failure,
            user_comment: bundle.user_comment,
            state: bundle.state.into(),
        }
    }
}
