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
use omicron_uuid_kinds::ZpoolKind;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "support_bundle_state", schema = "public"))]
    pub struct SupportBundleStateEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = SupportBundleStateEnum)]
    pub enum SupportBundleState;

    // Enum values
    Collecting => b"collecting"
    Cancelling => b"cancelling"
    Failed => b"failed"
    Active => b"active"
);

impl From<SupportBundleState> for SupportBundleStateView {
    fn from(state: SupportBundleState) -> Self {
        use SupportBundleState::*;

        match state {
            Collecting => SupportBundleStateView::Collecting,
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
    id: Uuid,
    time_created: DateTime<Utc>,
    reason_for_creation: String,
    reason_for_failure: Option<String>,
    state: SupportBundleState,
    zpool_id: DbTypedUuid<ZpoolKind>,
    dataset_id: DbTypedUuid<DatasetKind>,
    assigned_nexus: Option<Uuid>,
}

impl From<SupportBundle> for SupportBundleView {
    fn from(bundle: SupportBundle) -> Self {
        Self {
            id: bundle.id,
            time_created: bundle.time_created,
            reason_for_creation: bundle.reason_for_creation,
            reason_for_failure: bundle.reason_for_failure,
            state: bundle.state.into(),
        }
    }
}
