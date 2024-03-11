// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::schema::downstairs_client_stopped_notification;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use omicron_common::api::internal;
use omicron_uuid_kinds::DownstairsKind;
use omicron_uuid_kinds::UpstairsKind;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "downstairs_client_stop_reason_type", schema = "public"))]
    pub struct DownstairsClientStopReasonEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = DownstairsClientStopReasonEnum)]
    pub enum DownstairsClientStopReason;

    // Reason types
    Replacing => b"replacing"
    Disabled => b"disabled"
    FailedReconcile => b"failed_reconcile"
    IOError => b"io_error"
    BadNegotiationOrder => b"bad_negotiation_order"
    Incompatible => b"incompatible"
    FailedLiveRepair => b"failed_live_repair"
    TooManyOutstandingJobs => b"too_many_outstanding_jobs"
    Deactivated => b"deactivated"
);

impl From<internal::nexus::DownstairsClientStopReason> for DownstairsClientStopReason {
    fn from(v: internal::nexus::DownstairsClientStopReason) -> DownstairsClientStopReason {
        match v {
            internal::nexus::DownstairsClientStopReason::Replacing => DownstairsClientStopReason::Replacing,
            internal::nexus::DownstairsClientStopReason::Disabled => DownstairsClientStopReason::Disabled,
            internal::nexus::DownstairsClientStopReason::FailedReconcile => DownstairsClientStopReason::FailedReconcile,
            internal::nexus::DownstairsClientStopReason::IOError => DownstairsClientStopReason::IOError,
            internal::nexus::DownstairsClientStopReason::BadNegotiationOrder => DownstairsClientStopReason::BadNegotiationOrder,
            internal::nexus::DownstairsClientStopReason::Incompatible => DownstairsClientStopReason::Incompatible,
            internal::nexus::DownstairsClientStopReason::FailedLiveRepair => DownstairsClientStopReason::FailedLiveRepair,
            internal::nexus::DownstairsClientStopReason::TooManyOutstandingJobs => DownstairsClientStopReason::TooManyOutstandingJobs,
            internal::nexus::DownstairsClientStopReason::Deactivated => DownstairsClientStopReason::Deactivated,
        }
    }
}

/// A Record of when an Upstairs stopped a Downstairs client task
#[derive(Queryable, Insertable, Debug, Clone, Selectable)]
#[diesel(table_name = downstairs_client_stopped_notification)]
pub struct DownstairsClientStoppedNotification {
    // Importantly, this is client time, not Nexus' time that it received the
    // notification.
    pub time: DateTime<Utc>,

    // Which Upstairs sent this notification?
    pub upstairs_id: DbTypedUuid<UpstairsKind>,

    // Which Downstairs client was stopped?
    pub downstairs_id: DbTypedUuid<DownstairsKind>,

    pub reason: DownstairsClientStopReason,
}
