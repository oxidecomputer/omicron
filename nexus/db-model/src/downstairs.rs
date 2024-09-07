// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::schema::downstairs_client_stop_request_notification;
use crate::schema::downstairs_client_stopped_notification;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use omicron_common::api::internal;
use omicron_uuid_kinds::DownstairsKind;
use omicron_uuid_kinds::UpstairsKind;
use serde::{Deserialize, Serialize};

// Types for stop request notification

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "downstairs_client_stop_request_reason_type", schema = "public"))]
    pub struct DownstairsClientStopRequestReasonEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = DownstairsClientStopRequestReasonEnum)]
    pub enum DownstairsClientStopRequestReason;

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

impl From<internal::nexus::DownstairsClientStopRequestReason>
    for DownstairsClientStopRequestReason
{
    fn from(
        v: internal::nexus::DownstairsClientStopRequestReason,
    ) -> DownstairsClientStopRequestReason {
        match v {
            internal::nexus::DownstairsClientStopRequestReason::Replacing => DownstairsClientStopRequestReason::Replacing,
            internal::nexus::DownstairsClientStopRequestReason::Disabled => DownstairsClientStopRequestReason::Disabled,
            internal::nexus::DownstairsClientStopRequestReason::FailedReconcile => DownstairsClientStopRequestReason::FailedReconcile,
            internal::nexus::DownstairsClientStopRequestReason::IOError => DownstairsClientStopRequestReason::IOError,
            internal::nexus::DownstairsClientStopRequestReason::BadNegotiationOrder => DownstairsClientStopRequestReason::BadNegotiationOrder,
            internal::nexus::DownstairsClientStopRequestReason::Incompatible => DownstairsClientStopRequestReason::Incompatible,
            internal::nexus::DownstairsClientStopRequestReason::FailedLiveRepair => DownstairsClientStopRequestReason::FailedLiveRepair,
            internal::nexus::DownstairsClientStopRequestReason::TooManyOutstandingJobs => DownstairsClientStopRequestReason::TooManyOutstandingJobs,
            internal::nexus::DownstairsClientStopRequestReason::Deactivated => DownstairsClientStopRequestReason::Deactivated,
        }
    }
}

/// A Record of when an Upstairs requested a Downstairs client task stop
#[derive(Queryable, Insertable, Debug, Clone, Selectable)]
#[diesel(table_name = downstairs_client_stop_request_notification)]
pub struct DownstairsClientStopRequestNotification {
    // Importantly, this is client time, not Nexus' time that it received the
    // notification.
    pub time: DateTime<Utc>,

    // Which Upstairs sent this notification?
    pub upstairs_id: DbTypedUuid<UpstairsKind>,

    // Which Downstairs client was requested to stop?
    pub downstairs_id: DbTypedUuid<DownstairsKind>,

    pub reason: DownstairsClientStopRequestReason,
}

// Types for stopped notification

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "downstairs_client_stopped_reason_type", schema = "public"))]
    pub struct DownstairsClientStoppedReasonEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = DownstairsClientStoppedReasonEnum)]
    pub enum DownstairsClientStoppedReason;

    // Reason types
    ConnectionTimeout => b"connection_timeout"
    ConnectionFailed => b"connection_failed"
    Timeout => b"timeout"
    WriteFailed => b"write_failed"
    ReadFailed => b"read_failed"
    RequestedStop => b"requested_stop"
    Finished => b"finished"
    QueueClosed => b"queue_closed"
    ReceiveTaskCancelled => b"receive_task_cancelled"
);

impl From<internal::nexus::DownstairsClientStoppedReason>
    for DownstairsClientStoppedReason
{
    fn from(
        v: internal::nexus::DownstairsClientStoppedReason,
    ) -> DownstairsClientStoppedReason {
        match v {
            internal::nexus::DownstairsClientStoppedReason::ConnectionTimeout => DownstairsClientStoppedReason::ConnectionTimeout,
            internal::nexus::DownstairsClientStoppedReason::ConnectionFailed => DownstairsClientStoppedReason::ConnectionFailed,
            internal::nexus::DownstairsClientStoppedReason::Timeout => DownstairsClientStoppedReason::Timeout,
            internal::nexus::DownstairsClientStoppedReason::WriteFailed => DownstairsClientStoppedReason::WriteFailed,
            internal::nexus::DownstairsClientStoppedReason::ReadFailed => DownstairsClientStoppedReason::ReadFailed,
            internal::nexus::DownstairsClientStoppedReason::RequestedStop => DownstairsClientStoppedReason::RequestedStop,
            internal::nexus::DownstairsClientStoppedReason::Finished => DownstairsClientStoppedReason::Finished,
            internal::nexus::DownstairsClientStoppedReason::QueueClosed => DownstairsClientStoppedReason::QueueClosed,
            internal::nexus::DownstairsClientStoppedReason::ReceiveTaskCancelled => DownstairsClientStoppedReason::ReceiveTaskCancelled,
        }
    }
}

/// A Record of when a Downstairs client task stopped
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

    pub reason: DownstairsClientStoppedReason,
}
