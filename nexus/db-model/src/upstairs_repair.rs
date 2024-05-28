// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::ipv6;
use crate::schema::upstairs_repair_notification;
use crate::schema::upstairs_repair_progress;
use crate::typed_uuid::DbTypedUuid;
use crate::SqlU16;
use chrono::{DateTime, Utc};
use omicron_common::api::internal;
use omicron_uuid_kinds::DownstairsRegionKind;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::UpstairsKind;
use omicron_uuid_kinds::UpstairsRepairKind;
use omicron_uuid_kinds::UpstairsSessionKind;
use serde::{Deserialize, Serialize};
use std::net::SocketAddrV6; // internal::nexus::UpstairsRepairType;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "upstairs_repair_notification_type", schema = "public"))]
    pub struct UpstairsRepairNotificationTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = UpstairsRepairNotificationTypeEnum)]
    pub enum UpstairsRepairNotificationType;

    // Notification types
    Started => b"started"
    Succeeded => b"succeeded"
    Failed => b"failed"
);

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "upstairs_repair_type", schema = "public"))]
    pub struct UpstairsRepairTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq, Eq, Hash)]
    #[diesel(sql_type = UpstairsRepairTypeEnum)]
    pub enum UpstairsRepairType;

    // Types of repair a Crucible Upstairs can do
    Live => b"live"
    Reconciliation => b"reconciliation"
);

impl From<internal::nexus::UpstairsRepairType> for UpstairsRepairType {
    fn from(v: internal::nexus::UpstairsRepairType) -> UpstairsRepairType {
        match v {
            internal::nexus::UpstairsRepairType::Live => {
                UpstairsRepairType::Live
            }
            internal::nexus::UpstairsRepairType::Reconciliation => {
                UpstairsRepairType::Reconciliation
            }
        }
    }
}

/// A record of Crucible Upstairs repair notifications: when a repair started,
/// succeeded, failed, etc.
///
/// Each repair attempt is uniquely identified by the repair ID, upstairs ID,
/// session ID, and region ID. How those change tells Nexus about what is going
/// on:
///
/// - if all IDs are the same for different requests, Nexus knows that the
///   client is retrying the notification.
///
/// - if the upstairs ID, session ID, and region ID are all the same, but the
///   repair ID is different, then the same Upstairs is trying to repair that
///   region again. This could be due to a failed first attempt, or that
///   downstairs may have been kicked out again.
///
/// - if the upstairs ID and region ID are the same, but the session ID and
///   repair ID are different, then a different session of the same Upstairs is
///   trying to repair that Downstairs. Session IDs change each time the
///   Upstairs is created, so it could have crashed, or it could have been
///   migrated and the destination Propolis' Upstairs is attempting to repair
///   the same region.
#[derive(Queryable, Insertable, Debug, Clone, Selectable)]
#[diesel(table_name = upstairs_repair_notification)]
pub struct UpstairsRepairNotification {
    // Importantly, this is client time, not Nexus' time that it received the
    // notification.
    pub time: DateTime<Utc>,

    pub repair_id: DbTypedUuid<UpstairsRepairKind>,

    // There's a difference between the live repairs and reconciliation: the
    // Upstairs can go through reconciliation without there being any error from
    // a downstairs, or any region replacement request from Nexus. One example
    // is if the rack power is pulled: if everything is powered back up again
    // reconciliation could be required but this isn't the fault of any problem
    // with a physical disk, or any error that was returned.
    //
    // Alternatively any record of a live repair means that there was a problem:
    // Currently, either an Upstairs kicked out a Downstairs (or two) due to
    // some error or because it lagged behind the others, or Nexus has
    // instructed an Upstairs to perform a region replacement.
    pub repair_type: UpstairsRepairType,

    pub upstairs_id: DbTypedUuid<UpstairsKind>,
    pub session_id: DbTypedUuid<UpstairsSessionKind>,

    // The Downstairs being repaired
    pub region_id: DbTypedUuid<DownstairsRegionKind>,
    pub target_ip: ipv6::Ipv6Addr,
    pub target_port: SqlU16,

    pub notification_type: UpstairsRepairNotificationType,
}

impl UpstairsRepairNotification {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        time: DateTime<Utc>,
        repair_id: TypedUuid<UpstairsRepairKind>,
        repair_type: UpstairsRepairType,
        upstairs_id: TypedUuid<UpstairsKind>,
        session_id: TypedUuid<UpstairsSessionKind>,
        region_id: TypedUuid<DownstairsRegionKind>,
        target_addr: SocketAddrV6,
        notification_type: UpstairsRepairNotificationType,
    ) -> Self {
        Self {
            time,
            repair_id: repair_id.into(),
            repair_type,
            upstairs_id: upstairs_id.into(),
            session_id: session_id.into(),
            region_id: region_id.into(),
            target_ip: target_addr.ip().into(),
            target_port: target_addr.port().into(),
            notification_type,
        }
    }

    pub fn address(&self) -> SocketAddrV6 {
        SocketAddrV6::new(*self.target_ip, *self.target_port, 0, 0)
    }
}

/// A record of Crucible Upstairs repair progress.
#[derive(Queryable, Insertable, Debug, Clone, Selectable)]
#[diesel(table_name = upstairs_repair_progress)]
pub struct UpstairsRepairProgress {
    pub repair_id: DbTypedUuid<UpstairsRepairKind>,
    pub time: DateTime<Utc>,
    pub current_item: i64,
    pub total_items: i64,
}
