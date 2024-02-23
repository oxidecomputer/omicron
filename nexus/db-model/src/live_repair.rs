// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::ipv6;
use crate::schema::live_repair_notification;
use crate::typed_uuid::DbTypedUuid;
use crate::SqlU16;
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::DownstairsRegionKind;
use omicron_uuid_kinds::LiveRepairKind;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::UpstairsKind;
use omicron_uuid_kinds::UpstairsSessionKind;
use serde::{Deserialize, Serialize};
use std::net::SocketAddrV6;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "live_repair_notification_type", schema = "public"))]
    pub struct LiveRepairNotificationTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = LiveRepairNotificationTypeEnum)]
    pub enum LiveRepairNotificationType;

    // Notification types
    Started => b"started"
    Succeeded => b"succeeded"
    Failed => b"failed"
);

/// A record of Crucible live repair notifications: when a live repair started,
/// succeeded, failed, etc.
///
/// Each live repair attempt is uniquely identified by the repair ID, upstairs
/// ID, session ID, and region ID. How those change tells Nexus about what is
/// going on:
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
#[diesel(table_name = live_repair_notification)]
pub struct LiveRepairNotification {
    pub time: DateTime<Utc>,

    pub repair_id: DbTypedUuid<LiveRepairKind>,
    pub upstairs_id: DbTypedUuid<UpstairsKind>,
    pub session_id: DbTypedUuid<UpstairsSessionKind>,

    pub region_id: DbTypedUuid<DownstairsRegionKind>,
    pub target_ip: ipv6::Ipv6Addr,
    pub target_port: SqlU16,

    pub notification_type: LiveRepairNotificationType,
}

impl LiveRepairNotification {
    pub fn new(
        repair_id: TypedUuid<LiveRepairKind>,
        upstairs_id: TypedUuid<UpstairsKind>,
        session_id: TypedUuid<UpstairsSessionKind>,
        region_id: TypedUuid<DownstairsRegionKind>,
        target_addr: SocketAddrV6,
        notification_type: LiveRepairNotificationType,
    ) -> Self {
        Self {
            time: Utc::now(),
            repair_id: repair_id.into(),
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
