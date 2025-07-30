// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/5.0/.

// Copyright 2025 Oxide Computer Company

use std::net::IpAddr;

use crate::SqlU16;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::{audit_log, audit_log_complete};
use nexus_types::external_api::views;
use uuid::Uuid;

#[derive(Queryable, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = audit_log)]
pub struct AuditLogEntryInit {
    pub id: Uuid,
    /// Time operation started and audit log entry was initialized
    pub time_started: DateTime<Utc>,
    pub request_id: String,
    /// The API endpoint being logged, e.g., `project_create`
    pub request_uri: String,
    pub operation_id: String,
    pub source_ip: IpNetwork,
    pub user_agent: Option<String>,

    // TODO: For login attempts, we may want to initialize the row with
    // a potential actor so we can tell which account is being targeted by failed attempts. For password login, we should have the username on hand to log. For SAML, it's
    // less clear whether this makes sense because we only get those requests
    // from the IdP after a successful login on their end, and they're
    // cryptographically signed. So maybe this only applies to password login.

    // these are optional because of requests like login attempts, where there
    // is no actor until after the operation.
    pub actor_id: Option<Uuid>,
    pub actor_silo_id: Option<Uuid>,

    /// API token or session cookie. Optional because it will not be defined
    /// on unauthenticated requests like login attempts.
    pub access_method: Option<String>,
}

/// `audit_log_complete` is a view on `audit_log` filtering for rows with
/// non-null `time_completed`, not its own table.
#[derive(Queryable, Selectable, Clone, Debug)]
#[diesel(table_name = audit_log_complete)]
pub struct AuditLogEntry {
    pub id: Uuid,
    pub time_started: DateTime<Utc>,
    pub request_id: String,
    pub request_uri: String,
    pub operation_id: String,
    pub source_ip: IpNetwork,
    pub user_agent: Option<String>,
    pub actor_id: Option<Uuid>,
    pub actor_silo_id: Option<Uuid>,

    /// The name of the authn scheme used. None if unauthenticated.
    pub access_method: Option<String>,

    // Fields that are not present on init
    /// Time log entry was completed with info about result of operation
    pub time_completed: DateTime<Utc>,
    pub http_status_code: SqlU16,

    // Error information if the action failed
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl AuditLogEntryInit {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: String,
        operation_id: String,
        request_uri: String,
        source_ip: IpAddr,
        user_agent: Option<String>,
        actor_id: Option<Uuid>,
        actor_silo_id: Option<Uuid>,
        access_method: Option<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            time_started: Utc::now(),
            request_id,
            request_uri,
            operation_id,
            actor_id,
            actor_silo_id,
            source_ip: source_ip.into(),
            user_agent,
            access_method,
        }
    }
}

#[derive(AsChangeset, Clone)]
#[diesel(table_name = audit_log)]
pub struct AuditLogCompletion {
    pub time_completed: DateTime<Utc>,
    pub http_status_code: SqlU16,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl AuditLogCompletion {
    pub fn new(
        http_status_code: u16,
        error_code: Option<String>,
        error_message: Option<String>,
    ) -> Self {
        Self {
            time_completed: Utc::now(),
            http_status_code: SqlU16(http_status_code),
            error_code,
            error_message,
        }
    }
}

impl From<AuditLogEntry> for views::AuditLogEntry {
    fn from(entry: AuditLogEntry) -> Self {
        Self {
            id: entry.id,
            time_started: entry.time_started,
            request_id: entry.request_id,
            request_uri: entry.request_uri,
            operation_id: entry.operation_id,
            source_ip: entry.source_ip.ip(),
            user_agent: entry.user_agent,
            // TODO: make robust by writing down actor type at DB write time
            // rather than assuming it based on the presence or absence of user
            // and silo IDs
            actor: match (entry.actor_id, entry.actor_silo_id) {
                (Some(silo_user_id), Some(silo_id)) => {
                    views::AuditLogEntryActor::SiloUser {
                        silo_user_id,
                        silo_id,
                    }
                }
                (Some(user_builtin_id), None) => {
                    views::AuditLogEntryActor::UserBuiltin { user_builtin_id }
                }
                (None, None) => views::AuditLogEntryActor::Unauthenticated,
                (None, Some(_)) => {
                    unreachable!("Can't have a silo ID without an actor ID");
                }
            },
            access_method: entry.access_method,
            time_completed: entry.time_completed,
            http_status_code: entry.http_status_code.0,
            error_code: entry.error_code,
            error_message: entry.error_message,
        }
    }
}
