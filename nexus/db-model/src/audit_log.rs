// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/5.0/.

// Copyright 2025 Oxide Computer Company

use crate::schema::{audit_log, audit_log_complete};
use crate::SqlU16;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use nexus_types::external_api::views;
use uuid::Uuid;

#[derive(Queryable, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = audit_log)]
pub struct AuditLogEntryInit {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub request_id: String,
    /// The API endpoint being logged, e.g., `project_create`
    pub request_uri: String,
    pub operation_id: String,
    pub source_ip: String,
    // TODO: we probably want a dedicated enum for these columns and for that
    // we need a fancier set of columns. For example, we may want to initialize
    // the row with a _potential_ actor (probably a different field), like the
    // username or whatever is being used for login. This should probably be
    // preserved even after authentication determines an actual actor ID. See
    // the Actor struct in nexus/auth/src/authn/mod.ts

    // these are optional because of requests like login attempts, where there
    // is no actor until after the operation.
    pub actor_id: Option<Uuid>,
    pub actor_silo_id: Option<Uuid>,

    // TODO: fancier type for access method capturing possibility of login
    // attempts. might make sense to roll this all into the actor enum because
    // we have an access method if and only if we have an actor (I think)
    /// API token or session cookie. Optional because it will not be defined
    /// on unauthenticated requests like login attempts.
    pub access_method: Option<String>,
}

// TODO: doc comments
// TODO: figure out how this relates to the other struct. currently we're not
// retrieving partial entries at all, but I think we will probably want to have
// that capability
#[derive(Queryable, Selectable, Clone, Debug)]
#[diesel(table_name = audit_log_complete)]
pub struct AuditLogEntry {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub request_id: String,
    pub request_uri: String,
    pub operation_id: String,
    pub source_ip: String,
    pub actor_id: Option<Uuid>,
    pub actor_silo_id: Option<Uuid>,
    pub access_method: Option<String>,

    // TODO: RFD 523 says: "Additionally, the response (or error) data should be
    // included in the same log entry as the original request data. Separating
    // the response from the request into two different log entries is extremely
    // expensive for customers to identify which requests correspond to which
    // responses." I guess the typical thing is to include a duration of the
    // request rather than a second timestamp.

    // Seems like it has to be optional because at the beginning of the
    // operation, we have not yet resolved the resource selector to an ID
    pub resource_id: Option<Uuid>,

    // Fields that are not present on init
    /// Time log entry was completed with info about result of operation
    pub time_completed: DateTime<Utc>,
    pub http_status_code: SqlU16,

    // Error information if the action failed
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    // TODO: including a real response complicates things
    // Response data on success (if applicable)
    // pub success_response: Option<Value>,
}

impl AuditLogEntryInit {
    pub fn new(
        request_id: String,
        operation_id: String,
        request_uri: String,
        source_ip: String,
        actor_id: Option<Uuid>,
        actor_silo_id: Option<Uuid>,
        access_method: Option<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            request_id,
            request_uri,
            operation_id,
            actor_id,
            actor_silo_id,
            source_ip,
            access_method,
        }
    }
}

#[derive(AsChangeset, Clone)]
#[diesel(table_name = audit_log)]
pub struct AuditLogCompletion {
    pub time_completed: DateTime<Utc>,
    pub http_status_code: SqlU16,
}

impl AuditLogCompletion {
    pub fn new(http_status_code: u16) -> Self {
        Self {
            time_completed: Utc::now(),
            http_status_code: SqlU16(http_status_code),
        }
    }
}

// TODO: AuditLogActor
// pub enum AuditLogActor {
//     UserBuiltin { user_builtin_id: Uuid },
//     TODO: include info about computed roles at runtime?
//     SiloUser { silo_user_id: Uuid, silo_id: Uuid },
//     Unauthenticated,
// }

impl From<AuditLogEntry> for views::AuditLogEntry {
    fn from(entry: AuditLogEntry) -> Self {
        Self {
            id: entry.id,
            timestamp: entry.timestamp,
            request_id: entry.request_id,
            request_uri: entry.request_uri,
            operation_id: entry.operation_id,
            source_ip: entry.source_ip,
            resource_id: entry.resource_id,
            actor_id: entry.actor_id,
            actor_silo_id: entry.actor_silo_id,
            access_method: entry.access_method,
            time_completed: entry.time_completed,
            http_status_code: entry.http_status_code.0,
            error_code: entry.error_code,
            error_message: entry.error_message,
        }
    }
}
