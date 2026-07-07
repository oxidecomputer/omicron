// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Audit log types for version INITIAL.

use chrono::{DateTime, Utc};
use omicron_uuid_kinds::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuditLogEntryActor {
    UserBuiltin {
        #[schemars(with = "Uuid")]
        user_builtin_id: BuiltInUserUuid,
    },

    SiloUser {
        #[schemars(with = "Uuid")]
        silo_user_id: SiloUserUuid,

        silo_id: Uuid,
    },

    Scim {
        silo_id: Uuid,
    },

    Unauthenticated,
}

/// Result of an audit log entry
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuditLogEntryResult {
    /// The operation completed successfully
    Success {
        /// HTTP status code
        http_status_code: u16,
    },
    /// The operation failed
    Error {
        /// HTTP status code
        http_status_code: u16,
        error_code: Option<String>,
        error_message: String,
    },
    // Note that the DB model result kind analogous to Unknown is called Timeout
    // -- The name "Timeout" feels useful to write down for the DB but also
    // feels like too much of an implementation detail to expose to the user --
    // it makes it sounds like the operation timed out rather than the audit log
    // entry itself.
    /// After the logged operation completed, our attempt to write the result
    /// to the audit log failed, so it was automatically marked completed later
    /// by a background job. This does not imply that the operation itself timed
    /// out or failed, only our attempts to log its result.
    Unknown,
}

/// Audit log entry
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct AuditLogEntry {
    /// Unique identifier for the audit log entry
    pub id: Uuid,

    /// When the request was received
    pub time_started: DateTime<Utc>,

    /// Request ID for tracing requests through the system
    pub request_id: String,
    /// URI of the request, truncated to 512 characters. Will only include host
    /// and scheme for HTTP/2 requests. For HTTP/1.1, the URI will consist of
    /// only the path and query.
    pub request_uri: String,
    /// API endpoint ID, e.g., `project_create`
    pub operation_id: String,
    /// IP address that made the request
    pub source_ip: IpAddr,
    /// User agent string from the request, truncated to 256 characters.
    pub user_agent: Option<String>,

    pub actor: AuditLogEntryActor,

    /// How the user authenticated the request. Possible values are
    /// "session_cookie" and "access_token". Optional because it will not be
    /// defined on unauthenticated requests like login attempts.
    pub auth_method: Option<String>,

    // Fields that are optional because they get filled in after the action completes
    /// Time operation completed
    pub time_completed: DateTime<Utc>,

    /// Result of the operation
    pub result: AuditLogEntryResult,
}

// AUDIT PARAMS

/// Audit log has its own pagination scheme because it paginates by timestamp.
#[derive(Deserialize, JsonSchema, Serialize, PartialEq, Debug, Clone)]
pub struct AuditLogParams {
    /// Required, inclusive
    pub start_time: DateTime<Utc>,
    /// Exclusive
    pub end_time: Option<DateTime<Utc>>,
}
