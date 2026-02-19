// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Audit log types for version AUDIT_LOG_CREDENTIAL_ID.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

use crate::v2025_11_20_00::audit::{AuditLogEntryActor, AuditLogEntryResult};

/// Authentication method used for a request
#[derive(
    Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    /// Console session cookie
    SessionCookie,
    /// Device access token (OAuth 2.0 device authorization flow)
    AccessToken,
    /// SCIM client bearer token
    ScimToken,
    /// Spoof authentication (test only)
    #[schemars(skip)]
    Spoof,
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
    /// URI of the request, truncated to 512 characters. Will only include
    /// host and scheme for HTTP/2 requests. For HTTP/1.1, the URI will
    /// consist of only the path and query.
    pub request_uri: String,
    /// API endpoint ID, e.g., `project_create`
    pub operation_id: String,
    /// IP address that made the request
    pub source_ip: IpAddr,
    /// User agent string from the request, truncated to 256 characters.
    pub user_agent: Option<String>,

    pub actor: AuditLogEntryActor,

    /// How the user authenticated the request (access token, session, or SCIM
    /// token). Null for unauthenticated requests like login attempts.
    pub auth_method: Option<AuthMethod>,

    /// ID of the credential used for authentication. Null for unauthenticated
    /// requests. The value of `auth_method` indicates what kind of credential
    /// it is (access token, session, or SCIM token).
    pub credential_id: Option<Uuid>,

    /// Time operation completed
    pub time_completed: DateTime<Utc>,

    /// Result of the operation
    pub result: AuditLogEntryResult,
}
