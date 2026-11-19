// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Audit log types for version SERVICE_ACCOUNTS.

use chrono::{DateTime, Utc};
use omicron_uuid_kinds::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::audit::AuditLogEntryResult;
use crate::v2026_01_15_00;
use crate::v2026_01_15_01;

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

    ServiceAccount {
        service_account_id: Uuid,

        silo_id: Uuid,
    },

    Unauthenticated,
}

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
    /// Service account key (RFC 9421 HTTP message signature)
    RequestSignature,
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

    /// How the user authenticated the request (access token, session, SCIM
    /// token, or service account key). Null for unauthenticated requests like
    /// login attempts.
    pub auth_method: Option<AuthMethod>,

    /// ID of the credential used for authentication. Null for unauthenticated
    /// requests. The value of `auth_method` indicates what kind of credential
    /// it is (access token, session, SCIM token, or service account key).
    pub credential_id: Option<Uuid>,

    /// Time operation completed
    pub time_completed: DateTime<Utc>,

    /// Result of the operation
    pub result: AuditLogEntryResult,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "this audit log contains service account activity, which this API \
     version cannot represent"
)]
pub struct ServiceAccountAuditNotRepresentable;

impl TryFrom<AuditLogEntryActor> for v2025_11_20_00::audit::AuditLogEntryActor {
    type Error = ServiceAccountAuditNotRepresentable;

    fn try_from(new: AuditLogEntryActor) -> Result<Self, Self::Error> {
        Ok(match new {
            AuditLogEntryActor::UserBuiltin { user_builtin_id } => {
                Self::UserBuiltin { user_builtin_id }
            }
            AuditLogEntryActor::SiloUser { silo_user_id, silo_id } => {
                Self::SiloUser { silo_user_id, silo_id }
            }
            AuditLogEntryActor::Scim { silo_id } => Self::Scim { silo_id },
            AuditLogEntryActor::Unauthenticated => Self::Unauthenticated,
            AuditLogEntryActor::ServiceAccount { .. } => {
                return Err(ServiceAccountAuditNotRepresentable);
            }
        })
    }
}

impl TryFrom<AuthMethod> for v2026_01_15_00::audit::AuthMethod {
    type Error = ServiceAccountAuditNotRepresentable;

    fn try_from(new: AuthMethod) -> Result<Self, Self::Error> {
        Ok(match new {
            AuthMethod::SessionCookie => Self::SessionCookie,
            AuthMethod::AccessToken => Self::AccessToken,
            AuthMethod::ScimToken => Self::ScimToken,
            AuthMethod::Spoof => Self::Spoof,
            AuthMethod::RequestSignature => {
                return Err(ServiceAccountAuditNotRepresentable);
            }
        })
    }
}

impl TryFrom<AuditLogEntry> for v2026_01_15_01::audit::AuditLogEntry {
    type Error = ServiceAccountAuditNotRepresentable;

    fn try_from(new: AuditLogEntry) -> Result<Self, Self::Error> {
        Ok(Self {
            id: new.id,
            time_started: new.time_started,
            request_id: new.request_id,
            request_uri: new.request_uri,
            operation_id: new.operation_id,
            source_ip: new.source_ip,
            user_agent: new.user_agent,
            actor: new.actor.try_into()?,
            auth_method: new.auth_method.map(TryInto::try_into).transpose()?,
            credential_id: new.credential_id,
            time_completed: new.time_completed,
            result: new.result,
        })
    }
}
