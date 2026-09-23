// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

use crate::v2025_11_20_00::audit::AuditLogEntryResult;
use omicron_uuid_kinds::{BuiltInUserUuid, SiloUserUuid};

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
    Federated {
        session_id: Uuid,
        silo_id: Uuid,
    },
    Unauthenticated,
}

#[derive(
    Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    SessionCookie,
    AccessToken,
    ScimToken,
    FederationToken,
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

impl TryFrom<AuditLogEntry> for crate::v2026_01_15_01::audit::AuditLogEntry {
    type Error = dropshot::HttpError;

    fn try_from(new: AuditLogEntry) -> Result<Self, Self::Error> {
        let unsupported = || {
            dropshot::HttpError::for_client_error(
            Some("Not Acceptable".to_owned()),
            dropshot::ClientErrorStatusCode::NOT_ACCEPTABLE,
            "federation audit entries are not supported for this client version".to_owned(),
        )
        };
        use crate::v2025_11_20_00::audit::AuditLogEntryActor as OldActor;
        use crate::v2026_01_15_00::audit::AuthMethod as OldMethod;
        let actor = match new.actor {
            AuditLogEntryActor::UserBuiltin { user_builtin_id } => {
                OldActor::UserBuiltin { user_builtin_id }
            }
            AuditLogEntryActor::SiloUser { silo_user_id, silo_id } => {
                OldActor::SiloUser { silo_user_id, silo_id }
            }
            AuditLogEntryActor::Scim { silo_id } => OldActor::Scim { silo_id },
            AuditLogEntryActor::Unauthenticated => OldActor::Unauthenticated,
            AuditLogEntryActor::Federated { .. } => return Err(unsupported()),
        };
        let auth_method = new
            .auth_method
            .map(|method| {
                Ok(match method {
                    AuthMethod::SessionCookie => OldMethod::SessionCookie,
                    AuthMethod::AccessToken => OldMethod::AccessToken,
                    AuthMethod::ScimToken => OldMethod::ScimToken,
                    AuthMethod::Spoof => OldMethod::Spoof,
                    AuthMethod::FederationToken => return Err(unsupported()),
                })
            })
            .transpose()?;
        Ok(Self {
            id: new.id,
            time_started: new.time_started,
            request_id: new.request_id,
            request_uri: new.request_uri,
            operation_id: new.operation_id,
            source_ip: new.source_ip,
            user_agent: new.user_agent,
            actor,
            auth_method,
            credential_id: new.credential_id,
            time_completed: new.time_completed,
            result: new.result,
        })
    }
}
