// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::IpAddr;

use crate::{SqlU16, impl_enum_type};
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::{audit_log, audit_log_complete};
use nexus_types::external_api::audit;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::BuiltInUserUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloUserUuid;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Actor information for audit log initialization. Inspired by `authn::Actor`
#[derive(Clone, Debug)]
pub enum AuditLogActor {
    UserBuiltin {
        user_builtin_id: BuiltInUserUuid,
    },
    SiloUser {
        silo_user_id: SiloUserUuid,
        silo_id: Uuid,
    },
    Scim {
        silo_id: Uuid,
    },
    ServiceAccount {
        service_account_id: Uuid,
        silo_id: Uuid,
        federation: Option<audit::FederationIdentity>,
    },
    Unauthenticated,
}

/// Structured params for initializing an audit log entry. See
/// `AuditLogEntryInitRow` for the flat struct we use for DB inserts.
#[derive(Clone, Debug)]
pub struct AuditLogEntryInitParams {
    pub request_id: String,
    pub operation_id: String,
    pub request_uri: String,
    pub source_ip: IpAddr,
    pub user_agent: Option<String>,
    pub actor: AuditLogActor,
    pub auth_method: Option<AuditLogAuthMethod>,
    /// ID of the credential used to authenticate (session ID, access token ID,
    /// or SCIM token ID). Not set for unauthenticated requests or spoof auth.
    pub credential_id: Option<Uuid>,
}

impl_enum_type!(
    AuditLogActorKindEnum:

    #[derive(
        Clone,
        Copy,
        Debug,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        PartialEq,
        Eq,
    )]
    pub enum AuditLogActorKind;

    // Enum values
    UserBuiltin => b"user_builtin"
    SiloUser => b"silo_user"
    Unauthenticated => b"unauthenticated"
    Scim => b"scim"
    ServiceAccount => b"service_account"
);

impl_enum_type!(
    AuditLogResultKindEnum:

    #[derive(
        Clone,
        Copy,
        Debug,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        PartialEq,
        Eq,
    )]
    pub enum AuditLogResultKind;

    // Enum values
    Success => b"success"
    Error => b"error"
    Timeout => b"timeout"
);

impl_enum_type!(
    AuditLogAuthMethodEnum:

    #[derive(
        Clone,
        Copy,
        Debug,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        PartialEq,
        Eq,
    )]
    pub enum AuditLogAuthMethod;

    // Enum values
    SessionCookie => b"session_cookie"
    AccessToken => b"access_token"
    ScimToken => b"scim_token"
    ServiceAccountToken => b"service_account_token"
    Spoof => b"spoof"
);

impl From<AuditLogAuthMethod> for audit::AuthMethod {
    fn from(m: AuditLogAuthMethod) -> Self {
        match m {
            AuditLogAuthMethod::SessionCookie => {
                audit::AuthMethod::SessionCookie
            }
            AuditLogAuthMethod::AccessToken => audit::AuthMethod::AccessToken,
            AuditLogAuthMethod::ScimToken => audit::AuthMethod::ScimToken,
            AuditLogAuthMethod::ServiceAccountToken => {
                audit::AuthMethod::ServiceAccountToken
            }
            AuditLogAuthMethod::Spoof => audit::AuthMethod::Spoof,
        }
    }
}

impl From<&nexus_types::authn::SchemeName> for AuditLogAuthMethod {
    fn from(s: &nexus_types::authn::SchemeName) -> Self {
        use nexus_types::authn::SchemeName;
        match s {
            SchemeName::SessionCookie => AuditLogAuthMethod::SessionCookie,
            SchemeName::AccessToken => AuditLogAuthMethod::AccessToken,
            SchemeName::ScimToken => AuditLogAuthMethod::ScimToken,
            SchemeName::ServiceAccountToken => {
                AuditLogAuthMethod::ServiceAccountToken
            }
            SchemeName::Spoof => AuditLogAuthMethod::Spoof,
        }
    }
}

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

    // TODO: For login attempts, we may want to initialize the row with a
    // potential actor so we can tell which account is being targeted by failed
    // attempts. For password login, we should have the username on hand to log.
    // For SAML, it's less clear whether this makes sense because we only get
    // those requests from the IdP after a successful login on their end, and
    // they're cryptographically signed. So maybe this only applies to password
    // login.

    // see AuditLogActor for the allowed combinations
    /// Actor kind indicating builtin user, silo user, or unauthenticated
    pub actor_kind: AuditLogActorKind,
    pub actor_id: Option<Uuid>,
    pub actor_silo_id: Option<Uuid>,

    /// API token or session cookie. Optional because it will not be defined
    /// on unauthenticated requests like login attempts.
    pub auth_method: Option<AuditLogAuthMethod>,

    /// ID of the credential used to authenticate (session ID, access token ID,
    /// or SCIM token ID). Not set for unauthenticated requests or spoof auth.
    pub credential_id: Option<Uuid>,
    pub federation_idp_id: Option<Uuid>,
    pub federation_iss: Option<String>,
    pub federation_sub: Option<String>,
}

impl From<AuditLogEntryInitParams> for AuditLogEntryInit {
    fn from(params: AuditLogEntryInitParams) -> Self {
        let AuditLogEntryInitParams {
            request_id,
            operation_id,
            request_uri,
            source_ip,
            user_agent,
            actor,
            auth_method,
            credential_id,
        } = params;

        let (federation_idp_id, federation_iss, federation_sub) = match &actor {
            AuditLogActor::ServiceAccount {
                federation: Some(identity),
                ..
            } => (
                Some(identity.idp_id),
                Some(identity.iss.clone()),
                Some(identity.sub.clone()),
            ),
            _ => (None, None, None),
        };
        let (actor_id, actor_silo_id, actor_kind) = match actor {
            AuditLogActor::ServiceAccount {
                service_account_id,
                silo_id,
                ..
            } => (
                Some(service_account_id),
                Some(silo_id),
                AuditLogActorKind::ServiceAccount,
            ),
            AuditLogActor::UserBuiltin { user_builtin_id } => (
                Some(user_builtin_id.into_untyped_uuid()),
                None,
                AuditLogActorKind::UserBuiltin,
            ),
            AuditLogActor::SiloUser { silo_user_id, silo_id } => (
                Some(silo_user_id.into_untyped_uuid()),
                Some(silo_id),
                AuditLogActorKind::SiloUser,
            ),
            AuditLogActor::Scim { silo_id } => {
                (None, Some(silo_id), AuditLogActorKind::Scim)
            }
            AuditLogActor::Unauthenticated => {
                (None, None, AuditLogActorKind::Unauthenticated)
            }
        };

        Self {
            id: Uuid::new_v4(),
            time_started: Utc::now(),
            request_id,
            request_uri,
            operation_id,
            actor_id,
            actor_silo_id,
            actor_kind,
            source_ip: source_ip.into(),
            user_agent,
            auth_method,
            credential_id,
            federation_idp_id,
            federation_iss,
            federation_sub,
        }
    }
}

/// `audit_log_complete` is a view on `audit_log` filtering for rows with
/// non-null `time_completed`, not its own table.
#[derive(Queryable, Selectable, Clone, Debug, PartialEq)]
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
    /// Actor kind indicating builtin user, silo user, or unauthenticated
    pub actor_kind: AuditLogActorKind,

    // Fields that are not present on init
    /// Time log entry was completed with info about result of operation
    pub time_completed: DateTime<Utc>,
    /// Optional because not present for timeout result
    pub http_status_code: Option<SqlU16>,
    /// Optional even if result is an error
    pub error_code: Option<String>,
    /// Always present if result is an error
    pub error_message: Option<String>,
    /// Result kind indicating success, error, or timeout
    pub result_kind: AuditLogResultKind,

    /// The authn scheme used. None if unauthenticated.
    pub auth_method: Option<AuditLogAuthMethod>,

    /// ID of the credential used to authenticate (session ID, access token ID,
    /// or SCIM token ID). Not set for unauthenticated requests or spoof auth.
    pub credential_id: Option<Uuid>,
    pub federation_idp_id: Option<Uuid>,
    pub federation_iss: Option<String>,
    pub federation_sub: Option<String>,
}

/// Struct that we can use as a kind of constructor arg for our actual audit
/// log row update struct in order to make sure we're always writing a valid
/// combination of column values
#[derive(Clone)]
pub enum AuditLogCompletion {
    Success {
        http_status_code: u16,
    },
    Error {
        http_status_code: u16,
        error_code: Option<String>,
        error_message: String,
    },
    /// This doesn't mean the operation itself timed out (which would be an
    /// error, and I don't think we even have API timeouts) but rather that the
    /// attempts to complete the log entry failed (or were never even attempted
    /// because, e.g., Nexus crashed during the operation), and this entry had
    /// to be cleaned up later by a background job after a timeout. Note we
    /// represent this result status as "Unknown" in the external API because
    /// timeout is an implementation detail and makes it sound like the
    /// operation timed out.
    Timeout,
}

#[derive(AsChangeset, Clone)]
#[diesel(table_name = audit_log, treat_none_as_null = true)]
pub struct AuditLogCompletionUpdate {
    pub time_completed: DateTime<Utc>,
    pub result_kind: AuditLogResultKind,
    pub http_status_code: Option<SqlU16>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl From<AuditLogCompletion> for AuditLogCompletionUpdate {
    fn from(completion: AuditLogCompletion) -> Self {
        let time_completed = Utc::now();
        match completion {
            AuditLogCompletion::Success { http_status_code } => Self {
                time_completed,
                result_kind: AuditLogResultKind::Success,
                http_status_code: Some(SqlU16(http_status_code)),
                error_code: None,
                error_message: None,
            },
            AuditLogCompletion::Error {
                http_status_code,
                error_code,
                error_message,
            } => Self {
                time_completed,
                result_kind: AuditLogResultKind::Error,
                http_status_code: Some(SqlU16(http_status_code)),
                error_code,
                error_message: Some(error_message),
            },
            AuditLogCompletion::Timeout => Self {
                time_completed,
                result_kind: AuditLogResultKind::Timeout,
                http_status_code: None,
                error_code: None,
                error_message: None,
            },
        }
    }
}

/// None of the error cases here should be possible given the DB constraints and
/// the way we construct these rows when writing them to the database.
impl TryFrom<AuditLogEntry> for audit::AuditLogEntry {
    type Error = Error;

    fn try_from(entry: AuditLogEntry) -> Result<Self, Self::Error> {
        Ok(Self {
            id: entry.id,
            time_started: entry.time_started,
            request_id: entry.request_id,
            request_uri: entry.request_uri,
            operation_id: entry.operation_id,
            source_ip: entry.source_ip.ip(),
            user_agent: entry.user_agent,
            actor: match entry.actor_kind {
                AuditLogActorKind::ServiceAccount => {
                    audit::AuditLogEntryActor::ServiceAccount {
                        service_account_id: entry.actor_id.ok_or_else(
                            || {
                                Error::internal_error(
                                    "Service account actor missing actor_id",
                                )
                            },
                        )?,
                        silo_id: entry.actor_silo_id.ok_or_else(|| {
                            Error::internal_error(
                                "Service account actor missing actor_silo_id",
                            )
                        })?,
                        federation: match (
                            entry.federation_idp_id,
                            entry.federation_iss,
                            entry.federation_sub,
                        ) {
                            (None, None, None) => None,
                            (Some(idp_id), Some(iss), Some(sub)) => {
                                Some(audit::FederationIdentity {
                                    idp_id,
                                    iss,
                                    sub,
                                })
                            }
                            _ => {
                                return Err(Error::internal_error(
                                    "incomplete federation audit identity",
                                ));
                            }
                        },
                    }
                }
                AuditLogActorKind::UserBuiltin => {
                    let user_builtin_id = entry.actor_id.ok_or_else(|| {
                        Error::internal_error(
                            "UserBuiltin actor missing actor_id",
                        )
                    })?;
                    audit::AuditLogEntryActor::UserBuiltin {
                        user_builtin_id: BuiltInUserUuid::from_untyped_uuid(
                            user_builtin_id,
                        ),
                    }
                }
                AuditLogActorKind::SiloUser => {
                    let silo_user_id = entry.actor_id.ok_or_else(|| {
                        Error::internal_error("SiloUser actor missing actor_id")
                    })?;
                    let silo_id = entry.actor_silo_id.ok_or_else(|| {
                        Error::internal_error(
                            "SiloUser actor missing actor_silo_id",
                        )
                    })?;
                    audit::AuditLogEntryActor::SiloUser {
                        silo_user_id: SiloUserUuid::from_untyped_uuid(
                            silo_user_id,
                        ),
                        silo_id,
                    }
                }
                AuditLogActorKind::Scim => {
                    let silo_id = entry.actor_silo_id.ok_or_else(|| {
                        Error::internal_error(
                            "Scim actor missing actor_silo_id",
                        )
                    })?;
                    audit::AuditLogEntryActor::Scim { silo_id }
                }
                AuditLogActorKind::Unauthenticated => {
                    audit::AuditLogEntryActor::Unauthenticated
                }
            },
            auth_method: entry.auth_method.map(Into::into),
            time_completed: entry.time_completed,
            result: match entry.result_kind {
                AuditLogResultKind::Success => {
                    let http_status_code = entry.http_status_code
                        .ok_or_else(|| Error::internal_error(
                            "Audit log success result without http_status_code",
                        ))?;
                    audit::AuditLogEntryResult::Success {
                        http_status_code: http_status_code.0,
                    }
                }
                AuditLogResultKind::Error => {
                    let error_message =
                        entry.error_message.ok_or_else(|| {
                            Error::internal_error(
                                "Audit log error result without error_message",
                            )
                        })?;
                    let http_status_code = entry.http_status_code
                        .ok_or_else(|| Error::internal_error(
                            "Audit log error result without http_status_code",
                        ))?;
                    audit::AuditLogEntryResult::Error {
                        http_status_code: http_status_code.0,
                        error_code: entry.error_code,
                        error_message,
                    }
                }
                AuditLogResultKind::Timeout => {
                    audit::AuditLogEntryResult::Unknown
                }
            },
            credential_id: entry.credential_id,
        })
    }
}
