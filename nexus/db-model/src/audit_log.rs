// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/5.0/.

// Copyright 2025 Oxide Computer Company

use std::net::IpAddr;

use crate::{SqlU16, impl_enum_type};
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::{audit_log, audit_log_complete};
use nexus_types::external_api::views;
use omicron_common::api::external::Error;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Actor information for audit log initialization. Inspired by `authn::Actor`
#[derive(Clone, Debug)]
pub enum AuditLogActor {
    UserBuiltin { user_builtin_id: Uuid },
    SiloUser { silo_user_id: Uuid, silo_id: Uuid },
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
    pub access_method: Option<String>,
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
    pub access_method: Option<String>,
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
            access_method,
        } = params;

        let (actor_id, actor_silo_id, actor_kind) = match actor {
            AuditLogActor::UserBuiltin { user_builtin_id } => {
                (Some(user_builtin_id), None, AuditLogActorKind::UserBuiltin)
            }
            AuditLogActor::SiloUser { silo_user_id, silo_id } => {
                (Some(silo_user_id), Some(silo_id), AuditLogActorKind::SiloUser)
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
            access_method,
        }
    }
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
    /// Actor kind indicating builtin user, silo user, or unauthenticated
    pub actor_kind: AuditLogActorKind,

    /// The name of the authn scheme used. None if unauthenticated.
    pub access_method: Option<String>,

    // Fields that are not present on init
    /// Time log entry was completed with info about result of operation
    pub time_completed: DateTime<Utc>,
    /// Result kind indicating success, error, or timeout
    pub result_kind: AuditLogResultKind,
    /// Optional because not present for timeout result
    pub http_status_code: Option<SqlU16>,
    /// Optional even if result is an error
    pub error_code: Option<String>,
    /// Always present if result is an error
    pub error_message: Option<String>,
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
    /// to be cleaned up later by a background job (which doesn't exist yet)
    /// after a timeout. Note we represent this result status as "Unknown" in
    /// the external API because timeout is an implementation detail and makes
    /// it sound like the operation timed out.
    Timeout,
}

#[derive(AsChangeset, Clone)]
#[diesel(table_name = audit_log)]
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
impl TryFrom<AuditLogEntry> for views::AuditLogEntry {
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
                AuditLogActorKind::UserBuiltin => {
                    let user_builtin_id = entry.actor_id.ok_or_else(|| {
                        Error::internal_error(
                            "UserBuiltin actor missing actor_id",
                        )
                    })?;
                    views::AuditLogEntryActor::UserBuiltin { user_builtin_id }
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
                    views::AuditLogEntryActor::SiloUser {
                        silo_user_id,
                        silo_id,
                    }
                }
                AuditLogActorKind::Unauthenticated => {
                    views::AuditLogEntryActor::Unauthenticated
                }
            },
            access_method: entry.access_method,
            time_completed: entry.time_completed,
            result: match entry.result_kind {
                AuditLogResultKind::Success => {
                    let http_status_code = entry.http_status_code
                        .ok_or_else(|| Error::internal_error(
                            "Audit log success result without http_status_code",
                        ))?;
                    views::AuditLogEntryResult::Success {
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
                    views::AuditLogEntryResult::Error {
                        http_status_code: http_status_code.0,
                        error_code: entry.error_code,
                        error_message,
                    }
                }
                AuditLogResultKind::Timeout => {
                    views::AuditLogEntryResult::Unknown
                }
            },
        })
    }
}
