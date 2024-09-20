// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used for sagas
//!
//! Just like elsewhere, we run into Rust's orphan rules here.  There are types
//! in Steno that we want to put into the database, but we can't impl
//! `ToSql`/`FromSql` directly on them because they're in different crates.  We
//! could create wrapper types and impl `ToSql`/`FromSql` on those.  Instead, we
//! use the Steno types directly in our own types, and the handful of places that
//! actually serialize them to and from SQL take care of the necessary
//! conversions.

use super::schema::{saga, saga_node_event};
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid};
use std::convert::TryFrom;
use std::io::Write;
use std::sync::Arc;
use uuid::Uuid;

/// Unique identifier for an SEC (saga execution coordinator) instance
///
/// For us, these will generally be Nexus instances, and the SEC id will match
/// the Nexus id.
#[derive(
    AsExpression, FromSqlRow, Clone, Copy, Eq, Ord, PartialEq, PartialOrd,
)]
#[diesel(sql_type = sql_types::Uuid)]
pub struct SecId(pub Uuid);

impl<DB> ToSql<sql_types::Uuid, DB> for SecId
where
    DB: Backend,
    Uuid: ToSql<sql_types::Uuid, DB>,
{
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, DB>,
    ) -> serialize::Result {
        (&self.0 as &Uuid).to_sql(out)
    }
}

impl<DB> FromSql<sql_types::Uuid, DB> for SecId
where
    DB: Backend,
    Uuid: FromSql<sql_types::Uuid, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        let id = Uuid::from_sql(bytes)?;
        Ok(SecId(id))
    }
}

// TODO-cleanup figure out how to use custom_derive here?
NewtypeDebug! { () pub struct SecId(Uuid); }
NewtypeDisplay! { () pub struct SecId(Uuid); }
NewtypeFrom! { () pub struct SecId(Uuid); }

impl From<OmicronZoneUuid> for SecId {
    fn from(g: OmicronZoneUuid) -> Self {
        g.into_untyped_uuid().into()
    }
}

impl From<&SecId> for Uuid {
    fn from(g: &SecId) -> Self {
        g.0
    }
}

/// Newtype wrapper around [`steno::SagaId`] which implements
/// Diesel traits.
///
/// This exists because Omicron cannot implement foreign traits
/// for foreign types.
#[derive(
    AsExpression, Copy, Clone, Debug, FromSqlRow, PartialEq, PartialOrd, Ord, Eq,
)]
#[diesel(sql_type = sql_types::Uuid)]
pub struct SagaId(pub steno::SagaId);

NewtypeFrom! { () pub struct SagaId(steno::SagaId); }

impl ToSql<sql_types::Uuid, Pg> for SagaId {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        let id = Uuid::from(self.0);
        <Uuid as ToSql<sql_types::Uuid, Pg>>::to_sql(&id, &mut out.reborrow())
    }
}

impl<DB> FromSql<sql_types::Uuid, DB> for SagaId
where
    DB: Backend,
    Uuid: FromSql<sql_types::Uuid, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        let id = Uuid::from_sql(bytes)?;
        Ok(SagaId(steno::SagaId::from(id)))
    }
}

/// Newtype wrapper around [`steno::SagaNodeId`] which implements
/// Diesel traits.
///
/// This exists because Omicron cannot implement foreign traits
/// for foreign types.
#[derive(
    AsExpression, Copy, Clone, Debug, FromSqlRow, PartialEq, PartialOrd, Ord, Eq,
)]
#[diesel(sql_type = sql_types::BigInt)]
pub struct SagaNodeId(pub steno::SagaNodeId);

NewtypeFrom! { () pub struct SagaNodeId(steno::SagaNodeId); }

impl ToSql<sql_types::BigInt, Pg> for SagaNodeId {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        // Diesel newtype -> steno type -> u32 -> i64 -> SQL
        let id = i64::from(u32::from(self.0));
        <i64 as ToSql<sql_types::BigInt, Pg>>::to_sql(&id, &mut out.reborrow())
    }
}

impl<DB> FromSql<sql_types::BigInt, DB> for SagaNodeId
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        let id = u32::try_from(i64::from_sql(bytes)?)?;
        Ok(SagaNodeId(id.into()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, SqlType)]
#[diesel(postgres_type(name = "saga_state", schema = "public"))]
pub struct SagaCachedStateEnum;

/// Newtype wrapper around [`steno::SagaCachedState`] which implements
/// Diesel traits.
///
/// This exists because Omicron cannot implement foreign traits
/// for foreign types.
#[derive(AsExpression, FromSqlRow, Clone, Copy, Debug, PartialEq)]
#[diesel(sql_type = SagaCachedStateEnum)]
pub struct SagaCachedState(pub steno::SagaCachedState);

NewtypeFrom! { () pub struct SagaCachedState(steno::SagaCachedState); }

impl ToSql<SagaCachedStateEnum, Pg> for SagaCachedState {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        use steno::SagaCachedState;
        out.write_all(match self.0 {
            SagaCachedState::Running => b"running",
            SagaCachedState::Unwinding => b"unwinding",
            SagaCachedState::Done => b"done",
        })?;
        Ok(serialize::IsNull::No)
    }
}

impl FromSql<SagaCachedStateEnum, Pg> for SagaCachedState {
    fn from_sql(
        bytes: <Pg as Backend>::RawValue<'_>,
    ) -> deserialize::Result<Self> {
        let bytes = <Pg as Backend>::RawValue::as_bytes(&bytes);
        let s = std::str::from_utf8(bytes)?;
        let state = steno::SagaCachedState::try_from(s)?;
        Ok(Self(state))
    }
}

/// Represents a row in the "Saga" table
#[derive(Queryable, Insertable, Clone, Debug, Selectable, PartialEq)]
#[diesel(table_name = saga)]
pub struct Saga {
    pub id: SagaId,
    pub creator: SecId,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub name: String,
    pub saga_dag: serde_json::Value,
    pub saga_state: SagaCachedState,
    pub current_sec: Option<SecId>,
    pub adopt_generation: super::Generation,
    pub adopt_time: chrono::DateTime<chrono::Utc>,
}

impl Saga {
    pub fn new(creator: SecId, params: steno::SagaCreateParams) -> Self {
        let now = chrono::Utc::now();

        // This match will help us identify a case where Steno adds a new field
        // to `SagaCreateParams` that we aren't persisting in the database.  (If
        // you're getting a compilation failure here, you need to figure out
        // what to do with the new field.  The assumption as of this writing is
        // that we must store it into the database or we won't be able to
        // properly recover the saga.)
        let steno::SagaCreateParams { id, name, dag, state } = params;

        Self {
            id: id.into(),
            creator,
            time_created: now,
            name: name.to_string(),
            saga_dag: dag,
            saga_state: state.into(),
            current_sec: Some(creator),
            adopt_generation: Generation::new().into(),
            adopt_time: now,
        }
    }
}

/// Represents a row in the "SagaNodeEvent" table
#[derive(Queryable, Insertable, Clone, Debug, Selectable, PartialEq)]
#[diesel(table_name = saga_node_event)]
pub struct SagaNodeEvent {
    pub saga_id: SagaId,
    pub node_id: SagaNodeId,
    pub event_type: String,
    pub data: Option<serde_json::Value>,
    pub event_time: chrono::DateTime<chrono::Utc>,
    pub creator: SecId,
}

impl SagaNodeEvent {
    pub fn new(event: steno::SagaNodeEvent, creator: SecId) -> Self {
        let data = match event.event_type {
            steno::SagaNodeEventType::Succeeded(ref data) => {
                Some((**data).clone())
            }
            steno::SagaNodeEventType::Failed(ref err) => {
                // It's hard to imagine how this serialize step could fail.  If
                // we're worried that it could, we could instead store the
                // serialized value directly in the `SagaNodeEvent`.  We'd be
                // forced to construct it in a context where failure could be
                // handled.
                Some(serde_json::to_value(err).unwrap())
            }
            _ => None,
        };

        Self {
            saga_id: event.saga_id.into(),
            node_id: event.node_id.into(),
            event_type: event.event_type.label().to_string(),
            data,
            event_time: chrono::Utc::now(),
            creator,
        }
    }
}

impl TryFrom<SagaNodeEvent> for steno::SagaNodeEvent {
    type Error = Error;
    fn try_from(ours: SagaNodeEvent) -> Result<Self, Self::Error> {
        let event_type = match (ours.event_type.as_str(), ours.data) {
            ("started", None) => steno::SagaNodeEventType::Started,
            ("succeeded", Some(d)) => {
                steno::SagaNodeEventType::Succeeded(Arc::new(d))
            }
            ("failed", Some(d)) => {
                let error: steno::ActionError = serde_json::from_value(d)
                    .map_err(|error| {
                        Error::internal_error(&format!(
                            "failed to parse ActionError for \"failed\" \
                            SagaNodeEvent: {:#}",
                            error
                        ))
                    })?;
                steno::SagaNodeEventType::Failed(error)
            }
            ("undo_started", None) => steno::SagaNodeEventType::UndoStarted,
            ("undo_finished", None) => steno::SagaNodeEventType::UndoFinished,
            (name, data) => {
                return Err(Error::internal_error(&format!(
                    "bad SagaNodeEventRow: event_type = {:?}, data = {:?}",
                    name, data
                )));
            }
        };

        Ok(steno::SagaNodeEvent {
            saga_id: ours.saga_id.into(),
            node_id: ours.node_id.into(),
            event_type,
        })
    }
}
