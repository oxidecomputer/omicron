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

use super::impl_enum_type;

use chrono::DateTime;
use chrono::Utc;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use nexus_db_schema::schema::{saga, saga_node_event};
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

/// Unique identifier for an SEC (saga execution coordinator) instance
///
/// For us, these will generally be Nexus instances, and the SEC id will match
/// the Nexus id.
#[derive(
    AsExpression,
    FromSqlRow,
    Clone,
    Copy,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
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
NewtypeDisplay! { () pub struct SagaId(steno::SagaId); }

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

impl_enum_type!(
    SagaAbandonReasonEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
    )]
    pub enum SagaReasonAbandoned;

    Omdb => b"omdb"
    Unrecoverable => b"unrecoverable"
);

impl_enum_type!(
    SagaStateEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
    )]
    pub enum SagaState;

    Running => b"running"
    Unwinding => b"unwinding"
    Done => b"done"
    Abandoned => b"abandoned"
);

impl SagaState {
    /// A saga must be in this set of states to be a candidate for saga
    /// recovery.
    ///
    /// Sagas that are Done don't need to be run anymore. Sagas that are
    /// Abandoned have been explicitly opted out of being recovered.
    pub const RECOVERY_CANDIDATE_STATES: &'static [Self] =
        &[Self::Running, Self::Unwinding];
}

impl From<steno::SagaCachedState> for SagaState {
    fn from(value: steno::SagaCachedState) -> Self {
        match value {
            steno::SagaCachedState::Running => Self::Running,
            steno::SagaCachedState::Unwinding => Self::Unwinding,
            steno::SagaCachedState::Done => Self::Done,
        }
    }
}

/// Represents a raw row in the "saga" table.
///
/// This is the type Diesel reads and writes directly. It can represent states
/// that should never produced (for example, abandonment metadata columns set
/// without the saga being `Abandoned`), so it isn't constructed directly.
/// Reads produce it via `Queryable` and immediately validate it into [`Saga`]
/// (`Saga::try_from`). Writes lower a validated [`Saga`] into it via
/// `SagaRow::from(&saga)`.
///
/// This is `pub` only because Diesel needs to name the row type at query sites
/// in other crates (like omdb). It's `#[doc(hidden)]` to signal that it's an
/// internal type and not part of the supported API.
#[doc(hidden)]
#[derive(Queryable, Insertable, Clone, Debug, Selectable, PartialEq)]
#[diesel(table_name = saga)]
pub struct SagaRow {
    id: SagaId,
    creator: SecId,
    time_created: chrono::DateTime<chrono::Utc>,
    name: String,
    saga_dag: serde_json::Value,
    saga_state: SagaState,
    current_sec: Option<SecId>,
    adopt_generation: super::Generation,
    adopt_time: chrono::DateTime<chrono::Utc>,

    // Abandonment metadata. These are only set when `saga_state` is
    // `Abandoned` and are `None` otherwise.
    abandon_time: Option<DateTime<Utc>>,
    abandon_reason: Option<SagaReasonAbandoned>,
    abandon_comment: Option<String>,
}

impl SagaRow {
    pub fn id(&self) -> SagaId {
        self.id
    }

    pub fn current_sec(&self) -> Option<SecId> {
        self.current_sec
    }

    pub fn saga_state(&self) -> SagaState {
        self.saga_state
    }

    pub fn creator(&self) -> SecId {
        self.creator
    }

    pub fn adopt_generation(&self) -> super::Generation {
        self.adopt_generation
    }

    fn is_abandon_metadata_empty(&self) -> bool {
        self.abandon_comment.is_none()
            && self.abandon_reason.is_none()
            && self.abandon_time.is_none()
    }

    // Returns the abandonment metadata iff all three columns are set.
    fn valid_abandon_metadata(&self) -> Option<AbandonMetadata> {
        Some(AbandonMetadata {
            time: self.abandon_time?,
            reason: self.abandon_reason?,
            comment: self.abandon_comment.clone()?,
        })
    }
}

/// Abandonment metadata for a saga.
///
/// These three values are always written and cleared together, so bundling
/// them keeps them "all or none". A saga is either not abandoned (no
/// `AbandonMetadata`) or fully abandoned (an `AbandonMetadata` with every field
/// set).
#[derive(Clone, Debug, PartialEq)]
pub struct AbandonMetadata {
    pub time: DateTime<Utc>,
    pub reason: SagaReasonAbandoned,
    pub comment: String,
}

/// A saga loaded and validated from the database or built in memory for
/// insertion.
///
/// Compared to [`SagaRow`], the three abandon metadata columns are bundled
/// into a single `abandon` field, kept all or none. Reads go through
/// `TryFrom<SagaRow>`, which rejects rows whose metadata is inconsistent with
/// `saga_state` with an [`Error::internal_error`].
#[derive(Clone, Debug, PartialEq)]
pub struct Saga {
    pub id: SagaId,
    pub creator: SecId,
    pub time_created: DateTime<Utc>,
    pub name: String,
    pub saga_dag: serde_json::Value,
    pub saga_state: SagaState,
    pub current_sec: Option<SecId>,
    pub adopt_generation: super::Generation,
    pub adopt_time: DateTime<Utc>,
    // Abandon metadata, present only when `saga_state` is `Abandoned`.
    pub abandon_metadata: Option<AbandonMetadata>,
}

impl Saga {
    pub fn new(creator: SecId, params: steno::SagaCreateParams) -> Self {
        let now = now_db_precision();

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
            // A newly-created saga is never abandoned.
            abandon_metadata: None,
        }
    }

    // The sec store has no concept of an abandoned saga. To construct a new
    // saga in an abandoned state, this method should be used.
    //
    // Other than for testing, there should be no other use for this method.
    pub fn new_abandoned(
        creator: SecId,
        saga_id: SagaId,
        name: String,
        dag: serde_json::Value,
        abandon_metadata: AbandonMetadata,
    ) -> Self {
        let now = now_db_precision();

        Self {
            id: saga_id,
            creator,
            time_created: now,
            name,
            saga_dag: dag,
            saga_state: SagaState::Abandoned,
            current_sec: Some(creator),
            adopt_generation: Generation::new().into(),
            adopt_time: now,
            // An abandoned saga must always contain abandonment metadata.
            abandon_metadata: Some(abandon_metadata),
        }
    }
}

impl TryFrom<SagaRow> for Saga {
    type Error = Error;

    fn try_from(row: SagaRow) -> Result<Self, Self::Error> {
        let is_abandon_metadata_empty = row.is_abandon_metadata_empty();
        // If present and valid, convert the three nullable abandonment columns
        // into `AbandonMetadata`.
        let valid_abandon_metadata = row.valid_abandon_metadata();

        let SagaRow {
            id,
            creator,
            time_created,
            name,
            saga_dag,
            saga_state,
            current_sec,
            adopt_generation,
            adopt_time,
            abandon_time,
            abandon_reason,
            abandon_comment,
        } = row;

        // A partially-populated set is impossible per the
        // `abandoned_requires_metadata` and
        // `not_abandoned_requires_no_metadata` CHECK constraints, so treat
        // it as corruption.
        if !is_abandon_metadata_empty && valid_abandon_metadata.is_none() {
            return Err(Error::internal_error(&format!(
                "saga {id}: abandonment metadata is partially populated. \
                abandon_time: {abandon_time:?} abandon_reason {abandon_reason:?} \
                abandon_comment: {abandon_comment:?}"
            )));
        }

        // The abandon metadata must be present exactly when the saga is
        // abandoned.
        let abandon_metadata = match saga_state {
            SagaState::Abandoned => {
                let metadata = valid_abandon_metadata.ok_or_else(|| {
                    Error::internal_error(&format!(
                        "saga {id}: abandoned but has no abandonment metadata"
                    ))
                })?;

                Some(metadata)
            }
            SagaState::Done | SagaState::Running | SagaState::Unwinding => {
                if let Some(metadata) = valid_abandon_metadata {
                    return Err(Error::internal_error(&format!(
                        "saga {id}: has abandonment metadata but is not \
                        abandoned. abandon_time: {:?} abandon_reason {:?} \
                        abandon_comment: {:?}",
                        metadata.time, metadata.reason, metadata.comment
                    )));
                }

                None
            }
        };

        Ok(Saga {
            id,
            creator,
            time_created,
            name,
            saga_dag,
            saga_state,
            current_sec,
            adopt_generation,
            adopt_time,
            abandon_metadata,
        })
    }
}

/// Lowers a validated [`Saga`] into a raw [`SagaRow`] for insertion. This
/// expands the all or none `abandon_metadata` back into the three nullable
/// columns.
impl From<&Saga> for SagaRow {
    fn from(saga: &Saga) -> Self {
        let (abandon_time, abandon_reason, abandon_comment) =
            match &saga.abandon_metadata {
                Some(AbandonMetadata { time, reason, comment }) => {
                    (Some(*time), Some(*reason), Some(comment.clone()))
                }
                None => (None, None, None),
            };

        SagaRow {
            id: saga.id,
            creator: saga.creator,
            time_created: saga.time_created,
            name: saga.name.clone(),
            saga_dag: saga.saga_dag.clone(),
            saga_state: saga.saga_state,
            current_sec: saga.current_sec,
            adopt_generation: saga.adopt_generation,
            adopt_time: saga.adopt_time,
            abandon_time,
            abandon_reason,
            abandon_comment,
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

#[cfg(test)]
mod test {
    use super::*;

    fn fake_saga_row(
        saga_state: SagaState,
        abandon_time: Option<DateTime<Utc>>,
        abandon_reason: Option<SagaReasonAbandoned>,
        abandon_comment: Option<String>,
    ) -> SagaRow {
        SagaRow {
            id: SagaId(steno::SagaId(Uuid::new_v4())),
            creator: SecId(Uuid::new_v4()),
            time_created: Utc::now(),
            name: "test-saga".to_string(),
            saga_dag: serde_json::Value::Null,
            saga_state,
            current_sec: Some(SecId(Uuid::new_v4())),
            adopt_generation: Generation::new().into(),
            adopt_time: Utc::now(),
            abandon_time,
            abandon_reason,
            abandon_comment,
        }
    }

    #[test]
    fn test_try_from_validates_abandon_metadata() {
        let time = Utc::now();
        let reason = SagaReasonAbandoned::Unrecoverable;
        let comment = "test abandon".to_string();

        // Abandoned and fully-present metadata is valid, and the metadata is
        // carried through to the validated `Saga`.
        let row = fake_saga_row(
            SagaState::Abandoned,
            Some(time),
            Some(reason),
            Some(comment.clone()),
        );
        let saga = Saga::try_from(row)
            .expect("abandoned saga with full metadata should be valid");
        assert_eq!(saga.saga_state, SagaState::Abandoned);
        assert_eq!(
            saga.abandon_metadata,
            Some(AbandonMetadata { time, reason, comment: comment.clone() })
        );

        // Not abandoned and empty metadata is valid. No metadata is propagated
        // to the validated `Saga`.
        let row = fake_saga_row(SagaState::Running, None, None, None);
        let saga = Saga::try_from(row)
            .expect("non-abandoned saga without metadata should be valid");
        assert_eq!(saga.saga_state, SagaState::Running);
        assert_eq!(saga.abandon_metadata, None);

        // Abandoned and empty metadata is invalid.
        let row = fake_saga_row(SagaState::Abandoned, None, None, None);
        Saga::try_from(row)
            .expect_err("abandoned saga without metadata should be rejected");

        // Not abandoned and fully-present metadata is invalid.
        let row = fake_saga_row(
            SagaState::Done,
            Some(time),
            Some(reason),
            Some(comment.clone()),
        );
        Saga::try_from(row)
            .expect_err("non-abandoned saga with metadata should be rejected");

        // Partially-populated ("corrupted") metadata is invalid regardless of
        // the saga state. Cover every partial column combination.
        let partial_shapes = [
            (Some(time), None, None),
            (None, Some(reason), None),
            (None, None, Some(comment.clone())),
            (Some(time), Some(reason), None),
            (Some(time), None, Some(comment.clone())),
            (None, Some(reason), Some(comment.clone())),
        ];
        for saga_state in [SagaState::Abandoned, SagaState::Unwinding] {
            for (abandon_time, abandon_reason, abandon_comment) in
                partial_shapes.iter().cloned()
            {
                let row = fake_saga_row(
                    saga_state,
                    abandon_time,
                    abandon_reason,
                    abandon_comment,
                );
                Saga::try_from(row).expect_err(
                    "partially-populated abandonment metadata should be \
                     rejected",
                );
            }
        }
    }
}
