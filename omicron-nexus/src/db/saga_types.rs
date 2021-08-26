/*!
 * Types used for sagas
 *
 * Just like elsewhere, we run into Rust's orphan rules here.  There are types
 * in Steno that we want to put into the database, but we can't impl
 * `ToSql`/`FromSql` directly on them because they're in different crates.  We
 * could create wrapper types and impl `ToSql`/`FromSql` on those.  Instead, we
 * use the Steno types directly in our own types, and the handful of places that
 * actually serialize them to and from SQL take care of the necessary
 * conversions.
 */

use super::diesel_schema::{saga, saganodeevent};
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use std::convert::TryFrom;
use std::sync::Arc;
use steno::SagaId;
use uuid::Uuid;

/**
 * Unique identifier for an SEC (saga execution coordinator) instance
 *
 * For us, these will generally be Nexus instances, and the SEC id will match
 * the Nexus id.
 */
#[derive(
    AsExpression, FromSqlRow, Clone, Copy, Eq, Ord, PartialEq, PartialOrd,
)]
#[sql_type = "sql_types::Uuid"]
pub struct SecId(pub Uuid);

impl<DB> ToSql<sql_types::Uuid, DB> for SecId
where
    DB: Backend,
    Uuid: ToSql<sql_types::Uuid, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        (&self.0 as &Uuid).to_sql(out)
    }
}

impl<DB> FromSql<sql_types::Uuid, DB> for SecId
where
    DB: Backend,
    Uuid: FromSql<sql_types::Uuid, DB>,
{
    fn from_sql(bytes: Option<&DB::RawValue>) -> deserialize::Result<Self> {
        let id = Uuid::from_sql(bytes)?;
        Ok(SecId(id))
    }
}

// impl_sql_wrapping!(SecId, Uuid);

// TODO-cleanup figure out how to use custom_derive here?
NewtypeDebug! { () pub struct SecId(Uuid); }
NewtypeDisplay! { () pub struct SecId(Uuid); }
NewtypeFrom! { () pub struct SecId(Uuid); }

impl From<&SecId> for Uuid {
    fn from(g: &SecId) -> Self {
        g.0
    }
}

/**
 * Represents a row in the "Saga" table
 */
#[derive(Queryable, Insertable, Clone, Debug)]
#[table_name = "saga"]
pub struct Saga {
    pub id: SagaId,
    pub creator: SecId,
    pub template_name: String,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub saga_params: serde_json::Value,
    pub saga_state: steno::SagaCachedState,
    pub current_sec: Option<SecId>,
    pub adopt_generation: Generation,
    pub adopt_time: chrono::DateTime<chrono::Utc>,
}

// TODO: Make this one aligned with DB
// TODO: Make another struct representing the "parsed" thing
// TODO: do the same with other struct with parsed types
/// Represents a row in the "SagaNodeEvent" table
#[derive(Queryable, Insertable, Clone, Debug)]
#[table_name = "saganodeevent"]
pub struct SagaNodeEvent {
    pub saga_id: SagaId,
    pub node_id: steno::SagaNodeId,
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
            saga_id: event.saga_id,
            node_id: event.node_id,
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
            saga_id: ours.saga_id,
            node_id: ours.node_id,
            event_type,
        })
    }
}
