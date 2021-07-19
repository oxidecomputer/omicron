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

use crate::db;
use omicron_common::api::ApiError;
use omicron_common::api::ApiGeneration;
use omicron_common::db::sql_row_value;
use omicron_common::impl_sql_wrapping;
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
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct SecId(Uuid);
impl_sql_wrapping!(SecId, Uuid);

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
pub struct Saga {
    pub id: SagaId,
    pub creator: SecId,
    pub template_name: String,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub saga_params: serde_json::Value,
    pub saga_state: steno::SagaCachedState,
    pub current_sec: Option<SecId>,
    pub adopt_generation: ApiGeneration,
    pub adopt_time: chrono::DateTime<chrono::Utc>,
}

impl TryFrom<&tokio_postgres::Row> for Saga {
    type Error = ApiError;

    fn try_from(row: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let saga_state_str: String = sql_row_value(row, "saga_state")?;
        let saga_state = steno::SagaCachedState::try_from(
            saga_state_str.as_str(),
        )
        .map_err(|e| {
            ApiError::internal_error(&format!(
                "failed to parse saga state {:?}: {:#}",
                saga_state_str, e
            ))
        })?;
        Ok(Saga {
            id: SagaId(sql_row_value::<_, Uuid>(row, "id")?),
            creator: sql_row_value(row, "creator")?,
            template_name: sql_row_value(row, "template_name")?,
            time_created: sql_row_value(row, "time_created")?,
            saga_params: sql_row_value(row, "saga_params")?,
            saga_state: saga_state,
            current_sec: sql_row_value(row, "current_sec")?,
            adopt_generation: sql_row_value(row, "adopt_generation")?,
            adopt_time: sql_row_value(row, "adopt_time")?,
        })
    }
}

impl db::sql::SqlSerialize for Saga {
    fn sql_serialize(&self, output: &mut db::SqlValueSet) {
        output.set("id", &self.id.0);
        output.set("creator", &self.creator);
        output.set("template_name", &self.template_name);
        output.set("time_created", &self.time_created);
        output.set("saga_params", &self.saga_params);
        output.set("saga_state", &self.saga_state.to_string());
        output.set("current_sec", &self.current_sec);
        output.set("adopt_generation", &self.adopt_generation);
        output.set("adopt_time", &self.adopt_time);
    }
}

/**
 * Represents a row in the "SagaNodeEvent" table
 */
#[derive(Clone, Debug)]
pub struct SagaNodeEvent {
    pub saga_id: SagaId,
    pub node_id: steno::SagaNodeId,
    pub event_type: steno::SagaNodeEventType,
    pub event_time: chrono::DateTime<chrono::Utc>,
    pub creator: SecId,
}

impl TryFrom<&tokio_postgres::Row> for SagaNodeEvent {
    type Error = ApiError;

    fn try_from(row: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let event_data: Option<serde_json::Value> = sql_row_value(row, "data")?;
        let event_name: String = sql_row_value(row, "event_type")?;

        let event_type = match (event_name.as_str(), event_data) {
            ("started", None) => steno::SagaNodeEventType::Started,
            ("succeeded", Some(d)) => {
                steno::SagaNodeEventType::Succeeded(Arc::new(d))
            }
            ("failed", Some(d)) => {
                let error: steno::ActionError = serde_json::from_value(d)
                    .map_err(|error| {
                        ApiError::internal_error(&format!(
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
                return Err(ApiError::internal_error(&format!(
                    "bad SagaNodeEventRow: event_type = {:?}, data = {:?}",
                    name, data
                )));
            }
        };

        let node_id_i64: i64 = sql_row_value(row, "node_id")?;
        let node_id_u32 = u32::try_from(node_id_i64)
            .map_err(|_| ApiError::internal_error("node id out of range"))?;
        let node_id = steno::SagaNodeId::from(node_id_u32);

        Ok(SagaNodeEvent {
            saga_id: SagaId(sql_row_value::<_, Uuid>(row, "saga_id")?),
            node_id,
            event_type,
            event_time: sql_row_value(row, "event_time")?,
            creator: sql_row_value(row, "creator")?,
        })
    }
}

impl db::sql::SqlSerialize for SagaNodeEvent {
    fn sql_serialize(&self, output: &mut db::SqlValueSet) {
        let (event_name, event_data) = match self.event_type {
            steno::SagaNodeEventType::Started => ("started", None),
            steno::SagaNodeEventType::Succeeded(ref d) => {
                ("succeeded", Some((**d).clone()))
            }
            steno::SagaNodeEventType::Failed(ref d) => {
                /*
                 * It's hard to imagine how this serialize step could fail.  If
                 * we're worried that it could, we could instead store the
                 * serialized value directly in the `SagaNodeEvent`.  We'd be
                 * forced to construct it in a context where failure could be
                 * handled.
                 */
                let json = serde_json::to_value(d).unwrap();
                ("failed", Some(json))
            }
            steno::SagaNodeEventType::UndoStarted => ("undo_started", None),
            steno::SagaNodeEventType::UndoFinished => ("undo_finished", None),
        };

        output.set("saga_id", &Uuid::from(self.saga_id));
        output.set("node_id", &i64::from(u32::from(self.node_id)));
        output.set("event_type", &event_name);
        output.set("data", &event_data);
        output.set("event_time", &self.event_time);
        output.set("creator", &self.creator);
    }
}

impl From<&SagaNodeEvent> for steno::SagaNodeEvent {
    fn from(ours: &SagaNodeEvent) -> Self {
        steno::SagaNodeEvent {
            saga_id: ours.saga_id,
            node_id: ours.node_id,
            event_type: ours.event_type.clone(),
        }
    }
}
