/*!
 * Saga log persistence
 */

use crate::db::schema;
use crate::db::sql::SqlSerialize;
use crate::db::sql_operations::sql_insert;
use crate::db::Pool;
use crate::db::SqlValueSet;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use omicron_common::bail_unless;
use omicron_common::db::sql_row_value;
use omicron_common::error::ApiError;
use omicron_common::model::ApiGeneration;
use serde_json::Value as JsonValue;
use slog::Logger;
use std::convert::TryFrom;
use std::sync::Arc;
use steno::SagaLogSink;
use steno::SagaNodeEventType;
use uuid::Uuid;

#[derive(Debug)]
pub struct CockroachDbSagaLogSink {
    pool: Arc<Pool>,
    log: Logger,
}

impl CockroachDbSagaLogSink {
    pub fn new(pool: Arc<Pool>, log: Logger) -> Self {
        CockroachDbSagaLogSink { pool, log }
    }
}

#[async_trait]
impl SagaLogSink for CockroachDbSagaLogSink {
    async fn record(&self, event: &steno::SagaNodeEvent) {
        let mut values = SqlValueSet::new();
        event.sql_serialize(&mut values);
        // XXX unwrap
        let client = self.pool.acquire().await.unwrap();

        // TODO-robustness This INSERT ought to be conditional on this SEC still
        // owning this saga.
        let result =
            sql_insert::<schema::SagaNodeEvent>(&client, &values).await;
        // XXX unwrap
        result.unwrap();
    }
}

/** Represents a row in the "Saga" table */
pub struct Saga {
    id: Uuid,
    creator: String,
    template_name: String, /* XXX enum? */
    time_created: DateTime<Utc>,
    saga_params: JsonValue,
    saga_state: String, /* XXX enum */
    current_sec: Option<String>,
    adopt_generation: ApiGeneration,
    adopt_time: DateTime<Utc>,
}

impl TryFrom<&tokio_postgres::Row> for Saga {
    type Error = ApiError;

    fn try_from(row: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Saga {
            id: sql_row_value(row, "id")?,
            creator: sql_row_value(row, "creator")?,
            template_name: sql_row_value(row, "template_name")?,
            time_created: sql_row_value(row, "time_created")?,
            saga_params: sql_row_value(row, "saga_params")?,
            saga_state: sql_row_value(row, "saga_state")?,
            current_sec: sql_row_value(row, "current_sec")?,
            adopt_generation: sql_row_value(row, "adopt_generation")?,
            adopt_time: sql_row_value(row, "adopt_time")?,
        })
    }
}

impl SqlSerialize for Saga {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("id", &self.id);
        output.set("creator", &self.creator);
        output.set("template_name", &self.template_name);
        output.set("time_created", &self.time_created);
        output.set("saga_params", &self.saga_params);
        output.set("saga_state", &self.saga_state);
        output.set("current_sec", &self.current_sec);
        output.set("adopt_generation", &self.adopt_generation);
        output.set("adopt_time", &self.adopt_time);
    }
}

impl SqlSerialize for steno::SagaNodeEvent {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("saga_id", &self.saga_id.0);
        output.set("node_id", &(self.node_id as i64)); // XXX
        output.set("event_time", &self.event_time);
        output.set("creator", &self.creator);
        output.set("event_type", &self.event_type.label());

        let data: Option<JsonValue> = match &self.event_type {
            SagaNodeEventType::Succeeded(output) => Some((**output).clone()),
            SagaNodeEventType::Failed(error) => {
                // XXX unwrap
                Some(serde_json::to_value(error).unwrap())
            }
            SagaNodeEventType::Started => None,
            SagaNodeEventType::UndoStarted => None,
            SagaNodeEventType::UndoFinished => None,
        };

        output.set("data", &data);
    }
}

// This type exists only to work around the Rust orphan rules.
pub struct SagaNodeEventDeserializer(steno::SagaNodeEvent);
impl TryFrom<&tokio_postgres::Row> for SagaNodeEventDeserializer {
    type Error = ApiError;
    fn try_from(row: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let event_type_str: &str = sql_row_value(row, "event_type")?;
        let data: Option<JsonValue> = sql_row_value(row, "data")?;
        let event_type = match event_type_str {
            "started" => {
                bail_unless!(data.is_none());
                SagaNodeEventType::Started
            }
            "undo_started" => {
                bail_unless!(data.is_none());
                SagaNodeEventType::UndoStarted
            }
            "undo_finished" => {
                bail_unless!(data.is_none());
                SagaNodeEventType::UndoFinished
            }
            "succeeded" => {
                bail_unless!(data.is_some());
                SagaNodeEventType::Succeeded(Arc::new(data.unwrap()))
            }
            "failed" => {
                bail_unless!(data.is_some());
                let error: steno::ActionError =
                    serde_json::from_value(data.unwrap()).map_err(|e| {
                        ApiError::internal_error(&format!(
                            "error extracting steno::ActionError: {:#}",
                            e
                        ))
                    })?;
                SagaNodeEventType::Failed(error)
            }
            s => {
                return Err(ApiError::internal_error(&format!(
                    "unsupported saga node event type: {}",
                    s
                )))
            }
        };

        Ok(SagaNodeEventDeserializer(steno::SagaNodeEvent {
            saga_id: steno::SagaId(sql_row_value(row, "saga_id")?),
            node_id: sql_row_value::<_, i64>(row, "node_id")? as u64, // XXX
            event_time: sql_row_value(row, "event_time")?,
            creator: sql_row_value(row, "creator")?,
            event_type,
        }))
    }
}
