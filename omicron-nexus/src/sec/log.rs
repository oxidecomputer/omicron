/*!
 * Saga log persistence
 *
 * XXX The approach here to wrapping Steno's types is a mix.  We should probably
 * be more consistent.  There are two general approaches here:
 * - Use the most accurate type (e.g., steno::SagaId) directly.  When we need to
 *   read it from SQL or write it, we explicitly convert (e.g., read it as a
 *   Uuid, then wrap it in a steno::SagaId).  We could streamline this a bit
 *   with appropriate `From` impls.
 * - Use wrapper newtypes on which we impl ToSql/FromSql directly.  The
 *   advantage of this is that we can impl this behavior once and then have it
 *   used automatically in the various places that convert to/from SQL.  Of
 *   course, it's not really automatic: you still need to wrap the type (e.g.,
 *   SagaId) with the wrapper (e.g., SagaIdSql).  With the first approach, every
 *   place that needs to do SQL operations on a SagaId needs to
 *   marshal/unmarshal the Uuid inside it.  The downside is it becomes kind of a
 *   mess of types: we might wind up with a SagaIdSql(SagaId(Uuid)), and we _do_
 *   still need to do all those From/Into conversions where we want to use these
 *   values.
 *
 * I'm going to try option (1) for now.
 */

use crate::db;
use anyhow::Context;
use async_trait::async_trait;
use omicron_common::db::sql_row_value;
use omicron_common::error::ApiError;
use omicron_common::impl_sql_wrapping;
use omicron_common::model::ApiGeneration;
use serde_json::Value as JsonValue;
use slog::Logger;
use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;
use steno::SagaId;
use uuid::Uuid;

/**
 * Unique identifier for an SEC (saga execution coordinator) instance
 *
 * For us, these will generally be Nexus instances, and the uuids will match.
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

/** Represents a row in the "Saga" table */
pub struct Saga {
    pub id: SagaId,
    pub creator: SecId,
    pub template_name: String, /* XXX enum? */
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub saga_params: JsonValue,
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

/** Represents a row in the "SagaNodeEvent" table */
#[derive(Clone, Debug)]
pub struct SagaNodeEvent {
    saga_id: SagaId,
    node_id: steno::SagaNodeId,
    event_type: steno::SagaNodeEventType,
    event_time: chrono::DateTime<chrono::Utc>,
    creator: SecId,
}

impl TryFrom<&tokio_postgres::Row> for SagaNodeEvent {
    type Error = ApiError;

    fn try_from(row: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let event_data: Option<JsonValue> = sql_row_value(row, "data")?;
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
                let json = serde_json::to_value(d).unwrap(); // XXX unwrap
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

/*
 * SecStore
 */

/**
 * Implementation of [`steno::SecStore`] backed by the Omicron CockroachDB
 * database.
 */
pub struct CockroachDbSecStore {
    sec_id: SecId,
    datastore: Arc<db::DataStore>,
    log: Logger,
}

impl fmt::Debug for CockroachDbSecStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CockroachDbSecStore { ... }")
    }
}

impl CockroachDbSecStore {
    pub fn new(
        sec_id: SecId,
        datastore: Arc<db::DataStore>,
        log: Logger,
    ) -> Self {
        CockroachDbSecStore { sec_id, datastore, log }
    }
}

#[async_trait]
impl steno::SecStore for CockroachDbSecStore {
    async fn saga_create(
        &self,
        create_params: steno::SagaCreateParams,
    ) -> Result<(), anyhow::Error> {
        info!(&self.log, "creating saga";
            "saga_id" => create_params.id.to_string(),
            "template_name" => &create_params.template_name,
        );

        let now = chrono::Utc::now();
        let saga_record = Saga {
            id: create_params.id,
            creator: self.sec_id,
            template_name: create_params.template_name,
            time_created: now,
            saga_params: create_params.saga_params,
            saga_state: create_params.state,
            current_sec: Some(self.sec_id),
            adopt_generation: ApiGeneration::new(),
            adopt_time: now,
        };

        self.datastore
            .saga_create(&saga_record)
            .await
            .context("creating saga record")
    }

    // XXX SagaId is redundant in this interface
    async fn record_event(&self, id: SagaId, event: steno::SagaNodeEvent) {
        debug!(&self.log, "recording saga event";
            "saga_id" => id.to_string(),
            "node_id" => ?event.node_id,
            "event_type" => ?event.event_type,
        );
        let our_event = SagaNodeEvent {
            saga_id: id,
            node_id: event.node_id,
            event_type: event.event_type,
            creator: self.sec_id,
            event_time: chrono::Utc::now(),
        };

        // XXX The unwrap ought to be handled and the whole operation retried.
        self.datastore.saga_create_event(&our_event).await.unwrap();
    }

    async fn saga_update(&self, id: SagaId, update: steno::SagaCachedState) {
        // XXX We should track the current generation of the saga and use it
        // here.  We'll know this either from when it was created or when it was
        // recovered.
        // XXX retry loop instead of unwrap
        info!(&self.log, "updating state";
            "saga_id" => id.to_string(),
            "new_state" => update.to_string()
        );
        self.datastore
            .saga_update_state(id, update, self.sec_id, ApiGeneration::new())
            .await
            .unwrap();
    }
}
