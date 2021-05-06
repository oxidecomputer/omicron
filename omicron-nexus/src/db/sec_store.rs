/*!
 * Implementation of [`steno::SecStore`] backed by Omicron's database
 */

use crate::db;
use anyhow::Context;
use async_trait::async_trait;
use omicron_common::model::ApiGeneration;
use slog::Logger;
use std::fmt;
use std::sync::Arc;
use steno::SagaId;

/**
 * Implementation of [`steno::SecStore`] backed by the Omicron CockroachDB
 * database.
 */
pub struct CockroachDbSecStore {
    sec_id: db::SecId,
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
        sec_id: db::SecId,
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
        let saga_record = db::saga_types::Saga {
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
        let our_event = db::saga_types::SagaNodeEvent {
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
