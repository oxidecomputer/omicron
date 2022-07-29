// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of [`steno::SecStore`] backed by Omicron's database

use crate::db::{self, model::Generation};
use anyhow::Context;
use async_trait::async_trait;
use slog::Logger;
use std::fmt;
use std::sync::Arc;
use steno::SagaId;

/// Implementation of [`steno::SecStore`] backed by the Omicron CockroachDB
/// database.
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
        // XXX-dap include saga name in log
        info!(&self.log, "creating saga";
            "saga_id" => create_params.id.to_string(),
        );
        self.datastore
            .saga_create(&db::saga_types::Saga::new(self.sec_id, create_params))
            .await
            .context("creating saga record")
    }

    async fn record_event(&self, event: steno::SagaNodeEvent) {
        debug!(&self.log, "recording saga event";
            "saga_id" => event.saga_id.to_string(),
            "node_id" => ?event.node_id,
            "event_type" => ?event.event_type,
        );
        let our_event = db::saga_types::SagaNodeEvent::new(event, self.sec_id);

        // TODO-robustness This should be wrapped with a retry loop rather than
        // unwrapping the result.
        self.datastore.saga_create_event(&our_event).await.unwrap();
    }

    async fn saga_update(&self, id: SagaId, update: steno::SagaCachedState) {
        // TODO-robustness We should track the current generation of the saga
        // and use it.  We'll know this either from when it was created or when
        // it was recovered.
        info!(&self.log, "updating state";
            "saga_id" => id.to_string(),
            "new_state" => update.to_string()
        );

        // TODO-robustness This should be wrapped with a retry loop rather than
        // unwrapping the result.
        self.datastore
            .saga_update_state(id, update, self.sec_id, Generation::new())
            .await
            .unwrap();
    }
}
