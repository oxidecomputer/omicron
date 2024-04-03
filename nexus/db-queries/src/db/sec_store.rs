// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of [`steno::SecStore`] backed by Omicron's database

use crate::db::{self, model::Generation};
use anyhow::Context;
use async_trait::async_trait;
use futures::TryFutureExt;
use omicron_common::backoff;
use slog::Logger;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
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
        info!(&self.log, "creating saga";
            "saga_id" => create_params.id.to_string(),
            "saga_name" => create_params.name.to_string(),
        );
        self.datastore
            .saga_create(&db::saga_types::Saga::new(self.sec_id, create_params))
            .await
            .context("creating saga record")
    }

    async fn record_event(&self, event: steno::SagaNodeEvent) {
        let log = self.log.new(o!(
            "saga_id" => event.saga_id.to_string(),
            "node_id" => event.node_id.to_string(),
            "event_type" => format!("{:?}", event.event_type),
        ));

        debug!(&log, "recording saga event");
        let our_event = db::saga_types::SagaNodeEvent::new(event, self.sec_id);

        backoff::retry_notify_ext(
            // This is an internal service query to CockroachDB.
            backoff::retry_policy_internal_service(),
            || {
                // XXX: do we need to filter the kinds of errors we retry on?
                // Are there particular database errors which are unrecoverable
                // and where we should just panic.
                self.datastore
                    .saga_create_event(&our_event)
                    .map_err(backoff::BackoffError::transient)
            },
            move |error, call_count, total_duration| {
                if call_count == 0 {
                    info!(
                        &log,
                        "failed to record saga event, retrying";
                        "error" => ?error,
                    );
                } else if total_duration > Duration::from_secs(20) {
                    warn!(
                        &log,
                        "failed to record saga event, retrying";
                        "error" => ?error,
                        "call_count" => call_count,
                        "total_duration" => ?total_duration,
                    );
                }
            },
        )
        .await
        .expect("the above backoff retries forever")
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
        // unwrapping the result.  See omicron#2416.
        self.datastore
            .saga_update_state(id, update, self.sec_id, Generation::new())
            .await
            .unwrap();
    }
}
