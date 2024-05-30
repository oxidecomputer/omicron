// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of [`steno::SecStore`] backed by Omicron's database

use crate::db::{self, model::Generation};
use anyhow::Context;
use async_trait::async_trait;
use dropshot::HttpError;
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
            .context("creating saga record")?;

        Ok(())
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
                // An interesting question is how to handle errors.
                //
                // In general, there are some kinds of database errors that are
                // temporary/server errors (e.g. network failures), and some
                // that are permanent/client errors (e.g. conflict during
                // insertion). The permanent ones would require operator
                // intervention to fix.
                //
                // However, there is no way to bubble up errors here, and for
                // good reason: it is inherent to the nature of sagas that
                // progress is durably recorded. So within *this* code there is
                // no option but to retry forever. (Below, however, we do mark
                // errors that likely require operator intervention.)
                //
                // At a higher level, callers should plan for the fact that
                // record_event could potentially loop forever. See
                // https://github.com/oxidecomputer/omicron/issues/5406 and the
                // note in `nexus/src/app/saga.rs`'s `execute_saga` for more
                // details.
                self.datastore
                    .saga_create_event(&our_event)
                    .map_err(backoff::BackoffError::transient)
            },
            move |error, call_count, total_duration| {
                let http_error = HttpError::from(error.clone());
                if http_error.status_code.is_client_error() {
                    error!(
                        &log,
                        "client error while recording saga event (likely \
                         requires operator intervention), retrying anyway";
                        "error" => &error,
                        "call_count" => call_count,
                        "total_duration" => ?total_duration,
                    );
                } else if total_duration > Duration::from_secs(20) {
                    warn!(
                        &log,
                        "server error while recording saga event, retrying";
                        "error" => &error,
                        "call_count" => call_count,
                        "total_duration" => ?total_duration,
                    );
                } else {
                    info!(
                        &log,
                        "server error while recording saga event, retrying";
                        "error" => &error,
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
        info!(&self.log, "updating state";
            "saga_id" => id.to_string(),
            "new_state" => update.to_string()
        );

        // TODO(AJS): (Note to add to PR and remove before merge) - Steno has
        // no notion of generation numbers, and so we'd need a side channel
        // mechanism in order to keep track of the current generation numbers
        // per SagaId in memory. We'd then garbage collect them when this method
        // is called with `steno::SagaCachedState::Done`. However, while that
        // method shouldn't be called more than once if this function succeeds,
        // it may do to code changes. Furthermore, we have to maintain this
        // table of SagaId -> Generation mappings correctly and make a decision
        // about what to do if we fail a lookup? Do we panic? Do we use the
        // default of `Generation::new()`? Do we just read from the DB and get
        // the current value? I actually wrote this lookup map started to use
        // it and started to do the lookup from the DB if it wasn't in the cache
        // map. However, that made me wonder what in fact was being protected by
        // the generation here. What we really want to ensure is that this SEC
        // owns the saga. It doesn't matter when it became the owner, just that
        // it currently is. We already have that information and we filter on it
        // in `datastore.saga_update_state`.  This seems to buy us nothing.
        //
        // Furthermore, we also already need to add such a filter for the
        // `SecId` in `datastore.saga_create_event`, and we don't have a
        // generation number being used there. We'd have the same issue of
        // keeping the cache up to date with the DB, which which is why we've
        // generally eschewed using caches in the first place.
        //
        // Lastly, the generation in the saga is called `adopt_generation`. It's
        // most useful in ensuring that only a single entity can migrate from
        // one sec to another. That's where the conditional is actually used for
        // optimisitic concurrency control
        //
        // All this being said, we shouldn't use the generation number for
        // `steno::SecStore` operations at all IMO. We should ensure that we
        // only have one SEC running that owns the saga, and to prevent errant
        // logging, we do an extra safety check on the DB. It should be noted
        // that even with the extra safety check on the DB, having two SECs
        // attempt to run the same saga can cause an immense amount of damage.
        // We protect that via our expunge mechanism. In the short term the
        // expunge mechanism is to have the operator pull the sled before
        // expungement. In the longer term case we'll remove the sled from trust
        // quorum and reboot it so that it cannot even boot to get to the point
        // where it could possibly run sagas.
        //

        // TODO-robustness This should be wrapped with a retry loop rather than
        // unwrapping the result.  See omicron#2416.
        self.datastore
            .saga_update_state(id, update, self.sec_id)
            .await
            .unwrap();
    }
}
