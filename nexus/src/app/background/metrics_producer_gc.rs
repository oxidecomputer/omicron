// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for garbage collecting metrics producers that have not
//! renewed their lease

use super::common::BackgroundTask;
use chrono::TimeDelta;
use chrono::Utc;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use std::time::Duration;

/// Background task that prunes metrics producers that have failed to renew
/// their lease.
pub struct MetricProducerGc {
    datastore: Arc<DataStore>,
    lease_duration: Duration,
}

impl MetricProducerGc {
    pub fn new(datastore: Arc<DataStore>, lease_duration: Duration) -> Self {
        Self { datastore, lease_duration }
    }

    async fn activate(&mut self, opctx: &OpContext) -> serde_json::Value {
        // TODO We should turn this task on as a part of landing the rest of the
        // move to metric producer leases. For now, we leave it disabled to
        // avoid pruning producers that don't know to renew leases.
        if true {
            warn!(
                opctx.log,
                "Metric producer GC: statically disabled pending omicron#5284"
            );
            return json!({
                "error": "metric producer gc disabled (omicron#5284)",
            });
        }

        let Some(expiration) = TimeDelta::from_std(self.lease_duration)
            .ok()
            .and_then(|delta| Utc::now().checked_add_signed(delta))
        else {
            error!(
                opctx.log,
                "Metric producer GC: out of bounds lease_duration";
                "lease_duration" => ?self.lease_duration,
            );
            return json!({
                "error": "out of bounds lease duration",
                "lease_duration": self.lease_duration,
            });
        };

        info!(
            opctx.log, "Metric producer GC running";
            "expiration" => %expiration,
        );
        let pruned = match nexus_metrics_producer_gc::prune_expired_producers(
            opctx,
            &self.datastore,
            expiration,
        )
        .await
        {
            Ok(pruned) => pruned,
            Err(err) => {
                warn!(opctx.log, "Metric producer GC failed"; &err);
                return json!({
                    "error": InlineErrorChain::new(&err).to_string(),
                });
            }
        };

        if pruned.failures.is_empty() {
            info!(
                opctx.log, "Metric producer GC complete (no errors)";
                "expiration" => %expiration,
                "pruned" => ?pruned.successes,
            );
            json!({
                "expiration": expiration,
                "pruned": pruned.successes,
            })
        } else {
            warn!(
                opctx.log,
                "Metric producer GC complete ({} errors)",
                pruned.failures.len();
                "expiration" => %expiration,
                "pruned" => ?pruned.successes,
                "failures" => ?pruned.failures,
            );
            json!({
                "expiration": expiration,
                "pruned": pruned.successes,
                "errors": pruned.failures,
            })
        }
    }
}

impl BackgroundTask for MetricProducerGc {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        self.activate(opctx).boxed()
    }
}
