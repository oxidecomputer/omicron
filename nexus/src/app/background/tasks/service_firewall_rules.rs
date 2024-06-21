// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating VPC firewall rules for Omicron services.
//!
//! This is intended to propagate only the rules related to Oxide-managed
//! programs, like Nexus or external DNS. These are special -- they are very
//! unlikely to change and also relatively small. This task is not intended to
//! handle general changes to customer-visible VPC firewalls, and is mostly in
//! place to propagate changes in the IP allowlist for user-facing services.

use crate::app::background::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use std::sync::Arc;

pub struct ServiceRulePropagator {
    datastore: Arc<DataStore>,
}

impl ServiceRulePropagator {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

impl BackgroundTask for ServiceRulePropagator {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = opctx
                .log
                .new(slog::o!("component" => "service-firewall-rule-progator"));
            debug!(
                log,
                "starting background task for service \
                firewall rule propagation"
            );
            let start = std::time::Instant::now();
            let res = nexus_networking::plumb_service_firewall_rules(
                &self.datastore,
                opctx,
                &[],
                opctx,
                &log,
            )
            .await;
            if let Err(e) = res {
                error!(
                    log,
                    "failed to propagate service firewall rules";
                    "error" => ?e,
                );
                serde_json::json!({"error" : e.to_string()})
            } else {
                // No meaningful data to return, the duration is already
                // captured by the driver itself.
                debug!(
                    log,
                    "successfully propagated service firewall rules";
                    "elapsed" => ?start.elapsed()
                );
                serde_json::json!({})
            }
        }
        .boxed()
    }
}
