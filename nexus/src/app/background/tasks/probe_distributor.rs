// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for distributing networking probe zones to sleds.

use crate::app::background::BackgroundTask;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_networking::sled_client_from_address;
use nexus_types::deployment::SledFilter;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::ProbeDistributorStatus;
use nexus_types::internal_api::background::ProbeError;
use serde_json::json;
use sled_agent_client::types::ProbeSet;
use slog_error_chain::InlineErrorChain;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

pub struct ProbeDistributor {
    datastore: Arc<DataStore>,
    // Notify listeners when we're completed. There's currently no data to
    // share, so this is an empty notification.
    tx: watch::Sender<()>,
}

impl ProbeDistributor {
    pub fn new(datastore: Arc<DataStore>, tx: watch::Sender<()>) -> Self {
        Self { datastore, tx }
    }
}

impl BackgroundTask for ProbeDistributor {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            info!(log, "distributing networking probes to sleds");

            let sleds = match self
                .datastore
                .sled_list_all_batched(opctx, SledFilter::InService)
                .await
            {
                Ok(sleds) => sleds,
                Err(e) => {
                    let msg = format!(
                        "failed to enumerate sleds: {}",
                        InlineErrorChain::new(&e)
                    );
                    error!(&log, "{msg}");
                    return json!({"error": msg});
                }
            }
            .into_iter();

            // For each sled, list all the probes that it should have and notify
            // the sled-agent about them.
            let mut status = ProbeDistributorStatus {
                probes_by_sled: HashMap::new(),
                errors: Vec::new(),
            };
            let mut n_pushed_probes: usize = 0;
            for sled in sleds.into_iter() {
                let probes = match self
                    .datastore
                    .list_all_probe_create_params_for_sled_batched(
                        opctx,
                        sled.id(),
                    )
                    .await
                {
                    Ok(probes) => probes,
                    Err(e) => {
                        const MSG: &str = "failed to list or construct create \
                            parameters for networking probes";
                        let ec = InlineErrorChain::new(&e);
                        let msg = format!("{MSG}: {ec}");
                        error!(log, "{MSG}"; "error" => ec);
                        status.errors.push(ProbeError {
                            sled_id: sled.id(),
                            sled_ip: *sled.address().ip(),
                            error: msg,
                        });
                        continue;
                    }
                };

                let client =
                    sled_client_from_address(sled.id(), sled.address(), &log);

                // PUT the entire set of probes on the sled-agent.
                //
                // Using this atomic update endoint is important. If we post
                // each individual probe, it's both expensive and difficult to
                // get right, especially around _deleting_ probes. How do we
                // know when to tell the sled-agent to delete a probe? We either
                // need to explicitly make a DELETE request for every
                // soft-deleted probe in the database, or ask the sled-agent
                // what probes it currently has and delete those we don't want.
                // Instead, let the sled-agent do that itself.
                let n_probes = probes.len();
                match client.probes_put(&ProbeSet { probes }).await {
                    Ok(_) => {
                        debug!(
                            log,
                            "Update set of probes on sled";
                            "sled_id" => %sled.id(),
                            "n_probes" => n_probes,
                        );
                        status.probes_by_sled.insert(sled.id(), n_probes);
                        n_pushed_probes += n_probes;
                    }
                    Err(e) => {
                        const MSG: &str = "failed to update probes on sled";
                        let ec = InlineErrorChain::new(&e);
                        let msg = format!("{MSG}: {ec}");
                        error!(log, "{MSG}"; "error" => ec);
                        status.errors.push(ProbeError {
                            sled_id: sled.id(),
                            sled_ip: sled.ip(),
                            error: msg,
                        });
                    }
                }
            }
            info!(
                log,
                "finished distributing probes to sleds";
                "n_pushed_probes" => n_pushed_probes,
            );
            if self.tx.send(()).is_err() {
                error!(
                    log,
                    "failed to notify watching tasks, \
                    all receivers have been droppped"
                );
            }
            serde_json::json!(status)
        }
        .boxed()
    }
}
