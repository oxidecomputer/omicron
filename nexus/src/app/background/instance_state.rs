// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pulling instance state from sled-agents.

use super::common::BackgroundTask;
use futures::{future::BoxFuture, FutureExt};
use nexus_db_model::{InvSledAgent, SledInstance};
use nexus_db_queries::{
    context::OpContext, db::pagination::Paginator, db::DataStore,
};
use serde::Serialize;
use serde_json::json;
use sled_agent_client::{types::SledInstanceState, Client as SledAgentClient};
use std::num::NonZeroU32;
use std::sync::Arc;

/// Background task that periodically checks instance states.
pub struct InstanceWatcher {
    datastore: Arc<DataStore>,
}

const MAX_SLED_AGENTS: NonZeroU32 = unsafe {
    // Safety: last time I checked, 100 was greater than zero.
    NonZeroU32::new_unchecked(100)
};

impl BackgroundTask for InstanceWatcher {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let latest_collection = {
                let maybe_id = self
                    .datastore
                    .inventory_get_latest_collection_id(opctx)
                    .await;
                match maybe_id {
                    Ok(Some(collection)) => collection,
                    Ok(None) => {
                        slog::debug!(opctx.log, "no inventory collection exists, not querying sled agents.");
                        return json!({});
                        }
                    Err(e) => {
                        slog::warn!(opctx.log, "failed to get latest collection ID: {e}");
                        return json!({});
                    }
                }
            };

            let mut requests = tokio::task::JoinSet::new();
            let mut paginator = Paginator::new(MAX_SLED_AGENTS);
            while let Some(p) = paginator.next() {
                let maybe_batch = self.datastore.sled_instance_list_by_sled_agent(
                    opctx,
                    latest_collection,
                    &p.current_pagparams(),
                ).await;
                let batch = match maybe_batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        slog::warn!(opctx.log, "sled instances by sled agent query failed: {e}");
                        break;
                    }
                };
                paginator = p.found_batch(&batch, &|(sled_agent, _)| sled_agent.sled_id);
                let mut batch = batch.into_iter();

                if let Some((mut curr_sled_agent, sled_instance)) = batch.next() {
                    let mut client = mk_sled_agent_client(&opctx.log, &curr_sled_agent);

                    for (sled_agent, sled_instance) in batch {
                        // We're now talking to a new sled agent; update the client.
                        if sled_agent.sled_id != curr_sled_agent.sled_id {
                            client = mk_sled_agent_client(&opctx.log, &sled_agent);
                            curr_sled_agent = sled_agent;
                        }
                        spawn_get_state(&client, &mut requests, sled_instance);
                    }
                }
            }

            // All requests fired off, let's wait for them to come back.
            while let Some(result) = requests.join_next().await {
                match result {
                    Err(_) => unreachable!(
                        "a `JoinError` is returned if a spawned task \
                        panics, or if the task is aborted. we never abort \
                        tasks on this `JoinSet`, and nexus is compiled with \
                        `panic=\"abort\"`, so neither of these cases should \
                        ever occur."
                    ),
                    Ok(Ok(rsp)) => {
                        todo!("eliza");
                    }
                    Ok(Err(e)) => {
                        // Here is where it gets interesting. This is where we
                        // might learn that the sled-agent we were trying to
                        // talk to is dead.
                        todo!("eliza: implement the interesting parts!");
                    }
                };
            }

            todo!()
        }
        .boxed()
    }
}

type ClientError = sled_agent_client::Error<sled_agent_client::types::Error>;

fn spawn_get_state(
    client: &SledAgentClient,
    tasks: &mut tokio::task::JoinSet<
        Result<(SledInstance, SledInstanceState), ClientError>,
    >,
    instance: SledInstance,
) {
    let client = client.clone();
    tasks.spawn(async move {
        let state = client
            .instance_get_state(&instance.instance_id())
            .await?
            .into_inner();
        Ok((instance, state))
    });
}

fn mk_sled_agent_client(
    log: &slog::Logger,
    InvSledAgent {
        ref sled_id, ref sled_agent_ip, ref sled_agent_port, ..
    }: &InvSledAgent,
) -> SledAgentClient {
    // Ipv6Addr's `fmt::Debug` impl is the same as its Display impl, so we
    // should get the RFC 5952 textual representation here even though the DB
    // `Ipv6Addr` type doesn't expose `Display`.
    let url = format!("http://{sled_agent_ip:?}:{sled_agent_port}");
    let log =
        log.new(o!("sled_id" => sled_id.to_string(), "url" => url.clone()));
    SledAgentClient::new(&url, log)
}
