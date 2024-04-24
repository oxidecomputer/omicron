// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pulling instance state from sled-agents.

use super::common::BackgroundTask;
use futures::{future::BoxFuture, FutureExt};
use nexus_db_model::{InvSledAgent, SledInstance};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use omicron_common::backoff::{self, BackoffError};
use omicron_uuid_kinds::GenericUuid;
use serde_json::json;
use sled_agent_client::Client as SledAgentClient;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Background task that periodically checks instance states.
#[derive(Clone)]
pub(crate) struct InstanceWatcher {
    datastore: Arc<DataStore>,
    resolver: internal_dns::resolver::Resolver,
    max_retries: NonZeroU32,
}

const MAX_SLED_AGENTS: NonZeroU32 = unsafe {
    // Safety: last time I checked, 100 was greater than zero.
    NonZeroU32::new_unchecked(100)
};

impl InstanceWatcher {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        resolver: internal_dns::resolver::Resolver,
        max_retries: NonZeroU32,
    ) -> Self {
        Self { datastore, resolver, max_retries }
    }

    fn check_instance(
        &self,
        opctx: &OpContext,
        client: &SledAgentClient,
        instance: SledInstance,
    ) -> impl Future<Output = ()> + Send + 'static {
        let instance_id = instance.instance_id();
        let watcher = self.clone();
        let opctx = opctx.child(
            std::iter::once((
                "instance_id".to_string(),
                instance_id.to_string(),
            ))
            .collect(),
        );
        let client = client.clone();

        async move {
            let InstanceWatcher { datastore, resolver, max_retries } = watcher;
            slog::trace!(opctx.log, "checking on instance...");
            let backoff = backoff::retry_policy_internal_service();
            let mut retries = 0;
            let rsp = backoff::retry_notify(
                backoff,
                || async {
                    let rsp = client
                        .instance_get_state(&instance.instance_id())
                        .await;
                    match rsp {
                        Ok(rsp) => Ok(rsp.into_inner()),
                        Err(e) if retries == max_retries.get() => {
                            Err(BackoffError::Permanent(e))
                        }
                        Err(
                            e @ ClientError::InvalidRequest(_)
                            | e @ ClientError::InvalidUpgrade(_)
                            | e @ ClientError::UnexpectedResponse(_)
                            | e @ ClientError::PreHookError(_),
                        ) => Err(BackoffError::Permanent(e)),
                        Err(e) => Err(BackoffError::transient(e)),
                    }
                },
                |err, duration| {
                    slog::info!(
                        opctx.log,
                        "instance check failed; retrying: {err}";
                        "duration" => ?duration,
                        "retries_remaining" => max_retries.get() - retries,
                    );
                },
            )
            .await;
            let state = match rsp {
                Ok(state) => state,
                Err(error) => {
                    // TODO(eliza): here is where it gets interesting --- if the
                    // sled-agent is in a bad state, we need to:
                    // 1. figure out whether the instance's VMM is reachable directly
                    // 2. figure out whether we can recover the sled agent?
                    // 3. if the instances' VMMs are also gone, mark them as
                    //    "failed"
                    // 4. this might mean that the whole sled is super gone,
                    //    figure that out too.
                    //
                    // for now though, we'll just log a really big error.
                    slog::error!(
                        opctx.log,
                        "instance seems to be in a bad state: {error}"
                    );
                    return;
                }
            };

            slog::debug!(opctx.log, "updating instance state: {state:?}");
            let result = crate::app::instance::notify_instance_updated(
                &datastore,
                &resolver,
                &opctx,
                &opctx,
                &opctx.log,
                &instance_id,
                &state.into(),
            )
            .await;
            match result {
                Ok(_) => slog::debug!(opctx.log, "instance state updated"),
                Err(e) => slog::error!(
                    opctx.log,
                    "failed to update instance state: {e}"
                ),
            }
        }
    }
}

type ClientError = sled_agent_client::Error<sled_agent_client::types::Error>;

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

            let mut tasks = tokio::task::JoinSet::new();
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
                    let mk_client = |&InvSledAgent {
                        ref sled_id, sled_agent_ip, sled_agent_port, ..
                    }: &InvSledAgent| {
                        let address = std::net::SocketAddrV6::new(sled_agent_ip.into(), sled_agent_port.into(), 0, 0);
                        nexus_networking::sled_client_from_address(sled_id.into_untyped_uuid(), address, &opctx.log)
                    };

                    let mut client = mk_client(&curr_sled_agent);
                    tasks.spawn(self.check_instance(opctx, &client, sled_instance));

                    for (sled_agent, sled_instance) in batch {
                        // We're now talking to a new sled agent; update the client.
                        if sled_agent.sled_id != curr_sled_agent.sled_id {
                            client = mk_client(&sled_agent);
                            curr_sled_agent = sled_agent;
                        }
                        tasks.spawn(self.check_instance(opctx, &client, sled_instance));
                    }
                }
            }

            // All requests fired off, let's wait for them to come back.
            while let Some(result) = tasks.join_next().await {
                if let Err(e) = result {
                    unreachable!(
                        "a `JoinError` is returned if a spawned task \
                        panics, or if the task is aborted. we never abort \
                        tasks on this `JoinSet`, and nexus is compiled with \
                        `panic=\"abort\"`, so neither of these cases should \
                        ever occur: {e}",
                    );
                }
            }

            slog::trace!(opctx.log, "all instance checks complete");
            serde_json::json!({})
        }
        .boxed()
    }
}
