// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pulling instance state from sled-agents.

use super::common::BackgroundTask;
use crate::Error;
use futures::{future::BoxFuture, FutureExt};
use nexus_db_model::{Sled, SledInstance};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Asset;
use sled_agent_client::Client as SledAgentClient;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Background task that periodically checks instance states.
#[derive(Clone)]
pub(crate) struct InstanceWatcher {
    datastore: Arc<DataStore>,
    resolver: internal_dns::resolver::Resolver,
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
        Self { datastore, resolver }
    }

    fn check_instance(
        &self,
        opctx: &OpContext,
        client: &SledAgentClient,
        instance: SledInstance,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'static {
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
            let InstanceWatcher { datastore, resolver } = watcher;
            slog::trace!(opctx.log, "checking on instance...");
            let rsp = client.instance_get_state(&instance_id).await;
            let state = match rsp {
                Ok(rsp) => rsp.into_inner(),
                Err(ClientError::ErrorResponse(rsp))
                    if rsp.status() == http::StatusCode::NOT_FOUND
                        && rsp.as_ref().error_code.as_deref()
                            == Some("NO_SUCH_INSTANCE") =>
                {
                    slog::info!(opctx.log, "instance is wayyyyy gone");
                    todo!();
                }
                Err(e) => {
                    slog::warn!(
                        opctx.log,
                        "error checking up on instance: {e}"
                    );
                    return Err(e.into());
                }
            };

            slog::debug!(opctx.log, "updating instance state: {state:?}");
            crate::app::instance::notify_instance_updated(
                &datastore,
                &resolver,
                &opctx,
                &opctx,
                &opctx.log,
                &instance_id,
                &state.into(),
            )
            .await
        }
    }
}

struct CheckResult {}

type ClientError = sled_agent_client::Error<sled_agent_client::types::Error>;

impl BackgroundTask for InstanceWatcher {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let mut tasks = tokio::task::JoinSet::new();
            let mut paginator = Paginator::new(MAX_SLED_AGENTS);
            while let Some(p) = paginator.next() {
                let maybe_batch = self
                    .datastore
                    .sled_instance_list_by_sled_agent(
                        opctx,
                        &p.current_pagparams(),
                    )
                    .await;
                let batch = match maybe_batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        slog::warn!(
                            opctx.log,
                            "sled instances by sled agent query failed: {e}"
                        );
                        break;
                    }
                };
                paginator = p.found_batch(&batch, &|(sled, _)| sled.id());
                let mut batch = batch.into_iter();

                if let Some((mut curr_sled, sled_instance)) = batch.next() {
                    let mk_client = |sled: &Sled| {
                        nexus_networking::sled_client_from_address(
                            sled.id(),
                            sled.address(),
                            &opctx.log,
                        )
                    };

                    let mut client = mk_client(&curr_sled);
                    tasks.spawn(self.check_instance(
                        opctx,
                        &client,
                        sled_instance,
                    ));

                    for (sled, sled_instance) in batch {
                        // We're now talking to a new sled agent; update the client.
                        if sled.id() != curr_sled.id() {
                            client = mk_client(&sled);
                            curr_sled = sled;
                        }
                        tasks.spawn(self.check_instance(
                            opctx,
                            &client,
                            sled_instance,
                        ));
                    }
                }
            }

            // All requests fired off, let's wait for them to come back.
            let mut ok = 0;
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(())) => {
                        ok += 1;
                    }
                    Err(e) => unreachable!(
                        "a `JoinError` is returned if a spawned task \
                        panics, or if the task is aborted. we never abort \
                        tasks on this `JoinSet`, and nexus is compiled with \
                        `panic=\"abort\"`, so neither of these cases should \
                        ever occur: {e}",
                    ),
                    Ok(Err(e)) => {}
                }
            }

            slog::trace!(opctx.log, "all instance checks complete");
            serde_json::json!({
                "num_ok": ok,
            })
        }
        .boxed()
    }
}
