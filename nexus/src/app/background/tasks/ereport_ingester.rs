// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background tasks for ereport ingestion

use crate::app::background::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use internal_dns_types::names::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::backoff;
use omicron_common::backoff::BackoffError;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use parallel_task_set::ParallelTaskSet;
use std::sync::Arc;
use uuid::Uuid;

pub struct SpEreportIngester {
    pub datastore: Arc<DataStore>,
    pub resolver: internal_dns_resolver::Resolver,
    pub nexus_id: OmicronZoneUuid,
    pub rack_id: Uuid,
}

impl BackgroundTask for SpEreportIngester {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        todo!()
    }
}

impl SpEreportIngester {
    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> anyhow::Result<()> {
        // Find MGS clients.
        // TODO(eliza): reuse the same client across activations; qorb, etc.
        let mgs_clients = self
            .resolver
            .lookup_all_socket_v6(ServiceName::ManagementGatewayService)
            .await
            .context("looking up MGS addresses")?
            .into_iter()
            .map(|sockaddr| {
                let url = format!("http://{}", sockaddr);
                let log = opctx.log.new(o!("gateway_url" => url.clone()));
                gateway_client::Client::new(&url, log)
            })
            .collect::<Vec<_>>();

        // let mut tasks = ParallelTaskSet::new();
        // for sled in 0..32 {}

        Ok(())
    }
}

struct Sp {
    sp_type: gateway_client::types::SpType,
    slot: u32,
}

struct SpState {
    committed_ena: ereport_types::Ena,
    restart_id: EreporterRestartUuid,
}

const LIMIT: std::num::NonZeroU32 = match std::num::NonZeroU32::new(255) {
    None => unreachable!(),
    Some(l) => l,
};

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

async fn mgs_request(
    opctx: &OpContext,
    clients: Arc<Vec<gateway_client::Client>>,
    Sp { sp_type, slot }: Sp,
    sp_state: Option<SpState>,
) -> Result<gateway_client::types::Ereports, GatewayClientError> {
    let mut next_idx = 0;

    let (committed_ena, restart_id) = match sp_state {
        Some(state) => {
            (Some(state.committed_ena.clone()), state.restart_id.clone())
        }
        None => (None, EreporterRestartUuid::nil()),
    };

    backoff::retry_notify_ext(
        backoff::retry_policy_internal_service(),
        || {
            let clients = clients.clone();
            let idx = next_idx;
            // Next time, we'll try the other gateway, if this one is sad.
            next_idx = (next_idx + 1) % clients.len();
            async move {
                let client = &clients[idx];
                let res = client
                    .sp_ereports_ingest(
                        sp_type,
                        slot,
                        committed_ena.as_ref(),
                        LIMIT,
                        &restart_id,
                        committed_ena.as_ref(),
                    )
                    .await;
                match res {
                    Ok(ereports) => Ok(ereports.into_inner()),
                    Err(e)
                        if e.status()
                            .map(|status| status.is_client_error())
                            .unwrap_or(false) =>
                    {
                        Err(BackoffError::permanent(e))
                    }
                    Err(
                        e @ gateway_client::Error::InvalidRequest(_)
                        | e @ gateway_client::Error::PreHookError(_)
                        | e @ gateway_client::Error::PostHookError(_),
                    ) => Err(BackoffError::permanent(e)),
                    Err(e) => return Err(BackoffError::transient(e)),
                }
            }
        },
        |err, count, duration| {
            // TODO(eliza): log the error 'n' stuff
        },
    )
    .await
}
