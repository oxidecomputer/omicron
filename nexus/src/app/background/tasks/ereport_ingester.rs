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
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
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
        // TODO(eliza): reuse the same client across activations...
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

async fn mgs_request(
    opctx: &OpContext,
    clients: &[gateway_client::Client],
    Sp { sp_type, slot }: Sp,
    sp_state: Option<SpState>,
) -> anyhow::Result<()> {
    let mut idx = 0;
    let restart_id = sp_state
        .as_ref()
        .map(|state| state.restart_id.clone())
        .unwrap_or(EreporterRestartUuid::nil());
    backoff::retry_notify_ext(
        backoff::retry_policy_internal_service(),
        || async {
            let client = &clients[idx];
            let res = client
                .sp_ereports_ingest(
                    sp_type,
                    slot,
                    sp_state.as_ref().map(|state| &state.committed_ena),
                    LIMIT,
                    &restart_id,
                    sp_state.as_ref().map(|state| &state.committed_ena),
                )
                .await;

            Ok(())
        },
        |err, count, duration| todo!(),
    )
    .await
}
