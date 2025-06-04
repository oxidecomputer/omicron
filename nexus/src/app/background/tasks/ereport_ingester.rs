// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background tasks for ereport ingestion

use crate::app::background::BackgroundTask;
use anyhow::Context;
use chrono::Utc;
use ereport_types::Ena;
use ereport_types::Ereport;
use ereport_types::EreportId;
use futures::future::BoxFuture;
use internal_dns_types::names::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::EreporterStatus;
use nexus_types::internal_api::background::Sp;
use nexus_types::internal_api::background::SpEreportIngesterStatus;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use parallel_task_set::ParallelTaskSet;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;
use std::sync::Arc;

pub struct SpEreportIngester {
    resolver: internal_dns_resolver::Resolver,
    inner: Ingester,
}

#[derive(Clone)]
struct Ingester {
    datastore: Arc<DataStore>,
    nexus_id: OmicronZoneUuid,
}

impl BackgroundTask for SpEreportIngester {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let status =
                self.actually_activate(opctx).await.unwrap_or_else(|error| {
                    SpEreportIngesterStatus {
                        error: Some(error.to_string()),
                        ..Default::default()
                    }
                });
            serde_json::json!(status)
        })
    }
}

impl SpEreportIngester {
    #[must_use]
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: internal_dns_resolver::Resolver,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        Self { resolver, inner: Ingester { datastore, nexus_id } }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> anyhow::Result<SpEreportIngesterStatus> {
        // Find MGS clients.
        // TODO(eliza): reuse the same client across activations; qorb, etc.
        let mgs_clients = self
            .resolver
            .lookup_all_socket_v6(ServiceName::ManagementGatewayService)
            .await
            .context("looking up MGS addresses")?
            .into_iter()
            .map(|addr| {
                let url = format!("http://{addr}");
                let log = opctx.log.new(o!("gateway_url" => url.clone()));
                let client = gateway_client::Client::new(&url, log);
                GatewayClient { addr, client }
            })
            .collect::<Arc<[_]>>();

        // TODO(eliza): what seems like an appropriate parallelism? should we
        // just do 16?
        let mut tasks = ParallelTaskSet::new();
        let mut status = SpEreportIngesterStatus::default();

        let sleds =
            (0..32u16).map(|slot| (nexus_types::inventory::SpType::Sled, slot));
        let switches = (0..2u16)
            .map(|slot| (nexus_types::inventory::SpType::Switch, slot));
        let pscs =
            (0..2u16).map(|slot| (nexus_types::inventory::SpType::Power, slot));

        for (sp_type, slot) in switches.chain(pscs).chain(sleds) {
            let sp_result = tasks
                .spawn({
                    let opctx = opctx.child(BTreeMap::from([
                        // XXX(eliza): that's so many little strings... :(
                        ("sp_type".to_string(), sp_type.to_string()),
                        ("slot".to_string(), slot.to_string()),
                    ]));
                    let clients = mgs_clients.clone();
                    let ingester = self.inner.clone();
                    async move {
                        let status = ingester
                            .ingest_sp_ereports(opctx, &clients, sp_type, slot)
                            .await?;
                        Some((Sp { sp_type, slot }, status))
                    }
                })
                .await;
            if let Some(Some((sp, sp_status))) = sp_result {
                status.sps.insert(sp, sp_status);
            }
        }

        // Wait for remaining ingestion tasks to come back.
        while let Some(sp_result) = tasks.join_next().await {
            if let Some((sp, sp_status)) = sp_result {
                status.sps.insert(sp, sp_status);
            }
        }

        Ok(status)
    }
}

struct GatewayClient {
    addr: SocketAddrV6,
    client: gateway_client::Client,
}

const LIMIT: std::num::NonZeroU32 = match std::num::NonZeroU32::new(255) {
    None => unreachable!(),
    Some(l) => l,
};

impl Ingester {
    async fn ingest_sp_ereports(
        &self,
        opctx: OpContext,
        clients: &[GatewayClient],
        sp_type: nexus_types::inventory::SpType,
        slot: u16,
    ) -> Option<EreporterStatus> {
        // Fetch the latest ereport from this SP.
        let latest = match self
            .datastore
            .sp_latest_ereport_id(&opctx, sp_type, slot)
            .await
        {
            Ok(latest) => latest,
            Err(error) => {
                return Some(EreporterStatus {
                    errors: vec![format!(
                        "failed to query for latest ereport: {error}"
                    )],
                    ..Default::default()
                });
            }
        };

        let mut params = EreportQueryParams::from_latest(latest);
        let mut status = None;
        while let Some(gateway_client::types::Ereports {
            restart_id,
            items,
            next_page: _,
        }) = self
            .mgs_requests(&opctx, clients, &params, sp_type, slot, &mut status)
            .await
        {
            if items.is_empty() {
                slog::trace!(
                    &opctx.log,
                    "no ereports returned by SP";
                    "committed_ena" => ?params.committed_ena,
                    "start_ena" => ?params.start_ena,
                    "restart_id" => ?params.restart_id,
                    "total_ereports_received" => status
                        .as_ref()
                        .map(|s| s.ereports_received),
                    "total_new_ereports" => status
                        .as_ref()
                        .map(|s| s.new_ereports),
                );
                break;
            }
            let db_ereports = items
                .into_iter()
                .map(|ereport| {
                    let part_number = get_sp_metadata_string(
                        "baseboard_part_number",
                        &ereport,
                        &restart_id,
                        &opctx.log,
                    );
                    let serial_number = get_sp_metadata_string(
                        "baseboard_serial_number",
                        &ereport,
                        &restart_id,
                        &opctx.log,
                    );
                    db::model::SpEreport {
                        restart_id: restart_id.into(),
                        ena: ereport.ena.into(),
                        time_collected: Utc::now(),
                        collector_id: self.nexus_id.into(),
                        sp_type: sp_type.into(),
                        sp_slot: slot.into(),
                        part_number,
                        serial_number,
                        report: serde_json::Value::Object(ereport.data),
                    }
                })
                .collect::<Vec<_>>();
            let received = db_ereports.len();
            let status = status.get_or_insert_default();
            status.ereports_received += received;
            let created = match self
                .datastore
                .sp_ereports_insert(&opctx, sp_type, slot, db_ereports)
                .await
            {
                Ok((created, latest)) => {
                    params = EreportQueryParams::from_latest(latest);
                    created
                }
                Err(e) => {
                    slog::error!(
                        &opctx.log,
                        "error inserting {received} ereports!";
                        "committed_ena" => ?params.committed_ena,
                        "start_ena" => ?params.start_ena,
                        "restart_id" => ?params.restart_id,
                        "ereports_received" => received,
                        "error" => %e,
                    );
                    status.errors.push(format!(
                        "failed to insert {received} ereports: {e}"
                    ));
                    break;
                }
            };
            status.new_ereports += created;
            slog::info!(
                &opctx.log,
                "collected ereports from MGS";
                "requested_committed_ena" => ?params.committed_ena,
                "requested_start_ena" => ?params.start_ena,
                "requested_restart_id" => ?params.restart_id,
                "received_restart_id" => ?restart_id,
                "ereports_received" => received,
                "new_ereports" => created,
                "total_ereports_collected" => status.ereports_received,
                "total_new_ereports" => status.new_ereports,
                "total_requests" => status.requests,
            );
        }

        status
    }

    async fn mgs_requests(
        &self,
        opctx: &OpContext,
        clients: &[GatewayClient],
        EreportQueryParams { ref committed_ena,ref start_ena, restart_id }: &EreportQueryParams,
        sp_type: nexus_types::inventory::SpType,
        slot: u16,
        status: &mut Option<EreporterStatus>,
    ) -> Option<gateway_client::types::Ereports> {
        // If an attempt to collect ereports from one gateway fails, we will try
        // any other discovered gateways.
        for GatewayClient { addr, client } in clients.iter() {
            slog::debug!(
                &opctx.log,
                "attempting ereport collection from MGS";
                "sp_type" => ?sp_type,
                "slot" => slot,
                "committed_ena" => ?committed_ena,
                "start_ena" => ?start_ena,
                "restart_id" => ?restart_id,
                "gateway_addr" => %addr,
            );
            let res = client
                .sp_ereports_ingest(
                    sp_type,
                    slot as u32,
                    committed_ena.as_ref(),
                    LIMIT,
                    &restart_id,
                    start_ena.as_ref(),
                )
                .await;
            match res {
                Ok(ereports) => {
                    status.get_or_insert_default().requests += 1;
                    return Some(ereports.into_inner());
                }
                Err(e) if e.status() == Some(http::StatusCode::NOT_FOUND) => {
                    slog::debug!(
                        &opctx.log,
                        "ereport collection: MGS claims there is no SP in this slot";
                        "sp_type" => ?sp_type,
                        "slot" => slot,
                        "committed_ena" => ?committed_ena,
                        "start_ena" => ?start_ena,
                        "restart_id" => ?restart_id,
                        "gateway_addr" => %addr,
                    );
                }
                Err(e) => {
                    let stats = status.get_or_insert_default();
                    stats.requests += 1;
                    stats.errors.push(format!("MGS {addr}: {e}"));
                }
            }
        }

        None
    }
}

/// Attempt to extract a VPD metadata from an SP ereport, logging a warning if
/// it's missing. We still want to keep such ereports, as the error condition
/// could be that the SP couldn't determine the metadata field, but it's
/// uncomfortable, so we ought to complain about it.
fn get_sp_metadata_string(
    key: &str,
    ereport: &Ereport,
    restart_id: &EreporterRestartUuid,
    log: &slog::Logger,
) -> Option<String> {
    match ereport.data.get(key) {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(v) => {
            slog::warn!(
                &log,
                "malformed ereport: value for '{key}' should be a string, \
                 but found: {v:?}";
                "ena" => ?ereport.ena,
                "restart_id" => ?restart_id,
            );
            None
        }
        None => {
            slog::warn!(
                &log,
                "ereport missing '{key}'; perhaps the SP doesn't know its own \
                 VPD!";
                "ena" => ?ereport.ena,
                "restart_id" => ?restart_id,
            );
            None
        }
    }
}

struct EreportQueryParams {
    committed_ena: Option<Ena>,
    start_ena: Option<Ena>,
    restart_id: EreporterRestartUuid,
}

impl EreportQueryParams {
    fn from_latest(latest: Option<EreportId>) -> Self {
        match latest {
            Some(ereport_types::EreportId { ena, restart_id }) => Self {
                committed_ena: Some(ena),
                start_ena: Some(Ena(ena.0 + 1)),
                restart_id,
            },
            None => Self {
                committed_ena: None,
                start_ena: None,
                restart_id: EreporterRestartUuid::nil(),
            },
        }
    }
}
