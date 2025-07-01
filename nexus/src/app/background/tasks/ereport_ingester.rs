// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background tasks for ereport ingestion.
//!
//! The tasks in this module implement the Nexus portion of the ereport
//! ingestion protocol [described in RFD 520][rfd520].
//!
//! [rfd520]: https://rfd.shared.oxide.computer/rfd/520#_determinations

use crate::app::background::BackgroundTask;
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
use nexus_types::internal_api::background::SpEreportIngesterStatus;
use nexus_types::internal_api::background::SpEreporterStatus;
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
            let status = self.actually_activate(opctx).await;
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
    ) -> SpEreportIngesterStatus {
        let mut status = SpEreportIngesterStatus::default();
        // Find MGS clients.
        // TODO(eliza): reuse the same client across activations; qorb, etc.
        let mgs_clients = {
            let lookup = self
                .resolver
                .lookup_all_socket_v6(ServiceName::ManagementGatewayService)
                .await;
            let addrs = match lookup {
                Err(error) => {
                    const MSG: &str = "failed to resolve MGS addresses";
                    error!(opctx.log, "{MSG}"; "error" => ?error);
                    status.errors.push(format!("{MSG}: {error}"));
                    return status;
                }
                Ok(addrs) => addrs,
            };

            addrs
                .into_iter()
                .map(|addr| {
                    let url = format!("http://{addr}");
                    let log = opctx.log.new(o!("gateway_url" => url.clone()));
                    let client = gateway_client::Client::new(&url, log);
                    GatewayClient { addr, client }
                })
                .collect::<Arc<[_]>>()
        };

        if mgs_clients.is_empty() {
            const MSG: &str = "no MGS addresses resolved";
            error!(opctx.log, "{MSG}");
            status.errors.push(MSG.to_string());
            return status;
        };

        // Ask MGS for the list of all SP identifiers. If a request to the first
        // gateway fails, we'll try again for every resolved MGS before giving
        // up.
        let sps = {
            let mut gateways = mgs_clients.iter();
            loop {
                let Some(GatewayClient { addr, client }) = gateways.next()
                else {
                    const MSG: &str = "no MGS successfully returned SP ID list";
                    error!(opctx.log, "{MSG}");
                    status.errors.push(MSG.to_string());
                    return status;
                };
                match client.sp_all_ids().await {
                    Ok(ids) => break ids.into_inner(),
                    Err(err) => {
                        const MSG: &str = "failed to list SP IDs from MGS";
                        warn!(
                            opctx.log,
                            "{MSG}";
                            "error" => %err,
                            "gateway_addr" => %addr,
                        );
                        status.errors.push(format!("{MSG} ({addr}): {err}"));
                    }
                }
            }
        };

        // TODO(eliza): what seems like an appropriate parallelism? should we
        // just do 16?
        let mut tasks = ParallelTaskSet::new();

        for gateway_client::types::SpIdentifier { type_, slot } in sps {
            let sp_result = tasks
                .spawn({
                    let opctx = opctx.child(BTreeMap::from([
                        // XXX(eliza): that's so many little strings... :(
                        ("sp_type".to_string(), type_.to_string()),
                        ("slot".to_string(), slot.to_string()),
                    ]));
                    let clients = mgs_clients.clone();
                    let ingester = self.inner.clone();
                    async move {
                        let status = ingester
                            .ingest_sp_ereports(opctx, &clients, type_, slot)
                            .await?;
                        Some(SpEreporterStatus { sp_type: type_, slot, status })
                    }
                })
                .await;
            if let Some(Some(sp_status)) = sp_result {
                status.sps.push(sp_status);
            }
        }

        // Wait for remaining ingestion tasks to come back.
        while let Some(sp_result) = tasks.join_next().await {
            if let Some(sp_status) = sp_result {
                status.sps.push(sp_status);
            }
        }

        // Sort statuses for consistent output in OMDB commands.
        status.sps.sort_unstable_by_key(|sp| (sp.sp_type, sp.slot));

        status
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
                        "failed to query for latest ereport: {error:#}"
                    )],
                    ..Default::default()
                });
            }
        };

        let mut params = EreportQueryParams::from_latest(latest);
        let mut status = None;

        // Continue requesting ereports from this SP in a loop until we have
        // received all its ereports.
        while let Some(gateway_client::types::Ereports {
            restart_id,
            items,
            next_page: _,
        }) = self
            .mgs_requests(&opctx, clients, &params, sp_type, slot, &mut status)
            .await
        {
            if items.is_empty() {
                if let Some(ref mut status) = status {
                    status.requests += 1;
                }
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
            } else {
                status.get_or_insert_default().requests += 1;
            }
            let db_ereports = items
                .into_iter()
                .map(|ereport| {
                    const MISSING_VPD: &str =
                        " (perhaps the SP doesn't know its own VPD?)";
                    let part_number = get_sp_metadata_string(
                        "baseboard_part_number",
                        &ereport,
                        &restart_id,
                        &opctx.log,
                        MISSING_VPD,
                    );
                    let serial_number = get_sp_metadata_string(
                        "baseboard_serial_number",
                        &ereport,
                        &restart_id,
                        &opctx.log,
                        MISSING_VPD,
                    );
                    let class = get_sp_metadata_string(
                        "class",
                        &ereport,
                        &restart_id,
                        &opctx.log,
                        "",
                    );

                    db::model::SpEreport {
                        restart_id: restart_id.into(),
                        ena: ereport.ena.into(),
                        time_collected: Utc::now(),
                        time_deleted: None,
                        collector_id: self.nexus_id.into(),
                        sp_type: sp_type.into(),
                        sp_slot: slot.into(),
                        part_number,
                        serial_number,
                        class,
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
                        "failed to insert {received} ereports: {e:#}"
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
                "committed_ena" => ?committed_ena,
                "start_ena" => ?start_ena,
                "restart_id" => ?restart_id,
                "gateway_addr" => %addr,
            );
            let res = client
                .sp_ereports_ingest(
                    sp_type,
                    slot,
                    committed_ena.as_ref(),
                    LIMIT,
                    &restart_id,
                    start_ena.as_ref(),
                )
                .await;
            match res {
                Ok(ereports) => {
                    return Some(ereports.into_inner());
                }
                Err(gateway_client::Error::ErrorResponse(rsp))
                    if rsp.error_code.as_deref() == Some("InvalidSp") =>
                {
                    slog::debug!(
                        &opctx.log,
                        "ereport collection: MGS claims there is no SP in this slot";
                        "committed_ena" => ?committed_ena,
                        "start_ena" => ?start_ena,
                        "restart_id" => ?restart_id,
                        "gateway_addr" => %addr,
                    );
                }
                Err(e) => {
                    slog::warn!(
                        &opctx.log,
                        "ereport collection: unanticipated MGS request error: {e:#}";
                        "committed_ena" => ?committed_ena,
                        "start_ena" => ?start_ena,
                        "restart_id" => ?restart_id,
                        "gateway_addr" => %addr,
                        "error" => ?e,
                    );
                    let stats = status.get_or_insert_default();
                    stats.requests += 1;
                    stats.errors.push(format!("MGS {addr}: {e:#}"));
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
    extra_context: &'static str,
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
                "ereport missing '{key}'{extra_context}";
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

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_queries::db::pagination::Paginator;
    use nexus_test_utils_macros::nexus_test;
    use omicron_uuid_kinds::GenericUuid;
    use uuid::uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_sp_ereport_ingestion(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let mut ingester = SpEreportIngester::new(
            datastore.clone(),
            nexus.internal_resolver.clone(),
            nexus.id(),
        );

        let activation1 = ingester.actually_activate(&opctx).await;
        assert!(
            activation1.errors.is_empty(),
            "activation 1 should succeed without errors, but saw: {:#?}",
            activation1.errors
        );
        dbg!(&activation1);
        assert_eq!(
            activation1.sps.len(),
            3,
            "ereports from 3 SPs should be observed: {:?}",
            activation1.sps,
        );

        for SpEreporterStatus { sp_type, slot, status } in &activation1.sps {
            assert_eq!(
                &status.errors,
                &Vec::<String>::new(),
                "there should be no errors from SP {sp_type:?} {slot}",
            );
            assert_eq!(
                status.requests, 2,
                "two HTTP requests should have been sent for SP {sp_type:?} {slot}",
            );
        }

        // Now, let's check that the ereports exist in the database.

        let sled0_restart_id = EreporterRestartUuid::from_untyped_uuid(uuid!(
            "af1ebf85-36ba-4c31-bbec-b9825d6d9d8b"
        ));
        let sled1_restart_id = EreporterRestartUuid::from_untyped_uuid(uuid!(
            "55e30cc7-a109-492f-aca9-735ed725df3c"
        ));

        let sled0_ereports = [
            (
                Ena::from(1),
                serde_json::json!({
                    "baseboard_part_number": "SimGimletSp",
                    "baseboard_serial_number": "SimGimlet00",
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "task_apollo_server",
                    "hubris_task_gen": 13,
                    "hubris_uptime_ms": 1233,
                    "ereport_message_version": 0,
                    "class": "gov.nasa.apollo.o2_tanks.stir.begin",
                    "message": "stirring the tanks",
                }),
            ),
            (
                Ena::from(2),
                serde_json::json!({
                    "baseboard_part_number": "SimGimletSp",
                    "baseboard_serial_number": "SimGimlet00",
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "drv_ae35_server",
                    "hubris_task_gen": 1,
                    "hubris_uptime_ms": 1234,
                    "ereport_message_version": 0,
                    "class": "io.discovery.ae35.fault",
                    "message": "i've just picked up a fault in the AE-35 unit",
                    "de": {
                        "scheme": "fmd",
                        "mod-name": "ae35-diagnosis",
                        "authority": {
                            "product-id": "HAL-9000-series computer",
                            "server-id": "HAL 9000",
                        },
                    },
                    "hours_to_failure": 72,
                }),
            ),
            (
                Ena::from(3),
                serde_json::json!({
                    "baseboard_part_number": "SimGimletSp",
                    "baseboard_serial_number": "SimGimlet00",
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "task_apollo_server",
                    "hubris_task_gen": 13,
                    "hubris_uptime_ms": 1237,
                    "ereport_message_version": 0,
                    "class": "gov.nasa.apollo.fault",
                    "message": "houston, we have a problem",
                    "crew": [
                        "Lovell",
                        "Swigert",
                        "Haise",
                    ],
                }),
            ),
            (
                Ena::from(4),
                serde_json::json!({
                    "baseboard_part_number": "SimGimletSp",
                    "baseboard_serial_number": "SimGimlet00",
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "drv_thingy_server",
                    "hubris_task_gen": 2,
                    "hubris_uptime_ms": 1240,
                    "ereport_message_version": 0,
                    "class": "flagrant_error",
                    "computer": false,
                }),
            ),
            (
                Ena::from(5),
                serde_json::json!({
                    "baseboard_part_number": "SimGimletSp",
                    "baseboard_serial_number": "SimGimlet00",
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "task_latex_server",
                    "hubris_task_gen": 1,
                    "hubris_uptime_ms": 1245,
                    "ereport_message_version": 0,
                    "class": "overfull_hbox",
                    "badness": 10000,
                }),
            ),
        ];

        let sled1_ereports = [(
            Ena::from(1),
            serde_json::json!({
                "baseboard_part_number": "SimGimletSp",
                "baseboard_serial_number": "SimGimlet01",
                "hubris_archive_id": "ffffffff",
                "hubris_version": "0.0.2",
                "hubris_task_name": "task_thermal_server",
                "hubris_task_gen": 1,
                "hubris_uptime_ms": 1233,
                "ereport_message_version": 0,
                "class": "computer.oxide.gimlet.chassis_integrity.fault",
                "nosub_class": "chassis_integrity.cat_hair_detected",
                "message": "cat hair detected inside gimlet",
                "de": {
                    "scheme": "fmd",
                    "mod-name": "hubris-thermal-diagnosis",
                    "mod-version": "1.0",
                    "authority": {
                        "product-id": "oxide",
                        "server-id": "SimGimlet1",
                    }
                },
                "certainty": 0x64,
                "cat_hair_amount": 10000,
            }),
        )];

        check_sp_ereports_exist(
            datastore,
            &opctx,
            sled0_restart_id,
            &sled0_ereports,
        )
        .await;
        check_sp_ereports_exist(
            datastore,
            &opctx,
            sled1_restart_id,
            &sled1_ereports,
        )
        .await;

        // Activate the task again and assert that no new ereports were
        // ingested.
        let activation2 = ingester.actually_activate(&opctx).await;
        assert!(
            activation2.errors.is_empty(),
            "activation 2 should succeed without errors, but saw: {:#?}",
            activation2.errors
        );
        dbg!(&activation2);

        assert_eq!(activation2.sps, &[], "no new ereports should be observed");

        check_sp_ereports_exist(
            datastore,
            &opctx,
            sled0_restart_id,
            &sled0_ereports,
        )
        .await;
        check_sp_ereports_exist(
            datastore,
            &opctx,
            sled1_restart_id,
            &sled1_ereports,
        )
        .await;
    }

    async fn check_sp_ereports_exist(
        datastore: &DataStore,
        opctx: &OpContext,
        restart_id: EreporterRestartUuid,
        expected_ereports: &[(Ena, serde_json::Value)],
    ) {
        let mut paginator =
            Paginator::new(std::num::NonZeroU32::new(100).unwrap());
        let mut found_ereports = BTreeMap::new();
        while let Some(p) = paginator.next() {
            let batch = datastore
                .sp_ereport_list_by_restart(
                    &opctx,
                    restart_id,
                    &p.current_pagparams(),
                )
                .await
                .expect("should be able to query for ereports");
            paginator = p.found_batch(&batch, &|ereport| ereport.ena);
            found_ereports.extend(
                batch.into_iter().map(|ereport| (ereport.ena.0, ereport)),
            );
        }
        assert_eq!(
            expected_ereports.len(),
            found_ereports.len(),
            "expected {} ereports for {restart_id:?}",
            expected_ereports.len()
        );

        for (ena, report) in expected_ereports {
            let Some(found_report) = found_ereports.get(ena) else {
                panic!(
                    "expected ereport {restart_id:?}:{ena} not found in database"
                )
            };
            assert_eq!(
                report, &found_report.report,
                "ereport data for {restart_id:?}:{ena} doesn't match expected"
            );
        }
    }
}
