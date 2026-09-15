// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background tasks for ereport ingestion.
//!
//! The tasks in this module implement the Nexus portion of the ereport
//! ingestion protocol [described in RFD 520][rfd520].
//!
//! [rfd520]: https://rfd.shared.oxide.computer/rfd/520#_determinations

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use chrono::Utc;
use ereport_types::Ena;
use ereport_types::EreportId;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_networking::GatewayClient;
use nexus_types::fm::ereport::EreportData;
use nexus_types::internal_api::background::EreporterStatus;
use nexus_types::internal_api::background::SpEreportIngesterStatus;
use nexus_types::internal_api::background::SpEreporterStatus;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::RackUuid;
use parallel_task_set::ParallelTaskSet;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct SpEreportIngester {
    resolver: internal_dns_resolver::Resolver,
    fm_analysis: Activator,
    disabled: bool,
    inner: Ingester,
}

#[derive(Clone)]
struct Ingester {
    datastore: Arc<DataStore>,
    nexus_id: OmicronZoneUuid,
    rack_id: RackUuid,
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
        rack_id: RackUuid,
        fm_analysis: Activator,
        disabled: bool,
    ) -> Self {
        Self {
            resolver,
            inner: Ingester { datastore, nexus_id, rack_id },
            fm_analysis,
            disabled,
        }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> SpEreportIngesterStatus {
        use gateway_client::types::SpIgnitionInfo;
        use gateway_types::component::SpIdentifier;
        use gateway_types::ignition::SpIgnition;

        let mut status = SpEreportIngesterStatus::default();
        if self.disabled {
            status.disabled = true;
            slog::trace!(
                &opctx.log,
                "SP ereport ingestion disabled, doing nothing",
            );
            return status;
        }
        // Find MGS clients.
        // TODO(eliza): reuse the same client across activations; qorb, etc.
        //
        // TODO-multirack: eventually, we'll need a way to discover the MGS
        // clients for *all* the racks in the cluster, along with the rack ID of
        // the rack those clients will talk to. How this works has yet to be
        // determined. See also the 'TODO-multirack' comment in the
        // `mgs_requests()` function for where the rack ID would be used.
        let mgs_clients = match GatewayClient::resolve_all_gateways(
            &opctx.log,
            &self.resolver,
        )
        .await
        {
            Err(error) => {
                const MSG: &str = "no MGS successfully returned SP ID list";
                let error = InlineErrorChain::new(&*error);
                error!(opctx.log, "{MSG}"; "error" => &error);
                status.errors.push(format!("{MSG}: {error}"));
                return status;
            }
            Ok(clients) => clients.collect::<Arc<[_]>>(),
        };

        // Ask MGS for the list of all present SP identifiers. If a request to
        // the first gateway fails, we'll try again for every resolved MGS
        // before giving up.
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
                match client.ignition_list().await {
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
        let mut total_ereports = 0;
        let mut total_new_ereports = 0;

        for SpIgnitionInfo { details, id } in sps {
            let ignition_type = match details {
                SpIgnition::Present { id, .. } if details.is_sp_running() => id,
                _ => {
                    // This SP is not present or is powered off; skip it.
                    status.sps_not_present += 1;
                    continue;
                }
            };

            status.sps_found += 1;

            let SpIdentifier { typ: type_, slot } = id;
            let sp_result = tasks
                .spawn({
                    let opctx = opctx.child(BTreeMap::from([
                        // XXX(eliza): that's so many little strings... :(
                        ("sp_type".to_string(), type_.to_string()),
                        (
                            "ignition_type".to_string(),
                            format!("{ignition_type:?}"),
                        ),
                        ("slot".to_string(), slot.to_string()),
                    ]));
                    let clients = mgs_clients.clone();
                    let ingester = self.inner.clone();
                    async move {
                        let status = ingester
                            .ingest_sp_ereports(opctx, &clients, type_, slot)
                            .await;
                        SpEreporterStatus {
                            sp_type: type_,
                            slot,
                            ignition_type,
                            status,
                        }
                    }
                })
                .await;
            if let Some(sp_status) = sp_result {
                total_ereports += sp_status.status.ereports_received;
                total_new_ereports += sp_status.status.new_ereports;
                status.sps.push(sp_status);
            }
        }

        // Wait for remaining ingestion tasks to come back.
        while let Some(sp_status) = tasks.join_next().await {
            total_ereports += sp_status.status.ereports_received;
            total_new_ereports += sp_status.status.new_ereports;
            status.sps.push(sp_status);
        }

        // If any ereports were ingested that were not already in the database,
        // trigger a new FM analysis run.
        if total_new_ereports > 0 {
            slog::info!(
                opctx.log,
                "ingested {total_ereports} ({total_new_ereports} new) \
                 ereports from {} service processors",
                status.sps.len();
                "total_ereports" => total_ereports,
                "new_ereports" => total_new_ereports,
                "absent_sps" => status.sps_not_present,
            );
            self.fm_analysis.activate();
        } else {
            slog::debug!(
                opctx.log,
                "ingested {total_ereports} (0 new) \
                 ereports from {} service processors",
                status.sps.len();
                "total_ereports" => total_ereports,
                "new_ereports" => total_new_ereports,
                "absent_sps" => status.sps_not_present,
            );
        }

        // Sort statuses for consistent output in OMDB commands.
        status.sps.sort_unstable_by_key(|sp| (sp.sp_type, sp.slot));

        status
    }
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
    ) -> EreporterStatus {
        // Fetch the latest ereport from this SP.
        let reporter = nexus_types::fm::ereport::Reporter::Sp { sp_type, slot };
        let latest =
            match self.datastore.latest_ereport_id(&opctx, reporter).await {
                Ok(latest) => latest,
                Err(error) => {
                    return EreporterStatus {
                        errors: vec![format!(
                            "failed to query for latest ereport: {error:#}"
                        )],
                        ..Default::default()
                    };
                }
            };

        let mut params = EreportQueryParams::from_latest(latest);
        let mut status = EreporterStatus::default();

        // Continue requesting ereports from this SP in a loop until we have
        // received all its ereports.
        while let Some(ereport_types::Ereports { restart_id, reports }) = self
            .mgs_requests(&opctx, clients, &params, sp_type, slot, &mut status)
            .await
        {
            status.requests += 1;
            if reports.items.is_empty() {
                slog::trace!(
                    &opctx.log,
                    "no ereports returned by SP";
                    "committed_ena" => ?params.committed_ena,
                    "start_ena" => ?params.start_ena,
                    "restart_id" => ?params.restart_id,
                    "total_ereports_received" => status.ereports_received,
                    "total_new_ereports" => status.new_ereports,
                );
                break;
            }

            let time_collected = Utc::now();
            let received = reports.items.len();
            status.ereports_received += received;

            let db_ereports = reports.items.into_iter().map(|ereport| {
                EreportData::from_sp_ereport(&opctx.log, restart_id, ereport)
            });
            let created = match self
                .datastore
                .ereports_insert(
                    &opctx,
                    restart_id,
                    time_collected,
                    self.nexus_id,
                    // TODO-multirack: this argument to `ereports_insert` is
                    // used to determine the rack ID of the SP that generated
                    // this batch of ereports. Currently, using `self.rack_id`
                    // (the rack ID of the Nexus instance ingesting the
                    // ereports) is always correct, since this Nexus is only
                    // ingesting ereports from SPs in its own rack. This will
                    // not be the case if we begin ingesting ereports from SPs
                    // in other racks.
                    //
                    // If this code changes to ingest ereports from the
                    // management gateways in multiple racks, we'll need to
                    // change this to pass the rack ID of the target rack, not
                    // the one this Nexus lives in. Eventually, the rack ID will
                    // become a property of the MGS clients that are passed into
                    // this function: they'll have to become a type that says
                    // "here are the clients for talking to the management
                    // gateways in *that* rack, in particular".
                    self.rack_id,
                    reporter,
                    db_ereports,
                )
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
        EreportQueryParams { committed_ena,start_ena, restart_id }: &EreportQueryParams,
        sp_type: nexus_types::inventory::SpType,
        slot: u16,
        status: &mut EreporterStatus,
    ) -> Option<ereport_types::Ereports> {
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
                    &sp_type,
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
                    status.requests += 1;
                    status.errors.push(format!("MGS {addr}: {e:#}"));
                }
            }
        }

        None
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
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev::poll;
    use omicron_uuid_kinds::GenericUuid;
    use std::collections::BTreeSet;
    use std::time::Duration;
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

        let fm_analysis_activator = Activator::new();
        // in order to test that we actually activate the analysis task, we must
        // wire this up now so that using it does not panic.
        fm_analysis_activator.mark_wired_up().unwrap();

        let mut ingester = SpEreportIngester::new(
            datastore.clone(),
            nexus.internal_resolver.clone(),
            nexus.id(),
            nexus.rack_id(),
            fm_analysis_activator.clone(),
            false,
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
            4,
            "ereports from 4 SPs should be observed: {:?}",
            activation1.sps,
        );
        assert_eq!(activation1.sps_found, 4);
        fm_analysis_activator
            .assert_activated("fm analysis task should be activated");

        for SpEreporterStatus { sp_type, slot, status, ignition_type: _ } in
            &activation1.sps
        {
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

        let sled0 =
            ExpectedReporter { serial: "SimGimlet00", part: "SimGimletSp" };
        let sled0_ereports = [
            sled0.ereport(
                1,
                "ereport.data_loss.possible",
                serde_json::json!({
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "packrat",
                    "hubris_task_gen": 0,
                    "hubris_uptime_ms": 666,
                    "ereport_message_version": 0,
                    "lost": null,
                }),
            ),
            sled0.ereport(
                2,
                "gov.nasa.apollo.o2_tanks.stir.begin",
                serde_json::json!({
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "task_apollo_server",
                    "hubris_task_gen": 13,
                    "hubris_uptime_ms": 1233,
                    "ereport_message_version": 0,
                    "k": "gov.nasa.apollo.o2_tanks.stir.begin",
                    "message": "stirring the tanks",
                }),
            ),
            sled0.ereport(
                3,
                "io.discovery.ae35.fault",
                serde_json::json!({
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "drv_ae35_server",
                    "hubris_task_gen": 1,
                    "hubris_uptime_ms": 1234,
                    "ereport_message_version": 0,
                    "k": "io.discovery.ae35.fault",
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
            sled0.ereport(
                4,
                "gov.nasa.apollo.fault",
                serde_json::json!({
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "task_apollo_server",
                    "hubris_task_gen": 13,
                    "hubris_uptime_ms": 1237,
                    "ereport_message_version": 0,
                    "k": "gov.nasa.apollo.fault",
                    "message": "houston, we have a problem",
                    "crew": [
                        "Lovell",
                        "Swigert",
                        "Haise",
                    ],
                }),
            ),
            sled0.ereport(
                5,
                "flagrant_error",
                serde_json::json!({
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "drv_thingy_server",
                    "hubris_task_gen": 2,
                    "hubris_uptime_ms": 1240,
                    "ereport_message_version": 0,
                    "k": "flagrant_error",
                    "computer": false,
                }),
            ),
            sled0.ereport(
                6,
                "overfull_hbox",
                serde_json::json!({
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "task_latex_server",
                    "hubris_task_gen": 1,
                    "hubris_uptime_ms": 1245,
                    "ereport_message_version": 0,
                    "k": "overfull_hbox",
                    "badness": 10000,
                }),
            ),
        ];

        let sled1 =
            ExpectedReporter { part: "SimGimletSp", serial: "SimGimlet01" };
        let sled1_ereports = [
            sled1.ereport(
                1,
                "ereport.data_loss.possible",
                serde_json::json!({
                    "hubris_archive_id": "ffffffff",
                    "hubris_version": "0.0.2",
                    "hubris_task_name": "packrat",
                    "hubris_task_gen": 0,
                    "hubris_uptime_ms": 666,
                    "ereport_message_version": 0,
                    "lost": null,
                }),
            ),
            sled1.ereport(
                2,
                "computer.oxide.gimlet.chassis_integrity.fault",
                serde_json::json!({
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
            ),
        ];

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
        fm_analysis_activator.assert_not_activated(
            "fm analysis task should not be activated when no new ereports \
             have been ingested",
        );
        assert_eq!(
            activation2.sps_found, 4,
            "4 present SPs should have been found via ignition",
        );
        assert_eq!(
            activation2.sps.len(),
            4,
            "all 4 SPs should be reported in the status, even when no new \
             ereports were observed: {:?}",
            activation2.sps,
        );
        for SpEreporterStatus { sp_type, slot, status, ignition_type: _ } in
            &activation2.sps
        {
            assert_eq!(
                status.ereports_received, 0,
                "no ereports should have been received from SP \
                 {sp_type:?} {slot}",
            );
            assert_eq!(
                status.new_ereports, 0,
                "no new ereports should have been ingested from SP \
                 {sp_type:?} {slot}",
            );
            assert_eq!(
                status.requests, 1,
                "one HTTP request should have been sent for SP \
                 {sp_type:?} {slot} (empty response)",
            );
            assert_eq!(
                &status.errors,
                &Vec::<String>::new(),
                "there should be no errors from SP {sp_type:?} {slot}",
            );
        }

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

    /// Regression test for the potential busy loop described in
    /// [omicron#10738](https://github.com/oxidecomputer/omicron/issues/10738).
    ///
    /// This issue describes an issue that can occur when multiple Nexuses
    /// ingest ereports from two different restarts of a given SP, where the
    /// wall clock time at which the ereports from a later restart are inserted
    /// into the database is *earlier* than the wall clock time at which
    /// ereports from the current restart are inserted.
    ///
    /// > I think the `ingest_sp_ereports` function may be broken in this case:
    /// > - Suppose an SP has `Restart ID = X`, Nexus A queries for ereports...
    /// > - After the query starts, the SP restarts with `Restart ID = Y`,
    /// >   Nexus B queries for ereports...
    /// > - Suppose `Restart ID = Y` is the "steady-state" of the SP
    /// >
    /// > If the ereport insertion order is:
    /// > - Nexus B inserts ereports for `Restart ID = Y` (at time T1)
    /// > - Nexus A inserts ereports for `Restart ID = X` (at time T2 > T1)
    /// > - Nexus A's request for ereports is still pending when Nexus B
    /// >   inserts ereports for `Restart ID = Y`
    /// >
    /// > Then Nexus erroneously will think "Restart ID X > Y", which is untrue.
    /// > This is possible because the timestamps we're collecting here are
    /// > based on Nexus' wall-clock, which is queried after the request for
    /// > ereports.
    /// >
    /// > I think in `ingest_sp_ereports` this could potentially cause Nexus to
    /// > be stuck in a busy loop...
    /// >
    /// > - Nexus thinks restart ID = X is latest, so it sets up params with
    /// >   restart ID = X
    /// > - Nexus sends a request to the SP asking for ereports, the SP
    /// >   responds "I'm restart ID = Y, here are my ereports"
    /// > - If the SP doesn't have new ereports, then `ereports_insert` runs,
    /// >   and inserts no new ereports -- so latest doesn't change (recall:
    /// >   the original set of ereports for restart ID Y already got inserted
    /// >   by Nexus B)
    /// > - `ingest_sp_ereports` sets the new "params" value, but the in-DB
    /// >   notion of latest is still from the now-dead restart ID X
    /// > - Go to step (1). Turn the CPU into a space heater until more
    /// >   ereports arrive from the SP
    ///
    /// Rather than racing two ingesters, this test constructs the racy outcome
    /// directly. The test first asks the simulated SP for its current restart
    /// ID and buffered ereports, and then inserts those ereports into the
    /// database at T1, followed by ereports with a fabricated "previous"
    /// restart ID at T2 > T1. This simulates a situation where one Nexus is
    /// holding in memory a batch of ereports from the "earlier" restart in
    /// memory and inserts them into the database at a wall-clock time later
    /// than the ereports collected from the "current" restart.
    #[nexus_test(server = crate::Server)]
    async fn test_sp_ereport_ingestion_stale_restart_id(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Sled 0's simulated SP.
        let sp_type = nexus_types::inventory::SpType::Sled;
        let sp_slot = 0;
        let reporter =
            nexus_types::fm::ereport::Reporter::Sp { sp_type, slot: sp_slot };
        // A restart ID the SP has never heard of, standing in for the restart
        // that preceded its current one.
        let stale_restart_id = EreporterRestartUuid::from_untyped_uuid(uuid!(
            "3e0e701d-79cc-4d1a-a742-e0410324b1de"
        ));

        let clients = poll::wait_for_condition(
            || async {
                let gateways = GatewayClient::resolve_all_gateways(
                    &opctx.log,
                    &nexus.internal_resolver,
                )
                .await
                .map_err(|e| poll::CondCheckError::<Error>::NotYet {
                    status: Some(e.to_string()),
                })?
                .collect::<Vec<_>>();
                if gateways.is_empty() {
                    return Err(poll::CondCheckError::<Error>::NotYet {
                        status: Some("no gateways resolved".to_string()),
                    });
                }
                Ok(gateways)
            },
            &Duration::from_millis(200),
            &Duration::from_secs(60),
        )
        .await
        .unwrap();

        // Request all the ereports from the simulated SP and insert them into
        // the database, as though we have already ingested them.
        let ereport_types::Ereports { restart_id: current_restart_id, reports } =
            clients
                .first()
                .expect("at least one MGS client should be resolved")
                .client
                .sp_ereports_ingest(
                    &sp_type,
                    sp_slot,
                    None,
                    // Request the maximum possible limit to ensure all ereports
                    // that the SP simulator would possibly return are included.
                    std::num::NonZeroU32::new(255).unwrap(),
                    &EreporterRestartUuid::nil(),
                    None,
                )
                .await
                .expect("probing the SP's ereports via MGS should succeed")
                .into_inner();
        let current_restart_ereports = reports.items;
        assert!(
            !current_restart_ereports.is_empty(),
            "/!\\ TEST IS BROKEN! /!\\\n\
            this test requires the simulated SP to have ereports; if the \
             configuration for the SP simulator has changed to have no \
             ereports, this test cannot reproduce the bug! please change the \
             config file back!",
        );
        assert_ne!(current_restart_id, stale_restart_id);

        // "Nexus B" ingested the SP's entire current buffer a little while
        // ago...
        let time_current = Utc::now() - chrono::TimeDelta::seconds(10);
        // ...and "Nexus A" inserted ereports from the SP's previous
        // incarnation afterwards, because it queried the SP before the
        // restart but timestamped the ereports with its own wall clock at
        // insertion time.
        let time_stale = Utc::now();
        let rack_id = nexus.rack_id();

        // Insert the ereports previously collected by "Nexus B" from the
        // current restart. Imagine that this is actually a previous iteration
        // of the ereport ingester loop. It isn't actually, but the behavior
        // will be the same, and we cannot easily pause the loop mid-execution.
        datastore
            .ereports_insert(
                &opctx,
                current_restart_id,
                time_current,
                nexus.id(),
                rack_id,
                reporter,
                current_restart_ereports.iter().map(|ereport| {
                    EreportData::from_sp_ereport(
                        &opctx.log,
                        current_restart_id,
                        ereport.clone(),
                    )
                }),
            )
            .await
            .expect("ereports should be inserted");

        // Now, imagine that Nexus A finally wakes up, with a batch of ereports
        // from the SP's *previous* restart still in memory, and inserts them
        // into the database at a wall-clock time after when Nexus B inserted
        // ereports from the current restart.
        let stale_restart_enas = BTreeSet::from_iter(Some(Ena::from(1)));
        datastore
            .ereports_insert(
                &opctx,
                stale_restart_id,
                time_stale,
                OmicronZoneUuid::new_v4(),
                rack_id,
                reporter,
                vec![(
                    Ena::from(1),
                    EreportData {
                        // these don't matter...
                        serial_number: None,
                        part_number: None,
                        // since it's ENA 1, let's imagine it's a loss report
                        class: Some("ereport.data_loss.possible".to_string()),
                        report: serde_json::json!({"lost": null}),
                    },
                )],
            )
            .await
            .expect(
                "ereports from simulated previous restart should be inserted",
            );

        let ingester = Ingester {
            datastore: datastore.clone(),
            nexus_id: nexus.id(),
            rack_id: nexus.rack_id(),
        };

        // Generously longer than a terminating ingestion pass (a couple of
        // HTTP requests and a handful of database queries) could ever take.
        const TIMEOUT: std::time::Duration = Duration::from_secs(60);
        let status = tokio::time::timeout(
            TIMEOUT,
            ingester.ingest_sp_ereports(
                opctx.child(BTreeMap::new()),
                &clients,
                sp_type,
                sp_slot,
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "ingest_sp_ereports did not terminate within {TIMEOUT:?}; it \
                 is busy-looping, re-requesting ereports with the stale \
                 restart ID {stale_restart_id:?} while the SP keeps \
                 responding with already-ingested ereports from restart \
                 {current_restart_id:?} (omicron#10738)",
            )
        });

        // Make sure that the test reproduced the conditions necessary for the
        // bug, and did not pass incorrectly.
        //
        // First, the ingestion request should not have inserted any new ENAs
        // into the database, as doing so would have made the current ENA
        // "latest" again.
        let expected_current_restart_enas = current_restart_ereports
            .iter()
            .map(|e| e.ena)
            .collect::<BTreeSet<_>>();
        let db_current_restart_enas =
            restart_ereports_fetch(datastore, &opctx, current_restart_id)
                .await
                .keys()
                .copied()
                .collect::<BTreeSet<_>>();
        assert_eq!(
            db_current_restart_enas, expected_current_restart_enas,
            "/!\\ TEST IS BROKEN! /!\\\n\
            the ingestion attempt should not have created any new ENAs from \
            the current restart ID that were not previously in the database. \
            if it has, this test has 'passed' without reproducing the \
            conditions necessary for the bug to occur! this probably means \
            that the SP simulator has somehow produced additional ereports \
            when it was not asked to, which should not happen!",
        );
        // Second, the expected ereports from the "previous" restart must exist.
        let db_stale_restart_enas =
            restart_ereports_fetch(datastore, &opctx, stale_restart_id)
                .await
                .keys()
                .copied()
                .collect::<BTreeSet<_>>();
        assert_eq!(
            db_stale_restart_enas, stale_restart_enas,
            "/!\\ TEST IS BROKEN! /!\\\n\
            ENAs from the 'stale' previous restart are somehow missing from \
            the database. this test has 'passed' without reproducing the \
            conditions necessary for the bug to occur!",
        );
        // An MGS request error would also (spuriously) break the loop; fail
        // loudly rather than passing without having exercised the scenario.
        assert_eq!(
            &status.errors,
            &Vec::<String>::new(),
            "/!\\ TEST IS BROKEN! /!\\\n\
            MGS unexpectedly returned an error in response to an ereport \
            ingestion request! this test has 'passed' without reproducing the \
            conditions necessary for the bug to occur!",
        );
    }

    struct ExpectedReporter {
        serial: &'static str,
        part: &'static str,
    }

    struct ExpectedEreport {
        ena: Ena,
        class: Option<&'static str>,
        serial: Option<&'static str>,
        part: Option<&'static str>,
        json: serde_json::Value,
    }

    impl ExpectedReporter {
        fn ereport(
            &self,
            ena: u64,
            class: impl Into<Option<&'static str>>,
            mut json: serde_json::Value,
        ) -> ExpectedEreport {
            if let serde_json::Value::Object(map) = &mut json {
                map.insert(
                    "baseboard_part_number".to_string(),
                    self.part.into(),
                );
                map.insert(
                    "baseboard_serial_number".to_string(),
                    self.serial.into(),
                );
            } else {
                panic!(
                    "Expected ereport JSON to be an object, but it was some \
                     other thing. This is a bug in your test code!",
                );
            };
            ExpectedEreport {
                ena: Ena(ena),
                class: class.into(),
                serial: Some(self.serial),
                part: Some(self.part),
                json,
            }
        }
    }

    async fn restart_ereports_fetch(
        datastore: &DataStore,
        opctx: &OpContext,
        restart_id: EreporterRestartUuid,
    ) -> BTreeMap<Ena, nexus_db_model::Ereport> {
        let mut paginator = Paginator::new(
            std::num::NonZeroU32::new(100).unwrap(),
            dropshot::PaginationOrder::Ascending,
        );
        let mut ereports = BTreeMap::new();
        while let Some(p) = paginator.next() {
            let batch = datastore
                .ereport_list_by_restart(
                    &opctx,
                    restart_id,
                    &p.current_pagparams(),
                )
                .await
                .expect("should be able to query for ereports");
            paginator = p.found_batch(&batch, &|ereport| ereport.ena);
            ereports.extend(
                batch
                    .into_iter()
                    .map(|ereport| (Ena::from(ereport.ena), ereport)),
            );
        }
        ereports
    }

    async fn check_sp_ereports_exist(
        datastore: &DataStore,
        opctx: &OpContext,
        restart_id: EreporterRestartUuid,
        expected_ereports: &[ExpectedEreport],
    ) {
        let found_ereports =
            restart_ereports_fetch(datastore, opctx, restart_id).await;
        assert_eq!(
            expected_ereports.len(),
            found_ereports.len(),
            "expected {} ereports for {restart_id:?}",
            expected_ereports.len()
        );

        for ExpectedEreport { ena, json, serial, part, class } in
            expected_ereports
        {
            let Some(found_report) = found_ereports.get(ena) else {
                panic!(
                    "expected ereport {restart_id:?}:{ena} not found in \
                     database"
                )
            };
            assert_eq!(
                serial,
                &found_report.serial_number.as_deref(),
                "expected ereport {restart_id:?}:{ena} to have serial \
                 {serial:?}"
            );
            assert_eq!(
                part,
                &found_report.part_number.as_deref(),
                "expected ereport {restart_id:?}:{ena} to have part number \
                 {part:?}"
            );
            assert_eq!(
                class,
                &found_report.class.as_deref(),
                "expected ereport {restart_id:?}:{ena} to have class \
                 {class:?}"
            );
            assert_eq!(
                json, &found_report.report,
                "ereport data for {restart_id:?}:{ena} doesn't match expected"
            );
        }
    }
}
