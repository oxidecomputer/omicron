// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for realizing a plan blueprint

use crate::app::{
    background::{Activator, BackgroundTask},
    quiesce::NexusQuiesceHandle,
};
use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_reconfigurator_execution::{
    RealizeBlueprintOutput, RequiredRealizeArgs,
};
use nexus_types::deployment::{
    Blueprint, BlueprintTarget, PendingMgsUpdates, execution::EventBuffer,
};
use omicron_uuid_kinds::OmicronZoneUuid;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;
use update_engine::NestedError;

/// Background task that takes a [`Blueprint`] and realizes the change to
/// the state of the system based on the `Blueprint`.
pub struct BlueprintExecutor {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    rx_blueprint: watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    nexus_id: OmicronZoneUuid,
    tx: watch::Sender<usize>,
    saga_recovery: Activator,
    mgs_update_tx: watch::Sender<PendingMgsUpdates>,
    nexus_quiesce: NexusQuiesceHandle,
}

impl BlueprintExecutor {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        rx_blueprint: watch::Receiver<
            Option<Arc<(BlueprintTarget, Blueprint)>>,
        >,
        nexus_id: OmicronZoneUuid,
        saga_recovery: Activator,
        mgs_update_tx: watch::Sender<PendingMgsUpdates>,
        nexus_quiesce: NexusQuiesceHandle,
    ) -> BlueprintExecutor {
        let (tx, _) = watch::channel(0);
        BlueprintExecutor {
            datastore,
            resolver,
            rx_blueprint,
            nexus_id,
            tx,
            saga_recovery,
            mgs_update_tx,
            nexus_quiesce,
        }
    }

    pub fn watcher(&self) -> watch::Receiver<usize> {
        self.tx.subscribe()
    }

    /// Implementation for `BackgroundTask::activate` for `BlueprintExecutor`,
    /// added here to produce better compile errors.
    ///
    /// The presence of `boxed()` in `BackgroundTask::activate` has caused some
    /// confusion with compilation errors in the past. So separate this method
    /// out.
    async fn activate_impl(&mut self, opctx: &OpContext) -> serde_json::Value {
        // Get the latest blueprint, cloning to prevent holding a read lock
        // on the watch.
        let update = self.rx_blueprint.borrow_and_update().clone();

        let Some(update) = update else {
            warn!(
                &opctx.log, "Blueprint execution: skipped";
                "reason" => "no blueprint",
            );
            return json!({"error": "no blueprint" });
        };

        let (bp_target, blueprint) = &*update;

        // Regardless of anything else: propagate whatever this blueprint
        // says about our quiescing state.
        //
        // During startup under normal operation, the blueprint will reflect
        // that we're not quiescing.  Propagating this will enable sagas to
        // be created elsewhere in Nexus.
        //
        // At some point during an upgrade, we'll encounter a blueprint that
        // reflects that we are quiescing.  Propagating this will disable sagas
        // from being created.
        //
        // In all other cases, this will have no effect.
        //
        // We do this now, before doing anything else, for two reasons: (1)
        // during startup, we want to do this ASAP to minimize unnecessary saga
        // creation failures (i.e., don't wait until we try to execute the
        // blueprint before enabling sagas, since we already know if we're
        // quiescing or not); and (2) because we want to do it even if blueprint
        // execution is disabled.
        match blueprint.is_nexus_quiescing(self.nexus_id) {
            Ok(quiescing) => {
                debug!(
                    &opctx.log,
                    "blueprint execution: quiesce check";
                    "quiescing" => quiescing
                );
                self.nexus_quiesce.set_quiescing(quiescing);
            }
            Err(error) => {
                // This should be impossible.  But it doesn't really affect
                // anything else so there's no reason to stop execution.
                error!(
                    &opctx.log,
                    "blueprint execution: failed to determine if this Nexus \
                     is quiescing";
                    InlineErrorChain::new(&*error)
                );
            }
        };

        if !bp_target.enabled {
            warn!(&opctx.log,
                      "Blueprint execution: skipped";
                      "reason" => "blueprint disabled",
                      "target_id" => %blueprint.id);
            return json!({
                "target_id": blueprint.id.to_string(),
                "enabled": false,
            });
        }

        let (sender, mut receiver) = update_engine::channel();

        let receiver_task = tokio::spawn(async move {
            // TODO: report progress
            let mut event_buffer = EventBuffer::default();
            while let Some(event) = receiver.recv().await {
                event_buffer.add_event(event);
            }

            event_buffer.generate_report()
        });

        let result = nexus_reconfigurator_execution::realize_blueprint(
            RequiredRealizeArgs {
                opctx,
                datastore: &self.datastore,
                resolver: &self.resolver,
                creator: self.nexus_id,
                blueprint,
                sender,
                mgs_updates: self.mgs_update_tx.clone(),
                saga_quiesce: self.nexus_quiesce.sagas(),
            }
            .as_nexus(self.nexus_id),
        )
        .await;

        // Get the report for the receiver task.
        let event_report =
            receiver_task.await.map_err(|error| NestedError::new(&error));

        // Trigger anybody waiting for this to finish.
        self.tx.send_modify(|count| *count = *count + 1);

        // Return the result as a `serde_json::Value`
        match result {
            Ok(RealizeBlueprintOutput { needs_saga_recovery }) => {
                // If executing the blueprint requires activating the saga
                // recovery background task, do that now.
                if needs_saga_recovery {
                    info!(&opctx.log, "activating saga recovery task");
                    self.saga_recovery.activate();
                }

                json!({
                    "target_id": blueprint.id.to_string(),
                    "enabled": true,
                    // Note: The field "error" is treated as special by omdb,
                    // and if that field is present then nothing else is
                    // displayed.
                    "execution_error": null,
                    "needs_saga_recovery": needs_saga_recovery,
                    "event_report": event_report,
                })
            }
            Err(error) => {
                json!({
                    "target_id": blueprint.id.to_string(),
                    "enabled": true,
                    // Note: The field "error" is treated as special by omdb,
                    // and if that field is present then nothing else is
                    // displayed.
                    "execution_error": NestedError::new(error.as_ref()),
                    "event_report": event_report,
                })
            }
        }
    }
}

impl BackgroundTask for BlueprintExecutor {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        self.activate_impl(opctx).boxed()
    }
}

#[cfg(test)]
mod test {
    use super::BlueprintExecutor;
    use crate::app::background::{Activator, BackgroundTask};
    use crate::app::quiesce::NexusQuiesceHandle;
    use httptest::Expectation;
    use httptest::matchers::{not, request};
    use httptest::responders::status_code;
    use iddqd::{IdOrdMap, id_ord_map};
    use itertools::Itertools as _;
    use nexus_db_model::{
        ByteCount, SledBaseboard, SledCpuFamily, SledSystemHardware,
        SledUpdate, Zpool,
    };
    use nexus_db_queries::authn;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::DataStore;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::execution::{
        EventBuffer, EventReport, ExecutionComponent, ExecutionStepId,
        StepOutcome, StepStatus,
    };
    use nexus_types::deployment::{
        Blueprint, BlueprintHostPhase2DesiredSlots, BlueprintSledConfig,
        BlueprintSource, BlueprintTarget, BlueprintZoneConfig,
        BlueprintZoneDisposition, BlueprintZoneImageSource, BlueprintZoneType,
        CockroachDbPreserveDowngrade, OximeterReadMode, PendingMgsUpdates,
        blueprint_zone_type,
    };
    use nexus_types::external_api::views::SledState;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::api::external;
    use omicron_common::api::external::Generation;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use serde_json::json;
    use sled_agent_types_migrations::latest::inventory::OmicronZoneDataset;
    use std::collections::BTreeMap;
    use std::net::Ipv6Addr;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::sync::watch;
    use update_engine::{CompletionReason, NestedError, TerminalKind};
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    async fn create_blueprint(
        datastore: &DataStore,
        opctx: &OpContext,
        blueprint_zones: BTreeMap<SledUuid, IdOrdMap<BlueprintZoneConfig>>,
        dns_version: Generation,
    ) -> (BlueprintTarget, Blueprint) {
        let id = BlueprintUuid::new_v4();
        // Assume all sleds are active with no disks or datasets.
        let blueprint_sleds = blueprint_zones
            .into_iter()
            .map(|(sled_id, zones)| {
                (
                    sled_id,
                    BlueprintSledConfig {
                        state: SledState::Active,
                        subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
                        sled_agent_generation: Generation::new().next(),
                        disks: IdOrdMap::new(),
                        datasets: IdOrdMap::new(),
                        zones,
                        remove_mupdate_override: None,
                        host_phase_2:
                            BlueprintHostPhase2DesiredSlots::current_contents(),
                    },
                )
            })
            .collect();

        // Ensure the blueprint we're creating is the current target (required
        // for successful blueprint realization). This requires its parent to be
        // the existing target, so fetch that first.
        let current_target = datastore
            .blueprint_target_get_current(opctx)
            .await
            .expect("fetched current target blueprint");

        let target = BlueprintTarget {
            target_id: id,
            enabled: true,
            time_made_target: chrono::Utc::now(),
        };
        let blueprint = Blueprint {
            id,
            sleds: blueprint_sleds,
            pending_mgs_updates: PendingMgsUpdates::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            parent_blueprint_id: Some(current_target.target_id),
            internal_dns_version: dns_version,
            external_dns_version: dns_version,
            target_release_minimum_generation: Generation::new(),
            nexus_generation: Generation::new(),
            cockroachdb_fingerprint: String::new(),
            clickhouse_cluster_config: None,
            oximeter_read_version: Generation::new(),
            oximeter_read_mode: OximeterReadMode::SingleNode,
            time_created: chrono::Utc::now(),
            creator: "test".to_string(),
            comment: "test blueprint".to_string(),
            source: BlueprintSource::Test,
        };

        datastore
            .blueprint_insert(opctx, &blueprint)
            .await
            .expect("inserted new blueprint");
        datastore
            .blueprint_target_set_current(opctx, target)
            .await
            .expect("set new blueprint as current target");

        (target, blueprint)
    }

    #[nexus_test(server = crate::Server)]
    async fn test_deploy_omicron_config(cptestctx: &ControlPlaneTestContext) {
        // Set up the test.
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_background(
            cptestctx.logctx.log.clone(),
            nexus.authz.clone(),
            authn::Context::internal_api(),
            datastore.clone(),
        );

        // Create some fake sled-agent servers to respond to zone puts and add
        // sleds to CRDB.
        let mut s1 = httptest::Server::run();
        let mut s2 = httptest::Server::run();

        // The only sled-agent endpoint we care about in this test is `PUT
        // /omicron-config`. Add a closure to avoid repeating it multiple times
        // below. We don't do a careful check of the _contents_ of what's being
        // sent; for that, see the tests in nexus-reconfigurator-execution.
        let match_put_omicron_config =
            || request::method_path("PUT", "/omicron-config");

        // Helper for our mock sled-agent http servers to blanket ignore and
        // return 200 OK for anything _except_ `PUT /omicron-config`, which is
        // the endpoint we care about in this test.
        //
        // Other Nexus background tasks created by our test context may notice
        // the sled-agent records we're about to insert into CRDB and query them
        // (e.g., for inventory, vpc routing info, ...). We don't want those to
        // cause spurious test failures, so just tell our sled-agents to accept
        // any number of them.
        let mock_server_ignore_spurious_http_requests =
            |s: &mut httptest::Server| {
                s.expect(
                    Expectation::matching(not(match_put_omicron_config()))
                        .times(..)
                        .respond_with(status_code(200)),
                );
            };
        mock_server_ignore_spurious_http_requests(&mut s1);
        mock_server_ignore_spurious_http_requests(&mut s2);

        let sled_id1 = SledUuid::new_v4();
        let sled_id2 = SledUuid::new_v4();
        let rack_id = Uuid::new_v4();
        for (i, (sled_id, server)) in
            [(sled_id1, &s1), (sled_id2, &s2)].iter().enumerate()
        {
            let SocketAddr::V6(addr) = server.addr() else {
                panic!("Expected Ipv6 address. Got {}", server.addr());
            };
            let bogus_repo_depot_port = 0;
            let update = SledUpdate::new(
                *sled_id,
                addr,
                bogus_repo_depot_port,
                SledBaseboard {
                    serial_number: i.to_string(),
                    part_number: "test".into(),
                    revision: 1,
                },
                SledSystemHardware {
                    is_scrimlet: false,
                    usable_hardware_threads: 4,
                    usable_physical_ram: ByteCount(1000.into()),
                    reservoir_size: ByteCount(999.into()),
                    cpu_family: SledCpuFamily::AmdMilan,
                },
                rack_id,
                nexus_db_model::Generation::new(),
            );
            datastore
                .sled_upsert(update)
                .await
                .expect("Failed to insert sled to db");
        }

        let (blueprint_tx, blueprint_rx) = watch::channel(None);
        let (dummy_tx, _dummy_rx) = watch::channel(PendingMgsUpdates::new());
        let mut task = BlueprintExecutor::new(
            datastore.clone(),
            resolver.clone(),
            blueprint_rx.clone(),
            OmicronZoneUuid::new_v4(),
            Activator::new(),
            dummy_tx,
            NexusQuiesceHandle::new(
                datastore.clone(),
                OmicronZoneUuid::new_v4(),
                blueprint_rx,
                opctx.child(BTreeMap::new()),
            ),
        );

        // Now we're ready.
        //
        // With no target blueprint, the task should fail with an appropriate
        // message.
        let value = task.activate(&opctx).await;
        assert_eq!(value, json!({"error": "no blueprint"}));

        // With a target blueprint having no zones, the task should trivially
        // complete and report a successful (empty) summary.
        let generation = Generation::new();
        let blueprint = Arc::new(
            create_blueprint(&datastore, &opctx, BTreeMap::new(), generation)
                .await,
        );
        let blueprint_id = blueprint.1.id;
        blueprint_tx.send(Some(blueprint)).unwrap();
        let mut value = task.activate(&opctx).await;

        let event_buffer = extract_event_buffer(&mut value);

        println!("activating with no zones: {:?}", value);
        assert_eq!(
            value,
            json!({
                "target_id": blueprint_id,
                "execution_error": null,
                "enabled": true,
                "needs_saga_recovery": false,
            })
        );

        assert_event_buffer_completed(&event_buffer);

        // Create a non-empty blueprint describing two servers and verify that
        // the task correctly winds up making requests to both of them and
        // reporting success.
        fn make_zones(
            disposition: BlueprintZoneDisposition,
        ) -> IdOrdMap<BlueprintZoneConfig> {
            let pool_id = ZpoolUuid::new_v4();
            let zone_id = OmicronZoneUuid::new_v4();
            id_ord_map! {
                BlueprintZoneConfig {
                    disposition,
                    id: zone_id,
                    filesystem_pool: ZpoolName::new_external(pool_id),
                    zone_type: BlueprintZoneType::InternalDns(
                        blueprint_zone_type::InternalDns {
                            dataset: OmicronZoneDataset {
                                pool_name: format!("oxp_{}", pool_id)
                                    .parse()
                                    .unwrap(),
                            },
                            dns_address: "[::1]:0".parse().unwrap(),
                            gz_address: "::1".parse().unwrap(),
                            gz_address_index: 0,
                            http_address: "[::1]:12345".parse().unwrap(),
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                },
            }
        }

        let generation = generation.next();

        // In-service zones should be deployed.
        //
        // TODO: add expunged zones to the test (should not be deployed).
        let mut blueprint = create_blueprint(
            &datastore,
            &opctx,
            BTreeMap::from([
                (sled_id1, make_zones(BlueprintZoneDisposition::InService)),
                (sled_id2, make_zones(BlueprintZoneDisposition::InService)),
            ]),
            generation,
        )
        .await;

        // Insert records for the zpools backing the datasets in these zones.
        for (sled_id, config) in
            blueprint.1.all_omicron_zones(BlueprintZoneDisposition::any)
        {
            let Some(dataset) = config.zone_type.durable_dataset() else {
                continue;
            };

            let pool_id = dataset.dataset.pool_name.id();
            let zpool = Zpool::new(
                pool_id,
                sled_id,
                PhysicalDiskUuid::new_v4(),
                external::ByteCount::from(0).into(),
            );
            datastore
                .zpool_insert(&opctx, zpool)
                .await
                .expect("failed to upsert zpool");
        }

        blueprint_tx.send(Some(Arc::new(blueprint.clone()))).unwrap();

        // Make sure that requests get made to the sled agent.
        for s in [&mut s1, &mut s2] {
            s.expect(
                Expectation::matching(match_put_omicron_config())
                    .respond_with(status_code(204)),
            );
            mock_server_ignore_spurious_http_requests(s);
        }

        // Activate the task to trigger zone configuration on the sled-agents
        let mut value = task.activate(&opctx).await;
        let event_buffer = extract_event_buffer(&mut value);

        println!("activating two sled agents: {:?}", value);
        assert_eq!(
            value,
            json!({
                "target_id": blueprint.1.id.to_string(),
                "execution_error": null,
                "enabled": true,
                "needs_saga_recovery": false,
            })
        );
        assert_event_buffer_completed(&event_buffer);

        s1.verify_and_clear();
        s2.verify_and_clear();

        // Immediately reapply our blanket ignores.
        //
        // TODO-cleanup Is there a tiny race window between verify_and_clear()
        // and these calls where other bg tasks can cause spurious failures?
        mock_server_ignore_spurious_http_requests(&mut s1);
        mock_server_ignore_spurious_http_requests(&mut s2);

        // Now, disable the target and make sure that we _don't_ invoke the sled
        // agent. It's enough to just not set expectations on
        // match_put_omicron_config().
        blueprint.1.internal_dns_version =
            blueprint.1.internal_dns_version.next();
        blueprint.0.enabled = false;
        blueprint_tx.send(Some(Arc::new(blueprint.clone()))).unwrap();
        let value = task.activate(&opctx).await;
        println!("when disabled: {:?}", value);
        assert_eq!(
            value,
            json!({
                "enabled": false,
                "target_id": blueprint.1.id.to_string()
            })
        );
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Immediately reapply our blanket ignores.
        //
        // TODO-cleanup Is there a tiny race window between verify_and_clear()
        // and these calls where other bg tasks can cause spurious failures?
        mock_server_ignore_spurious_http_requests(&mut s1);
        mock_server_ignore_spurious_http_requests(&mut s2);

        // Do it all again, but configure one of the servers to fail so we can
        // verify the task's returned summary of what happened.
        blueprint.0.enabled = true;
        blueprint_tx.send(Some(Arc::new(blueprint))).unwrap();
        s1.expect(
            Expectation::matching(match_put_omicron_config())
                .respond_with(status_code(204)),
        );
        s2.expect(
            Expectation::matching(match_put_omicron_config())
                .respond_with(status_code(500)),
        );

        let mut value = task.activate(&opctx).await;
        let event_buffer = extract_event_buffer(&mut value);

        let ensure_configs_outcome = {
            let ensure_id =
                serde_json::to_value(ExecutionStepId::Ensure).unwrap();
            let sled_agent_component =
                serde_json::to_value(ExecutionComponent::SledAgent).unwrap();
            match event_buffer
                .iter_steps_recursive()
                .filter_map(|(_, step)| {
                    let info = step.step_info();
                    if info.id == ensure_id
                        && info.component == sled_agent_component
                    {
                        match step.step_status() {
                            StepStatus::Completed {
                                reason: CompletionReason::StepCompleted(info),
                            } => Some(&info.outcome),
                            status => {
                                panic!("unexpected step status: {status:?}")
                            }
                        }
                    } else {
                        None
                    }
                })
                .exactly_one()
            {
                Ok(outcome) => outcome,
                Err(outcomes) => panic!(
                    "expected exactly one step outcome, but found {}",
                    outcomes.count()
                ),
            }
        };

        match ensure_configs_outcome {
            StepOutcome::Warning { message, .. } => {
                assert!(
                    message.contains("Failed to put OmicronSledConfig"),
                    "unexpected warning message: {message}"
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        s1.verify_and_clear();
        s2.verify_and_clear();
    }

    fn extract_event_buffer(value: &mut serde_json::Value) -> EventBuffer {
        let event_report = value
            .as_object_mut()
            .expect("value is an object")
            .remove("event_report")
            .expect("event_report exists");
        let event_report: Result<EventReport, NestedError> =
            serde_json::from_value(event_report)
                .expect("event_report is valid");
        let event_report = event_report.expect("event_report is Ok");

        let mut event_buffer = EventBuffer::default();
        event_buffer.add_event_report(event_report);
        event_buffer
    }

    fn assert_event_buffer_completed(event_buffer: &EventBuffer) {
        let execution_status = event_buffer
            .root_execution_summary()
            .expect("event buffer has root execution summary")
            .execution_status;
        let terminal_info =
            execution_status.terminal_info().unwrap_or_else(|| {
                panic!(
                    "execution status has terminal info: {:?}",
                    execution_status
                );
            });
        assert_eq!(
            terminal_info.kind,
            TerminalKind::Completed,
            "execution should have completed successfully"
        );
    }
}
