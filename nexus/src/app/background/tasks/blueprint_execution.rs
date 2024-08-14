// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for realizing a plan blueprint

use crate::app::background::{Activator, BackgroundTask};
use futures::future::BoxFuture;
use futures::FutureExt;
use internal_dns::resolver::Resolver;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::{Blueprint, BlueprintTarget};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

/// Background task that takes a [`Blueprint`] and realizes the change to
/// the state of the system based on the `Blueprint`.
pub struct BlueprintExecutor {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    rx_blueprint: watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    nexus_id: Uuid,
    tx: watch::Sender<usize>,
    saga_recovery: Activator,
}

impl BlueprintExecutor {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        rx_blueprint: watch::Receiver<
            Option<Arc<(BlueprintTarget, Blueprint)>>,
        >,
        nexus_id: Uuid,
        saga_recovery: Activator,
    ) -> BlueprintExecutor {
        let (tx, _) = watch::channel(0);
        BlueprintExecutor {
            datastore,
            resolver,
            rx_blueprint,
            nexus_id,
            tx,
            saga_recovery,
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
    async fn activate_impl<'a>(
        &mut self,
        opctx: &OpContext,
    ) -> serde_json::Value {
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
        if !bp_target.enabled {
            warn!(&opctx.log,
                      "Blueprint execution: skipped";
                      "reason" => "blueprint disabled",
                      "target_id" => %blueprint.id);
            return json!({
                "target_id": blueprint.id.to_string(),
                "error": "blueprint disabled"
            });
        }

        let result = nexus_reconfigurator_execution::realize_blueprint(
            opctx,
            &self.datastore,
            &self.resolver,
            blueprint,
            self.nexus_id,
        )
        .await;

        // Trigger anybody waiting for this to finish.
        self.tx.send_modify(|count| *count = *count + 1);

        // If executing the blueprint requires activating the saga recovery
        // background task, do that now.
        info!(&opctx.log, "activating saga recovery task");
        if let Ok(true) = result {
            self.saga_recovery.activate();
        }

        // Return the result as a `serde_json::Value`
        match result {
            Ok(_) => json!({
                "target_id": blueprint.id.to_string(),
            }),
            Err(errors) => {
                let errors: Vec<_> =
                    errors.into_iter().map(|e| format!("{:#}", e)).collect();
                json!({
                    "target_id": blueprint.id.to_string(),
                    "errors": errors
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
    use crate::app::saga::create_saga_dag;
    use crate::app::sagas::demo;
    use anyhow::Context;
    use futures::TryStreamExt;
    use httptest::matchers::{all_of, request};
    use httptest::responders::status_code;
    use httptest::Expectation;
    use nexus_client::types::{BlueprintTargetSet, SagaState};
    use nexus_db_model::{
        ByteCount, SledBaseboard, SledSystemHardware, SledUpdate, Zpool,
    };
    use nexus_db_queries::authn;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::DataStore;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        blueprint_zone_type, Blueprint, BlueprintPhysicalDisksConfig,
        BlueprintTarget, BlueprintZoneConfig, BlueprintZoneDisposition,
        BlueprintZoneType, BlueprintZonesConfig, CockroachDbPreserveDowngrade,
        OmicronZoneExternalFloatingIp, SledDetails, SledDisk, SledResources,
    };
    use nexus_types::deployment::{BlueprintZoneFilter, PlanningInputBuilder};
    use nexus_types::external_api::views::{
        PhysicalDiskPolicy, PhysicalDiskState, SledPolicy, SledProvisionPolicy,
        SledState,
    };
    use omicron_common::address::{IpRange, Ipv4Range, Ipv6Subnet};
    use omicron_common::api::external::{Generation, MacAddr, Vni};
    use omicron_common::api::internal::shared::{
        NetworkInterface, NetworkInterfaceKind,
    };
    use omicron_common::disk::DiskIdentity;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
    use omicron_uuid_kinds::ZpoolUuid;
    use omicron_uuid_kinds::{DemoSagaUuid, GenericUuid};
    use omicron_uuid_kinds::{ExternalIpUuid, SledUuid};
    use omicron_uuid_kinds::{OmicronZoneUuid, PhysicalDiskUuid};
    use oxnet::IpNet;
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    async fn create_blueprint(
        datastore: &DataStore,
        opctx: &OpContext,
        blueprint_zones: BTreeMap<SledUuid, BlueprintZonesConfig>,
        blueprint_disks: BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
        dns_version: Generation,
        enabled: bool,
    ) -> (BlueprintTarget, Blueprint) {
        let id = Uuid::new_v4();
        // Assume all sleds are active.
        let sled_state = blueprint_zones
            .keys()
            .copied()
            .map(|sled_id| (sled_id, SledState::Active))
            .collect::<BTreeMap<_, _>>();

        // Ensure the blueprint we're creating is the current target (required
        // for successful blueprint realization). This requires its parent to be
        // the existing target, so fetch that first.
        let current_target = datastore
            .blueprint_target_get_current(opctx)
            .await
            .expect("fetched current target blueprint");

        let target = BlueprintTarget {
            target_id: id,
            enabled,
            time_made_target: chrono::Utc::now(),
        };
        let blueprint = Blueprint {
            id,
            blueprint_zones,
            blueprint_disks,
            sled_state,
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            parent_blueprint_id: Some(current_target.target_id),
            internal_dns_version: dns_version,
            external_dns_version: dns_version,
            cockroachdb_fingerprint: String::new(),
            time_created: chrono::Utc::now(),
            creator: "test".to_string(),
            comment: "test blueprint".to_string(),
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
    async fn test_deploy_omicron_zones(cptestctx: &ControlPlaneTestContext) {
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
        let sled_id1 = SledUuid::new_v4();
        let sled_id2 = SledUuid::new_v4();
        let rack_id = Uuid::new_v4();
        for (i, (sled_id, server)) in
            [(sled_id1, &s1), (sled_id2, &s2)].iter().enumerate()
        {
            let SocketAddr::V6(addr) = server.addr() else {
                panic!("Expected Ipv6 address. Got {}", server.addr());
            };
            let update = SledUpdate::new(
                sled_id.into_untyped_uuid(),
                addr,
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
        let mut task = BlueprintExecutor::new(
            datastore.clone(),
            resolver.clone(),
            blueprint_rx,
            Uuid::new_v4(),
            Activator::new(),
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
            create_blueprint(
                &datastore,
                &opctx,
                BTreeMap::new(),
                BTreeMap::new(),
                generation,
                true,
            )
            .await,
        );
        let blueprint_id = blueprint.1.id;
        blueprint_tx.send(Some(blueprint)).unwrap();
        let value = task.activate(&opctx).await;
        println!("activating with no zones: {:?}", value);
        assert_eq!(value, json!({"target_id": blueprint_id}));

        // Create a non-empty blueprint describing two servers and verify that
        // the task correctly winds up making requests to both of them and
        // reporting success.
        fn make_zones(
            disposition: BlueprintZoneDisposition,
        ) -> BlueprintZonesConfig {
            let pool_id = ZpoolUuid::new_v4();
            BlueprintZonesConfig {
                generation: Generation::new(),
                zones: vec![BlueprintZoneConfig {
                    disposition,
                    id: OmicronZoneUuid::new_v4(),
                    underlay_address: "::1".parse().unwrap(),
                    filesystem_pool: Some(ZpoolName::new_external(pool_id)),
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
                }],
            }
        }

        let generation = generation.next();

        // Both in-service and quiesced zones should be deployed.
        //
        // TODO: add expunged zones to the test (should not be deployed).
        let mut blueprint = create_blueprint(
            &datastore,
            &opctx,
            BTreeMap::from([
                (sled_id1, make_zones(BlueprintZoneDisposition::InService)),
                (sled_id2, make_zones(BlueprintZoneDisposition::Quiesced)),
            ]),
            BTreeMap::new(),
            generation,
            true,
        )
        .await;
        let blueprint_id = blueprint.1.id;

        // Insert records for the zpools backing the datasets in these zones.
        for (sled_id, config) in
            blueprint.1.all_omicron_zones(BlueprintZoneFilter::All)
        {
            let Some(dataset) = config.zone_type.durable_dataset() else {
                continue;
            };

            let pool_id = dataset.dataset.pool_name.id();
            let zpool = Zpool::new(
                pool_id.into_untyped_uuid(),
                sled_id.into_untyped_uuid(),
                Uuid::new_v4(), // physical_disk_id
            );
            datastore
                .zpool_insert(&opctx, zpool)
                .await
                .expect("failed to upsert zpool");
        }

        blueprint_tx.send(Some(Arc::new(blueprint.clone()))).unwrap();

        // Make sure that requests get made to the sled agent.  This is not a
        // careful check of exactly what gets sent.  For that, see the tests in
        // nexus-reconfigurator-execution.
        for s in [&mut s1, &mut s2] {
            s.expect(
                Expectation::matching(all_of![request::method_path(
                    "PUT",
                    "/omicron-zones"
                ),])
                .respond_with(status_code(204)),
            );
        }

        // Activate the task to trigger zone configuration on the sled-agents
        let value = task.activate(&opctx).await;
        println!("activating two sled agents: {:?}", value);
        assert_eq!(value, json!({"target_id": blueprint_id}));
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Now, disable the target and make sure that we _don't_ invoke the sled
        // agent.  It's enough to just not set expectations.
        blueprint.1.internal_dns_version =
            blueprint.1.internal_dns_version.next();
        blueprint.0.enabled = false;
        blueprint_tx.send(Some(Arc::new(blueprint.clone()))).unwrap();
        let value = task.activate(&opctx).await;
        println!("when disabled: {:?}", value);
        assert_eq!(
            value,
            json!({
                "error": "blueprint disabled",
                "target_id": blueprint.1.id.to_string()
            })
        );
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Do it all again, but configure one of the servers to fail so we can
        // verify the task's returned summary of what happened.
        blueprint.0.enabled = true;
        blueprint_tx.send(Some(Arc::new(blueprint))).unwrap();
        s1.expect(
            Expectation::matching(request::method_path(
                "PUT",
                "/omicron-zones",
            ))
            .respond_with(status_code(204)),
        );
        s2.expect(
            Expectation::matching(request::method_path(
                "PUT",
                "/omicron-zones",
            ))
            .respond_with(status_code(500)),
        );

        #[derive(Deserialize)]
        struct ErrorResult {
            errors: Vec<String>,
        }

        let value = task.activate(&opctx).await;
        println!("after failure: {:?}", value);
        let result: ErrorResult = serde_json::from_value(value).unwrap();
        assert_eq!(result.errors.len(), 1);
        assert!(
            result.errors[0].starts_with("Failed to put OmicronZonesConfig")
        );
        s1.verify_and_clear();
        s2.verify_and_clear();
    }

    // Tests that sagas assigned to expunged Nexus zones get re-assigned,
    // recovered, and completed.
    #[nexus_test(server = crate::Server)]
    async fn test_saga_reassignment(cptestctx: &ControlPlaneTestContext) {
        let nexus_client = cptestctx.nexus_internal_client().await;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let logctx = &cptestctx.logctx;
        let opctx = OpContext::for_tests(logctx.log.clone(), datastore.clone());

        // First, create a blueprint whose execution will assign sagas to this
        // Nexus.
        let (new_nexus_zone_id, blueprint) = {
            let sled_id = SledUuid::new_v4();
            let rack_id = Uuid::new_v4();
            let update = SledUpdate::new(
                sled_id.into_untyped_uuid(),
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 12345, 0, 0),
                SledBaseboard {
                    serial_number: "one".into(),
                    part_number: "test".into(),
                    revision: 1,
                },
                SledSystemHardware {
                    is_scrimlet: false,
                    usable_hardware_threads: 4,
                    usable_physical_ram: ByteCount(1000.into()),
                    reservoir_size: ByteCount(999.into()),
                },
                rack_id,
                nexus_db_model::Generation::new(),
            );
            datastore
                .sled_upsert(update)
                .await
                .expect("Failed to insert sled to db");

            let new_nexus_zone_id = OmicronZoneUuid::new_v4();
            let pool_id = ZpoolUuid::new_v4();
            let zones = BTreeMap::from([(
                sled_id,
                BlueprintZonesConfig {
                    generation: Generation::new(),
                    zones: vec![BlueprintZoneConfig {
                        disposition: BlueprintZoneDisposition::Expunged,
                        id: new_nexus_zone_id,
                        underlay_address: "::1".parse().unwrap(),
                        filesystem_pool: Some(ZpoolName::new_external(pool_id)),
                        zone_type: BlueprintZoneType::Nexus(
                            blueprint_zone_type::Nexus {
                                internal_address: "[::1]:0".parse().unwrap(),
                                external_ip: OmicronZoneExternalFloatingIp {
                                    id: ExternalIpUuid::new_v4(),
                                    ip: Ipv4Addr::LOCALHOST.into(),
                                },
                                nic: NetworkInterface {
                                    id: Uuid::new_v4(),
                                    kind: NetworkInterfaceKind::Service {
                                        id: new_nexus_zone_id
                                            .into_untyped_uuid(),
                                    },
                                    name: "test-nic".parse().unwrap(),
                                    ip: Ipv4Addr::LOCALHOST.into(),
                                    mac: MacAddr::random_system(),
                                    subnet: IpNet::new(
                                        Ipv4Addr::LOCALHOST.into(),
                                        8,
                                    )
                                    .unwrap(),
                                    vni: Vni::SERVICES_VNI,
                                    primary: true,
                                    slot: 0,
                                    transit_ips: Default::default(),
                                },
                                external_tls: false,
                                external_dns_servers: Vec::new(),
                            },
                        ),
                    }],
                },
            )]);
            let (_, blueprint) = create_blueprint(
                &datastore,
                &opctx,
                zones,
                BTreeMap::new(),
                Generation::new(),
                false,
            )
            .await;
            (new_nexus_zone_id, blueprint)
        };

        // Create an entry in the database for a new "demo" saga owned by that
        // expunged Nexus.  There will be no log entries, so it's as though that
        // Nexus has just created the saga when it was expunged.
        let new_saga_id = steno::SagaId(Uuid::new_v4());
        println!("new saga id {}", new_saga_id);
        let other_nexus_sec_id =
            nexus_db_model::SecId(new_nexus_zone_id.into_untyped_uuid());
        let demo_saga_id = DemoSagaUuid::from_untyped_uuid(Uuid::new_v4());
        let saga_params = demo::Params { id: demo_saga_id };
        let dag = serde_json::to_value(
            create_saga_dag::<demo::SagaDemo>(saga_params)
                .expect("create demo saga DAG"),
        )
        .expect("serialize demo saga DAG");
        let params = steno::SagaCreateParams {
            id: new_saga_id,
            name: steno::SagaName::new("test saga"),
            dag,
            state: steno::SagaCachedState::Running,
        };
        let new_saga = nexus_db_model::Saga::new(other_nexus_sec_id, params);
        datastore.saga_create(&new_saga).await.expect("created saga");

        // Set the blueprint that we created as the target and enable execution.
        println!("setting blueprint target {}", blueprint.id);
        nexus_client
            .blueprint_target_set_enabled(&BlueprintTargetSet {
                target_id: blueprint.id,
                enabled: true,
            })
            .await
            .expect("set blueprint target");

        // Helper to take the response from bgtask_list() and pull out the
        // status for a particular background task.
        fn last_task_status_map<'a>(
            tasks: &'a BTreeMap<String, nexus_client::types::BackgroundTask>,
            name: &'a str,
        ) -> Result<
            &'a serde_json::Map<String, serde_json::Value>,
            CondCheckError<()>,
        > {
            let task = tasks
                .get(name)
                .with_context(|| format!("missing task {:?}", name))
                .unwrap();
            let nexus_client::types::LastResult::Completed(r) = &task.last
            else {
                println!(
                    "waiting for task {:?} to complete at least once",
                    name
                );
                return Err(CondCheckError::NotYet);
            };
            let details = &r.details;
            Ok(details
                .as_object()
                .with_context(|| {
                    format!("task {}: last status was not a map", name)
                })
                .unwrap())
        }

        fn status_target_id(
            status: &serde_json::Map<String, serde_json::Value>,
        ) -> Result<Uuid, CondCheckError<()>> {
            Ok(status
                .get("target_id")
                .ok_or_else(|| {
                    println!("waiting for task to report a target_id");
                    CondCheckError::NotYet
                })?
                .as_str()
                .expect("target_id was not a string")
                .parse()
                .expect("target_id was not a uuid"))
        }

        // Wait a bit for:
        //
        // - the new target blueprint to be loaded
        // - the new target blueprint to be executed
        // - the saga to be transferred
        //
        // We could just wait for the end result but debugging will be easier if
        // we can report where things got stuck.
        wait_for_condition(
            || async {
                let task_status = nexus_client
                    .bgtask_list()
                    .await
                    .expect("list background tasks");
                let tasks = task_status
                    .into_inner()
                    .into_iter()
                    .collect::<BTreeMap<_, _>>();

                // Wait for the loader to report our new blueprint as the
                // current target.
                let loader_status =
                    last_task_status_map(&tasks, "blueprint_loader")?;
                println!("waiting: found loader status: {:?}", loader_status);
                let target_id = status_target_id(&loader_status)?;
                if target_id != blueprint.id {
                    println!(
                        "waiting: taking lap (loader target id is not ours)"
                    );
                    return Err(CondCheckError::NotYet);
                }

                // Wait for the execution task to report having tried to execute
                // our blueprint.  For our purposes, it's not critical that it
                // succeeded.  It might fail for reasons not having to do with
                // what we're testing.
                let execution_status =
                    last_task_status_map(&tasks, "blueprint_executor")?;
                println!(
                    "waiting: found execution status: {:?}",
                    execution_status
                );
                let target_id = status_target_id(&execution_status)?;
                if target_id != blueprint.id {
                    println!(
                        "waiting: taking lap (execution target id is not ours)"
                    );
                    return Err(CondCheckError::NotYet);
                }

                // For debugging, print out the saga recovery task's status.
                // It's not worth trying to parse it.  And it might not yet have
                // even carried out recovery.
                println!(
                    "waiting: found saga recovery status: {:?}",
                    last_task_status_map(&tasks, "saga_recovery"),
                );

                // Wait too for our demo saga to show up in the saga list.  That
                // means recovery has completed.
                let sagas = nexus_client
                    .saga_list_stream(None, None)
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("listed sagas");
                if sagas.iter().any(|s| s.id == new_saga_id.0) {
                    Ok(())
                } else {
                    println!("waiting: taking lap (saga not yet found)");
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &Duration::from_millis(50),
            &Duration::from_secs(60),
        )
        .await
        .expect("execution completion");

        // Now complete the demo saga.
        nexus_client
            .saga_demo_complete(&demo_saga_id)
            .await
            .expect("demo saga complete");

        // And wait for it to actually finish.
        wait_for_condition(
            || async {
                let saga = nexus_client
                    .saga_list_stream(None, None)
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("listed sagas")
                    .into_iter()
                    .find(|s| s.id == new_saga_id.0)
                    .expect("demo saga in list of sagas");
                println!("checking saga status: {:?}", saga);
                if saga.state == SagaState::Succeeded {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &Duration::from_millis(50),
            &Duration::from_secs(10),
        )
        .await
        .expect("saga completion");
    }
}
