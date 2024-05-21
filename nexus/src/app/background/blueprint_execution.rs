// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for realizing a plan blueprint

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::{Blueprint, BlueprintTarget};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

/// Background task that takes a [`Blueprint`] and realizes the change to
/// the state of the system based on the `Blueprint`.
pub struct BlueprintExecutor {
    datastore: Arc<DataStore>,
    rx_blueprint: watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    nexus_label: String,
    tx: watch::Sender<usize>,
}

impl BlueprintExecutor {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_blueprint: watch::Receiver<
            Option<Arc<(BlueprintTarget, Blueprint)>>,
        >,
        nexus_label: String,
    ) -> BlueprintExecutor {
        let (tx, _) = watch::channel(0);
        BlueprintExecutor { datastore, rx_blueprint, nexus_label, tx }
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
            warn!(&opctx.log,
                      "Blueprint execution: skipped";
                      "reason" => "no blueprint");
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
            blueprint,
            &self.nexus_label,
        )
        .await;

        // Trigger anybody waiting for this to finish.
        self.tx.send_modify(|count| *count = *count + 1);

        // Return the result as a `serde_json::Value`
        match result {
            Ok(()) => json!({}),
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
    use crate::app::background::common::BackgroundTask;
    use httptest::matchers::{all_of, request};
    use httptest::responders::status_code;
    use httptest::Expectation;
    use nexus_db_model::{
        ByteCount, SledBaseboard, SledSystemHardware, SledUpdate,
    };
    use nexus_db_queries::authn;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        blueprint_zone_type, Blueprint, BlueprintPhysicalDisksConfig,
        BlueprintTarget, BlueprintZoneConfig, BlueprintZoneDisposition,
        BlueprintZoneType, BlueprintZonesConfig,
    };
    use nexus_types::external_api::views::SledState;
    use nexus_types::inventory::OmicronZoneDataset;
    use omicron_common::api::external::Generation;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SledUuid;
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::sync::watch;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    fn create_blueprint(
        blueprint_zones: BTreeMap<SledUuid, BlueprintZonesConfig>,
        blueprint_disks: BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
        dns_version: Generation,
    ) -> (BlueprintTarget, Blueprint) {
        let id = Uuid::new_v4();
        // Assume all sleds are active.
        let sled_state = blueprint_zones
            .keys()
            .copied()
            .map(|sled_id| (sled_id, SledState::Active))
            .collect::<BTreeMap<_, _>>();
        (
            BlueprintTarget {
                target_id: id,
                enabled: true,
                time_made_target: chrono::Utc::now(),
            },
            Blueprint {
                id,
                blueprint_zones,
                blueprint_disks,
                sled_state,
                cockroachdb_setting_preserve_downgrade: None,
                parent_blueprint_id: None,
                internal_dns_version: dns_version,
                external_dns_version: dns_version,
                cockroachdb_fingerprint: String::new(),
                time_created: chrono::Utc::now(),
                creator: "test".to_string(),
                comment: "test blueprint".to_string(),
            },
        )
    }

    #[nexus_test(server = crate::Server)]
    async fn test_deploy_omicron_zones(cptestctx: &ControlPlaneTestContext) {
        // Set up the test.
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
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
            blueprint_rx,
            String::from("test-suite"),
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
        let blueprint = Arc::new(create_blueprint(
            BTreeMap::new(),
            BTreeMap::new(),
            generation,
        ));
        blueprint_tx.send(Some(blueprint)).unwrap();
        let value = task.activate(&opctx).await;
        println!("activating with no zones: {:?}", value);
        assert_eq!(value, json!({}));

        // Create a non-empty blueprint describing two servers and verify that
        // the task correctly winds up making requests to both of them and
        // reporting success.
        fn make_zones(
            disposition: BlueprintZoneDisposition,
        ) -> BlueprintZonesConfig {
            BlueprintZonesConfig {
                generation: Generation::new(),
                zones: vec![BlueprintZoneConfig {
                    disposition,
                    id: OmicronZoneUuid::new_v4(),
                    underlay_address: "::1".parse().unwrap(),
                    zone_type: BlueprintZoneType::InternalDns(
                        blueprint_zone_type::InternalDns {
                            dataset: OmicronZoneDataset {
                                pool_name: format!("oxp_{}", Uuid::new_v4())
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
            BTreeMap::from([
                (sled_id1, make_zones(BlueprintZoneDisposition::InService)),
                (sled_id2, make_zones(BlueprintZoneDisposition::Quiesced)),
            ]),
            BTreeMap::new(),
            generation,
        );

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
        assert_eq!(value, json!({}));
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
}
