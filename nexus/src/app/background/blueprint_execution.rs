// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for realizing a plan blueprint

use super::common::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::stream;
use futures::FutureExt;
use futures::StreamExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::{Blueprint, OmicronZonesConfig};
use serde_json::json;
use sled_agent_client::Client as SledAgentClient;
use slog::Logger;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

/// Background task that takes a [`Blueprint`] and realizes the change to
/// the state of the system based on the `Blueprint`.
pub struct BlueprintExecutor {
    datastore: Arc<DataStore>,
    rx_blueprint: watch::Receiver<Option<Arc<Blueprint>>>,
}

impl BlueprintExecutor {
    // Temporary until we wire up the background task
    #[allow(unused)]
    pub fn new(
        datastore: Arc<DataStore>,
        rx_blueprint: watch::Receiver<Option<Arc<Blueprint>>>,
    ) -> BlueprintExecutor {
        BlueprintExecutor { datastore, rx_blueprint }
    }

    // This is a modified copy of the functionality from `nexus/src/app/sled.rs`.
    // There's no good way to access this functionality right now since it is a
    // method on the `Nexus` type. We want to have a more constrained type we can
    // pass into background tasks for this type of functionality, but for now we
    // just copy the functionality.
    async fn sled_client(
        &self,
        opctx: &OpContext,
        sled_id: &Uuid,
    ) -> Result<SledAgentClient, anyhow::Error> {
        let (.., sled) = LookupPath::new(opctx, &self.datastore)
            .sled_id(*sled_id)
            .fetch()
            .await
            .with_context(|| {
                format!(
                    "Failed to create sled_agent::Client for sled_id: {}",
                    sled_id
                )
            })?;
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .unwrap();
        Ok(SledAgentClient::new_with_client(
            &format!("http://{}", sled.address()),
            client,
            opctx.log.clone(),
        ))
    }

    async fn realize_blueprint(
        &self,
        opctx: &OpContext,
        blueprint: &std::sync::Arc<Blueprint>,
    ) -> Result<(), Vec<anyhow::Error>> {
        let log = opctx.log.new(o!("comment" => blueprint.comment.clone()));
        self.deploy_zones(&log, opctx, &blueprint.omicron_zones).await
    }

    async fn deploy_zones(
        &self,
        log: &Logger,
        opctx: &OpContext,
        zones: &BTreeMap<Uuid, OmicronZonesConfig>,
    ) -> Result<(), Vec<anyhow::Error>> {
        let errors: Vec<_> = stream::iter(zones.clone())
            .filter_map(|(sled_id, config)| async move {
                let client = match self.sled_client(&opctx, &sled_id).await {
                    Ok(client) => client,
                    Err(err) => {
                        warn!(log, "{err:#}");
                        return Some(err);
                    }
                };
                let result = client
                    .omicron_zones_put(&config)
                    .await
                    .with_context(|| {
                        format!("Failed to put {config:#?} to sled {sled_id}")
                    });

                match result {
                    Err(error) => {
                        warn!(log, "{error:#}");
                        Some(error)
                    }
                    Ok(_) => {
                        info!(
                            log,
                            "Successfully deployed zones for sled agent";
                            "sled_id" => %sled_id,
                            "generation" => config.generation.to_string()
                        );
                        None
                    }
                }
            })
            .collect()
            .await;

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl BackgroundTask for BlueprintExecutor {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            // Get the latest blueprint, cloning to prevent holding a read lock
            // on the watch.
            let blueprint = self.rx_blueprint.borrow_and_update().clone();

            let Some(blueprint) = blueprint else {
                warn!(&opctx.log,
                      "Plan execution: skipped";
                      "reason" => "no blueprint");
                return json!({"error": "no blueprint" });
            };

            let result = self.realize_blueprint(opctx, &blueprint).await;

            // Return the result as a `serde_json::Value`
            match result {
                Ok(()) => json!({}),
                Err(errors) => {
                    let errors: Vec<_> = errors
                        .into_iter()
                        .map(|e| format!("{:#}", e))
                        .collect();
                    json!({"errors": errors})
                }
            }
        }
        .boxed()
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::common::BackgroundTask;
    use httptest::matchers::{all_of, json_decoded, request};
    use httptest::responders::status_code;
    use httptest::Expectation;
    use nexus_db_model::{
        ByteCount, SledBaseboard, SledSystemHardware, SledUpdate,
    };
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::inventory::{
        OmicronZoneConfig, OmicronZoneDataset, OmicronZoneType,
    };
    use omicron_common::api::external::Generation;
    use serde::Deserialize;
    use std::collections::BTreeSet;
    use std::net::SocketAddr;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    fn create_blueprint(
        omicron_zones: BTreeMap<Uuid, OmicronZonesConfig>,
    ) -> Blueprint {
        Blueprint {
            id: Uuid::new_v4(),
            omicron_zones,
            zones_in_service: BTreeSet::new(),
            parent_blueprint_id: None,
            time_created: chrono::Utc::now(),
            creator: "test".to_string(),
            comment: "test blueprint".to_string(),
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_deploy_omicron_zones(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let (blueprint_tx, blueprint_rx) = watch::channel(None);
        let mut task = BlueprintExecutor::new(datastore.clone(), blueprint_rx);

        // With no blueprint we should fail with an appropriate message.
        let value = task.activate(&opctx).await;
        assert_eq!(value, json!({"error": "no blueprint"}));

        // Get a success (empty) result back when the blueprint has an empty set of zones
        let blueprint = Arc::new(create_blueprint(BTreeMap::new()));
        blueprint_tx.send(Some(blueprint)).unwrap();
        let value = task.activate(&opctx).await;
        assert_eq!(value, json!({}));

        // Create some fake sled-agent servers to respond to zone puts and add
        // sleds to CRDB.
        let mut s1 = httptest::Server::run();
        let mut s2 = httptest::Server::run();
        let sled_id1 = Uuid::new_v4();
        let sled_id2 = Uuid::new_v4();
        let rack_id = Uuid::new_v4();
        for (i, (sled_id, server)) in
            [(sled_id1, &s1), (sled_id2, &s2)].iter().enumerate()
        {
            let SocketAddr::V6(addr) = server.addr() else {
                panic!("Expected Ipv6 address. Got {}", server.addr());
            };
            let update = SledUpdate::new(
                *sled_id,
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
            );
            datastore
                .sled_upsert(update)
                .await
                .expect("Failed to insert sled to db");
        }

        // The particular dataset doesn't matter for this test.
        // We re-use the same one to not obfuscate things
        let dataset = OmicronZoneDataset {
            pool_name: format!("oxp_{}", Uuid::new_v4()).parse().unwrap(),
        };

        let generation = Generation::new();

        // Zones are updated in a particular order, but each request contains
        // the full set of zones that must be running.
        // See `rack_setup::service::ServiceInner::run` for more details.
        let mut zones = OmicronZonesConfig {
            generation,
            zones: vec![OmicronZoneConfig {
                id: Uuid::new_v4(),
                underlay_address: "::1".parse().unwrap(),
                zone_type: OmicronZoneType::InternalDns {
                    dataset,
                    dns_address: "oh-hello-internal-dns".into(),
                    gz_address: "::1".parse().unwrap(),
                    gz_address_index: 0,
                    http_address: "some-ipv6-address".into(),
                },
            }],
        };

        // Create a blueprint with only the `InternalDns` zone for both servers
        // We reuse the same `OmicronZonesConfig` because the details don't
        // matter for this test.
        let blueprint = Arc::new(create_blueprint(BTreeMap::from([
            (sled_id1, zones.clone()),
            (sled_id2, zones.clone()),
        ])));

        // Send the blueprint with the first set of zones to the task
        blueprint_tx.send(Some(blueprint)).unwrap();

        // Check that the initial requests were sent to the fake sled-agents
        for s in [&mut s1, &mut s2] {
            s.expect(
                Expectation::matching(all_of![
                    request::method_path("PUT", "/omicron-zones",),
                    // Our generation number should be 1 and there should
                    // be only a single zone.
                    request::body(json_decoded(|c: &OmicronZonesConfig| {
                        c.generation == 1u32.into() && c.zones.len() == 1
                    }))
                ])
                .respond_with(status_code(204)),
            );
        }

        // Activate the task to trigger zone configuration on the sled-agents
        let value = task.activate(&opctx).await;
        assert_eq!(value, json!({}));
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Do it again. This should trigger the same request.
        for s in [&mut s1, &mut s2] {
            s.expect(
                Expectation::matching(request::method_path(
                    "PUT",
                    "/omicron-zones",
                ))
                .respond_with(status_code(204)),
            );
        }
        let value = task.activate(&opctx).await;
        assert_eq!(value, json!({}));
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Take another lap, but this time, have one server fail the request and
        // try again.
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

        // Define a type we can use to pick stuff out of error objects.
        #[derive(Deserialize)]
        struct ErrorResult {
            errors: Vec<String>,
        }

        let value = task.activate(&opctx).await;
        println!("{:?}", value);
        let result: ErrorResult = serde_json::from_value(value).unwrap();
        assert_eq!(result.errors.len(), 1);
        assert!(
            result.errors[0].starts_with("Failed to put OmicronZonesConfig")
        );
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Add an `InternalNtp` zone for our next update
        zones.generation = generation.next();
        zones.zones.push(OmicronZoneConfig {
            id: Uuid::new_v4(),
            underlay_address: "::1".parse().unwrap(),
            zone_type: OmicronZoneType::InternalNtp {
                address: "::1".into(),
                dns_servers: vec!["::1".parse().unwrap()],
                domain: None,
                ntp_servers: vec!["some-ntp-server-addr".into()],
            },
        });

        // Update our watch channel
        let blueprint = Arc::new(create_blueprint(BTreeMap::from([
            (sled_id1, zones.clone()),
            (sled_id2, zones.clone()),
        ])));
        blueprint_tx.send(Some(blueprint)).unwrap();

        // Set our new expectations
        for s in [&mut s1, &mut s2] {
            s.expect(
                Expectation::matching(all_of![
                    request::method_path("PUT", "/omicron-zones",),
                    // Our generation number should be bumped and there should
                    // be two zones.
                    request::body(json_decoded(|c: &OmicronZonesConfig| {
                        c.generation == 2u32.into() && c.zones.len() == 2
                    }))
                ])
                .respond_with(status_code(204)),
            );
        }

        // Activate the task
        let value = task.activate(&opctx).await;
        assert_eq!(value, json!({}));
        s1.verify_and_clear();
        s2.verify_and_clear();
    }
}
