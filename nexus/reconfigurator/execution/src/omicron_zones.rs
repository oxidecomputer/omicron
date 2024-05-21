// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages deployment of Omicron zones to Sled Agents

use crate::Sled;
use anyhow::anyhow;
use anyhow::Context;
use futures::stream;
use futures::StreamExt;
use nexus_db_queries::context::OpContext;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZonesConfig;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use slog::info;
use slog::warn;
use std::collections::BTreeMap;

/// Idempotently ensure that the specified Omicron zones are deployed to the
/// corresponding sleds
pub(crate) async fn deploy_zones(
    opctx: &OpContext,
    sleds_by_id: &BTreeMap<SledUuid, Sled>,
    zones: &BTreeMap<SledUuid, BlueprintZonesConfig>,
) -> Result<(), Vec<anyhow::Error>> {
    let errors: Vec<_> = stream::iter(zones)
        .filter_map(|(sled_id, config)| async move {
            let db_sled = match sleds_by_id.get(sled_id) {
                Some(sled) => sled,
                None => {
                    if config.are_all_zones_expunged() {
                        info!(
                            opctx.log,
                            "Skipping zone deployment to expunged sled";
                            "sled_id" => %sled_id
                        );
                        return None;
                    }
                    let err = anyhow!("sled not found in db list: {sled_id}");
                    warn!(opctx.log, "{err:#}");
                    return Some(err);
                }
            };

            let client = nexus_networking::sled_client_from_address(
                sled_id.into_untyped_uuid(),
                db_sled.sled_agent_address,
                &opctx.log,
            );
            let omicron_zones = config
                .to_omicron_zones_config(BlueprintZoneFilter::ShouldBeRunning);
            let result = client
                .omicron_zones_put(&omicron_zones)
                .await
                .with_context(|| {
                    format!(
                        "Failed to put {omicron_zones:#?} to sled {sled_id}"
                    )
                });
            match result {
                Err(error) => {
                    warn!(opctx.log, "{error:#}");
                    Some(error)
                }
                Ok(_) => {
                    info!(
                        opctx.log,
                        "Successfully deployed zones for sled agent";
                        "sled_id" => %sled_id,
                        "generation" => %config.generation,
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

#[cfg(test)]
mod test {
    use super::deploy_zones;
    use crate::Sled;
    use httptest::matchers::{all_of, json_decoded, request};
    use httptest::responders::status_code;
    use httptest::Expectation;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        blueprint_zone_type, BlueprintZoneType, OmicronZonesConfig,
    };
    use nexus_types::deployment::{
        Blueprint, BlueprintTarget, BlueprintZoneConfig,
        BlueprintZoneDisposition, BlueprintZonesConfig,
    };
    use nexus_types::inventory::OmicronZoneDataset;
    use omicron_common::api::external::Generation;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SledUuid;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    fn create_blueprint(
        blueprint_zones: BTreeMap<SledUuid, BlueprintZonesConfig>,
    ) -> (BlueprintTarget, Blueprint) {
        let id = Uuid::new_v4();
        (
            BlueprintTarget {
                target_id: id,
                enabled: true,
                time_made_target: chrono::Utc::now(),
            },
            Blueprint {
                id,
                blueprint_zones,
                blueprint_disks: BTreeMap::new(),
                sled_state: BTreeMap::new(),
                cockroachdb_setting_preserve_downgrade: None,
                parent_blueprint_id: None,
                internal_dns_version: Generation::new(),
                external_dns_version: Generation::new(),
                cockroachdb_fingerprint: String::new(),
                time_created: chrono::Utc::now(),
                creator: "test".to_string(),
                comment: "test blueprint".to_string(),
            },
        )
    }

    #[nexus_test]
    async fn test_deploy_omicron_zones(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Create some fake sled-agent servers to respond to zone puts and add
        // sleds to CRDB.
        let mut s1 = httptest::Server::run();
        let mut s2 = httptest::Server::run();
        let sled_id1 = SledUuid::new_v4();
        let sled_id2 = SledUuid::new_v4();
        let sleds_by_id: BTreeMap<SledUuid, Sled> =
            [(sled_id1, &s1), (sled_id2, &s2)]
                .into_iter()
                .map(|(sled_id, server)| {
                    let SocketAddr::V6(addr) = server.addr() else {
                        panic!("Expected Ipv6 address. Got {}", server.addr());
                    };
                    let sled = Sled {
                        id: sled_id,
                        sled_agent_address: addr,
                        is_scrimlet: false,
                    };
                    (sled_id, sled)
                })
                .collect();

        // Get a success result back when the blueprint has an empty set of
        // zones.
        let (_, blueprint) = create_blueprint(BTreeMap::new());
        deploy_zones(&opctx, &sleds_by_id, &blueprint.blueprint_zones)
            .await
            .expect("failed to deploy no zones");

        // Zones are updated in a particular order, but each request contains
        // the full set of zones that must be running.
        // See `rack_setup::service::ServiceInner::run` for more details.
        fn make_zones() -> BlueprintZonesConfig {
            BlueprintZonesConfig {
                generation: Generation::new(),
                zones: vec![BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
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
                            http_address: "[::1]:0".parse().unwrap(),
                        },
                    ),
                }],
            }
        }

        // Create a blueprint with only the `InternalDns` zone for both servers
        // We reuse the same `OmicronZonesConfig` because the details don't
        // matter for this test.
        let mut zones1 = make_zones();
        let mut zones2 = make_zones();
        let (_, blueprint) = create_blueprint(BTreeMap::from([
            (sled_id1, zones1.clone()),
            (sled_id2, zones2.clone()),
        ]));

        // Set expectations for the initial requests sent to the fake
        // sled-agents.
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

        // Execute it.
        deploy_zones(&opctx, &sleds_by_id, &blueprint.blueprint_zones)
            .await
            .expect("failed to deploy initial zones");

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
        deploy_zones(&opctx, &sleds_by_id, &blueprint.blueprint_zones)
            .await
            .expect("failed to deploy same zones");
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

        let errors =
            deploy_zones(&opctx, &sleds_by_id, &blueprint.blueprint_zones)
                .await
                .expect_err("unexpectedly succeeded in deploying zones");

        println!("{:?}", errors);
        assert_eq!(errors.len(), 1);
        assert!(errors[0]
            .to_string()
            .starts_with("Failed to put OmicronZonesConfig"));
        s1.verify_and_clear();
        s2.verify_and_clear();

        // Add an `InternalNtp` zone for our next update
        fn append_zone(
            zones: &mut BlueprintZonesConfig,
            disposition: BlueprintZoneDisposition,
        ) {
            zones.zones.push(BlueprintZoneConfig {
                disposition,
                id: OmicronZoneUuid::new_v4(),
                underlay_address: "::1".parse().unwrap(),
                zone_type: BlueprintZoneType::InternalNtp(
                    blueprint_zone_type::InternalNtp {
                        address: "[::1]:0".parse().unwrap(),
                        dns_servers: vec!["::1".parse().unwrap()],
                        domain: None,
                        ntp_servers: vec!["some-ntp-server-addr".into()],
                    },
                ),
            });
        }

        // Both in-service and quiesced zones should be deployed.
        //
        // The expunged zone should not be deployed.
        append_zone(&mut zones1, BlueprintZoneDisposition::InService);
        append_zone(&mut zones1, BlueprintZoneDisposition::Expunged);
        append_zone(&mut zones2, BlueprintZoneDisposition::Quiesced);
        // Bump the generation for each config
        zones1.generation = zones1.generation.next();
        zones2.generation = zones2.generation.next();

        let (_, blueprint) = create_blueprint(BTreeMap::from([
            (sled_id1, zones1),
            (sled_id2, zones2),
        ]));

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
        deploy_zones(&opctx, &sleds_by_id, &blueprint.blueprint_zones)
            .await
            .expect("failed to deploy last round of zones");
        s1.verify_and_clear();
        s2.verify_and_clear();
    }
}
