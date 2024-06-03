// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages deployment of Omicron zones to Sled Agents

use crate::Sled;
use anyhow::anyhow;
use anyhow::Context;
use cockroach_admin_client::types::NodeDecommission;
use cockroach_admin_client::types::NodeId;
use futures::stream;
use futures::StreamExt;
use internal_dns::resolver::Resolver;
use internal_dns::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::BlueprintZonesConfig;
use omicron_common::address::COCKROACH_ADMIN_PORT;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use slog::info;
use slog::warn;
use slog::Logger;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;

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

/// Itempontently perform any cleanup actions necessary for expunged zones.
///
/// # Panics
///
/// Panics if any of the zones yielded by the `expunged_zones` iterator has a
/// disposition other than `Expunged`.
pub(crate) async fn clean_up_expunged_zones(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &Resolver,
    expunged_zones: impl Iterator<Item = (SledUuid, &BlueprintZoneConfig)>,
) -> Result<(), Vec<anyhow::Error>> {
    let errors: Vec<anyhow::Error> = stream::iter(expunged_zones)
        .filter_map(|(sled_id, config)| async move {
            // It is a programmer error to call this function on a non-expunged
            // zone.
            assert_eq!(
                config.disposition,
                BlueprintZoneDisposition::Expunged,
                "clean_up_expunged_zones called with \
                 non-expunged zone {config:?}"
            );

            let log = opctx.log.new(slog::o!(
                "sled_id" => sled_id.to_string(),
                "zone_id" => config.id.to_string(),
                "zone_type" => config.zone_type.kind().to_string(),
            ));

            let result = match &config.zone_type {
                // Zones which need no cleanup work after expungement.
                BlueprintZoneType::Nexus(_) => None,

                // Zones which need cleanup after expungement.
                BlueprintZoneType::CockroachDb(_) => Some(
                    decommission_cockroachdb_node(
                        opctx, datastore, resolver, config.id, &log,
                    )
                    .await,
                ),

                // Zones that may or may not need cleanup work - we haven't
                // gotten to these yet!
                BlueprintZoneType::BoundaryNtp(_)
                | BlueprintZoneType::Clickhouse(_)
                | BlueprintZoneType::ClickhouseKeeper(_)
                | BlueprintZoneType::Crucible(_)
                | BlueprintZoneType::CruciblePantry(_)
                | BlueprintZoneType::ExternalDns(_)
                | BlueprintZoneType::InternalDns(_)
                | BlueprintZoneType::InternalNtp(_)
                | BlueprintZoneType::Oximeter(_) => {
                    warn!(
                        log,
                        "unsupported zone type for expungement cleanup; \
                         not performing any cleanup";
                    );
                    None
                }
            }?;

            match result {
                Err(error) => {
                    warn!(
                        log, "failed to clean up expunged zone";
                        "error" => #%error,
                    );
                    Some(error)
                }
                Ok(()) => {
                    info!(log, "successfully cleaned up after expunged zone");
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

async fn decommission_cockroachdb_node(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &Resolver,
    zone_id: OmicronZoneUuid,
    log: &Logger,
) -> anyhow::Result<()> {
    // We need the node ID of this zone. Check and see whether the
    // crdb_node_id_collector RPW found the node ID for this zone. If it hasn't,
    // we're in trouble: we don't know whether this zone came up far enough to
    // join the cockroach cluster (and therefore needs decommissioning) but was
    // expunged before the collector RPW could identify it, or if the zone never
    // joined the cockroach cluster (and therefore doesn't have a node ID and
    // doesn't need decommissioning).
    //
    // For now, we punt on this problem. If we were certain that the zone's
    // socket address could never be reused, we could ask one of the running
    // cockroach nodes for the status of all nodes, find the ID of the one with
    // this zone's listening address (if one exists), and decommission that ID.
    // But if the address could be reused, that risks decommissioning a live
    // node! We'll just log a warning and require a human to figure out how to
    // clean this up.
    let Some(node_id) =
        datastore.cockroachdb_node_id(opctx, zone_id).await.with_context(
            || format!("failed to look up node ID of cockroach zone {zone_id}"),
        )?
    else {
        warn!(
            log,
            "expunged cockroach zone has no known node ID; \
             support intervention is required for zone cleanup"
        );
        return Ok(());
    };

    // To decommission a CRDB node, we need to talk to one of the still-running
    // nodes; look one up in DNS and construct a client. We'll just do a DNS
    // lookup and take the first result; this might fail (e.g., if DNS still has
    // an entry for the expunged zone we're trying to decommission!), but we're
    // running from an RPW so any failure will be retried soon.
    //
    // TODO-cleanup: Replace this with a qorb-based connection pool once it's
    // ready.
    let crdb_ip = resolver
        .lookup_ipv6(ServiceName::Cockroach)
        .await
        .context("failed to resolve cockroach IP in internal DNS")?;
    let crdb_addr = format!(
        "http://{}",
        SocketAddrV6::new(crdb_ip, COCKROACH_ADMIN_PORT, 0, 0)
    );
    let log = log.new(slog::o!("crdb_addr" => crdb_addr.clone()));
    let client = cockroach_admin_client::Client::new(&crdb_addr, log.clone());

    let body = NodeId { node_id: node_id.clone() };
    let NodeDecommission {
        is_decommissioning,
        is_draining,
        is_live,
        membership,
        node_id: _,
        notes,
        replicas,
    } = client
        .node_decommission(&body)
        .await
        .with_context(|| {
            format!(
                "failed to decommission cockroach zone {zone_id} \
            (node id {node_id})"
            )
        })?
        .into_inner();

    info!(
        log, "successfully sent cockroach decommission request";
        "is_decommissioning" => is_decommissioning,
        "is_draining" => is_draining,
        "is_live" => is_live,
        "membership" => ?membership,
        "notes" => ?notes,
        "replicas" => replicas,
    );

    Ok(())
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
        blueprint_zone_type, BlueprintZoneType, CockroachDbPreserveDowngrade,
        OmicronZonesConfig,
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
                cockroachdb_setting_preserve_downgrade:
                    CockroachDbPreserveDowngrade::DoNotModify,
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
