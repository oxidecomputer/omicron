// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod common;

use crate::common::reconfigurator::blueprint_load_target_enabled;
use crate::common::reconfigurator::blueprint_wait_sled_configs_propagated;
use anyhow::Context;
use common::LiveTestContext;
use common::reconfigurator::blueprint_edit_current_target;
use internal_dns_types::config::DnsRecord;
use internal_dns_types::names::ServiceName;
use live_tests_macros::live_test;
use nexus_db_model::DbMetadataNexusState;
use nexus_lockstep_client::types::QuiesceState;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::blueprint_editor::ExternalNetworkingAllocator;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::PlannerConfig;
use nexus_types::deployment::blueprint_zone_type;
use omicron_common::api::external::Generation;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use slog::debug;
use slog::info;
use slog::o;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::time::Duration;

#[live_test]
async fn test_nexus_handoff(lc: &LiveTestContext) {
    // Test setup
    let log = lc.log();
    let opctx = lc.opctx();
    let datastore = lc.datastore();
    let initial_nexus_clients = lc.all_internal_nexus_clients().await.unwrap();
    let nexus = initial_nexus_clients.first().expect("internal Nexus client");

    // Make sure we're starting from a known-normal state.
    // First, we have an enabled target blueprint.
    let blueprint_initial = blueprint_load_target_enabled(log, nexus)
        .await
        .expect("loading initial target blueprint");
    // That blueprint should be propagated to all sleds.  We wait just a bit
    // here to deal with races set up by other tests failing or other ongoing
    // activity.
    let _collection = blueprint_wait_sled_configs_propagated(
        opctx,
        datastore,
        &blueprint_initial,
        nexus,
        Duration::from_secs(15),
    )
    .await
    .expect("verifying initial blueprints' sled configs propagated");
    // Check that there's no Nexus handoff already pending.  That means that
    // there exist no Nexus zones with a generation newer than the blueprint's
    // `nexus_generation`.
    let new_zones = blueprint_initial
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(_sled_id, z)| {
            let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                nexus_generation,
                ..
            }) = &z.zone_type
            else {
                return None;
            };
            (*nexus_generation > blueprint_initial.nexus_generation)
                .then_some(z.id)
        })
        .collect::<Vec<_>>();
    if !new_zones.is_empty() {
        panic!(
            "handoff in progress!  found zones with generation newer than \
             current blueprint generation ({}): {}",
            blueprint_initial.nexus_generation,
            new_zones
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Identify the current generation of Nexus zones.
    struct CurrentNexusZone<'a> {
        sled_id: SledUuid,
        image_source: &'a BlueprintZoneImageSource,
        cfg: &'a blueprint_zone_type::Nexus,
    }
    let current_nexus_zones: BTreeMap<OmicronZoneUuid, _> = blueprint_initial
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(sled_id, z)| {
            let BlueprintZoneType::Nexus(
                cfg @ blueprint_zone_type::Nexus { nexus_generation, .. },
            ) = &z.zone_type
            else {
                return None;
            };
            (*nexus_generation == blueprint_initial.nexus_generation).then(
                || {
                    (
                        z.id,
                        CurrentNexusZone {
                            sled_id,
                            image_source: &z.image_source,
                            cfg,
                        },
                    )
                },
            )
        })
        .collect();
    assert!(
        !current_nexus_zones.is_empty(),
        "found no Nexus instances at current nexus generation"
    );
    let nexus_clients: BTreeMap<_, _> = current_nexus_zones
        .iter()
        .map(|(id, current)| {
            (
                id,
                lc.specific_internal_nexus_client(
                    current.cfg.lockstep_address(),
                ),
            )
        })
        .collect();

    // All Nexus instances ought to be running normally, not quiesced.
    info!(log, "checking current Nexus status");
    for (id, nexus) in &nexus_clients {
        let qq = nexus
            .quiesce_get()
            .await
            .expect("fetching quiesce state")
            .into_inner();
        let QuiesceState::Running = qq.state else {
            panic!("Nexus {id} is in unexpected quiesce state: {qq:?}");
        };
    }
    info!(log, "all test prerequisites complete");

    // We're finally ready to start the test!
    //
    // For each zone in the current generation, create a replacement at the next
    // generation.
    let next_generation = blueprint_initial.nexus_generation.next();
    let planner_config = datastore
        .reconfigurator_config_get_latest(opctx)
        .await
        .expect("obtained latest reconfigurator config")
        .map_or_else(PlannerConfig::default, |cs| cs.config.planner_config);
    let planning_input =
        PlanningInputFromDb::assemble(opctx, datastore, planner_config)
            .await
            .expect("planning input");
    let (_blueprint_initial, blueprint_new_nexus) =
        blueprint_edit_current_target(
            log,
            &planning_input,
            &nexus,
            &|builder: &mut BlueprintBuilder| {
                let mut external_networking_alloc =
                    ExternalNetworkingAllocator::from_current_zones(
                        builder,
                        planning_input.external_ip_policy(),
                    )
                    .context("constructing ExternalNetworkingAllocator")?;
                for current_nexus in current_nexus_zones.values() {
                    let external_ip = external_networking_alloc
                        .for_new_nexus()
                        .context("choosing external IP for new Nexus")?;
                    builder
                        .sled_add_zone_nexus(
                            current_nexus.sled_id,
                            current_nexus.image_source.clone(),
                            external_ip,
                            next_generation,
                        )
                        .context("adding Nexus zone")?;
                }
                Ok(())
            },
        )
        .await
        .expect("editing blueprint to add zones");
    info!(
        log,
        "wrote new target blueprint with new Nexus zones";
        "blueprint_id" => %blueprint_new_nexus.id
    );

    // Find the new Nexus zones and make clients for them.
    let new_nexus_clients = blueprint_new_nexus
        .all_nexus_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(_sled_id, zone_cfg, nexus_config)| {
            (nexus_config.nexus_generation == next_generation).then(|| {
                (
                    zone_cfg.id,
                    lc.specific_internal_nexus_client(
                        nexus_config.lockstep_address(),
                    ),
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(new_nexus_clients.len(), current_nexus_zones.len());

    // Wait for the zones to be running.
    //
    // This does not mean that their Nexus instances are immediately available.
    // SMF may still be starting up the zone.  Even once the Nexus process
    // starts, it will become blocked on the "not yet" DbMetadataNexusState,
    // waiting for handoff.
    let _collection = blueprint_wait_sled_configs_propagated(
        opctx,
        datastore,
        &blueprint_new_nexus,
        nexus,
        Duration::from_secs(180),
    )
    .await
    .expect("three new Nexus zones running");
    info!(
        log,
        "blueprint configs propagated";
        "blueprint_id" => %blueprint_new_nexus.id
    );

    // Check that the db_metadata_nexus records for the new Nexus instances
    // exist.
    let new_records = datastore
        .database_nexus_access_all(
            opctx,
            &new_nexus_clients.keys().cloned().collect(),
        )
        .await
        .expect("fetching db_metadata_nexus records");
    assert_eq!(new_records.len(), new_nexus_clients.len());
    assert!(
        new_records.iter().all(|r| r.state() == DbMetadataNexusState::NotYet)
    );

    // Create a demo saga that will hold up quiescing.
    let demo_nexus =
        nexus_clients.values().next().expect("at least one Nexus client");
    let demo_saga =
        demo_nexus.saga_demo_create().await.expect("demo saga create");
    info!(log, "created demo saga"; "demo_saga" => ?demo_saga);

    // Now update the target blueprint to trigger a handoff.
    let planning_input =
        PlanningInputFromDb::assemble(opctx, datastore, planner_config)
            .await
            .expect("planning input");
    let (_blueprint_new_nexus, blueprint_handoff) =
        blueprint_edit_current_target(
            log,
            &planning_input,
            &nexus,
            &|builder: &mut BlueprintBuilder| {
                builder.set_nexus_generation(next_generation);
                Ok(())
            },
        )
        .await
        .expect("editing blueprint to bump nexus_generation");
    info!(
        log,
        "wrote new target blueprint with new nexus generation";
        "blueprint_id" => %blueprint_handoff.id
    );

    // The old Nexus zones should pretty soon report that they're quiescing, but
    // not quiesced because of the one outstanding saga.
    for (id, nexus) in &nexus_clients {
        wait_for_condition(
            || async {
                let qq = nexus
                    .quiesce_get()
                    .await
                    .expect("fetching quiesce status")
                    .into_inner();
                debug!(
                    log,
                    "Nexus quiesce state";
                    "nexus_id" => %id,
                    "state" => ?qq.state
                );
                match qq.state {
                    QuiesceState::DrainingSagas { .. } => Ok(()),
                    _ => Err(CondCheckError::<()>::NotYet),
                }
            },
            &Duration::from_secs(1),
            &Duration::from_secs(90),
        )
        .await
        .expect("waiting for Nexus to report quiescing");

        info!(log, "Nexus reports quiescing"; "nexus_id" => %id);
    }

    // At this point, we should not yet be able to reach the new Nexus instances
    // because they're sitting waiting for the database to be ready for handoff.
    for (id, nexus) in &new_nexus_clients {
        if let Ok(_) = nexus.quiesce_get().await {
            panic!("unexpectedly reached Nexus {id}");
        }
    }
    info!(log, "new Nexus instances are still not reachable (good)");

    // This is the last point before triggering the handoff.  Check the contents
    // of both internal and external DNS for Nexus.  We should only see the
    // original Nexus zones.  It's a little hard to be sure this isn't working
    // by accident: it's conceivable that the DNS behavior is wrong, but that we
    // just haven't updated the DNS servers.  Since we expect no change to the
    // DNS servers, there's nothing for us to wait for to be sure nothing
    // changed.  The only way to be sure would be to dig into the blueprint
    // execution state to confirm that we successfully propagated DNS for the
    // blueprint that we executed.  That seems more trouble than it's worth
    // here.  We do know that we got through at least some of this blueprint's
    // execution, based on having seen the sled configurations get propagated to
    // sled agent.
    check_internal_dns(
        log,
        &blueprint_initial,
        blueprint_initial.nexus_generation,
    )
    .await
    .expect("internal DNS (before)");
    check_external_dns(
        log,
        &blueprint_initial,
        blueprint_initial.nexus_generation,
    )
    .await
    .expect("external DNS (before)");

    // Complete the demo saga to unblock quiescing.
    demo_nexus
        .saga_demo_complete(&demo_saga.demo_saga_id)
        .await
        .expect("complete demos aga");

    // Now wait for the old Nexus zones to report being fully quiesced.
    for (id, nexus) in &nexus_clients {
        wait_for_condition(
            || async {
                let qq = nexus
                    .quiesce_get()
                    .await
                    .expect("fetching quiesce status")
                    .into_inner();
                debug!(
                    log,
                    "Nexus quiesce state";
                    "nexus_id" => %id,
                    "state" => ?qq.state
                );
                match qq.state {
                    QuiesceState::Quiesced { .. } => Ok(()),
                    _ => Err(CondCheckError::<()>::NotYet),
                }
            },
            &Duration::from_secs(1),
            &Duration::from_secs(90),
        )
        .await
        .expect("waiting for old Nexus to report quiesced");

        info!(log, "Nexus reports quiesced"; "nexus_id" => %id);
    }
    info!(log, "all old Nexus instances report quiescing!");

    // Now wait for the new Nexus zones to report being online.
    for (id, nexus) in &new_nexus_clients {
        wait_for_condition(
            || async {
                match nexus.quiesce_get().await {
                    Ok(qq) => {
                        debug!(
                            log,
                            "new Nexus quiesce state";
                            "state" => ?qq.state
                        );
                        match qq.state {
                            QuiesceState::Undetermined => {
                                Err(CondCheckError::<()>::NotYet)
                            }
                            QuiesceState::Running => Ok(()),
                            _ => panic!("unexpected new Nexus quiesce state"),
                        }
                    }
                    Err(error) => {
                        debug!(
                            log,
                            "error fetching new Nexus quiesce state";
                            InlineErrorChain::new(&error),
                        );
                        Err(CondCheckError::NotYet)
                    }
                }
            },
            &Duration::from_secs(1),
            &Duration::from_secs(90),
        )
        .await
        .expect("waiting for old Nexus to report quiesced");
        info!(log, "new Nexus reports running"; "id" => %id);
    }
    info!(log, "all new Nexus instances report running!");

    // Clean up: expunge the old Nexus instances.
    let planning_input =
        PlanningInputFromDb::assemble(opctx, datastore, planner_config)
            .await
            .expect("planning input");
    let new_nexus =
        new_nexus_clients.values().next().expect("one new Nexus client");
    let (_blueprint_handoff, blueprint_cleanup) =
        blueprint_edit_current_target(
            log,
            &planning_input,
            new_nexus,
            &|builder: &mut BlueprintBuilder| {
                for (id, current_zone) in &current_nexus_zones {
                    builder
                        .sled_expunge_zone(current_zone.sled_id, *id)
                        .context("expunging zone")?;
                }

                Ok(())
            },
        )
        .await
        .expect("editing blueprint to expunge old Nexus zones");
    info!(
        log,
        "wrote new target blueprint with expunged zones";
        "blueprint_id" => %blueprint_cleanup.id
    );

    // Wait for this to get propagated everywhere.  This way, when the test
    // completes, the system will be at rest again, not still cleaning up.
    let _latest_collection = blueprint_wait_sled_configs_propagated(
        opctx,
        datastore,
        &blueprint_cleanup,
        new_nexus,
        Duration::from_secs(120),
    )
    .await
    .expect("waiting for cleanup sled configs");

    // Verify that DNS has been updated to reflect the handoff.  This time,
    // since we expect a change, we can wait for it to happen.
    wait_for_condition(
        || async {
            check_internal_dns(log, &blueprint_handoff, next_generation)
                .await?;
            check_external_dns(log, &blueprint_handoff, next_generation)
                .await?;
            Ok::<_, CondCheckError<anyhow::Error>>(())
        },
        &Duration::from_secs(1),
        &Duration::from_secs(30),
    )
    .await
    .expect("waiting for post-handoff DNS update");
}

/// Checks that current internal DNS reflects that the Nexus instances
/// in-service right now are the ones having generation `active_generation`.
///
/// Returns `CondCheckError` on failure for use in `wait_for_condition`.
async fn check_internal_dns(
    log: &slog::Logger,
    blueprint: &Blueprint,
    active_generation: Generation,
) -> Result<(), CondCheckError<anyhow::Error>> {
    // Compute what we expect to find, based on which Nexus instances in the
    // blueprint have the specified generation.
    let expected_nexus_addrs = blueprint
        .all_nexus_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(_sled_id, _zone_cfg, nexus_config)| {
            (nexus_config.nexus_generation == active_generation)
                .then_some(nexus_config.internal_address)
        })
        .collect::<BTreeSet<_>>();

    // Find the DNS server based on what's currently in the blueprint.
    let dns_sockaddr = blueprint
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .find_map(|(_sled_id, zone_cfg)| {
            if let BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns { dns_address, .. },
            ) = &zone_cfg.zone_type
            {
                Some(dns_address)
            } else {
                None
            }
        })
        .expect("at least one internal DNS server");

    // Make a resolver using this DNS server.
    let resolver_log = log.new(o!("component" => "VerifyInternalDnsResolver"));
    let resolver = internal_dns_resolver::Resolver::new_from_addrs(
        resolver_log,
        &[SocketAddr::from(*dns_sockaddr)],
    )
    .context("creating resolver")
    .map_err(CondCheckError::Failed)?;

    // Finally, look up Nexus in DNS and compare it to what we expected.
    let found_nexus_addrs = resolver
        .lookup_all_socket_v6(ServiceName::Nexus)
        .await
        .map_err(|_| CondCheckError::NotYet)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    debug!(
        log,
        "check_internal_dns";
        "expected" => ?expected_nexus_addrs,
        "found" => ?found_nexus_addrs,
    );

    if expected_nexus_addrs == found_nexus_addrs {
        Ok(())
    } else {
        Err(CondCheckError::NotYet)
    }
}

/// Checks that current external DNS reflects that the Nexus instances
/// in-service right now are the ones having generation `active_generation`.
///
/// Returns `CondCheckError` on failure for use in `wait_for_condition`.
async fn check_external_dns(
    log: &slog::Logger,
    blueprint: &Blueprint,
    active_generation: Generation,
) -> Result<(), CondCheckError<anyhow::Error>> {
    // Compute which Nexus instances we expect to find in external DNS based on
    // what's in-service in the blueprint.
    let expected_nexus_addrs = blueprint
        .all_nexus_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(_sled_id, _zone_cfg, nexus_config)| {
            (nexus_config.nexus_generation == active_generation)
                .then_some(nexus_config.external_ip.ip)
        })
        .collect::<BTreeSet<_>>();

    // Find the DNS server based on what's currently in the blueprint.
    let dns_http_sockaddr = blueprint
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .find_map(|(_sled_id, zone_cfg)| {
            if let BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns { http_address, .. },
            ) = &zone_cfg.zone_type
            {
                Some(http_address)
            } else {
                None
            }
        })
        .expect("at least one external DNS server");

    // Unfortunately for us, the external DNS servers are not necessarily
    // reachable from where we are.  So we can't directly look up names in DNS.
    // Instead, use the HTTP (config) interface.
    let url = format!("http://{}", dns_http_sockaddr);
    let client = dns_service_client::Client::new(&url, log.clone());
    let config = client
        .dns_config_get()
        .await
        .map_err(|_| CondCheckError::NotYet)?
        .into_inner();

    let found_nexus_addrs = config
        .zones
        .into_iter()
        .next()
        .expect("at least one external DNS zone")
        .records
        .into_iter()
        .find_map(|(name, dns_records)| {
            if !name.ends_with(".sys") {
                return None;
            }

            Some(
                dns_records
                    .into_iter()
                    .filter_map(|record| match record {
                        DnsRecord::A(addr) => Some(IpAddr::from(addr)),
                        DnsRecord::Aaaa(addr) => Some(IpAddr::from(addr)),
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>(),
            )
        })
        .expect("at least one silo");

    debug!(
        log,
        "check_external_dns";
        "expected" => ?expected_nexus_addrs,
        "found" => ?found_nexus_addrs,
    );

    if expected_nexus_addrs == found_nexus_addrs {
        Ok(())
    } else {
        Err(CondCheckError::NotYet)
    }
}
