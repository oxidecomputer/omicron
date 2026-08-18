// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handle sagas from expunged Nexus zones

use nexus_db_model::SecId;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::{Blueprint, BlueprintExpungedZoneAccessReason};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid};
use slog::{debug, error, info, warn};

/// For each expunged Nexus zone in the same generation as the current Nexus,
/// re-assign sagas owned by that Nexus to the specified nexus (`nexus_id`).
///
/// Reassigning sagas in this way is how we ensure that that sagas complete even
/// when the Nexus that was running them fails permanently (leading to
/// expungement).
///
/// Reassignment of sagas assigned to any expunged Nexus nodes is a prerequisite
/// for Nexus handoff (during upgrade).  That's because in general, Nexus is
/// tightly coupled to saga implementations and it's not safe for different
/// versions of a Nexus to operate on the same saga (even at different times).
/// For more on this, see RFD 289.  As a result, we finish all sagas prior to
/// handing off from one version to the next.
///
/// Strictly speaking, it should not be required to limit re-assignment to Nexus
/// instances of the same generation.  As mentioned above, sagas do not survive
/// across generations, so the only sagas that exist ought to be from the same
/// generation that Nexus is running.  This is a belt-and-suspenders check put
/// in place because the impact of getting this wrong (running a saga created by
/// an older version) could be pretty bad.
///
/// We could use the Nexus image for this rather than the generation.
/// In practice, these should be equivalent here because we bump the Nexus
/// generation on deployed systems when (and only when) there's a new Nexus
/// image.  But handoff works in terms of generations -- that's the abstraction
/// we've created for "a group of Nexus instances that speak the same database
/// version and can exchange sagas".  So that's what we use here.
pub(crate) async fn reassign_sagas_from_expunged(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus_id: SecId,
) -> Result<bool, Error> {
    let log = &opctx.log;

    let nexus_zone_ids = find_expunged_same_generation(blueprint, nexus_id)?;
    debug!(
        log,
        "re-assign sagas: found expunged Nexus instances with matching generation";
        "nexus_zone_ids" => ?nexus_zone_ids
    );

    let result =
        datastore.sagas_reassign_sec(opctx, &nexus_zone_ids, nexus_id).await;

    match result {
        Ok(count) => {
            info!(log, "re-assigned sagas";
                "nexus_zone_ids" => ?nexus_zone_ids,
                "count" => count,
            );

            Ok(count != 0)
        }
        Err(error) => {
            warn!(log, "failed to re-assign sagas";
                "nexus_zone_ids" => ?nexus_zone_ids,
                &error,
            );

            Err(error)
        }
    }
}

/// Returns the list of Nexus ids for expunged (and ready-to-cleanup) Nexus
/// zones in the same generation as the given Nexus id
fn find_expunged_same_generation(
    blueprint: &Blueprint,
    nexus_id: SecId,
) -> Result<Vec<SecId>, Error> {
    let nexus_zone_id = OmicronZoneUuid::from_untyped_uuid(nexus_id.0);
    let active_nexus_generation =
        blueprint.find_generation_for_self(nexus_zone_id)?;
    Ok(blueprint
        .expunged_nexus_zones_ready_for_cleanup(
            BlueprintExpungedZoneAccessReason::NexusSagaReassignment,
        )
        .filter_map(|(_sled_id, zone_config, nexus_config)| {
            (nexus_config.nexus_generation == active_nexus_generation)
                .then_some(zone_config.id)
        })
        .map(|id| SecId(id.into_untyped_uuid()))
        .collect())
}

/// Abandon sagas that can never be reassigned.
///
/// Sagas are only ever reassigned to an in-service Nexus of the *same*
/// generation (see [`reassign_sagas_from_expunged`]). So once a Nexus has been
/// expunged and no in-service Nexus remains in its generation, any saga still
/// assigned to it can never be adopted and would otherwise stay stuck forever.
/// These sagas also never go through saga recovery, as each Nexus only attempts
/// to recover sagas assigned to itself.
///
/// We abandon all sagas from an earlier generation than the current running
/// Nexus, as there is no way to salvage them. Note that abandoning a saga does
/// not unwind it: any partial work it has already done is left in place.
///
/// Any Nexus that isn't in the target blueprint is left untouched, since we
/// can't confirm its state.
///
/// This is all safe even if we're currently executing an old blueprint because:
/// - A zone can never become un-expunged.
/// - The generation never goes backwards.
///
/// The combination means that no other Nexus can ever assign this saga.
pub(crate) async fn abandon_orphan_sagas(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus_id: SecId,
) -> Result<(), anyhow::Error> {
    let log = &opctx.log;

    // Find ids of stale expunged Nexus zones
    let stale_sec_ids = find_expunged_older_generation(blueprint, nexus_id)?;

    info!(
        log,
        "abandon orphan sagas: abandoning running or unwinding sagas from \
        expunged (and ready-for-cleanup) Nexus zones of an older generation \
        than the current running Nexus zone";
        "stale_expunged_sec_ids" => ?stale_sec_ids,
    );
    let result = datastore
        .sagas_abandon_orphans(
            opctx,
            &stale_sec_ids,
            "orphan: current_sec is expunged and too old for this saga to be \
            re-assigned"
                .to_string(),
        )
        .await;

    match result {
        Ok(count) => {
            if count == 0 {
                info!(log, "no orphaned sagas to abandon");
            } else {
                error!(log, "abandoned orphan sagas";
                    "nexus_zone_ids" => ?stale_sec_ids,
                    "count" => count,
                );
            };

            Ok(())
        }
        Err(error) => {
            warn!(log, "failed to abandon orphan sagas";
                "nexus_zone_ids" => ?stale_sec_ids,
                &error,
            );

            Err(error.into())
        }
    }
}

/// Returns the ids of expunged (and ready-for-cleanup) Nexus zones whose
/// generation is older than the generation of the given Nexus id
fn find_expunged_older_generation(
    blueprint: &Blueprint,
    nexus_id: SecId,
) -> Result<Vec<SecId>, Error> {
    let nexus_zone_id = OmicronZoneUuid::from_untyped_uuid(nexus_id.0);
    let active_nexus_generation =
        blueprint.find_generation_for_self(nexus_zone_id)?;

    // SEC ids of expunged and ready-for-cleanup Nexus zones whose generation is
    // strictly older than the given Nexus generation.
    //
    // We source SEC ids strictly from the target blueprint as these are the
    // only Nexuses we can confidently confirm the state of. Any Nexus not in
    // the target blueprint is effectively ignored.
    Ok(blueprint
        .expunged_nexus_zones_ready_for_cleanup(
            BlueprintExpungedZoneAccessReason::NexusOrphanSagaAbandonment,
        )
        .filter_map(|(_sled_id, config, nexus)| {
            (nexus.nexus_generation < active_nexus_generation)
                .then_some(SecId(config.id.into_untyped_uuid()))
        })
        .collect())
}

#[cfg(test)]
mod test {
    use super::*;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use diesel::prelude::*;
    use iddqd::IdOrdMap;
    use nexus_db_model::{
        AbandonMetadata, Saga, SagaExecState, SagaId, SagaReasonAbandoned,
        SagaState,
    };
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_inventory::now_db_precision;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::blueprint_editor::ExternalNetworkingAllocator;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_reconfigurator_planning::planner::PlannerRng;
    use nexus_types::deployment::{
        BlueprintHostPhase2DesiredSlots, BlueprintMeasurements,
        BlueprintSledConfig, BlueprintSource, BlueprintZoneConfig,
        BlueprintZoneDisposition, BlueprintZoneImageSource, BlueprintZoneType,
        CockroachDbPreserveDowngrade, LastAllocatedSubnetIpOffset,
        OmicronZoneExternalFloatingIp, OximeterReadMode, PendingMgsUpdates,
        blueprint_zone_type,
    };
    use nexus_types::external_api::sled::SledState;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::api::external::Generation;
    use omicron_common::api::external::MacAddr;
    use omicron_common::api::external::Vni;
    use omicron_common::api::internal::shared::PrivateIpConfig;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::ExternalIpUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_agent_types::inventory::NetworkInterface;
    use sled_agent_types::inventory::NetworkInterfaceKind;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::net::IpAddr;
    use std::net::Ipv6Addr;
    use uuid::Uuid;

    // Build an in-memory blueprint containing exactly the given top level
    // Nexus generation, and Nexus zones, each with the specified disposition
    // and generation.
    fn create_test_blueprint_with_custom_nexus(
        nexus_generation: Generation,
        nexus_zones: Vec<(
            OmicronZoneUuid,
            BlueprintZoneDisposition,
            Generation,
        )>,
    ) -> Blueprint {
        let blueprint_id = BlueprintUuid::new_v4();
        let sled_id = SledUuid::new_v4();

        let ip_config = PrivateIpConfig::new_ipv4(
            "192.168.1.1".parse().unwrap(),
            "192.168.1.0/24".parse().unwrap(),
        )
        .unwrap();
        let zones: IdOrdMap<BlueprintZoneConfig> = nexus_zones
            .into_iter()
            .map(|(zone_id, disposition, nexus_generation)| {
                BlueprintZoneConfig {
                    disposition,
                    id: zone_id,
                    filesystem_pool: ZpoolName::new_external(
                        ZpoolUuid::new_v4(),
                    ),
                    zone_type: BlueprintZoneType::Nexus(
                        blueprint_zone_type::Nexus {
                            internal_address: "[::1]:0".parse().unwrap(),
                            lockstep_port: 0,
                            external_dns_servers: Vec::new(),
                            external_ip: OmicronZoneExternalFloatingIp {
                                id: ExternalIpUuid::new_v4(),
                                ip: IpAddr::V6(Ipv6Addr::LOCALHOST),
                            },
                            external_tls: true,
                            nic: NetworkInterface {
                                id: uuid::Uuid::new_v4(),
                                kind: NetworkInterfaceKind::Service {
                                    id: zone_id.into_untyped_uuid(),
                                },
                                name: "test-nic".parse().unwrap(),
                                ip_config: ip_config.clone(),
                                mac: MacAddr::random_system(),
                                vni: Vni::try_from(100).unwrap(),
                                primary: true,
                                slot: 0,
                            },
                            nexus_generation,
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                }
            })
            .collect();

        let mut sleds = BTreeMap::new();
        sleds.insert(
            sled_id,
            BlueprintSledConfig {
                state: SledState::Active,
                subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
                last_allocated_ip_subnet_offset:
                    LastAllocatedSubnetIpOffset::initial(),
                sled_agent_generation: Generation::new(),
                zones,
                disks: IdOrdMap::new(),
                datasets: IdOrdMap::new(),
                remove_mupdate_override: None,
                host_phase_2: BlueprintHostPhase2DesiredSlots::current_contents(
                ),
                measurements: BlueprintMeasurements::InstallDataset,
            },
        );

        Blueprint {
            id: blueprint_id,
            sleds,
            pending_mgs_updates: PendingMgsUpdates::new(),
            parent_blueprint_id: None,
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            target_release_minimum_generation: Generation::new(),
            nexus_generation,
            external_networking_generation: Generation::new(),
            cockroachdb_fingerprint: String::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            clickhouse_cluster_config: None,
            oximeter_read_mode: OximeterReadMode::SingleNode,
            oximeter_read_version: Generation::new(),
            time_created: now_db_precision(),
            creator: "test suite".to_string(),
            comment: "test blueprint".to_string(),
            source: BlueprintSource::Test,
        }
    }

    // Summarize a saga's state ignoring the abandonment timestamp, so we can
    // compare states across a database round-trip without timestamp precision
    // flakiness.
    fn state_summary(
        state: &SagaExecState,
    ) -> (SagaState, Option<(SagaReasonAbandoned, String)>) {
        match state {
            SagaExecState::Running => (SagaState::Running, None),
            SagaExecState::Unwinding => (SagaState::Unwinding, None),
            SagaExecState::Done => (SagaState::Done, None),
            SagaExecState::Abandoned(metadata) => (
                SagaState::Abandoned,
                Some((metadata.reason, metadata.comment.clone())),
            ),
        }
    }

    #[test]
    fn test_find_expunged_same_generation() {
        const TEST_NAME: &str = "test_find_expunged_same_generation";

        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;

        // To do an exhaustive test of `find_expunged_same_generation()`, we
        // want some expunged zones and some non-expunged zones in each of two
        // different generations.

        // First, create a basic blueprint with several Nexus zones in
        // generation 1.
        let (example, blueprint1) =
            ExampleSystemBuilder::new(log, TEST_NAME).nexus_count(4).build();
        let g1 = Generation::new();
        let g1_nexus_ids: Vec<_> = blueprint1
            .in_service_nexus_zones()
            .map(|(sled_id, zone_config, nexus_config)| {
                assert_eq!(nexus_config.nexus_generation, g1);
                (sled_id, zone_config.id, zone_config.image_source.clone())
            })
            .collect();

        // Expunge two of these Nexus zones and mark them ready for cleanup
        // immediately.
        let (g1_expunge_ids, g1_keep_ids) = g1_nexus_ids.split_at(2);
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &blueprint1,
            "test suite",
            PlannerRng::from_entropy(),
        )
        .expect("new blueprint builder");

        for (sled_id, expunge_id, _image_source) in g1_expunge_ids {
            builder
                .sled_expunge_zone(*sled_id, *expunge_id)
                .expect("expunge zone");
            builder
                .sled_mark_expunged_zone_ready_for_cleanup(
                    *sled_id,
                    *expunge_id,
                )
                .expect("mark zone for cleanup");
        }

        // Create the same number of Nexus zones in the next generation.
        // We'll use the same images.
        let g2 = g1.next();
        let mut external_networking_alloc =
            ExternalNetworkingAllocator::from_current_zones(
                &builder,
                example.input.external_ip_policy(),
            )
            .expect("constructed ExternalNetworkingAllocator");
        let nexus_config = example
            .input
            .external_service_networking_policy()
            .operator_nexus_config();
        for (sled_id, _zone_id, image_source) in &g1_nexus_ids {
            let external_ip = external_networking_alloc
                .for_new_nexus()
                .expect("found external IP for Nexus");
            builder
                .sled_add_zone_nexus(
                    *sled_id,
                    image_source.clone(),
                    external_ip,
                    g2,
                    &nexus_config,
                )
                .expect("add Nexus zone");
        }

        let blueprint2 = builder.build(BlueprintSource::Test);
        let g2_nexus_ids: Vec<_> = blueprint2
            .in_service_nexus_zones()
            .filter_map(|(sled_id, zone_config, nexus_config)| {
                (nexus_config.nexus_generation == g2)
                    .then_some((sled_id, zone_config.id))
            })
            .collect();

        // Now expunge a few of those, too.  This time, only the first is going
        // to be marked ready for cleanup.
        let (g2_expunge_ids, g2_keep_ids) = g2_nexus_ids.split_at(2);
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &blueprint2,
            "test suite",
            PlannerRng::from_entropy(),
        )
        .expect("new blueprint builder");

        let (sled_id, g2_expunged_cleaned_up) = g2_expunge_ids[0];
        builder
            .sled_expunge_zone(sled_id, g2_expunged_cleaned_up)
            .expect("expunge zone");
        builder
            .sled_mark_expunged_zone_ready_for_cleanup(
                g2_expunge_ids[0].0,
                g2_expunged_cleaned_up,
            )
            .expect("mark zone for cleanup");

        let (sled_id, g2_expunged_not_cleaned_up) = g2_expunge_ids[1];
        builder
            .sled_expunge_zone(sled_id, g2_expunged_not_cleaned_up)
            .expect("expunge zone");

        let blueprint3 = builder.build(BlueprintSource::Test);

        // Finally, we have:
        //
        // - g1_keep_ids:    two in-service Nexus zones in generation 1
        // - g1_expunge_ids: two expunged Nexus zones in generation 1,
        //                   both cleaned up
        // - g2_keep_ids:    two in-service Nexus zones in generation 2
        // - g2_expunge_ids: expunged Nexus zones in generation 2,
        //                   only the first of which is ready for cleanup
        //
        // Now we can exhaustively test various cases.

        // For the in-service zones in generation 1, we should find the expunged
        // zones in generation 1.
        let g1_matched: BTreeSet<SecId> = g1_expunge_ids
            .into_iter()
            .map(|(_sled_id, zone_id, _image_source)| {
                SecId(zone_id.into_untyped_uuid())
            })
            .collect();
        for (_sled_id, zone_id, _image_source) in g1_keep_ids {
            let matched = find_expunged_same_generation(
                &blueprint3,
                SecId(zone_id.into_untyped_uuid()),
            )
            .unwrap();
            assert_eq!(
                matched.into_iter().collect::<BTreeSet<_>>(),
                g1_matched
            );
        }

        // It should be impossible in a real system for the
        // expunged-and-ready-to-cleanup zones to execute this function.  Being
        // ready to cleanup means we know they're gone.  So there's nothing to
        // test here.

        // For the in-service zones in generation 2, we should find the
        // expunged-and-ready-for-cleanup zone in generation 2.
        let g2_matched = SecId(g2_expunged_cleaned_up.into_untyped_uuid());
        for (_sled_id, zone_id) in g2_keep_ids {
            let matched = find_expunged_same_generation(
                &blueprint3,
                SecId(zone_id.into_untyped_uuid()),
            )
            .unwrap();
            assert_eq!(matched.len(), 1);
            assert_eq!(matched[0], g2_matched);
        }

        // It is possible for the expunged and not-yet-ready-for-cleanup zone in
        // generation 2 to wind up calling this function.  It should not find
        // itself!
        let matched = find_expunged_same_generation(
            &blueprint3,
            SecId(g2_expunged_not_cleaned_up.into_untyped_uuid()),
        )
        .unwrap();
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0], g2_matched);

        // Test the sole error case: if we cannot figure out which generation we
        // were given.
        let error =
            find_expunged_same_generation(&blueprint3, SecId(Uuid::new_v4()))
                .expect_err("made-up Nexus should not exist");
        assert!(matches!(error, Error::InternalError { internal_message }
            if internal_message.contains("did not find Nexus")));

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_abandon_orphan_sagas() {
        let logctx = test_setup_log("test_abandon_orphan_sagas");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Generations: g1 is older than the live generation g2.
        let g1 = Generation::new();
        let g2 = g1.next();

        // Build a blueprint with:
        // - an in-service Nexus at the live generation (g2)
        // - an expunged + ready_for_cleanup Nexus at the live generation (g2)
        // - an expunged + ready_for_cleanup Nexus at an older generation (g1)
        //
        // Only the orphan_zone, with a generation older than the current Nexus,
        // has sagas that can never be reassigned.
        let in_service_zone = OmicronZoneUuid::new_v4();
        let expunged_same_gen_zone = OmicronZoneUuid::new_v4();
        let orphan_zone = OmicronZoneUuid::new_v4();

        let blueprint = create_test_blueprint_with_custom_nexus(
            g2,
            vec![
                (in_service_zone, BlueprintZoneDisposition::InService, g2),
                (
                    expunged_same_gen_zone,
                    BlueprintZoneDisposition::Expunged {
                        as_of_generation: g2,
                        ready_for_cleanup: true,
                    },
                    g2,
                ),
                (
                    orphan_zone,
                    BlueprintZoneDisposition::Expunged {
                        as_of_generation: g1,
                        ready_for_cleanup: true,
                    },
                    g1,
                ),
            ],
        );

        // We create sagas for four different SECs:
        //
        // - `in_service_sec`:        a live, in-service Nexus
        // - `expunged_same_gen_sec`: expunged, but same generation as the live
        //                            Nexus (so still reassignable)
        // - `absent_sec`:            a Nexus that isn't in the blueprint at all
        // - `orphan_sec`:            expunged and older than the live Nexus
        //
        // Only `orphan_sec`'s running/unwinding sagas should be abandoned. All
        // other sagas, for every SEC and in every state, must be left untouched
        let in_service_sec = SecId(in_service_zone.into_untyped_uuid());
        let expunged_same_gen_sec =
            SecId(expunged_same_gen_zone.into_untyped_uuid());
        let absent_sec = SecId(Uuid::new_v4());
        let orphan_sec = SecId(orphan_zone.into_untyped_uuid());

        // For each SEC, create one saga in each state (running, unwinding,
        // done, and abandoned).
        let mut sagas_to_insert = Vec::new();
        for sec in
            [in_service_sec, expunged_same_gen_sec, absent_sec, orphan_sec]
        {
            for state in [
                steno::SagaCachedState::Running,
                steno::SagaCachedState::Unwinding,
                steno::SagaCachedState::Done,
            ] {
                sagas_to_insert.push(Saga::new(
                    sec,
                    steno::SagaCreateParams {
                        id: steno::SagaId(Uuid::new_v4()),
                        name: steno::SagaName::new("test saga"),
                        dag: serde_json::Value::Null,
                        state,
                    },
                ));
            }
            sagas_to_insert.push(Saga::new_abandoned(
                sec,
                SagaId(steno::SagaId(Uuid::new_v4())),
                "test saga".to_string(),
                serde_json::Value::Null,
                AbandonMetadata {
                    time: Utc::now(),
                    reason: SagaReasonAbandoned::Unrecoverable,
                    comment: "preexisting abandonment".to_string(),
                },
            ));
        }

        // Record the initial state of every saga so we can confirm which ones
        // changed.
        let initial_state: BTreeMap<SagaId, SagaExecState> = sagas_to_insert
            .iter()
            .map(|saga| (saga.id, saga.saga_state.clone()))
            .collect();

        // The only sagas that should end up newly abandoned are those from `orphan_sec`
        // that started out running or unwinding.
        let expected_newly_abandoned: BTreeSet<SagaId> = sagas_to_insert
            .iter()
            .filter_map(|saga| {
                (saga.creator == orphan_sec
                    && matches!(
                        saga.saga_state,
                        SagaExecState::Running | SagaExecState::Unwinding
                    ))
                .then_some(saga.id)
            })
            .collect();
        assert_eq!(expected_newly_abandoned.len(), 2);

        // Insert the sagas.
        {
            use nexus_db_schema::schema::saga::dsl;
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::insert_into(dsl::saga)
                .values(sagas_to_insert.iter().collect::<Vec<_>>())
                .execute_async(&*conn)
                .await
                .expect("successful insertion");
        }

        // Run the orphan saga abandoner.
        abandon_orphan_sagas(opctx, datastore, &blueprint, in_service_sec)
            .await
            .expect("abandon_orphan_sagas failed");

        // Read every saga we inserted back. Query by primary key to avoid a
        // full table scan.
        let all_ids: Vec<SagaId> = initial_state.keys().copied().collect();
        let loaded: Vec<Saga> = {
            use nexus_db_schema::schema::saga::dsl;
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            dsl::saga
                .filter(dsl::id.eq_any(all_ids))
                .select(Saga::as_select())
                .load_async(&*conn)
                .await
                .expect("failed to load sagas")
        };
        assert_eq!(loaded.len(), sagas_to_insert.len());

        // Split what we loaded into the sagas we expect to have been abandoned
        // and the ones we expect to be untouched.
        let expected_unchanged_count =
            sagas_to_insert.len() - expected_newly_abandoned.len();

        let (abandoned, unchanged): (Vec<Saga>, Vec<Saga>) = loaded
            .into_iter()
            .partition(|saga| expected_newly_abandoned.contains(&saga.id));

        assert_eq!(abandoned.len(), expected_newly_abandoned.len());
        assert_eq!(unchanged.len(), expected_unchanged_count);

        // `orphan_sec`'s running/unwinding sagas should now be abandoned as
        // orphaned.
        for saga in abandoned {
            assert_eq!(
                state_summary(&saga.saga_state),
                (
                    SagaState::Abandoned,
                    Some((
                        SagaReasonAbandoned::Orphaned,
                        "orphan: current_sec is expunged and too old for this \
                        saga to be re-assigned"
                            .to_string(),
                    )),
                ),
                "saga {} should have been abandoned as orphaned, but is {:?}",
                saga.id,
                saga.saga_state,
            );
        }

        // Every other saga must be exactly as we inserted it.
        for saga in unchanged {
            let initial = &initial_state[&saga.id];
            assert_eq!(
                state_summary(&saga.saga_state),
                state_summary(initial),
                "saga {} (creator {:?}) should have been left untouched",
                saga.id,
                saga.creator,
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
