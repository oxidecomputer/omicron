// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages deployment of records into the database.

use anyhow::anyhow;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneDisposition;
use omicron_uuid_kinds::OmicronZoneUuid;
use std::collections::BTreeSet;

/// Idempotently ensure that the Nexus records for the zones are populated
/// in the database.
pub(crate) async fn deploy_db_metadata_nexus_records(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus_id: OmicronZoneUuid,
) -> Result<(), anyhow::Error> {
    // To determine what state to use for new records, we need to know which is
    // the currently active Nexus generation.  This is not quite the same as the
    // blueprint's `nexus_generation`.  That field describes which generation
    // the system is *trying* to put in control.  It gets bumped in order to
    // trigger the handoff process.  But between when it gets bumped and when
    // the handoff has finished, that generation number is ahead of the one
    // currently in control.
    //
    // The actual generation number that's currently active is necessarily the
    // generation number of the Nexus instance that's doing the execution.
    let active_generation = blueprint
        .all_nexus_zones(BlueprintZoneDisposition::is_in_service)
        .find_map(|(_sled_id, zone_cfg, nexus_config)| {
            (zone_cfg.id == nexus_id).then_some(nexus_config.nexus_generation)
        })
        .ok_or_else(|| {
            anyhow!(
                "did not find nexus generation for current \
                 Nexus zone ({nexus_id})"
            )
        })?;

    let mut active = BTreeSet::new();
    let mut not_yet = BTreeSet::new();
    for (_sled_id, zone_config, nexus_config) in
        blueprint.all_nexus_zones(BlueprintZoneDisposition::is_in_service)
    {
        if nexus_config.nexus_generation == active_generation {
            active.insert(zone_config.id);
        } else if nexus_config.nexus_generation > active_generation {
            not_yet.insert(zone_config.id);
        }
    }

    datastore
        .database_nexus_access_create(opctx, blueprint.id, &active, &not_yet)
        .await
        .map_err(|err| anyhow!(err))?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use id_map::IdMap;
    use nexus_db_model::DbMetadataNexus;
    use nexus_db_model::DbMetadataNexusState;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_inventory::now_db_precision;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
    use nexus_types::deployment::BlueprintSledConfig;
    use nexus_types::deployment::BlueprintSource;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::CockroachDbPreserveDowngrade;
    use nexus_types::deployment::OximeterReadMode;
    use nexus_types::deployment::PendingMgsUpdates;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::external_api::views::SledState;
    use nexus_types::inventory::NetworkInterface;
    use nexus_types::inventory::NetworkInterfaceKind;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::Generation;
    use omicron_common::api::external::MacAddr;
    use omicron_common::api::external::Vni;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::ExternalIpUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::collections::BTreeMap;

    fn create_test_blueprint(
        top_level_nexus_generation: Generation,
        nexus_zones: Vec<(
            OmicronZoneUuid,
            BlueprintZoneDisposition,
            Generation,
        )>,
    ) -> Blueprint {
        let blueprint_id = BlueprintUuid::new_v4();
        let sled_id = SledUuid::new_v4();

        let zones: IdMap<BlueprintZoneConfig> = nexus_zones
            .into_iter()
            .map(|(zone_id, disposition, nexus_generation)| BlueprintZoneConfig {
                disposition,
                id: zone_id,
                filesystem_pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
                zone_type: BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    internal_address: "[::1]:0".parse().unwrap(),
                    lockstep_port: 0,
                    external_dns_servers: Vec::new(),
                    external_ip: nexus_types::deployment::OmicronZoneExternalFloatingIp {
                        id: ExternalIpUuid::new_v4(),
                        ip: std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
                    },
                    external_tls: true,
                    nic: NetworkInterface {
                        id: uuid::Uuid::new_v4(),
                        kind: NetworkInterfaceKind::Service {
                            id: zone_id.into_untyped_uuid(),
                        },
                        name: "test-nic".parse().unwrap(),
                        ip: "192.168.1.1".parse().unwrap(),
                        mac: MacAddr::random_system(),
                        subnet: ipnetwork::IpNetwork::V4(
                            "192.168.1.0/24".parse().unwrap()
                        ).into(),
                        vni: Vni::try_from(100).unwrap(),
                        primary: true,
                        slot: 0,
                        transit_ips: Vec::new(),
                    },
                    nexus_generation,
                }),
                image_source: BlueprintZoneImageSource::InstallDataset,
            })
            .collect();

        let mut sleds = BTreeMap::new();
        sleds.insert(
            sled_id,
            BlueprintSledConfig {
                state: SledState::Active,
                sled_agent_generation: Generation::new(),
                zones,
                disks: IdMap::new(),
                datasets: IdMap::new(),
                remove_mupdate_override: None,
                host_phase_2: BlueprintHostPhase2DesiredSlots::current_contents(
                ),
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
            nexus_generation: top_level_nexus_generation,
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

    async fn database_nexus_access(
        opctx: &OpContext,
        datastore: &DataStore,
        nexus_id: OmicronZoneUuid,
    ) -> Result<Option<DbMetadataNexus>, Error> {
        datastore
            .database_nexus_access_all(
                &opctx,
                &std::iter::once(nexus_id).collect(),
            )
            .await
            .map(|v| v.into_iter().next())
    }

    #[tokio::test]
    async fn test_database_nexus_access_create() {
        let logctx = dev::test_setup_log("test_database_nexus_access_create");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = db.opctx();

        // Create a blueprint with in-service Nexus zones, and one expunged
        // Nexus.
        let nexus1_id = OmicronZoneUuid::new_v4();
        let nexus2_id = OmicronZoneUuid::new_v4();
        let expunged_nexus = OmicronZoneUuid::new_v4();

        // Our currently-running Nexus must already have a record
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        datastore
            .initialize_nexus_access_from_blueprint_on_connection(
                &conn,
                vec![nexus1_id],
            )
            .await
            .unwrap();

        let blueprint = create_test_blueprint(
            Generation::new(),
            vec![
                // This nexus matches the top-level generation, and already
                // exists as "active".
                (
                    nexus1_id,
                    BlueprintZoneDisposition::InService,
                    Generation::new(),
                ),
                // This nexus is ahead of the the top-level nexus generation,
                // and will be created as "not yet".
                (
                    nexus2_id,
                    BlueprintZoneDisposition::InService,
                    Generation::new().next(),
                ),
                (
                    expunged_nexus,
                    BlueprintZoneDisposition::Expunged {
                        as_of_generation: Generation::new(),
                        ready_for_cleanup: true,
                    },
                    Generation::new(),
                ),
            ],
        );

        // Insert the blueprint and make it the target
        datastore
            .blueprint_insert(&opctx, &blueprint)
            .await
            .expect("Failed to insert blueprint");
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: blueprint.id,
                    enabled: false,
                    time_made_target: chrono::Utc::now(),
                },
            )
            .await
            .expect("Failed to set blueprint target");

        // Create nexus access records
        deploy_db_metadata_nexus_records(
            &opctx, datastore, &blueprint, nexus1_id,
        )
        .await
        .expect("Failed to create nexus access");

        // Verify records were created for in-service Nexuses.
        let nexus1_access = database_nexus_access(&opctx, datastore, nexus1_id)
            .await
            .expect("Failed to get nexus1 access");
        let nexus2_access = database_nexus_access(opctx, datastore, nexus2_id)
            .await
            .expect("Failed to get nexus2 access");
        let expunged_access =
            database_nexus_access(opctx, datastore, expunged_nexus)
                .await
                .expect("Failed to get expunged access");

        assert!(nexus1_access.is_some(), "nexus1 should have access record");
        assert!(nexus2_access.is_some(), "nexus2 should have access record");
        assert!(
            expunged_access.is_none(),
            "expunged nexus should not have access record"
        );

        // See above for the rationale here:
        //
        // Nexus 1 already existed, and was active.
        // Nexus 2 has a higher generation number (e.g., it represents
        // a new deployment that has not yet been activated).
        // The expunged Nexus was ignored.
        let nexus1_record = nexus1_access.unwrap();
        let nexus2_record = nexus2_access.unwrap();
        assert_eq!(nexus1_record.state(), DbMetadataNexusState::Active);
        assert_eq!(nexus2_record.state(), DbMetadataNexusState::NotYet);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_database_nexus_access_create_during_quiesce() {
        let logctx = dev::test_setup_log(
            "test_database_nexus_access_create_during_quiesce",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = db.opctx();

        // Create a blueprint with in-service Nexus zones, and one expunged
        // Nexus.
        let nexus1_id = OmicronZoneUuid::new_v4();
        let nexus2_id = OmicronZoneUuid::new_v4();
        let nexus3_id = OmicronZoneUuid::new_v4();

        // Our currently-running Nexus must already have a record
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        datastore
            .initialize_nexus_access_from_blueprint_on_connection(
                &conn,
                vec![nexus1_id],
            )
            .await
            .unwrap();

        let blueprint = create_test_blueprint(
            // NOTE: This is using a "Generation = 2", implying that all
            // nexuses using "Generation = 1" should start quiescing.
            Generation::new().next(),
            vec![
                // This Nexus already exists as active - even though it's
                // quiescing currently.
                (
                    nexus1_id,
                    BlueprintZoneDisposition::InService,
                    Generation::new(),
                ),
                // This Nexus matches the the top-level nexus generation,
                // and will be created as "not yet", because "nexus1" is still
                // running.
                (
                    nexus2_id,
                    BlueprintZoneDisposition::InService,
                    Generation::new().next(),
                ),
                // This Nexus will quiesce soon after starting, but can still be
                // created as active.
                (
                    nexus3_id,
                    BlueprintZoneDisposition::InService,
                    Generation::new(),
                ),
            ],
        );

        // Insert the blueprint and make it the target
        datastore
            .blueprint_insert(&opctx, &blueprint)
            .await
            .expect("Failed to insert blueprint");
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: blueprint.id,
                    enabled: false,
                    time_made_target: chrono::Utc::now(),
                },
            )
            .await
            .expect("Failed to set blueprint target");

        // Create nexus access records
        deploy_db_metadata_nexus_records(
            &opctx, datastore, &blueprint, nexus1_id,
        )
        .await
        .expect("Failed to create nexus access");

        // Verify records were created for in-service Nexuses.
        let nexus1_access = database_nexus_access(opctx, datastore, nexus1_id)
            .await
            .expect("Failed to get nexus1 access");
        let nexus2_access = database_nexus_access(opctx, datastore, nexus2_id)
            .await
            .expect("Failed to get nexus2 access");
        let nexus3_access = database_nexus_access(opctx, datastore, nexus3_id)
            .await
            .expect("Failed to get nexus3 access");

        assert!(nexus1_access.is_some(), "nexus1 should have access record");
        assert!(nexus2_access.is_some(), "nexus2 should have access record");
        assert!(nexus2_access.is_some(), "nexus3 should have access record");

        // See above for the rationale here:
        //
        // Nexus 1 already existed, and was active.
        // Nexus 2 has a higher generation number (e.g., it represents
        // a new deployment that has not yet been activated).
        // Nexus 3 is getting a new record, but using the old generation number.
        // It'll be treated as active.
        let nexus1_record = nexus1_access.unwrap();
        let nexus2_record = nexus2_access.unwrap();
        let nexus3_record = nexus3_access.unwrap();
        assert_eq!(nexus1_record.state(), DbMetadataNexusState::Active);
        assert_eq!(nexus2_record.state(), DbMetadataNexusState::NotYet);
        assert_eq!(nexus3_record.state(), DbMetadataNexusState::Active);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_database_nexus_access_create_idempotent() {
        let logctx =
            dev::test_setup_log("test_database_nexus_access_create_idempotent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = db.opctx();

        // Create a blueprint with a couple Nexus zones
        let nexus1_id = OmicronZoneUuid::new_v4();
        let nexus2_id = OmicronZoneUuid::new_v4();
        let blueprint = create_test_blueprint(
            Generation::new(),
            vec![
                (
                    nexus1_id,
                    BlueprintZoneDisposition::InService,
                    Generation::new(),
                ),
                (
                    nexus2_id,
                    BlueprintZoneDisposition::InService,
                    Generation::new(),
                ),
            ],
        );

        // Insert the blueprint and make it the target
        datastore
            .blueprint_insert(&opctx, &blueprint)
            .await
            .expect("Failed to insert blueprint");
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: blueprint.id,
                    enabled: false,
                    time_made_target: chrono::Utc::now(),
                },
            )
            .await
            .expect("Failed to set blueprint target");

        // Create nexus access records (first time)
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        datastore
            .initialize_nexus_access_from_blueprint_on_connection(
                &conn,
                vec![nexus1_id, nexus2_id],
            )
            .await
            .unwrap();

        // Verify record was created
        async fn confirm_state(
            opctx: &OpContext,
            datastore: &DataStore,
            nexus_id: OmicronZoneUuid,
            expected_state: DbMetadataNexusState,
        ) {
            let state = database_nexus_access(opctx, datastore, nexus_id)
                .await
                .expect("Failed to get nexus access after first create")
                .expect("Entry for Nexus should have been inserted");
            assert_eq!(state.state(), expected_state);
        }

        confirm_state(
            opctx,
            datastore,
            nexus1_id,
            DbMetadataNexusState::Active,
        )
        .await;
        confirm_state(
            opctx,
            datastore,
            nexus2_id,
            DbMetadataNexusState::Active,
        )
        .await;

        // Creating the record again: not an error.
        deploy_db_metadata_nexus_records(
            &opctx, datastore, &blueprint, nexus1_id,
        )
        .await
        .expect("Failed to create nexus access");
        confirm_state(
            opctx,
            datastore,
            nexus1_id,
            DbMetadataNexusState::Active,
        )
        .await;
        confirm_state(
            opctx,
            datastore,
            nexus2_id,
            DbMetadataNexusState::Active,
        )
        .await;

        // Manually make the record "Quiesced".
        datastore
            .database_nexus_access_update_quiesced(nexus1_id)
            .await
            .unwrap();
        confirm_state(
            opctx,
            datastore,
            nexus1_id,
            DbMetadataNexusState::Quiesced,
        )
        .await;
        confirm_state(
            opctx,
            datastore,
            nexus2_id,
            DbMetadataNexusState::Active,
        )
        .await;

        // Create nexus access records another time - should be idempotent,
        // but should be "on-conflict, ignore".
        deploy_db_metadata_nexus_records(
            &opctx, datastore, &blueprint, nexus1_id,
        )
        .await
        .expect("Failed to create nexus access");
        confirm_state(
            opctx,
            datastore,
            nexus1_id,
            DbMetadataNexusState::Quiesced,
        )
        .await;
        confirm_state(
            opctx,
            datastore,
            nexus2_id,
            DbMetadataNexusState::Active,
        )
        .await;

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_database_nexus_access_create_fails_wrong_target_blueprint() {
        let logctx = dev::test_setup_log(
            "test_database_nexus_access_create_fails_wrong_target_blueprint",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = db.opctx();

        // Create two different blueprints, each with two Nexuses.
        //
        // One of these Nexuses will have a "db_metadata_nexus" record
        // for bootstrapping, the other won't exist (yet).
        let nexus1_id = OmicronZoneUuid::new_v4();
        let nexus2_id = OmicronZoneUuid::new_v4();
        let both_nexuses = vec![
            (nexus1_id, BlueprintZoneDisposition::InService, Generation::new()),
            (nexus2_id, BlueprintZoneDisposition::InService, Generation::new()),
        ];

        let target_blueprint =
            create_test_blueprint(Generation::new(), both_nexuses.clone());
        let non_target_blueprint =
            create_test_blueprint(Generation::new(), both_nexuses);

        // Initialize the "db_metadata_nexus" record for one of the Nexuses
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        datastore
            .initialize_nexus_access_from_blueprint_on_connection(
                &conn,
                vec![nexus1_id],
            )
            .await
            .unwrap();

        // Insert both blueprints
        datastore
            .blueprint_insert(&opctx, &target_blueprint)
            .await
            .expect("Failed to insert target blueprint");
        datastore
            .blueprint_insert(&opctx, &non_target_blueprint)
            .await
            .expect("Failed to insert non-target blueprint");

        // Set the first blueprint as the target
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: target_blueprint.id,
                    enabled: false,
                    time_made_target: chrono::Utc::now(),
                },
            )
            .await
            .expect("Failed to set target blueprint");

        // Try to create nexus access records using the non-target blueprint.
        // This should fail because the transaction should check if the
        // blueprint is the current target
        let result = deploy_db_metadata_nexus_records(
            &opctx,
            datastore,
            &non_target_blueprint,
            nexus1_id,
        )
        .await;
        assert!(
            result.is_err(),
            "Creating nexus access with wrong target blueprint should fail"
        );

        // Verify no records were created for the second nexus
        let access = database_nexus_access(opctx, datastore, nexus2_id)
            .await
            .expect("Failed to get nexus access");
        assert!(
            access.is_none(),
            "No access record should exist when wrong blueprint is used"
        );

        // Verify that using the correct target blueprint works
        deploy_db_metadata_nexus_records(
            &opctx,
            datastore,
            &target_blueprint,
            nexus1_id,
        )
        .await
        .expect("Failed to create nexus access");

        let access_after_correct =
            database_nexus_access(opctx, datastore, nexus2_id)
                .await
                .expect("Failed to get nexus access after correct blueprint");
        assert!(
            access_after_correct.is_some(),
            "Access record should exist after using correct target blueprint"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
