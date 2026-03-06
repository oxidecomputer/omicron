// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for managing Support Bundles

use crate::app::background::BackgroundTask;
use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use nexus_db_model::SupportBundle;
use nexus_db_model::SupportBundleState;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::SupportBundleCleanupReport;
use nexus_types::internal_api::background::SupportBundleCollectionReport;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use serde_json::json;
use sled_agent_types::support_bundle::NESTED_DATASET_NOT_FOUND;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;

use super::support_bundle::collection::BundleCollection;
use super::support_bundle::request::BundleRequest;

fn authz_support_bundle_from_id(id: SupportBundleUuid) -> authz::SupportBundle {
    authz::SupportBundle::new(authz::FLEET, id, LookupType::by_id(id))
}

// Result of asking a sled agent to clean up a bundle
enum SledAgentBundleCleanupResult {
    Deleted,
    NotFound,
    Failed,
}

// Result of asking the database to delete a bundle
enum DatabaseBundleCleanupResult {
    DestroyingBundleRemoved,
    FailingBundleUpdated,
    BadState,
}

/// The background task responsible for cleaning and collecting support bundles
pub struct SupportBundleCollector {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    disable: bool,
    nexus_id: OmicronZoneUuid,
}

impl SupportBundleCollector {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        disable: bool,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        SupportBundleCollector { datastore, resolver, disable, nexus_id }
    }

    // Tells a sled agent to delete a support bundle
    async fn cleanup_bundle_from_sled_agent(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        bundle: &SupportBundle,
    ) -> anyhow::Result<SledAgentBundleCleanupResult> {
        let sled_client = nexus_networking::sled_client(
            &self.datastore,
            &opctx,
            sled_id,
            &opctx.log,
        )
        .await?;

        let result = sled_client
            .support_bundle_delete(
                &ZpoolUuid::from(bundle.zpool_id),
                &DatasetUuid::from(bundle.dataset_id),
                &SupportBundleUuid::from(bundle.id),
            )
            .await;

        match result {
            Ok(_) => {
                info!(
                    &opctx.log,
                    "SupportBundleCollector deleted bundle";
                    "id" => %bundle.id
                );
                return Ok(SledAgentBundleCleanupResult::Deleted);
            }
            Err(progenitor_client::Error::ErrorResponse(err))
                if err.status() == http::StatusCode::NOT_FOUND
                    && err.error_code.as_ref().is_some_and(|code| {
                        code.contains(NESTED_DATASET_NOT_FOUND)
                    }) =>
            {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector could not delete bundle (not found)";
                    "id" => %bundle.id,
                    "err" => ?err
                );

                return Ok(SledAgentBundleCleanupResult::NotFound);
            }
            err => {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector could not delete bundle";
                    "id" => %bundle.id,
                    "err" => ?err,
                );

                return Ok(SledAgentBundleCleanupResult::Failed);
            }
        }
    }

    async fn cleanup_bundle_from_database(
        &self,
        opctx: &OpContext,
        bundle: &SupportBundle,
    ) -> anyhow::Result<DatabaseBundleCleanupResult> {
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        match bundle.state {
            SupportBundleState::Destroying => {
                // Destroying is a terminal state; no one should be able to
                // change this state from underneath us.
                self.datastore.support_bundle_delete(
                    opctx,
                    &authz_bundle,
                ).await.map_err(|err| {
                    warn!(
                        &opctx.log,
                        "SupportBundleCollector: Could not delete 'destroying' bundle";
                        "err" => %err
                    );
                    anyhow::anyhow!("Could not delete 'destroying' bundle: {:#}", err)
                })?;

                return Ok(
                    DatabaseBundleCleanupResult::DestroyingBundleRemoved,
                );
            }
            SupportBundleState::Failing => {
                if let Err(err) = self
                    .datastore
                    .support_bundle_update(
                        &opctx,
                        &authz_bundle,
                        SupportBundleState::Failed,
                    )
                    .await
                {
                    if matches!(err, Error::InvalidRequest { .. }) {
                        // It's possible that the bundle is marked "destroying" by a
                        // user request, concurrently with our operation.
                        //
                        // In this case, we log that this happened, but do nothing.
                        // The next iteration of this background task should treat
                        // this as the "Destroying" case, and delete the bundle.
                        info!(
                            &opctx.log,
                            "SupportBundleCollector: Concurrent state change failing bundle";
                            "bundle" => %bundle.id,
                            "err" => ?err,
                        );
                        return Ok(DatabaseBundleCleanupResult::BadState);
                    } else {
                        warn!(
                            &opctx.log,
                            "Could not delete 'failing' bundle";
                            "err" => ?err,
                        );
                        anyhow::bail!(
                            "Could not delete 'failing' bundle: {:#}",
                            err
                        );
                    }
                }

                return Ok(DatabaseBundleCleanupResult::FailingBundleUpdated);
            }
            other => {
                // We should be filtering to only see "Destroying" and
                // "Failing" bundles in our database request above.
                error!(
                    &opctx.log,
                    "SupportBundleCollector: Cleaning bundle in unexpected state";
                    "id" => %bundle.id,
                    "state" => ?other
                );
                return Ok(DatabaseBundleCleanupResult::BadState);
            }
        }
    }

    // Monitors all bundles that are "destroying" or "failing" and assigned to
    // this Nexus, and attempts to clear their storage from Sled Agents.
    async fn cleanup_destroyed_bundles(
        &self,
        opctx: &OpContext,
    ) -> anyhow::Result<SupportBundleCleanupReport> {
        let pagparams = DataPageParams::max_page();
        let result = self
            .datastore
            .support_bundle_list_assigned_to_nexus(
                opctx,
                &pagparams,
                self.nexus_id,
                vec![
                    SupportBundleState::Destroying,
                    SupportBundleState::Failing,
                ],
            )
            .await;

        let bundles_to_destroy = match result {
            Ok(r) => r,
            Err(err) => {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector: Failed to list terminating bundles";
                    "err" => %err
                );
                anyhow::bail!("failed to query database: {:#}", err);
            }
        };

        let mut report = SupportBundleCleanupReport::default();

        // NOTE: This could be concurrent, but the priority for that also seems low
        for bundle in bundles_to_destroy {
            info!(
                &opctx.log,
                "SupportBundleCollector starting bundle deletion";
                "id" => %bundle.id
            );

            // Find the sled where we're storing this bundle.
            let result = self
                .datastore
                .zpool_get_sled_if_in_service(&opctx, bundle.zpool_id.into())
                .await;

            let delete_from_db = match result {
                Ok(sled_id) => {
                    match self
                        .cleanup_bundle_from_sled_agent(
                            &opctx, sled_id, &bundle,
                        )
                        .await?
                    {
                        SledAgentBundleCleanupResult::Deleted => {
                            report.sled_bundles_deleted_ok += 1;
                            true
                        }
                        SledAgentBundleCleanupResult::NotFound => {
                            report.sled_bundles_deleted_not_found += 1;
                            true
                        }
                        SledAgentBundleCleanupResult::Failed => {
                            report.sled_bundles_delete_failed += 1;

                            // If the sled agent reports any other error, don't
                            // delete the bundle from the database. It might be
                            // transiently unavailable.
                            false
                        }
                    }
                }
                Err(Error::ObjectNotFound {
                    type_name: ResourceType::Zpool,
                    ..
                }) => {
                    // If the pool wasn't found in the database, it was
                    // expunged. Delete the support bundle, since there is no
                    // sled agent state to manage anymore.
                    true
                }
                Err(_) => false,
            };

            if delete_from_db {
                match self.cleanup_bundle_from_database(opctx, &bundle).await? {
                    DatabaseBundleCleanupResult::DestroyingBundleRemoved => {
                        report.db_destroying_bundles_removed += 1;
                    }
                    DatabaseBundleCleanupResult::FailingBundleUpdated => {
                        report.db_failing_bundles_updated += 1;
                    }
                    DatabaseBundleCleanupResult::BadState => {}
                }
            }
        }
        Ok(report)
    }

    async fn collect_bundle(
        &self,
        opctx: &OpContext,
        request: &BundleRequest,
    ) -> anyhow::Result<Option<SupportBundleCollectionReport>> {
        let pagparams = DataPageParams::max_page();
        let result = self
            .datastore
            .support_bundle_list_assigned_to_nexus(
                opctx,
                &pagparams,
                self.nexus_id,
                vec![SupportBundleState::Collecting],
            )
            .await;

        let bundle = match result {
            Ok(bundles) => {
                if let Some(bundle) = bundles.get(0) {
                    info!(
                        &opctx.log,
                        "SupportBundleCollector: Found bundle to collect";
                        "bundle" => %bundle.id,
                        "bundles_in_queue" => bundles.len()
                    );
                    bundle.clone()
                } else {
                    info!(&opctx.log, "No bundles to collect");
                    return Ok(None);
                }
            }
            Err(err) => {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector: Failed to list collecting bundles";
                    "err" => %err
                );
                anyhow::bail!("failed to query database: {:#}", err);
            }
        };

        let collection = Arc::new(BundleCollection::new(
            self.datastore.clone(),
            self.resolver.clone(),
            opctx.log.new(slog::o!("bundle" => bundle.id.to_string())),
            opctx.child(std::collections::BTreeMap::new()),
            request.clone(),
            bundle.clone(),
            request.transfer_chunk_size,
        ));

        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        let mut report = collection.collect_bundle_and_store_on_sled().await?;
        if let Err(err) = self
            .datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Active,
            )
            .await
        {
            if matches!(err, Error::InvalidRequest { .. }) {
                info!(
                    &opctx.log,
                    "SupportBundleCollector: Concurrent state change activating bundle";
                    "bundle" => %bundle.id,
                    "err" => ?err,
                );
                return Ok(Some(report));
            } else {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector: Unexpected error activating bundle";
                    "bundle" => %bundle.id,
                    "err" => ?err,
                );
                anyhow::bail!("failed to activate bundle: {:#}", err);
            }
        }
        report.activated_in_db_ok = true;
        Ok(Some(report))
    }
}

impl BackgroundTask for SupportBundleCollector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            if self.disable {
                return json!({ "error": "task disabled" });
            }

            let mut cleanup_report = None;
            let mut cleanup_err = None;
            let mut collection_report = None;
            let mut collection_err = None;

            match self.cleanup_destroyed_bundles(&opctx).await {
                Ok(report) => cleanup_report = Some(report),
                Err(err) => {
                    cleanup_err =
                        Some(json!({ "cleanup_error": err.to_string() }))
                }
            };

            let request = BundleRequest::default();
            match self.collect_bundle(&opctx, &request).await {
                Ok(report) => collection_report = Some(report),
                Err(err) => {
                    collection_err =
                        Some(json!({ "collect_error": InlineErrorChain::new(err.as_ref()).to_string() }))
                }
            };

            json!({
                "cleanup_report": cleanup_report,
                "cleanup_err": cleanup_err,
                "collection_report": collection_report,
                "collection_err": collection_err,
            })
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::app::background::tasks::support_bundle::perfetto;
    use crate::app::background::tasks::support_bundle::request::BundleData;
    use crate::app::support_bundles::SupportBundleQueryType;
    use http_body_util::BodyExt;
    use illumos_utils::zpool::ZpoolHealth;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::RendezvousDebugDataset;
    use nexus_db_model::Zpool;
    use nexus_test_utils::SLED_AGENT_UUID;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::fm::ereport::{EreportData, EreportId, Reporter};
    use nexus_types::identity::Asset;
    use nexus_types::internal_api::background::SupportBundleCollectionStep;
    use nexus_types::internal_api::background::SupportBundleEreportStatus;
    use nexus_types::inventory::SpType;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::internal::shared::DatasetKind;
    use omicron_common::disk::DatasetConfig;
    use omicron_common::disk::DatasetName;
    use omicron_common::disk::DatasetsConfig;
    use omicron_common::disk::SharedDatasetConfig;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::{
        BlueprintUuid, DatasetUuid, EreporterRestartUuid, OmicronZoneUuid,
        PhysicalDiskUuid, SledUuid,
    };
    use std::collections::HashSet;
    use std::num::NonZeroU64;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    // If we have not populated any bundles needing cleanup, the cleanup
    // process should succeed with an empty cleanup report.
    #[nexus_test(server = crate::Server)]
    async fn test_cleanup_noop(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should succeed with no work to do");

        assert_eq!(report, SupportBundleCleanupReport::default());
    }

    // If there are no bundles in need of collection, the collection task should
    // run without error, but return nothing.
    #[nexus_test(server = crate::Server)]
    async fn test_collect_noop(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        let request = BundleRequest::default();
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should succeed with no bundles");
        assert!(report.is_none());
    }

    async fn add_zpool_and_debug_dataset(
        datastore: &DataStore,
        opctx: &OpContext,
        id: PhysicalDiskUuid,
        sled_id: SledUuid,
        blueprint_id: BlueprintUuid,
    ) -> (ZpoolUuid, DatasetUuid) {
        let zpool = datastore
            .zpool_insert(
                opctx,
                Zpool::new(
                    ZpoolUuid::new_v4(),
                    sled_id,
                    id,
                    ByteCount::from(0).into(),
                ),
            )
            .await
            .unwrap();

        let dataset = datastore
            .debug_dataset_insert_if_not_exists(
                opctx,
                RendezvousDebugDataset::new(
                    DatasetUuid::new_v4(),
                    zpool.id(),
                    blueprint_id,
                ),
            )
            .await
            .unwrap()
            .expect("inserted new dataset");
        (zpool.id(), dataset.id())
    }

    async fn make_disk_in_db(
        datastore: &DataStore,
        opctx: &OpContext,
        i: usize,
        sled_id: SledUuid,
    ) -> PhysicalDiskUuid {
        let id = PhysicalDiskUuid::new_v4();
        let physical_disk = PhysicalDisk::new(
            id,
            "v".into(),
            format!("s-{i})"),
            "m".into(),
            PhysicalDiskKind::U2,
            sled_id,
        );
        datastore
            .physical_disk_insert(&opctx, physical_disk.clone())
            .await
            .unwrap();
        id
    }

    async fn make_fake_ereports(datastore: &DataStore, opctx: &OpContext) {
        const SP_SERIAL: &str = "BRM42000069";
        const HOST_SERIAL: &str = "BRM66600042";
        const GIMLET_PN: &str = "9130000019";
        // Make some SP ereports...
        let sp_restart_id = EreporterRestartUuid::new_v4();
        datastore.ereports_insert(&opctx, Reporter::Sp { sp_type: SpType::Sled, slot: 8}, vec![
            EreportData {
                id: EreportId { restart_id: sp_restart_id, ena: ereport_types::Ena(1) },
                time_collected: chrono::Utc::now(),
                collector_id: OmicronZoneUuid::new_v4(),
                part_number: Some(GIMLET_PN.to_string()),
                serial_number: Some(SP_SERIAL.to_string()),
                class: Some("ereport.fake.whatever".to_string()),
                report: serde_json::json!({"hello world": true})
            },
            EreportData {
                id: EreportId { restart_id: sp_restart_id, ena: ereport_types::Ena(2) },
                time_collected: chrono::Utc::now(),
                collector_id: OmicronZoneUuid::new_v4(),
                part_number: Some(GIMLET_PN.to_string()),
                serial_number: Some(SP_SERIAL.to_string()),
                class: Some("ereport.something.blah".to_string()),
                report: serde_json::json!({"system_working": "seems to be",})
            },
            EreportData {
                id: EreportId { restart_id: EreporterRestartUuid::new_v4(), ena: ereport_types::Ena(1) },
                time_collected: chrono::Utc::now(),
                collector_id: OmicronZoneUuid::new_v4(),
                // Let's do a silly one! No VPD, to make sure that's also
                // handled correctly.
                part_number: None,
                serial_number: None,
                class: Some("ereport.fake.whatever".to_string()),
                report: serde_json::json!({"hello_world": true})
            },
        ]).await.expect("failed to insert fake SP ereports");
        // And one from a different serial. N.B. that I made sure the number of
        // host-OS and SP ereports are different for when we make assertions
        // about the bundle report.
        datastore
            .ereports_insert(
                &opctx,
                Reporter::Sp { sp_type: SpType::Switch, slot: 1 },
                vec![EreportData {
                    id: EreportId {
                        restart_id: EreporterRestartUuid::new_v4(),
                        ena: ereport_types::Ena(1),
                    },
                    time_collected: chrono::Utc::now(),
                    collector_id: OmicronZoneUuid::new_v4(),
                    part_number: Some("9130000006".to_string()),
                    serial_number: Some("BRM41000555".to_string()),
                    class: Some("ereport.fake.whatever".to_string()),
                    report: serde_json::json!({"im_a_sidecar": true}),
                }],
            )
            .await
            .expect("failed to insert another fake SP ereport");
        // And some host OS ones...
        let restart_id = EreporterRestartUuid::new_v4();
        datastore
            .ereports_insert(
                &opctx,
                Reporter::HostOs { sled: SledUuid::new_v4() },
                vec![
                    EreportData {
                        id: EreportId {
                            restart_id,
                            ena: ereport_types::Ena(1),
                        },
                        time_collected: chrono::Utc::now(),
                        collector_id: OmicronZoneUuid::new_v4(),
                        serial_number: Some(HOST_SERIAL.to_string()),
                        part_number: Some(GIMLET_PN.to_string()),
                        class: Some("ereport.fake.whatever".to_string()),
                        report: serde_json::json!({"hello_world": true}),
                    },
                    EreportData {
                        id: EreportId {
                            restart_id,
                            ena: ereport_types::Ena(2),
                        },
                        time_collected: chrono::Utc::now(),
                        collector_id: OmicronZoneUuid::new_v4(),
                        serial_number: Some(HOST_SERIAL.to_string()),
                        part_number: Some(GIMLET_PN.to_string()),
                        class: Some("ereport.fake.whatever.thingy".to_string()),
                        report: serde_json::json!({"goodbye_world": false}),
                    },
                ],
            )
            .await
            .expect("failed to insert fake host OS ereports");
        datastore
            .ereports_insert(
                &opctx,
                Reporter::HostOs { sled: SledUuid::new_v4() },
                vec![
                    EreportData {
                        id: EreportId { restart_id: EreporterRestartUuid::new_v4(), ena:  ereport_types::Ena(1) },
                        time_collected: chrono::Utc::now(),
                        collector_id: OmicronZoneUuid::new_v4(),
                        serial_number: Some(HOST_SERIAL.to_string()),
                        part_number: Some(GIMLET_PN.to_string()),
                        class: Some("ereport.something.hostos_related".to_string()),
                        report: serde_json::json!({"illumos": "very yes", "whatever": 42}),
                    },
                ],
            )
            .await
            .expect("failed to insert another fake host OS ereport");
    }

    struct TestDataset {
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    }

    impl TestDataset {
        // For the purposes of this test, we're going to provision support
        // bundles to a single sled.
        async fn setup(
            cptestctx: &ControlPlaneTestContext,
            datastore: &Arc<DataStore>,
            opctx: &OpContext,
            count: usize,
        ) -> Vec<Self> {
            let sled_id = SledUuid::from_untyped_uuid(
                SLED_AGENT_UUID.parse::<Uuid>().unwrap(),
            );

            let mut disks = vec![];

            // The fake disks/datasets we create aren't really part of the test
            // blueprint, but that's fine for our purposes.
            let blueprint_id = cptestctx.initial_blueprint_id;

            for i in 0..count {
                // Create the (disk, zpool, dataset) tuple in Nexus
                let disk_id =
                    make_disk_in_db(datastore, opctx, i, sled_id).await;
                let (zpool_id, dataset_id) = add_zpool_and_debug_dataset(
                    &datastore,
                    &opctx,
                    disk_id,
                    sled_id,
                    blueprint_id,
                )
                .await;

                // Tell the simulated sled agent to create this storage.
                //
                // (We could do this via HTTP request, but it's in the test process,
                // so we can just directly call the method on the sled agent)
                cptestctx.first_sled_agent().create_zpool(
                    zpool_id,
                    disk_id,
                    1 << 40,
                    ZpoolHealth::Online,
                );
                disks.push(Self { zpool_id, dataset_id })
            }

            // Create a configuration for the sled agent consisting of all these
            // debug datasets.
            let datasets = disks
                .iter()
                .map(|TestDataset { zpool_id, dataset_id }| {
                    (
                        *dataset_id,
                        DatasetConfig {
                            id: *dataset_id,
                            name: DatasetName::new(
                                ZpoolName::new_external(*zpool_id),
                                DatasetKind::Debug,
                            ),
                            inner: SharedDatasetConfig::default(),
                        },
                    )
                })
                .collect();

            // Read current sled config generation from zones (this will change
            // slightly once the simulator knows how to keep the unified config
            // and be a little less weird)
            let current_generation =
                cptestctx.first_sled_agent().omicron_zones_list().generation;

            let dataset_config = DatasetsConfig {
                generation: current_generation.next(),
                datasets,
            };

            let res = cptestctx
                .first_sled_agent()
                .datasets_ensure(dataset_config)
                .unwrap();
            assert!(!res.has_error());

            disks
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_collect_one(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // Make up some ereports so that we can test that they're included in
        // the bundle.
        make_fake_ereports(&datastore, &opctx).await;

        // Assign a bundle to ourselves. We expect to collect it on
        // the next call to "collect_bundle".
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        // The bundle collection should complete successfully.
        // NOTE: The support bundle querying interface isn't supported on
        // the simulated sled agent (yet?) so we're using an empty sled selection.
        let mut request = BundleRequest::default();
        request.data_selection.insert(BundleData::HostInfo(HashSet::new()));
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());
        // Verify that we spawned steps to query sleds and SPs
        let step_names: Vec<_> =
            report.steps.iter().map(|s| s.name.as_str()).collect();
        assert!(
            step_names.contains(&SupportBundleCollectionStep::STEP_SPAWN_SLEDS)
        );
        assert!(
            step_names
                .contains(&SupportBundleCollectionStep::STEP_SPAWN_SP_DUMPS)
        );
        assert!(report.activated_in_db_ok);
        assert_eq!(
            report.ereports,
            Some(SupportBundleEreportStatus {
                n_collected: 7,
                n_found: 7,
                errors: Vec::new()
            })
        );

        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Bundle should definitely be in db by this point");
        assert_eq!(observed_bundle.state, SupportBundleState::Active);

        // If we retry bundle collection, nothing should happen.
        // The bundle has already been collected.
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should be a no-op the second time");
        assert!(report.is_none());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_trace_file_generated(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // Create a bundle to collect
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For trace file testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        // Collect the bundle
        let mut request = BundleRequest::default();
        request.data_selection.insert(BundleData::HostInfo(HashSet::new()));
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded")
            .expect("Should have generated a report");

        // Download the trace file from the bundle
        let head = false;
        let range = None;
        let response = nexus
            .support_bundle_download(
                &opctx,
                bundle.id.into(),
                SupportBundleQueryType::Path {
                    file_path: "meta/trace.json".to_string(),
                },
                head,
                range,
            )
            .await
            .expect("Should be able to download trace file");

        // Parse the trace file using our Perfetto structs
        let body_bytes =
            response.into_body().collect().await.unwrap().to_bytes();
        let trace: perfetto::Trace = serde_json::from_slice(&body_bytes)
            .expect("Trace file should be valid Perfetto JSON");

        // Verify display time unit
        assert_eq!(
            trace.display_time_unit, "ms",
            "Display time unit should be milliseconds"
        );

        // We should have at least the main collection steps
        assert!(
            !trace.trace_events.is_empty(),
            "Should have at least one trace event"
        );

        // Verify each event has the expected structure
        for event in &trace.trace_events {
            // Verify category
            assert_eq!(
                event.cat, "bundle_collection",
                "Event should have category 'bundle_collection'"
            );
            // Verify phase type
            assert_eq!(event.ph, "X", "Event should be Complete event type");
            // Verify timestamps are positive
            assert!(event.ts >= 0, "Event timestamp should be non-negative");
            assert!(event.dur >= 0, "Event duration should be non-negative");
            // Verify process and thread IDs are set
            assert_eq!(event.pid, 1, "All events should have pid=1");
            assert!(event.tid > 0, "Event thread ID should be positive");
        }

        // Verify we have the same number of events as steps in the report
        assert_eq!(
            trace.trace_events.len(),
            report.steps.len(),
            "Number of events should match number of steps"
        );

        // Verify step names match between report and trace
        let trace_names: std::collections::HashSet<_> =
            trace.trace_events.iter().map(|e| e.name.as_str()).collect();
        let report_names: std::collections::HashSet<_> =
            report.steps.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            trace_names, report_names,
            "Trace event names should match report step names"
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_collect_chunked(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        // The bundle collection should complete successfully.
        //
        // We're going to use a really small chunk size here to force the bundle
        // to get split up.
        let request = BundleRequest {
            transfer_chunk_size: NonZeroU64::new(16).unwrap(),
            data_selection: [
                BundleData::Reconfigurator,
                BundleData::HostInfo(HashSet::new()),
                BundleData::SledCubbyInfo,
                BundleData::SpDumps,
            ]
            .into_iter()
            .collect(),
        };

        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());
        // Verify that we spawned steps to query sleds and SPs
        let step_names: Vec<_> =
            report.steps.iter().map(|s| s.name.as_str()).collect();
        assert!(
            step_names.contains(&SupportBundleCollectionStep::STEP_SPAWN_SLEDS)
        );
        assert!(
            step_names
                .contains(&SupportBundleCollectionStep::STEP_SPAWN_SP_DUMPS)
        );
        assert!(report.activated_in_db_ok);

        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Bundle should definitely be in db by this point");
        assert_eq!(observed_bundle.state, SupportBundleState::Active);

        // Download a file from the bundle, to verify that it was transferred
        // successfully.
        let head = false;
        let range = None;
        let response = nexus
            .support_bundle_download(
                &opctx,
                observed_bundle.id.into(),
                SupportBundleQueryType::Path {
                    file_path: "bundle_id.txt".to_string(),
                },
                head,
                range,
            )
            .await
            .unwrap();

        // Read the body to bytes, then convert to string
        let body_bytes =
            response.into_body().collect().await.unwrap().to_bytes();
        let body_string = String::from_utf8(body_bytes.to_vec()).unwrap();

        // Verify the content matches the bundle ID
        assert_eq!(body_string, observed_bundle.id.to_string());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_collect_many(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 2).await;

        // Assign two bundles to ourselves.
        let bundle1 = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        let bundle2 = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a second support bundle");

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        // Each time we call "collect_bundle", we collect a SINGLE bundle.
        let mut request = BundleRequest::default();
        request.data_selection.insert(BundleData::HostInfo(HashSet::new()));
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle1.id.into());
        // Verify that we spawned steps to query sleds and SPs
        let step_names: Vec<_> =
            report.steps.iter().map(|s| s.name.as_str()).collect();
        assert!(
            step_names.contains(&SupportBundleCollectionStep::STEP_SPAWN_SLEDS)
        );
        assert!(
            step_names
                .contains(&SupportBundleCollectionStep::STEP_SPAWN_SP_DUMPS)
        );
        assert!(report.activated_in_db_ok);

        // This is observable by checking the state of bundle1 and bundle2:
        // the first is active, the second is still collecting.
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle1.id.into())
            .await
            .unwrap();
        assert_eq!(observed_bundle.state, SupportBundleState::Active);
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle2.id.into())
            .await
            .unwrap();
        assert_eq!(observed_bundle.state, SupportBundleState::Collecting);

        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle2.id.into());
        // Verify that we spawned steps to query sleds and SPs
        let step_names: Vec<_> =
            report.steps.iter().map(|s| s.name.as_str()).collect();
        assert!(
            step_names.contains(&SupportBundleCollectionStep::STEP_SPAWN_SLEDS)
        );
        assert!(
            step_names
                .contains(&SupportBundleCollectionStep::STEP_SPAWN_SP_DUMPS)
        );
        assert!(report.activated_in_db_ok);

        // After another collection request, we'll see that both bundles have
        // been collected.
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle1.id.into())
            .await
            .unwrap();
        assert_eq!(observed_bundle.state, SupportBundleState::Active);
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle2.id.into())
            .await
            .unwrap();
        assert_eq!(observed_bundle.state, SupportBundleState::Active);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_bundle_cleanup_cancel_before_collect(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 2).await;

        // If we delete the bundle before we start collection, we can delete it
        // immediately.
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        // Cancel the bundle immediately
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Destroying,
            )
            .await
            .unwrap();

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should succeed with no work to do");
        assert_eq!(
            report,
            SupportBundleCleanupReport {
                // Nothing was provisioned on the sled, since we hadn't started
                // collection yet.
                sled_bundles_deleted_not_found: 1,
                // The database state was "destroying", and now it's gone.
                db_destroying_bundles_removed: 1,
                ..Default::default()
            }
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_bundle_cleanup_cancel_after_collect(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // We can allocate a support bundle and collect it
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );
        let mut request = BundleRequest::default();
        request.data_selection.insert(BundleData::HostInfo(HashSet::new()));
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());
        // Verify that we spawned steps to query sleds and SPs
        let step_names: Vec<_> =
            report.steps.iter().map(|s| s.name.as_str()).collect();
        assert!(
            step_names.contains(&SupportBundleCollectionStep::STEP_SPAWN_SLEDS)
        );
        assert!(
            step_names
                .contains(&SupportBundleCollectionStep::STEP_SPAWN_SP_DUMPS)
        );
        assert!(report.activated_in_db_ok);

        // Cancel the bundle after collection has completed
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Destroying,
            )
            .await
            .unwrap();

        // When we perform cleanup, we should see that it was removed from the
        // underlying sled.
        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should succeed with no work to do");
        assert_eq!(
            report,
            SupportBundleCleanupReport {
                // Nothing was provisioned on the sled, since we hadn't started
                // collection yet.
                sled_bundles_deleted_ok: 1,
                // The database state was "destroying", and now it's gone.
                db_destroying_bundles_removed: 1,
                ..Default::default()
            }
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_bundle_cleanup_failed_bundle_before_collection(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // We can allocate a support bundle, though we'll fail it before it gets
        // collected.
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        // Mark the bundle as "failing" - this should be triggered
        // automatically by the blueprint executor if the corresponding
        // storage has been expunged.
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Failing,
            )
            .await
            .unwrap();

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should delete failing bundle");
        assert_eq!(
            report,
            SupportBundleCleanupReport {
                // Nothing was provisioned on the sled, since we hadn't started
                // collection yet.
                sled_bundles_deleted_not_found: 1,
                // The database state was "failing", and now it's updated.
                //
                // Note that it isn't immediately deleted, so the end-user
                // should have a chance to observe the new state.
                db_failing_bundles_updated: 1,
                ..Default::default()
            }
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_bundle_cleanup_failed_bundle_after_collection(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // We can allocate a support bundle and collect it
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );
        let mut request = BundleRequest::default();
        request.data_selection.insert(BundleData::HostInfo(HashSet::new()));
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());

        // Mark the bundle as "failing" - this should be triggered
        // automatically by the blueprint executor if the corresponding
        // storage has been expunged.
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Failing,
            )
            .await
            .unwrap();

        // We can cleanup this bundle, even though it has already been
        // collected.
        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should delete failing bundle");
        assert_eq!(
            report,
            SupportBundleCleanupReport {
                // The bundle was provisioned on the sled, so we should have
                // successfully removed it when we later talk to the sled.
                //
                // This won't always be the case for removal - if the entire
                // underlying sled was expunged, we won't get an affirmative
                // HTTP response from it. But in our simulated environment,
                // we have simply marked the bundle "failed" and kept the
                // simulated server running.
                sled_bundles_deleted_ok: 1,
                // The database state was "failing", and now it's updated.
                //
                // Note that it isn't immediately deleted, so the end-user
                // should have a chance to observe the new state.
                db_failing_bundles_updated: 1,
                ..Default::default()
            }
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_bundle_cleanup_after_zpool_deletion(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // We can allocate a support bundle and collect it
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );
        let mut request = BundleRequest::default();
        request.data_selection.insert(BundleData::HostInfo(HashSet::new()));
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());

        // Mark the bundle as "failing" - this should be triggered
        // automatically by the blueprint executor if the corresponding
        // storage has been expunged.
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Failing,
            )
            .await
            .unwrap();

        // Delete the zpool holding the bundle.
        //
        // This should call the "zpool_get_sled_if_in_service" call to fail!
        datastore
            .zpool_delete_self_and_all_datasets(&opctx, bundle.zpool_id.into())
            .await
            .unwrap();

        // We can cleanup this bundle, even though it has already been
        // collected.
        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should delete failing bundle");
        assert_eq!(
            report,
            SupportBundleCleanupReport {
                // The database state was "failing", and now it's updated.
                //
                // Note that it isn't immediately deleted, so the end-user
                // should have a chance to observe the new state.
                db_failing_bundles_updated: 1,
                ..Default::default()
            }
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_reconfigurator_state_collected(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // Create a support bundle
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "Testing reconfigurator state collection",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        // Collect the bundle
        let mut request = BundleRequest::default();
        request.data_selection.insert(BundleData::HostInfo(HashSet::new()));
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());

        // Verify bundle is active
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Bundle should be in db");
        assert_eq!(observed_bundle.state, SupportBundleState::Active);

        // Download the reconfigurator_state.json file
        let head = false;
        let range = None;
        let response = nexus
            .support_bundle_download(
                &opctx,
                observed_bundle.id.into(),
                SupportBundleQueryType::Path {
                    file_path: "reconfigurator_state.json".to_string(),
                },
                head,
                range,
            )
            .await
            .expect("Should be able to download reconfigurator_state.json");

        // Read and parse the JSON
        let body_bytes =
            response.into_body().collect().await.unwrap().to_bytes();
        let body_string = String::from_utf8(body_bytes.to_vec()).unwrap();
        let state: serde_json::Value =
            serde_json::from_str(&body_string).expect("Should be valid JSON");

        // Verify the JSON has the expected structure
        //
        // We don't really care about the contents that much, we just want to
        // verify that the UnstableReconfiguratorState object got serialized
        // at all.

        assert!(
            !state
                .get("target_blueprint")
                .expect("missing target blueprint")
                .is_null(),
            "Should have target blueprint"
        );
        assert!(
            !state
                .get("blueprints")
                .expect("missing blueprints")
                .as_array()
                .expect("blueprints should be an array")
                .is_empty(),
            "Should have blueprints"
        );
    }
}
