// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures dataset records required by a given blueprint

use crate::Sled;

use anyhow::anyhow;
use anyhow::Context;
use futures::stream;
use futures::StreamExt;
use nexus_db_model::CrucibleDataset;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::identity::Asset;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetsConfig;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use slog::info;
use slog::o;
use slog::warn;
use std::collections::BTreeMap;

/// Idempotently ensures that the specified datasets are deployed to the
/// corresponding sleds
pub(crate) async fn deploy_datasets(
    opctx: &OpContext,
    sleds_by_id: &BTreeMap<SledUuid, Sled>,
    sled_configs: &BTreeMap<SledUuid, BlueprintDatasetsConfig>,
) -> Result<(), Vec<anyhow::Error>> {
    let errors: Vec<_> = stream::iter(sled_configs)
        .filter_map(|(sled_id, config)| async move {
            let log = opctx.log.new(o!(
                "sled_id" => sled_id.to_string(),
                "generation" => config.generation.to_string(),
            ));

            let db_sled = match sleds_by_id.get(&sled_id) {
                Some(sled) => sled,
                None => {
                    let err = anyhow!("sled not found in db list: {}", sled_id);
                    warn!(log, "{err:#}");
                    return Some(err);
                }
            };

            let client = nexus_networking::sled_client_from_address(
                sled_id.into_untyped_uuid(),
                db_sled.sled_agent_address(),
                &log,
            );

            let config: DatasetsConfig = config.clone().into();
            let result =
                client.datasets_put(&config).await.with_context(
                    || format!("Failed to put {config:#?} to sled {sled_id}"),
                );
            match result {
                Err(error) => {
                    warn!(log, "{error:#}");
                    Some(error)
                }
                Ok(result) => {
                    let (errs, successes): (Vec<_>, Vec<_>) = result
                        .into_inner()
                        .status
                        .into_iter()
                        .partition(|status| status.err.is_some());

                    if !errs.is_empty() {
                        warn!(
                            log,
                            "Failed to deploy datasets for sled agent";
                            "successfully configured datasets" => successes.len(),
                            "failed dataset configurations" => errs.len(),
                        );
                        for err in &errs {
                            warn!(log, "{err:?}");
                        }
                        return Some(anyhow!(
                            "failure deploying datasets: {:?}",
                            errs
                        ));
                    }

                    info!(
                        log,
                        "Successfully deployed datasets for sled agent";
                        "successfully configured datasets" => successes.len(),
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

#[allow(dead_code)]
pub(crate) struct EnsureDatasetsResult {
    pub(crate) inserted: usize,
    pub(crate) updated: usize,
    pub(crate) removed: usize,
}

/// For all datasets we expect to see in the blueprint, ensure that a corresponding
/// database record exists in `datastore`.
///
/// Updates all existing dataset records that don't match the blueprint.
/// Returns the number of datasets changed.
pub(crate) async fn ensure_crucible_dataset_records_exist(
    opctx: &OpContext,
    datastore: &DataStore,
    bp_id: BlueprintUuid,
    bp_datasets: impl Iterator<Item = &BlueprintDatasetConfig>,
) -> anyhow::Result<EnsureDatasetsResult> {
    // Before attempting to insert any datasets, first query for any existing
    // dataset records so we can filter them out. This looks like a typical
    // TOCTOU issue, but it is purely a performance optimization. We expect
    // almost all executions of this function to do nothing: new datasets are
    // created very rarely relative to how frequently blueprint realization
    // happens. We could remove this check and filter and instead run the below
    // "insert if not exists" query on every dataset, and the behavior would
    // still be correct. However, that would issue far more queries than
    // necessary in the very common case of "we don't need to do anything at
    // all".
    let existing_datasets = datastore
        .crucible_dataset_list_all_batched(opctx)
        .await
        .context("failed to list all datasets")?
        .into_iter()
        .map(|dataset| (dataset.id(), dataset))
        .collect::<BTreeMap<DatasetUuid, _>>();

    let mut num_inserted = 0;
    let mut num_unchanged = 0;

    // Filter down to in-service Crucible datasets.
    let wanted_datasets = bp_datasets.filter_map(|d| {
        match d.disposition {
            BlueprintDatasetDisposition::InService => (),
            BlueprintDatasetDisposition::Expunged => return None,
        }
        // We only insert records for Crucible datasets.
        let dataset = match (&d.kind, d.address) {
            (DatasetKind::Crucible, Some(addr)) => CrucibleDataset::new(
                d.id,
                d.pool.id().into_untyped_uuid(),
                addr,
            ),
            (DatasetKind::Crucible, None) => {
                // This should be impossible! Ideally we'd prevent it
                // statically, but for now just fail at runtime.
                return Some(Err(anyhow!(
                    "invalid blueprint dataset: \
                     {} has kind Crucible but no address",
                    d.id
                )));
            }
            _ => return None,
        };
        Some(Ok(dataset))
    });

    for dataset in wanted_datasets {
        let dataset = dataset?;
        let id = dataset.id();

        // If this dataset already exists, do nothing.
        if existing_datasets.contains_key(&id) {
            num_unchanged += 1;
            continue;
        }

        datastore
            .crucible_dataset_upsert_if_blueprint_is_current_target(
                &opctx, bp_id, dataset,
            )
            .await
            .with_context(|| {
                format!("failed to upsert dataset record for dataset {id}")
            })?;
        num_inserted += 1;

        info!(
            opctx.log,
            "ensuring Crucible dataset record in database";
            "action" => "insert",
            "id" => %id,
        );
    }

    // Region and snapshot replacement cannot happen without the database
    // record, even if the dataset has been expunged.
    //
    // Dataset records for expunged Crucible datasets will be deleted, but it
    // will happen as a part of the "decommissioned_disk_cleaner" background
    // task. We do not do so here.

    info!(
        opctx.log,
        "ensured all Crucible datasets have database records";
        "num_inserted" => num_inserted,
        "num_unchanged" => num_unchanged,
    );

    Ok(EnsureDatasetsResult { inserted: num_inserted, updated: 0, removed: 0 })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_model::Zpool;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::deployment::BlueprintZoneType;
    use omicron_common::api::internal::shared::DatasetKind;
    use omicron_common::disk::CompressionAlgorithm;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::ZpoolUuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    fn get_all_crucible_datasets_from_zones(
        blueprint: &Blueprint,
    ) -> Vec<BlueprintDatasetConfig> {
        blueprint
            .all_omicron_zones(BlueprintZoneFilter::All)
            .filter_map(|(_, zone)| {
                if !matches!(zone.zone_type, BlueprintZoneType::Crucible(_)) {
                    return None;
                }
                let dataset = zone
                    .zone_type
                    .durable_dataset()
                    .expect("crucible zones have datasets");
                Some(BlueprintDatasetConfig {
                    disposition: BlueprintDatasetDisposition::InService,
                    id: DatasetUuid::new_v4(),
                    pool: dataset.dataset.pool_name.clone(),
                    kind: dataset.kind,
                    address: Some(dataset.address),
                    quota: None,
                    reservation: None,
                    compression: CompressionAlgorithm::Off,
                })
            })
            .collect::<Vec<_>>()
    }

    #[nexus_test]
    async fn test_dataset_record_create(cptestctx: &ControlPlaneTestContext) {
        const TEST_NAME: &str = "test_dataset_record_create";

        // Set up.
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let opctx = &opctx;

        // Use the standard example system.
        let (example, mut blueprint) =
            ExampleSystemBuilder::new(&opctx.log, TEST_NAME).nsleds(5).build();

        // Set the target so our database-modifying operations know they
        // can safely act on the current target blueprint.
        update_blueprint_target(&datastore, &opctx, &mut blueprint).await;
        let collection = example.collection;

        // Record the sleds and zpools.
        crate::tests::insert_sled_records(datastore, &blueprint).await;
        crate::tests::create_disks_for_zones_using_datasets(
            datastore, opctx, &blueprint,
        )
        .await;

        // Prior to ensuring datasets exist, there should be none.
        assert_eq!(
            datastore
                .crucible_dataset_list_all_batched(opctx)
                .await
                .unwrap()
                .len(),
            0
        );

        // Let's allocate datasets for all the zones with durable datasets.
        //
        // Finding these datasets is normally the responsibility of the planner,
        // but we're kinda hand-rolling it.
        let all_datasets = get_all_crucible_datasets_from_zones(&blueprint);

        // How many zones are there with durable datasets?
        let nzones_with_durable_datasets = all_datasets.len();
        assert!(nzones_with_durable_datasets > 0);

        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_crucible_dataset_records_exist(
                opctx,
                datastore,
                blueprint.id,
                all_datasets.iter(),
            )
            .await
            .expect("failed to ensure datasets");

        // We should have inserted a dataset for each zone with a durable
        // dataset.
        assert_eq!(inserted, nzones_with_durable_datasets);
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);
        assert_eq!(
            datastore
                .crucible_dataset_list_all_batched(opctx)
                .await
                .unwrap()
                .len(),
            nzones_with_durable_datasets,
        );

        // Ensuring the same datasets again should insert no new records.
        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_crucible_dataset_records_exist(
                opctx,
                datastore,
                blueprint.id,
                all_datasets.iter(),
            )
            .await
            .expect("failed to ensure datasets");
        assert_eq!(inserted, 0);
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);
        assert_eq!(
            datastore
                .crucible_dataset_list_all_batched(opctx)
                .await
                .unwrap()
                .len(),
            nzones_with_durable_datasets,
        );

        // Create another zpool on one of the sleds, so we can add new
        // zones that use it.
        let new_zpool_id = ZpoolUuid::new_v4();
        for &sled_id in collection.sled_agents.keys().take(1) {
            let zpool = Zpool::new(
                new_zpool_id.into_untyped_uuid(),
                sled_id.into_untyped_uuid(),
                PhysicalDiskUuid::new_v4(),
            );
            datastore
                .zpool_insert(opctx, zpool)
                .await
                .expect("failed to upsert zpool");
        }

        // Call `ensure_crucible_dataset_records_exist` again, adding new
        // datasets.
        //
        // It should only insert these new zones.
        let new_zones = [
            BlueprintDatasetConfig {
                disposition: BlueprintDatasetDisposition::InService,
                id: DatasetUuid::new_v4(),
                pool: ZpoolName::new_external(new_zpool_id),
                kind: DatasetKind::Crucible,
                address: Some("[::1]:0".parse().unwrap()),
                quota: None,
                reservation: None,
                compression: CompressionAlgorithm::Off,
            },
            BlueprintDatasetConfig {
                disposition: BlueprintDatasetDisposition::InService,
                id: DatasetUuid::new_v4(),
                pool: ZpoolName::new_external(new_zpool_id),
                kind: DatasetKind::Crucible,
                address: Some("[::1]:0".parse().unwrap()),
                quota: None,
                reservation: None,
                compression: CompressionAlgorithm::Off,
            },
        ];

        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_crucible_dataset_records_exist(
                opctx,
                datastore,
                blueprint.id,
                all_datasets.iter().chain(&new_zones),
            )
            .await
            .expect("failed to ensure datasets");
        assert_eq!(inserted, 2);
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);
        assert_eq!(
            datastore
                .crucible_dataset_list_all_batched(opctx)
                .await
                .unwrap()
                .len(),
            nzones_with_durable_datasets + 2,
        );
    }

    // Sets the target blueprint to "blueprint"
    //
    // Reads the current target, and uses it as the "parent" blueprint
    async fn update_blueprint_target(
        datastore: &DataStore,
        opctx: &OpContext,
        blueprint: &mut Blueprint,
    ) {
        // Fetch the initial blueprint installed during rack initialization.
        let parent_blueprint_target = datastore
            .blueprint_target_get_current(&opctx)
            .await
            .expect("failed to read current target blueprint");
        blueprint.parent_blueprint_id = Some(parent_blueprint_target.target_id);
        datastore.blueprint_insert(&opctx, &blueprint).await.unwrap();
        datastore
            .blueprint_target_set_current(
                &opctx,
                nexus_types::deployment::BlueprintTarget {
                    target_id: blueprint.id,
                    enabled: true,
                    time_made_target: nexus_inventory::now_db_precision(),
                },
            )
            .await
            .unwrap();
    }

    #[nexus_test]
    async fn test_dataset_records_delete(cptestctx: &ControlPlaneTestContext) {
        const TEST_NAME: &str = "test_dataset_records_delete";

        // Set up.
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let opctx = &opctx;

        // Use the standard example system.
        let (_example, mut blueprint) =
            ExampleSystemBuilder::new(&opctx.log, TEST_NAME).nsleds(5).build();

        // Set the target so our database-modifying operations know they
        // can safely act on the current target blueprint.
        update_blueprint_target(&datastore, &opctx, &mut blueprint).await;

        // Record the sleds and zpools.
        crate::tests::insert_sled_records(datastore, &blueprint).await;
        crate::tests::create_disks_for_zones_using_datasets(
            datastore, opctx, &blueprint,
        )
        .await;

        let mut all_datasets = get_all_crucible_datasets_from_zones(&blueprint);
        assert!(
            !all_datasets.is_empty(),
            "blueprint should have at least one crucible dataset"
        );

        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_crucible_dataset_records_exist(
                opctx,
                datastore,
                blueprint.id,
                all_datasets.iter(),
            )
            .await
            .expect("failed to ensure datasets");
        assert_eq!(inserted, all_datasets.len());
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);

        // Expunge a Crucible dataset
        let crucible_dataset = all_datasets
            .iter_mut()
            .find(|dataset| matches!(dataset.kind, DatasetKind::Crucible))
            .expect("No crucible dataset found");
        assert_eq!(
            crucible_dataset.disposition,
            BlueprintDatasetDisposition::InService
        );
        crucible_dataset.disposition = BlueprintDatasetDisposition::Expunged;
        let crucible_dataset_id = crucible_dataset.id;

        // Observe that we don't remove it.
        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_crucible_dataset_records_exist(
                opctx,
                datastore,
                blueprint.id,
                all_datasets.iter(),
            )
            .await
            .expect("failed to ensure datasets");
        assert_eq!(inserted, 0);
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);

        // Make sure the dataset still exists.
        let observed_datasets =
            datastore.crucible_dataset_list_all_batched(opctx).await.unwrap();
        assert!(observed_datasets
            .iter()
            .any(|d| d.id() == crucible_dataset_id));
    }

    #[nexus_test]
    async fn test_dataset_record_blueprint_removal_without_expunging(
        cptestctx: &ControlPlaneTestContext,
    ) {
        const TEST_NAME: &str =
            "test_dataset_record_blueprint_removal_without_expunging";

        // Set up.
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let opctx = &opctx;

        // Use the standard example system.
        let (_example, mut blueprint) =
            ExampleSystemBuilder::new(&opctx.log, TEST_NAME).nsleds(5).build();

        // Set the target so our database-modifying operations know they
        // can safely act on the current target blueprint.
        update_blueprint_target(&datastore, &opctx, &mut blueprint).await;

        // Record the sleds and zpools.
        crate::tests::insert_sled_records(datastore, &blueprint).await;
        crate::tests::create_disks_for_zones_using_datasets(
            datastore, opctx, &blueprint,
        )
        .await;

        let mut all_datasets = get_all_crucible_datasets_from_zones(&blueprint);
        assert!(
            !all_datasets.is_empty(),
            "blueprint should have at least one crucible dataset"
        );

        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_crucible_dataset_records_exist(
                opctx,
                datastore,
                blueprint.id,
                all_datasets.iter(),
            )
            .await
            .expect("failed to ensure datasets");
        assert_eq!(inserted, all_datasets.len());
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);

        // Rather than expunging a dataset, which is the normal way to "delete"
        // a dataset, we'll just remove it from the "blueprint".
        //
        // This situation mimics a scenario where we are an "old Nexus,
        // executing an old blueprint" - more datasets might be created
        // concurrently with our execution, and we should leave them alone.
        let removed_dataset_id = all_datasets.pop().unwrap().id;

        // Observe that no datasets are removed.
        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_crucible_dataset_records_exist(
                opctx,
                datastore,
                blueprint.id,
                all_datasets.iter(),
            )
            .await
            .expect("failed to ensure datasets");
        assert_eq!(inserted, 0);
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);

        // Make sure the dataset still exists, even if it isn't tracked by our
        // "blueprint".
        let observed_datasets =
            datastore.crucible_dataset_list_all_batched(opctx).await.unwrap();
        assert!(observed_datasets.iter().any(|d| d.id() == removed_dataset_id));
    }
}
