// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures dataset records required by a given blueprint

use crate::Sled;

use anyhow::anyhow;
use anyhow::Context;
use futures::stream;
use futures::StreamExt;
use nexus_db_model::Dataset;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::identity::Asset;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetsConfig;
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
                db_sled.sled_agent_address,
                &log,
            );

            let config: DatasetsConfig = match config.clone().try_into() {
                Ok(config) => config,
                Err(err) => return Some(err)
            };

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
pub(crate) async fn ensure_dataset_records_exist(
    opctx: &OpContext,
    datastore: &DataStore,
    bp_datasets: impl Iterator<Item = &BlueprintDatasetConfig>,
) -> anyhow::Result<EnsureDatasetsResult> {
    // Before attempting to insert any datasets, first query for any existing
    // dataset records so we can filter them out. This looks like a typical
    // TOCTOU issue, but it is purely a performance optimization. We expect
    // almost all executions of this function to do nothing: new datasets are
    // created very rarely relative to how frequently blueprint realization
    // happens. We could remove this check and filter and instead run the below
    // "insert if not exists" query on every dataset, and the behavior would still
    // be correct. However, that would issue far more queries than necessary in
    // the very common case of "we don't need to do anything at all".
    let mut existing_datasets = datastore
        .dataset_list_all_batched(opctx, None)
        .await
        .context("failed to list all datasets")?
        .into_iter()
        .map(|dataset| (DatasetUuid::from_untyped_uuid(dataset.id()), dataset))
        .collect::<BTreeMap<DatasetUuid, _>>();

    let mut num_inserted = 0;
    let mut num_updated = 0;
    let mut num_unchanged = 0;
    let mut num_removed = 0;

    let (wanted_datasets, unwanted_datasets): (Vec<_>, Vec<_>) = bp_datasets
        .partition(|d| match d.disposition {
            BlueprintDatasetDisposition::InService => true,
            BlueprintDatasetDisposition::Expunged => false,
        });

    for bp_dataset in wanted_datasets {
        let id = bp_dataset.id;
        let kind = &bp_dataset.kind;

        // If this dataset already exists, only update it if it appears different from what exists
        // in the database already.
        let action = if let Some(db_dataset) = existing_datasets.remove(&id) {
            let db_config: DatasetConfig = db_dataset.try_into()?;
            let bp_config: DatasetConfig = bp_dataset.clone().try_into()?;

            if db_config == bp_config {
                num_unchanged += 1;
                continue;
            }
            num_updated += 1;
            "update"
        } else {
            num_inserted += 1;
            "insert"
        };

        let dataset = Dataset::from(bp_dataset.clone());
        datastore.dataset_upsert(dataset).await.with_context(|| {
            format!("failed to upsert dataset record for dataset {id}")
        })?;

        info!(
            opctx.log,
            "ensuring dataset record in database";
            "action" => action,
            "id" => %id,
            "kind" => ?kind,
        );
    }

    for bp_dataset in unwanted_datasets {
        if existing_datasets.remove(&bp_dataset.id).is_some() {
            if matches!(
                bp_dataset.kind,
                omicron_common::disk::DatasetKind::Crucible
            ) {
                // Region and snapshot replacement cannot happen without the
                // database record, even if the dataset has been expunged.
                //
                // This record will still be deleted, but it will happen as a
                // part of the "decommissioned_disk_cleaner" background task.
                continue;
            }

            datastore.dataset_delete(&opctx, bp_dataset.id).await?;
            num_removed += 1;
        }
    }

    // We support removing expunged datasets - if we read a dataset that hasn't
    // been explicitly expunged, log this as an oddity.
    //
    // This could be possible in conditions where multiple Nexuses are executing
    // distinct blueprints.
    if !existing_datasets.is_empty() {
        warn!(
            opctx.log,
            "database contains {} unexpected datasets",
            existing_datasets.len();
            "dataset_ids" => ?existing_datasets,
        );
    }

    info!(
        opctx.log,
        "ensured all Omicron datasets have database records";
        "num_inserted" => num_inserted,
        "num_updated" => num_updated,
        "num_unchanged" => num_unchanged,
        "num_removed" => num_removed,
    );

    Ok(EnsureDatasetsResult {
        inserted: num_inserted,
        updated: num_updated,
        removed: num_removed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_model::Zpool;
    use nexus_reconfigurator_planning::example::example;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintZoneFilter;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::internal::shared::DatasetKind;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    fn get_all_datasets_from_zones(
        blueprint: &Blueprint,
    ) -> Vec<BlueprintDatasetConfig> {
        blueprint
            .all_omicron_zones(BlueprintZoneFilter::All)
            .filter_map(|(_, zone)| {
                if let Some(dataset) = zone.zone_type.durable_dataset() {
                    Some(BlueprintDatasetConfig {
                        disposition: BlueprintDatasetDisposition::InService,
                        id: DatasetUuid::new_v4(),
                        pool: dataset.dataset.pool_name.clone(),
                        kind: dataset.kind,
                        address: Some(dataset.address),
                        quota: None,
                        reservation: None,
                        compression: String::new(),
                    })
                } else {
                    None
                }
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
        let (collection, _, blueprint) = example(&opctx.log, TEST_NAME, 5);

        // Record the sleds and zpools.
        crate::tests::insert_sled_records(datastore, &blueprint).await;
        crate::tests::create_disks_for_zones_using_datasets(
            datastore, opctx, &blueprint,
        )
        .await;

        // Prior to ensuring datasets exist, there should be none.
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, None)
                .await
                .unwrap()
                .len(),
            0
        );

        // Let's allocate datasets for all the zones with durable datasets.
        //
        // Finding these datasets is normally the responsibility of the planner,
        // but we're kinda hand-rolling it.
        let all_datasets = get_all_datasets_from_zones(&blueprint);

        // How many zones are there with durable datasets?
        let nzones_with_durable_datasets = all_datasets.len();
        assert!(nzones_with_durable_datasets > 0);

        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_dataset_records_exist(opctx, datastore, all_datasets.iter())
                .await
                .expect("failed to ensure datasets");

        // We should have inserted a dataset for each zone with a durable
        // dataset.
        assert_eq!(inserted, nzones_with_durable_datasets);
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, None)
                .await
                .unwrap()
                .len(),
            nzones_with_durable_datasets,
        );

        // Ensuring the same datasets again should insert no new records.
        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_dataset_records_exist(opctx, datastore, all_datasets.iter())
                .await
                .expect("failed to ensure datasets");
        assert_eq!(inserted, 0);
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, None)
                .await
                .unwrap()
                .len(),
            nzones_with_durable_datasets,
        );

        // Create another zpool on one of the sleds, so we can add new
        // zones that use it.
        let new_zpool_id = ZpoolUuid::new_v4();
        for &sled_id in collection.omicron_zones.keys().take(1) {
            let zpool = Zpool::new(
                new_zpool_id.into_untyped_uuid(),
                sled_id.into_untyped_uuid(),
                Uuid::new_v4(), // physical_disk_id
            );
            datastore
                .zpool_insert(opctx, zpool)
                .await
                .expect("failed to upsert zpool");
        }

        // Call `ensure_dataset_records_exist` again, adding new datasets.
        //
        // It should only insert these new zones.
        let new_zones = [
            BlueprintDatasetConfig {
                disposition: BlueprintDatasetDisposition::InService,
                id: DatasetUuid::new_v4(),
                pool: ZpoolName::new_external(new_zpool_id),
                kind: DatasetKind::Debug,
                address: None,
                quota: None,
                reservation: None,
                compression: String::new(),
            },
            BlueprintDatasetConfig {
                disposition: BlueprintDatasetDisposition::InService,
                id: DatasetUuid::new_v4(),
                pool: ZpoolName::new_external(new_zpool_id),
                kind: DatasetKind::ZoneRoot,
                address: None,
                quota: None,
                reservation: None,
                compression: String::new(),
            },
        ];

        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_dataset_records_exist(
                opctx,
                datastore,
                all_datasets.iter().chain(&new_zones),
            )
            .await
            .expect("failed to ensure datasets");
        assert_eq!(inserted, 2);
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, None)
                .await
                .unwrap()
                .len(),
            nzones_with_durable_datasets + 2,
        );
    }

    #[nexus_test]
    async fn test_dataset_records_update(cptestctx: &ControlPlaneTestContext) {
        const TEST_NAME: &str = "test_dataset_records_update";

        // Set up.
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let opctx = &opctx;

        // Use the standard example system.
        let (_, _, blueprint) = example(&opctx.log, TEST_NAME, 5);

        // Record the sleds and zpools.
        crate::tests::insert_sled_records(datastore, &blueprint).await;
        crate::tests::create_disks_for_zones_using_datasets(
            datastore, opctx, &blueprint,
        )
        .await;

        let mut all_datasets = get_all_datasets_from_zones(&blueprint);
        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_dataset_records_exist(opctx, datastore, all_datasets.iter())
                .await
                .expect("failed to ensure datasets");
        assert_eq!(inserted, all_datasets.len());
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);

        // These values don't *really* matter, we just want to make sure we can
        // change them and see the update.
        let first_dataset = &mut all_datasets[0];
        assert_eq!(first_dataset.quota, None);
        assert_eq!(first_dataset.reservation, None);
        assert_eq!(first_dataset.compression, "");

        first_dataset.quota = Some(ByteCount::from_kibibytes_u32(1));
        first_dataset.reservation = Some(ByteCount::from_kibibytes_u32(2));
        first_dataset.compression = String::from("lz4");
        let _ = first_dataset;

        // Update the datastore
        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_dataset_records_exist(opctx, datastore, all_datasets.iter())
                .await
                .expect("failed to ensure datasets");
        assert_eq!(inserted, 0);
        assert_eq!(updated, 1);
        assert_eq!(removed, 0);

        // Observe that the update stuck
        let observed_datasets =
            datastore.dataset_list_all_batched(opctx, None).await.unwrap();
        let first_dataset = &mut all_datasets[0];
        let observed_dataset = observed_datasets
            .into_iter()
            .find(|dataset| {
                dataset.id() == first_dataset.id.into_untyped_uuid()
            })
            .expect("Couldn't find dataset we tried to update?");
        let observed_dataset: DatasetConfig =
            observed_dataset.try_into().unwrap();
        assert_eq!(observed_dataset.quota, first_dataset.quota,);
        assert_eq!(observed_dataset.reservation, first_dataset.reservation,);
        assert_eq!(
            observed_dataset.compression.to_string(),
            first_dataset.compression,
        );
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
        let (_, _, blueprint) = example(&opctx.log, TEST_NAME, 5);

        // Record the sleds and zpools.
        crate::tests::insert_sled_records(datastore, &blueprint).await;
        crate::tests::create_disks_for_zones_using_datasets(
            datastore, opctx, &blueprint,
        )
        .await;

        let mut all_datasets = get_all_datasets_from_zones(&blueprint);

        // Ensure that a non-crucible dataset exists
        all_datasets.push(BlueprintDatasetConfig {
            disposition: BlueprintDatasetDisposition::InService,
            id: DatasetUuid::new_v4(),
            pool: all_datasets[0].pool.clone(),
            kind: DatasetKind::Debug,
            address: None,
            quota: None,
            reservation: None,
            compression: String::new(),
        });
        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_dataset_records_exist(opctx, datastore, all_datasets.iter())
                .await
                .expect("failed to ensure datasets");
        assert_eq!(inserted, all_datasets.len());
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);

        // Expunge two datasets -- one for Crucible, and one for any other
        // service.

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
        let _ = crucible_dataset;

        let non_crucible_dataset = all_datasets
            .iter_mut()
            .find(|dataset| !matches!(dataset.kind, DatasetKind::Crucible))
            .expect("No non-crucible dataset found");
        assert_eq!(
            non_crucible_dataset.disposition,
            BlueprintDatasetDisposition::InService
        );
        non_crucible_dataset.disposition =
            BlueprintDatasetDisposition::Expunged;
        let non_crucible_dataset_id = non_crucible_dataset.id;
        let _ = non_crucible_dataset;

        // Observe that we only remove one dataset.
        //
        // This is a property of "special-case handling" of the Crucible
        // dataset, where we punt the deletion to a background task.

        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_dataset_records_exist(opctx, datastore, all_datasets.iter())
                .await
                .expect("failed to ensure datasets");
        assert_eq!(inserted, 0);
        assert_eq!(updated, 0);
        assert_eq!(removed, 1);

        // Make sure the Crucible dataset still exists, even if the other
        // dataset got deleted.

        let observed_datasets =
            datastore.dataset_list_all_batched(opctx, None).await.unwrap();
        assert!(observed_datasets
            .iter()
            .any(|d| d.id() == crucible_dataset_id.into_untyped_uuid()));
        assert!(!observed_datasets
            .iter()
            .any(|d| d.id() == non_crucible_dataset_id.into_untyped_uuid()));
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
        let (_, _, blueprint) = example(&opctx.log, TEST_NAME, 5);

        // Record the sleds and zpools.
        crate::tests::insert_sled_records(datastore, &blueprint).await;
        crate::tests::create_disks_for_zones_using_datasets(
            datastore, opctx, &blueprint,
        )
        .await;

        let mut all_datasets = get_all_datasets_from_zones(&blueprint);

        // Ensure that a deletable dataset exists
        let dataset_id = DatasetUuid::new_v4();
        all_datasets.push(BlueprintDatasetConfig {
            disposition: BlueprintDatasetDisposition::InService,
            id: dataset_id,
            pool: all_datasets[0].pool.clone(),
            kind: DatasetKind::Debug,
            address: None,
            quota: None,
            reservation: None,
            compression: String::new(),
        });

        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_dataset_records_exist(opctx, datastore, all_datasets.iter())
                .await
                .expect("failed to ensure datasets");
        assert_eq!(inserted, all_datasets.len());
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);

        // Rather than expunging a dataset, which is the normal way to "delete"
        // a dataset, we'll just remove it from the "blueprint".
        //
        // This situation mimics a scenario where we are an "old Nexus,
        // executing and old blueprint" - more datasets might be created
        // concurrently with our execution, and we should leave them alone.
        assert_eq!(dataset_id, all_datasets.pop().unwrap().id);

        // Observe that no datasets are removed.
        let EnsureDatasetsResult { inserted, updated, removed } =
            ensure_dataset_records_exist(opctx, datastore, all_datasets.iter())
                .await
                .expect("failed to ensure datasets");
        assert_eq!(inserted, 0);
        assert_eq!(updated, 0);
        assert_eq!(removed, 0);

        // Make sure the dataset still exists, even if it isn't tracked by our
        // "blueprint".
        let observed_datasets =
            datastore.dataset_list_all_batched(opctx, None).await.unwrap();
        assert!(observed_datasets
            .iter()
            .any(|d| d.id() == dataset_id.into_untyped_uuid()));
    }
}
