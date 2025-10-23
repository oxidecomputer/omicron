// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Recording Crucible datasets in their rendezvous table

use anyhow::Context;
use anyhow::anyhow;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::model::CrucibleDataset;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::CrucibleDatasetsRendezvousStats;
use omicron_common::api::internal::shared::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use slog::info;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub(crate) async fn record_new_crucible_datasets(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint_datasets: impl Iterator<Item = &BlueprintDatasetConfig>,
    inventory_datasets: &BTreeSet<DatasetUuid>,
) -> anyhow::Result<CrucibleDatasetsRendezvousStats> {
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

    let mut stats = CrucibleDatasetsRendezvousStats::default();

    for bp_dataset in blueprint_datasets {
        // Filter down to Crucible datasets...
        let dataset = match (&bp_dataset.kind, bp_dataset.address) {
            (DatasetKind::Crucible, Some(addr)) => {
                CrucibleDataset::new(bp_dataset.id, bp_dataset.pool.id(), addr)
            }
            (DatasetKind::Crucible, None) => {
                // This should be impossible! Ideally we'd prevent it
                // statically, but for now just fail at runtime.
                return Err(anyhow!(
                    "invalid blueprint dataset: \
                     {} has kind Crucible but no address",
                    bp_dataset.id
                ));
            }
            _ => continue,
        };
        let id = dataset.id();

        // ... that are in service ...
        match bp_dataset.disposition {
            BlueprintDatasetDisposition::InService => (),
            BlueprintDatasetDisposition::Expunged => continue,
        }

        // ... and not already present in the database ...
        if existing_datasets.contains_key(&id) {
            stats.num_already_exist += 1;
            continue;
        }

        // ... and also present in inventory.
        if !inventory_datasets.contains(&id) {
            stats.num_not_in_inventory += 1;
            continue;
        }

        let did_insert = datastore
            .crucible_dataset_insert_if_not_exists(dataset)
            .await
            .with_context(|| {
                format!("failed to upsert dataset record for dataset {id}")
            })?
            .is_some();

        if did_insert {
            stats.num_inserted += 1;

            info!(
                opctx.log,
                "ensuring Crucible dataset record in database";
                "action" => "insert",
                "id" => %id,
            );
        } else {
            // This means we hit the TOCTOU race mentioned above: when we
            // queried the DB this row didn't exist, but another Nexus must have
            // beat us to actually inserting it.
            stats.num_already_exist += 1;
        }
    }

    info!(
        opctx.log,
        "ensured all Crucible datasets present in inventory have database \
         records";
        &stats,
    );

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::ArbitraryDisposition;
    use crate::tests::DatasetPrep;
    use crate::tests::usize_to_id;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use nexus_db_model::Generation;
    use nexus_db_model::SledBaseboard;
    use nexus_db_model::SledCpuFamily;
    use nexus_db_model::SledSystemHardware;
    use nexus_db_model::SledUpdate;
    use nexus_db_model::Zpool;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
    use nexus_types::inventory::ZpoolName;
    use omicron_common::api::external::ByteCount;
    use omicron_common::disk::CompressionAlgorithm;
    use omicron_test_utils::dev;

    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use proptest::prelude::*;
    use proptest::proptest;
    use uuid::Uuid;

    async fn proptest_do_prep(
        opctx: &OpContext,
        datastore: &DataStore,
        prep: &BTreeMap<usize, DatasetPrep>,
    ) -> (Vec<BlueprintDatasetConfig>, BTreeSet<DatasetUuid>) {
        let mut datasets = Vec::with_capacity(prep.len());
        let mut inventory = BTreeSet::new();

        // Clean up from any previous proptest cases
        {
            use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
            use nexus_db_schema::schema::sled::dsl as sled_dsl;
            use nexus_db_schema::schema::zpool::dsl as zpool_dsl;
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            datastore
                .transaction_non_retry_wrapper("proptest_prep_cleanup")
                .transaction(&conn, |conn| async move {
                    conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
                    diesel::delete(sled_dsl::sled).execute_async(&conn).await?;
                    diesel::delete(zpool_dsl::zpool)
                        .execute_async(&conn)
                        .await?;
                    diesel::delete(dataset_dsl::crucible_dataset)
                        .execute_async(&conn)
                        .await?;
                    Ok::<_, diesel::result::Error>(())
                })
                .await
                .unwrap();
        }

        for (&id, prep) in prep {
            let dataset_id: DatasetUuid = usize_to_id(id);
            let zpool_id: ZpoolUuid = usize_to_id(10000 + id);
            let sled_id: SledUuid = usize_to_id(20000 + id);
            let disk_id: PhysicalDiskUuid = usize_to_id(30000 + id);

            // Ensure the sled and zpool this dataset claim to be on exist in
            // the DB.
            datastore
                .sled_upsert(SledUpdate::new(
                    sled_id,
                    "[::1]:0".parse().unwrap(),
                    0,
                    SledBaseboard {
                        serial_number: format!("test-sn-{id}"),
                        part_number: "test-pn".to_string(),
                        revision: 0,
                    },
                    SledSystemHardware {
                        is_scrimlet: false,
                        usable_hardware_threads: 128,
                        usable_physical_ram: (64 << 30).try_into().unwrap(),
                        reservoir_size: (16 << 30).try_into().unwrap(),
                        cpu_family: SledCpuFamily::Unknown,
                    },
                    Uuid::new_v4(),
                    Generation::new(),
                ))
                .await
                .expect("should have inserted sled");
            datastore
                .zpool_insert(
                    opctx,
                    Zpool::new(
                        zpool_id,
                        sled_id,
                        disk_id,
                        ByteCount::from(0).into(),
                    ),
                )
                .await
                .expect("should have inserted zpool backing dataset");

            // Create the blueprint version of this proptest dataset, then add
            // it to "inventory" and/or insert it into the database.
            let d = BlueprintDatasetConfig {
                disposition: prep.disposition.into(),
                id: dataset_id,
                pool: ZpoolName::new_external(zpool_id),
                kind: DatasetKind::Crucible,
                address: Some("[::1]:0".parse().unwrap()),
                quota: None,
                reservation: None,
                compression: CompressionAlgorithm::Off,
            };

            if prep.in_inventory {
                inventory.insert(d.id);
            }

            if prep.in_database {
                datastore
                    .crucible_dataset_insert_if_not_exists(
                        CrucibleDataset::new(
                            d.id,
                            d.pool.id(),
                            d.address.unwrap(),
                        ),
                    )
                    .await
                    .expect("query should have succeeded")
                    .expect("should have inserted dataset");
            }

            datasets.push(d);
        }

        (datasets, inventory)
    }

    #[test]
    fn proptest_reconciliation() {
        // We create our own runtime so we can interleave expensive async code
        // (setting up a datastore) with a proptest that itself runs some async
        // code (querying the datastore).
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .enable_io()
            .build()
            .expect("tokio Runtime built successfully");

        let logctx = dev::test_setup_log("tombstone");
        let db =
            runtime.block_on(TestDatabase::new_with_datastore(&logctx.log));
        let (opctx, datastore) = (db.opctx(), db.datastore());

        proptest!(ProptestConfig::with_cases(32),
        |(prep in proptest::collection::vec(
            any::<DatasetPrep>(),
            0..20,
        ))| {
            let prep: BTreeMap<_, _> = prep.into_iter().enumerate().collect();
            let (result_stats, datastore_datasets) = runtime.block_on(async {
                let (blueprint_datasets, inventory_datasets) = proptest_do_prep(
                    opctx,
                    datastore,
                    &prep,
                ).await;

                let result_stats = record_new_crucible_datasets(
                    opctx,
                    datastore,
                    blueprint_datasets.iter(),
                    &inventory_datasets,
                )
                .await
                .expect("reconciled debug dataset");

                 let datastore_datasets = datastore
                    .crucible_dataset_list_all_batched(opctx)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|d| (d.id(), d))
                    .collect::<BTreeMap<_, _>>();

                 (result_stats, datastore_datasets)
            });

            let mut expected_stats = CrucibleDatasetsRendezvousStats::default();

            for (id, prep) in prep {
                let id: DatasetUuid = usize_to_id(id);

                let in_db_before = prep.in_database;
                let in_db_after = datastore_datasets.contains_key(&id);
                let in_service =
                    prep.disposition == ArbitraryDisposition::InService;
                let in_inventory = prep.in_inventory;

                // Validate rendezvous output
                match (in_db_before, in_service, in_inventory) {
                    // Datasets not in service should be skipped entirely.
                    (_, false, _) => (),
                    // In service but already existed
                    (true, true, _) => {
                        expected_stats.num_already_exist += 1;
                    }
                    // In service, not in db yet, but not in inventory
                    (false, true, false) => {
                        expected_stats.num_not_in_inventory += 1;
                    }
                    // In service, not in db yet, present in inventory
                    (false, true, true) => {
                        expected_stats.num_inserted += 1;
                    }
                }

                // Validate database state
                match (in_db_before, in_service, in_inventory) {
                    // We only add rows; if this dataset was present before, it
                    // should still be after we've executed.
                    (true, _, _) => {
                        assert!(
                            in_db_after,
                            "existing dataset missing from database: \
                            {id}, {prep:?}"
                        );
                    }
                    // If it wasn't in service, we shouldn't have inserted it.
                    (false, false, _) => {
                        assert!(
                            !in_db_after,
                            "expunged dataset inserted: {id}, {prep:?}"
                        );
                    }
                    // If it wasn't in inventory, we shouldn't have inserted it.
                    (false, true, false) => {
                        assert!(
                            !in_db_after,
                            "dataset inserted when not in inventory: \
                             {id}, {prep:?}"
                        );
                    }
                    // If it was in service and in inventory, we should have
                    // inserted it.
                    (false, true, true) => {
                        assert!(
                            in_db_after,
                            "new dataset missing from database: {id}, {prep:?}"
                        );
                    }
                }
            }

            assert_eq!(result_stats, expected_stats);
        });

        runtime.block_on(db.terminate());
        logctx.cleanup_successful();
    }
}
