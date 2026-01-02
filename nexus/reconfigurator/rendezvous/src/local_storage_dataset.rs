// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Recording Local storage datasets in their rendezvous table

use anyhow::Context;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::model::RendezvousLocalStorageDataset;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::internal_api::background::DatasetsRendezvousStats;
use omicron_common::api::internal::shared::DatasetKind;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetUuid;
use slog::info;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub(crate) async fn reconcile_local_storage_datasets(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint_id: BlueprintUuid,
    blueprint_datasets: impl Iterator<Item = &BlueprintDatasetConfig>,
    inventory_datasets: &BTreeSet<DatasetUuid>,
) -> anyhow::Result<DatasetsRendezvousStats> {
    // We expect basically all executions of this task to do nothing: we're
    // activated periodically, and only do work when a dataset has been
    // newly-added or newly-expunged.
    //
    // This is a performance optimization. If we removed this fetch, the code
    // below would still be correct, but it would issue a bunch of do-nothing
    // queries for every individual dataset in `blueprint_datasets`.
    let existing_datasets = datastore
        .local_storage_dataset_list_all_batched(opctx)
        .await
        .context("failed to list all local storage datasets")?
        .into_iter()
        .map(|dataset| (dataset.id(), dataset))
        .collect::<BTreeMap<DatasetUuid, _>>();

    let mut stats = DatasetsRendezvousStats::default();

    for bp_dataset in blueprint_datasets {
        // Filter down to LocalStorage datasets...
        let dataset = match (&bp_dataset.kind, bp_dataset.address) {
            (DatasetKind::LocalStorage, None) => {
                RendezvousLocalStorageDataset::new(
                    bp_dataset.id,
                    bp_dataset.pool.id(),
                    blueprint_id,
                )
            }
            _ => continue,
        };
        let id = dataset.id();

        match bp_dataset.disposition {
            // Add any datasets that are in-service
            BlueprintDatasetDisposition::InService => {
                // Skip adding if already in the database
                if existing_datasets.contains_key(&id) {
                    stats.num_already_exist += 1;
                    continue;
                }

                // Skip adding if not in the inventory
                if !inventory_datasets.contains(&id) {
                    stats.num_not_in_inventory += 1;
                    continue;
                }

                let did_insert = datastore
                    .local_storage_dataset_insert_if_not_exists(opctx, dataset)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to upsert dataset record for dataset \
                        {id}"
                        )
                    })?
                    .is_some();

                if did_insert {
                    stats.num_inserted += 1;

                    info!(
                        opctx.log,
                        "inserted LocalStorage dataset record in database";
                        "action" => "insert",
                        "id" => %id,
                    );
                } else {
                    // This means we hit the TOCTOU race mentioned above: when
                    // we queried the DB this row didn't exist, but another
                    // Nexus must have beat us to actually inserting it.
                    stats.num_already_exist += 1;
                }
            }

            // Tombstone any datasets that are expunged
            BlueprintDatasetDisposition::Expunged => {
                // Only attempt to tombstone this dataset if it isn't already
                // marked as tombstoned in the database.
                let already_tombstoned = existing_datasets
                    .get(&dataset.id())
                    .map(|d| d.is_tombstoned())
                    .unwrap_or(false);

                if already_tombstoned {
                    stats.num_already_tombstoned += 1;
                } else {
                    if datastore
                        .local_storage_dataset_tombstone(
                            opctx,
                            dataset.id(),
                            blueprint_id,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "failed to tombstone dataset {}",
                                dataset.id()
                            )
                        })?
                    {
                        stats.num_tombstoned += 1;
                        info!(
                            opctx.log,
                            "tombstoned expunged local storage dataset";
                            "dataset_id" => %dataset.id(),
                        );
                    } else {
                        // Similar TOCTOU race lost as above; this dataset was
                        // either already tombstoned by another racing Nexus, or
                        // has been hard deleted.
                        stats.num_already_tombstoned += 1;
                    }
                }
            }
        }
    }

    info!(
        opctx.log,
        "all LocalStorage datasets reconciled";
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
        blueprint_id: BlueprintUuid,
        prep: &BTreeMap<usize, DatasetPrep>,
    ) -> (Vec<BlueprintDatasetConfig>, BTreeSet<DatasetUuid>) {
        let mut datasets = Vec::with_capacity(prep.len());
        let mut inventory = BTreeSet::new();

        // Clean up from any previous proptest cases
        {
            use nexus_db_schema::schema::rendezvous_local_storage_dataset::dsl;
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
                    diesel::delete(dsl::rendezvous_local_storage_dataset)
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
                kind: DatasetKind::LocalStorage,
                address: None,
                quota: None,
                reservation: None,
                compression: CompressionAlgorithm::Off,
            };

            if prep.in_inventory {
                inventory.insert(d.id);
            }

            if prep.in_database {
                datastore
                    .local_storage_dataset_insert_if_not_exists(
                        opctx,
                        RendezvousLocalStorageDataset::new(
                            d.id,
                            d.pool.id(),
                            blueprint_id,
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
            let blueprint_id = BlueprintUuid::new_v4();

            let (result_stats, datastore_datasets) = runtime.block_on(async {
                let (blueprint_datasets, inventory_datasets) = proptest_do_prep(
                    opctx,
                    datastore,
                    blueprint_id,
                    &prep,
                ).await;

                let result_stats = reconcile_local_storage_datasets(
                    opctx,
                    datastore,
                    blueprint_id,
                    blueprint_datasets.iter(),
                    &inventory_datasets,
                )
                .await
                .expect("reconciled debug dataset");

                 let datastore_datasets = datastore
                    .local_storage_dataset_list_all_batched(opctx)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|d| (d.id(), d))
                    .collect::<BTreeMap<_, _>>();

                 (result_stats, datastore_datasets)
            });

            let mut expected_stats = DatasetsRendezvousStats::default();

            for (id, prep) in prep {
                let id: DatasetUuid = usize_to_id(id);

                let in_db_before = prep.in_database;
                let in_db_tombstoned = datastore_datasets
                    .get(&id)
                    .map(|d| d.is_tombstoned());
                let in_db_after = datastore_datasets.contains_key(&id);
                let in_service =
                    prep.disposition == ArbitraryDisposition::InService;
                let in_inventory = prep.in_inventory;

                // Validate rendezvous output
                match (in_db_before, in_service, in_inventory) {
                    // "Not in database and expunged" is consistent with "hard
                    // deleted", which we can't separate from "already
                    // tombstoned".
                    (false, false, _) => {
                        expected_stats.num_already_tombstoned += 1;
                    }
                    // "In database and expunged" should result in tombstoning.
                    (true, false, _) => {
                        expected_stats.num_tombstoned += 1;
                    }
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
                    // Wasn't in DB, isn't in service: should still not be in db
                    (false, false, _) => {
                        assert!(
                            !in_db_after,
                            "expunged dataset inserted: {id}, {prep:?}",
                        );
                    }
                    // Wasn't in DB, isn't in inventory: should still not be in
                    // db
                    (false, true, false) => {
                        assert!(
                            !in_db_after,
                            "dataset inserted but not in inventory: \
                             {id}, {prep:?}",
                        );
                    }
                    // Was in DB, expunged: should be in the DB and tombstoned
                    (true, false, _) => {
                        assert_eq!(
                            in_db_tombstoned, Some(true),
                            "expunged dataset should be tombstoned: \
                             {id}, {prep:?}",
                        );
                    }
                    // Wasn't in DB, in-service, and in inventory: should have
                    // been added to the DB and not tombstoned
                    (false, true, true) => {
                        assert_eq!(
                            in_db_tombstoned, Some(false),
                            "in-service dataset should have been inserted: \
                             {id}, {prep:?}",
                        );
                    }
                    // Was in DB, in-service: should still be in the DB, not
                    // tombstoned, regardless of inventory presence
                    (true, true, _) => {
                        assert_eq!(
                            in_db_tombstoned, Some(false),
                            "in-service dataset should not be tombstoned: \
                             {id}, {prep:?}",
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
