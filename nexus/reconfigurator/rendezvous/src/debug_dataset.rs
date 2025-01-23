// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Recording debug datasets in their rendezvous table

use anyhow::Context;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::model::RendezvousDebugDataset;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use omicron_common::api::internal::shared::DatasetKind;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetUuid;
use slog::info;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub(crate) async fn reconcile_debug_datasets(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint_id: BlueprintUuid,
    blueprint_datasets: impl Iterator<Item = &BlueprintDatasetConfig>,
    inventory_datasets: &BTreeSet<DatasetUuid>,
) -> anyhow::Result<()> {
    // We expect basically all executions of this task to do nothing: we're
    // activated periodically, and only do work when a dataset has been
    // newly-added or newly-expunged.
    //
    // This is a performance optimization. If we removed this fetch, the code
    // below would still be correct, but it would issue a bunch of do-nothing
    // queries for every individual dataset in `blueprint_datasets`.
    let existing_db_datasets = datastore
        .debug_dataset_list_all_batched(opctx)
        .await
        .context("failed to list all debug datasets")?
        .into_iter()
        .map(|d| (d.id(), d))
        .collect::<BTreeMap<_, _>>();

    for dataset in blueprint_datasets.filter(|d| d.kind == DatasetKind::Debug) {
        match dataset.disposition {
            BlueprintDatasetDisposition::InService => {
                // Only attempt to insert this dataset if it has shown up in
                // inventory (required for correctness) and isn't already
                // present in the db (performance optimization only). Inserting
                // an already-present row is a no-op, so it's safe to skip.
                if inventory_datasets.contains(&dataset.id)
                    && !existing_db_datasets.contains_key(&dataset.id)
                {
                    let db_dataset = RendezvousDebugDataset::new(
                        dataset.id,
                        dataset.pool.id(),
                        blueprint_id,
                    );
                    datastore
                        .debug_dataset_insert_if_not_exists(opctx, db_dataset)
                        .await
                        .with_context(|| {
                            format!("failed to insert dataset {}", dataset.id)
                        })?;
                }
            }
            BlueprintDatasetDisposition::Expunged => {
                // Only attempt to tombstone this dataset if it isn't already
                // marked as tombstoned in the database.
                //
                // The `.unwrap_or(false)` means we'll attempt to tombstone a
                // row even if it wasn't present in the db at all when we
                // queried it above. This is _probably_ unnecessary (tombstoning
                // a nonexistent row is a no-op), but I'm not positive there
                // isn't a case where we might be operating on a blueprint
                // simultaneously to some other Nexus operating on an older
                // blueprint/inventory where it might insert this row after we
                // queried above and before we tombstone below. It seems safer
                // to issue a probably-do-nothing query than to _not_ issue a
                // probably-do-nothing-but-might-do-something-if-I'm-wrong
                // query.
                let already_tombstoned = existing_db_datasets
                    .get(&dataset.id)
                    .map(|d| d.is_tombstoned())
                    .unwrap_or(false);
                if !already_tombstoned {
                    if datastore
                        .debug_dataset_tombstone(
                            opctx,
                            dataset.id,
                            blueprint_id,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "failed to tombstone dataset {}",
                                dataset.id
                            )
                        })?
                    {
                        info!(
                            opctx.log, "tombstoned expunged dataset";
                            "dataset_id" => %dataset.id,
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
    use nexus_types::inventory::ZpoolName;
    use omicron_common::disk::CompressionAlgorithm;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{GenericUuid, TypedUuid, TypedUuidKind};
    use proptest::prelude::*;
    use proptest::proptest;
    use test_strategy::Arbitrary;
    use uuid::Uuid;

    // Helpers to describe how a dataset should be prepared for the proptest
    // below.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Arbitrary)]
    enum ArbitraryDisposition {
        InService,
        Expunged,
    }

    impl From<ArbitraryDisposition> for BlueprintDatasetDisposition {
        fn from(value: ArbitraryDisposition) -> Self {
            match value {
                ArbitraryDisposition::InService => Self::InService,
                ArbitraryDisposition::Expunged => Self::Expunged,
            }
        }
    }

    #[derive(Debug, Clone, Copy, Arbitrary)]
    struct DatasetPrep {
        disposition: ArbitraryDisposition,
        in_inventory: bool,
        in_database: bool,
    }

    fn u32_to_id<T: TypedUuidKind>(n: u32) -> TypedUuid<T> {
        let untyped = Uuid::from_u128(u128::from(n));
        TypedUuid::from_untyped_uuid(untyped)
    }

    async fn proptest_do_prep(
        opctx: &OpContext,
        datastore: &DataStore,
        blueprint_id: BlueprintUuid,
        prep: &BTreeMap<u32, DatasetPrep>,
    ) -> (Vec<BlueprintDatasetConfig>, BTreeSet<DatasetUuid>) {
        let mut datasets = Vec::with_capacity(prep.len());
        let mut inventory = BTreeSet::new();

        // Clean up from any previous proptest cases
        {
            use nexus_db_model::schema::rendezvous_debug_dataset::dsl;
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            datastore
                .transaction_non_retry_wrapper("proptest_prep_cleanup")
                .transaction(&conn, |conn| async move {
                    conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
                    diesel::delete(dsl::rendezvous_debug_dataset)
                        .execute_async(&conn)
                        .await?;
                    Ok::<_, diesel::result::Error>(())
                })
                .await
                .unwrap();
        }

        for (&id, prep) in prep {
            let d = BlueprintDatasetConfig {
                disposition: prep.disposition.into(),
                id: u32_to_id(id),
                pool: ZpoolName::new_external(u32_to_id(id)),
                kind: DatasetKind::Debug,
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
                    .debug_dataset_insert_if_not_exists(
                        opctx,
                        RendezvousDebugDataset::new(
                            d.id,
                            d.pool.id(),
                            blueprint_id,
                        ),
                    )
                    .await
                    .expect("query succeeded")
                    .expect("inserted dataset");
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

        proptest!(ProptestConfig::with_cases(64),
        |(prep in proptest::collection::btree_map(
            any::<u32>(),
            any::<DatasetPrep>(),
            0..20,
        ))| {
            let blueprint_id = BlueprintUuid::new_v4();

            let datastore_datasets = runtime.block_on(async {
                let (blueprint_datasets, inventory_datasets) = proptest_do_prep(
                    opctx,
                    datastore,
                    blueprint_id,
                    &prep,
                ).await;

                reconcile_debug_datasets(
                    opctx,
                    datastore,
                    blueprint_id,
                    blueprint_datasets.iter(),
                    &inventory_datasets,
                )
                .await
                .expect("reconciled debug dataset");

                 datastore
                    .debug_dataset_list_all_batched(opctx)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|d| (d.id(), d))
                    .collect::<BTreeMap<_, _>>()
            });

            for (id, prep) in prep {
                let id: DatasetUuid = u32_to_id(id);

                // If the dataset wasn't in the database already, we should not
                // have inserted it if either:
                //
                // * it wasn't present in inventory
                // * it was expunged (tombstoning an already-hard-deleted
                //   dataset is a no-op)
                if !prep.in_database && (!prep.in_inventory
                    || prep.disposition == ArbitraryDisposition::Expunged
                ){
                    assert!(
                        !datastore_datasets.contains_key(&id),
                        "unexpected dataset present in database: \
                        {id}, {prep:?}"
                    );
                    continue;
                }

                // Otherwise, we should have either inserted or attempted to
                // update the record. Get it from the datastore and confirm that
                // its tombstoned bit is correct based on the blueprint
                // disposition.
                let db = datastore_datasets
                    .get(&id)
                    .expect("missing entry in database");
                match prep.disposition {
                    ArbitraryDisposition::InService => {
                        assert!(!db.is_tombstoned());
                    }
                    ArbitraryDisposition::Expunged => {
                        assert!(db.is_tombstoned())
                    }
                }
            }
        });

        runtime.block_on(db.terminate());
        logctx.cleanup_successful();
    }
}
