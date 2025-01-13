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
    // newly-added or newly-expunged. For the newly-added case, we can first
    // fetch all the existing datasets and then avoid trying to spuriously
    // insert them again.
    //
    // This is a minor performance optimization. If we removed this fetch, the
    // code below would still be correct, but it would issue a bunch of
    // do-nothing inserts for already-existing datasets.
    let existing_datasets = datastore
        .debug_dataset_list_all_batched(opctx)
        .await
        .context("failed to list all debug datasets")?
        .into_iter()
        .map(|d| d.id())
        .collect::<BTreeSet<_>>();

    // We want to insert any in-service datasets (according to the blueprint)
    // that are also present in `inventory_datasets` but that are not already
    // present in the database (described by `existing_datasets`).
    let datasets_to_insert = inventory_datasets
        .difference(&existing_datasets)
        .collect::<BTreeSet<_>>();

    for dataset in blueprint_datasets.filter(|d| d.kind == DatasetKind::Debug) {
        match dataset.disposition {
            BlueprintDatasetDisposition::InService => {
                if datasets_to_insert.contains(&dataset.id) {
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
                // We don't have a way to short-circuit tombstoning, assuming we
                // don't want to query the db for "all tombstoned datasets"
                // first.
                //
                // We could do that or we could add a
                // `debug_dataset_tombstone_batched` that takes the entire set
                // of expunged datasets? Or we could just no-op tombstone every
                // expunged dataset on every execution like this.
                if datastore
                    .debug_dataset_tombstone(opctx, dataset.id)
                    .await
                    .with_context(|| {
                        format!("failed to tombstone dataset {}", dataset.id)
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
    use std::collections::BTreeMap;
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
