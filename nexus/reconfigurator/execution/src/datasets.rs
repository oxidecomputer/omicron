// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures dataset records required by a given blueprint

use anyhow::Context;
use nexus_db_model::Dataset;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::DurableDataset;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use slog::info;
use slog::warn;
use std::collections::BTreeSet;

/// For each zone in `all_omicron_zones` that has an associated durable dataset,
/// ensure that a corresponding dataset record exists in `datastore`.
///
/// Does not modify any existing dataset records. Returns the number of
/// datasets inserted.
pub(crate) async fn ensure_dataset_records_exist(
    opctx: &OpContext,
    datastore: &DataStore,
    all_omicron_zones: impl Iterator<Item = &BlueprintZoneConfig>,
) -> anyhow::Result<usize> {
    // Before attempting to insert any datasets, first query for any existing
    // dataset records so we can filter them out. This looks like a typical
    // TOCTOU issue, but it is purely a performance optimization. We expect
    // almost all executions of this function to do nothing: new datasets are
    // created very rarely relative to how frequently blueprint realization
    // happens. We could remove this check and filter and instead run the below
    // "insert if not exists" query on every zone, and the behavior would still
    // be correct. However, that would issue far more queries than necessary in
    // the very common case of "we don't need to do anything at all".
    let mut existing_datasets = datastore
        .dataset_list_all_batched(opctx, None)
        .await
        .context("failed to list all datasets")?
        .into_iter()
        .map(|dataset| OmicronZoneUuid::from_untyped_uuid(dataset.id()))
        .collect::<BTreeSet<_>>();

    let mut num_inserted = 0;
    let mut num_already_exist = 0;

    for zone in all_omicron_zones {
        let Some(DurableDataset { dataset, kind, address }) =
            zone.zone_type.durable_dataset()
        else {
            continue;
        };

        let id = zone.id;

        // If already present in the datastore, move on.
        if existing_datasets.remove(&id) {
            num_already_exist += 1;
            continue;
        }

        let pool_id = dataset.pool_name.id();
        let dataset = Dataset::new(
            id.into_untyped_uuid(),
            pool_id.into_untyped_uuid(),
            Some(address),
            kind.clone(),
        );
        let maybe_inserted = datastore
            .dataset_insert_if_not_exists(dataset)
            .await
            .with_context(|| {
                format!("failed to insert dataset record for dataset {id}")
            })?;

        // If we succeeded in inserting, log it; if `maybe_dataset` is `None`,
        // we must have lost the TOCTOU race described above, and another Nexus
        // must have inserted this dataset before we could.
        if maybe_inserted.is_some() {
            info!(
                opctx.log,
                "inserted new dataset for Omicron zone";
                "id" => %id,
                "kind" => ?kind,
            );
            num_inserted += 1;
        } else {
            num_already_exist += 1;
        }
    }

    // We don't currently support removing datasets, so this would be
    // surprising: the database contains dataset records that are no longer in
    // our blueprint. We can't do anything about this, so just warn.
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
        "ensured all Omicron zones have dataset records";
        "num_inserted" => num_inserted,
        "num_already_existed" => num_already_exist,
    );

    Ok(num_inserted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_model::Zpool;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::deployment::BlueprintZoneType;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    #[nexus_test]
    async fn test_ensure_dataset_records_exist(
        cptestctx: &ControlPlaneTestContext,
    ) {
        const TEST_NAME: &str = "test_ensure_dataset_records_exist";

        // Set up.
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let opctx = &opctx;

        // Use the standard example system.
        let (example, blueprint) =
            ExampleSystemBuilder::new(&opctx.log, TEST_NAME).nsleds(5).build();
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
                .dataset_list_all_batched(opctx, None)
                .await
                .unwrap()
                .len(),
            0
        );

        // Collect all the blueprint zones.
        let all_omicron_zones = blueprint
            .all_omicron_zones(BlueprintZoneFilter::All)
            .map(|(_, zone)| zone)
            .collect::<Vec<_>>();

        // How many zones are there with durable datasets?
        let nzones_with_durable_datasets = all_omicron_zones
            .iter()
            .filter(|z| z.zone_type.durable_dataset().is_some())
            .count();

        let ndatasets_inserted = ensure_dataset_records_exist(
            opctx,
            datastore,
            all_omicron_zones.iter().copied(),
        )
        .await
        .expect("failed to ensure datasets");

        // We should have inserted a dataset for each zone with a durable
        // dataset.
        assert_eq!(nzones_with_durable_datasets, ndatasets_inserted);
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, None)
                .await
                .unwrap()
                .len(),
            nzones_with_durable_datasets,
        );

        // Ensuring the same datasets again should insert no new records.
        let ndatasets_inserted = ensure_dataset_records_exist(
            opctx,
            datastore,
            all_omicron_zones.iter().copied(),
        )
        .await
        .expect("failed to ensure datasets");
        assert_eq!(0, ndatasets_inserted);
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

        // Call `ensure_dataset_records_exist` again, adding new crucible and
        // cockroach zones. It should insert only these new zones.
        let new_zones = [
            BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: OmicronZoneUuid::new_v4(),
                filesystem_pool: Some(ZpoolName::new_external(new_zpool_id)),
                zone_type: BlueprintZoneType::Crucible(
                    blueprint_zone_type::Crucible {
                        address: "[::1]:0".parse().unwrap(),
                        dataset: OmicronZoneDataset {
                            pool_name: ZpoolName::new_external(new_zpool_id),
                        },
                    },
                ),
            },
            BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: OmicronZoneUuid::new_v4(),
                filesystem_pool: Some(ZpoolName::new_external(new_zpool_id)),
                zone_type: BlueprintZoneType::CockroachDb(
                    blueprint_zone_type::CockroachDb {
                        address: "[::1]:0".parse().unwrap(),
                        dataset: OmicronZoneDataset {
                            pool_name: ZpoolName::new_external(new_zpool_id),
                        },
                    },
                ),
            },
        ];
        let ndatasets_inserted = ensure_dataset_records_exist(
            opctx,
            datastore,
            all_omicron_zones.iter().copied().chain(&new_zones),
        )
        .await
        .expect("failed to ensure datasets");
        assert_eq!(ndatasets_inserted, 2);
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, None)
                .await
                .unwrap()
                .len(),
            nzones_with_durable_datasets + 2,
        );
    }
}
