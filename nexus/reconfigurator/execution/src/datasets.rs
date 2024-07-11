// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures dataset records required by a given blueprint

use anyhow::Context;
use nexus_db_model::Dataset;
use nexus_db_model::DatasetKind;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use slog::info;
use slog::warn;
use std::collections::BTreeSet;

/// For each crucible zone in `all_omicron_zones`, ensure that a corresponding
/// dataset record exists in `datastore`
///
/// Does not modify any existing dataset records. Returns the number of
/// datasets inserted.
pub(crate) async fn ensure_crucible_dataset_records_exist(
    opctx: &OpContext,
    datastore: &DataStore,
    all_omicron_zones: impl Iterator<Item = &BlueprintZoneConfig>,
) -> anyhow::Result<usize> {
    // Before attempting to insert any datasets, first query for any existing
    // dataset records so we can filter them out. This looks like a typical
    // TOCTOU issue, but it is purely a performance optimization. We expect
    // almost all executions of this function to do nothing: new crucible
    // datasets are created very rarely relative to how frequently blueprint
    // realization happens. We could remove this check and filter and instead
    // run the below "insert if not exists" query on every crucible zone, and
    // the behavior would still be correct. However, that would issue far more
    // queries than necessary in the very common case of "we don't need to do
    // anything at all".
    let mut crucible_datasets = datastore
        .dataset_list_all_batched(opctx, Some(DatasetKind::Crucible))
        .await
        .context("failed to list all datasets")?
        .into_iter()
        .map(|dataset| OmicronZoneUuid::from_untyped_uuid(dataset.id()))
        .collect::<BTreeSet<_>>();

    let mut num_inserted = 0;
    let mut num_already_exist = 0;

    for zone in all_omicron_zones {
        let BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
            address,
            dataset,
        }) = &zone.zone_type
        else {
            continue;
        };

        let id = zone.id;

        // If already present in the datastore, move on.
        if crucible_datasets.remove(&id) {
            num_already_exist += 1;
            continue;
        }

        let pool_id = dataset.pool_name.id();
        let dataset = Dataset::new(
            id.into_untyped_uuid(),
            pool_id.into_untyped_uuid(),
            Some(*address),
            DatasetKind::Crucible,
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
                "inserted new dataset for crucible zone";
                "id" => %id,
            );
            num_inserted += 1;
        } else {
            num_already_exist += 1;
        }
    }

    // We don't currently support removing datasets, so this would be
    // surprising: the database contains dataset records that are no longer in
    // our blueprint. We can't do anything about this, so just warn.
    if !crucible_datasets.is_empty() {
        warn!(
            opctx.log,
            "database contains {} unexpected crucible datasets",
            crucible_datasets.len();
            "dataset_ids" => ?crucible_datasets,
        );
    }

    info!(
        opctx.log,
        "ensured all crucible zones have dataset records";
        "num_inserted" => num_inserted,
        "num_already_existed" => num_already_exist,
    );

    Ok(num_inserted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_model::Generation;
    use nexus_db_model::SledBaseboard;
    use nexus_db_model::SledSystemHardware;
    use nexus_db_model::SledUpdate;
    use nexus_db_model::Zpool;
    use nexus_reconfigurator_planning::example::example;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneFilter;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_agent_client::types::OmicronZoneDataset;
    use sled_agent_client::types::OmicronZoneType;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    #[nexus_test]
    async fn test_ensure_crucible_dataset_records_exist(
        cptestctx: &ControlPlaneTestContext,
    ) {
        const TEST_NAME: &str = "test_ensure_crucible_dataset_records_exist";

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

        // Record the sleds and zpools contained in this collection.
        let rack_id = Uuid::new_v4();
        for (&sled_id, config) in &collection.omicron_zones {
            let sled = SledUpdate::new(
                sled_id.into_untyped_uuid(),
                "[::1]:0".parse().unwrap(),
                SledBaseboard {
                    serial_number: format!("test-{sled_id}"),
                    part_number: "test-sled".to_string(),
                    revision: 0,
                },
                SledSystemHardware {
                    is_scrimlet: false,
                    usable_hardware_threads: 128,
                    usable_physical_ram: (64 << 30).try_into().unwrap(),
                    reservoir_size: (16 << 30).try_into().unwrap(),
                },
                rack_id,
                Generation::new(),
            );
            datastore.sled_upsert(sled).await.expect("failed to upsert sled");

            for zone in &config.zones.zones {
                let OmicronZoneType::Crucible { dataset, .. } = &zone.zone_type
                else {
                    continue;
                };
                let zpool = Zpool::new(
                    dataset.pool_name.id().into_untyped_uuid(),
                    sled_id.into_untyped_uuid(),
                    Uuid::new_v4(), // physical_disk_id
                );
                datastore
                    .zpool_insert(opctx, zpool)
                    .await
                    .expect("failed to upsert zpool");
            }
        }

        // How many crucible zones are there?
        let ncrucible_zones = collection
            .all_omicron_zones()
            .filter(|z| matches!(z.zone_type, OmicronZoneType::Crucible { .. }))
            .count();

        // Prior to ensuring datasets exist, there should be none.
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, Some(DatasetKind::Crucible))
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

        let ndatasets_inserted = ensure_crucible_dataset_records_exist(
            opctx,
            datastore,
            all_omicron_zones.iter().copied(),
        )
        .await
        .expect("failed to ensure crucible datasets");

        // We should have inserted a dataset for each crucible zone.
        assert_eq!(ncrucible_zones, ndatasets_inserted);
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, Some(DatasetKind::Crucible))
                .await
                .unwrap()
                .len(),
            ncrucible_zones,
        );

        // Ensuring the same crucible datasets again should insert no new
        // records.
        let ndatasets_inserted = ensure_crucible_dataset_records_exist(
            opctx,
            datastore,
            all_omicron_zones.iter().copied(),
        )
        .await
        .expect("failed to ensure crucible datasets");
        assert_eq!(0, ndatasets_inserted);
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, Some(DatasetKind::Crucible))
                .await
                .unwrap()
                .len(),
            ncrucible_zones,
        );

        // Create another zpool on one of the sleds, so we can add a new
        // crucible zone that uses it.
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

        // Call `ensure_crucible_dataset_records_exist` again, adding a new
        // crucible zone. It should insert only this new zone.
        let new_zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: OmicronZoneUuid::new_v4(),
            underlay_address: "::1".parse().unwrap(),
            filesystem_pool: Some(ZpoolName::new_external(new_zpool_id)),
            zone_type: BlueprintZoneType::Crucible(
                blueprint_zone_type::Crucible {
                    address: "[::1]:0".parse().unwrap(),
                    dataset: OmicronZoneDataset {
                        pool_name: ZpoolName::new_external(new_zpool_id),
                    },
                },
            ),
        };
        let ndatasets_inserted = ensure_crucible_dataset_records_exist(
            opctx,
            datastore,
            all_omicron_zones.iter().copied().chain(std::iter::once(&new_zone)),
        )
        .await
        .expect("failed to ensure crucible datasets");
        assert_eq!(ndatasets_inserted, 1);
        assert_eq!(
            datastore
                .dataset_list_all_batched(opctx, Some(DatasetKind::Crucible))
                .await
                .unwrap()
                .len(),
            ncrucible_zones + 1,
        );
    }
}
