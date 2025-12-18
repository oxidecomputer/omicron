// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages deployment of Omicron physical disks to Sled Agents.

use anyhow::anyhow;
use futures::StreamExt;
use futures::stream;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use slog::info;
use slog::warn;

/// Decommissions all disks which are currently expunged.
pub(crate) async fn decommission_expunged_disks(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
) -> Result<(), Vec<anyhow::Error>> {
    decommission_expunged_disks_impl(
        opctx,
        datastore,
        blueprint
            .all_omicron_disks(
                BlueprintPhysicalDiskDisposition::is_ready_for_cleanup,
            )
            .map(|(sled_id, config)| (sled_id, config.id)),
    )
    .await
}

async fn decommission_expunged_disks_impl(
    opctx: &OpContext,
    datastore: &DataStore,
    expunged_disks: impl Iterator<Item = (SledUuid, PhysicalDiskUuid)>,
) -> Result<(), Vec<anyhow::Error>> {
    let errors: Vec<anyhow::Error> = stream::iter(expunged_disks)
        .filter_map(async |(sled_id, disk_id)| {
            let log = opctx.log.new(slog::o!(
                "sled_id" => sled_id.to_string(),
                "disk_id" => disk_id.to_string(),
            ));

            match datastore.physical_disk_decommission(&opctx, disk_id).await {
                Err(error) => {
                    warn!(
                        log,
                        "failed to decommission expunged disk";
                        "error" => #%error
                    );
                    Some(anyhow!(error).context(format!(
                        "failed to decommission: disk_id = {disk_id}",
                    )))
                }
                Ok(()) => {
                    info!(log, "successfully decommissioned expunged disk");
                    None
                }
            }
        })
        .collect()
        .await;

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

#[cfg(test)]
mod test {
    use crate::DataStore;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use nexus_db_model::CrucibleDataset;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::PhysicalDiskState;
    use nexus_db_model::Region;
    use nexus_db_model::RendezvousLocalStorageDataset;
    use nexus_db_model::Zpool;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::SLED_AGENT_UUID;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::DiskFilter;
    use nexus_types::identity::Asset;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::DataPageParams;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::VolumeUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

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

    async fn add_zpool_dataset_and_region(
        datastore: &DataStore,
        opctx: &OpContext,
        id: PhysicalDiskUuid,
        sled_id: SledUuid,
    ) {
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
            .crucible_dataset_upsert(CrucibleDataset::new(
                DatasetUuid::new_v4(),
                zpool.id(),
                std::net::SocketAddrV6::new(
                    std::net::Ipv6Addr::LOCALHOST,
                    0,
                    0,
                    0,
                ),
            ))
            .await
            .unwrap();

        // There isn't a great API to insert regions (we normally allocate!)
        // so insert the record manually here.
        let region = {
            let volume_id = VolumeUuid::new_v4();
            Region::new(
                dataset.id(),
                volume_id,
                512_i64.try_into().unwrap(),
                10,
                10,
                1,
                false,
            )
        };
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        use nexus_db_schema::schema::region::dsl;
        diesel::insert_into(dsl::region)
            .values(region)
            .execute_async(&*conn)
            .await
            .unwrap();

        datastore
            .local_storage_dataset_insert_if_not_exists(
                opctx,
                RendezvousLocalStorageDataset::new(
                    DatasetUuid::new_v4(),
                    zpool.id(),
                    BlueprintUuid::new_v4(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
    }

    async fn get_pools(
        datastore: &DataStore,
        id: PhysicalDiskUuid,
    ) -> Vec<ZpoolUuid> {
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        use nexus_db_schema::schema::zpool::dsl;
        dsl::zpool
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::physical_disk_id.eq(id.into_untyped_uuid()))
            .select(dsl::id)
            .load_async::<Uuid>(&*conn)
            .await
            .map(|ids| {
                ids.into_iter()
                    .map(|id| ZpoolUuid::from_untyped_uuid(id))
                    .collect()
            })
            .unwrap()
    }

    async fn get_crucible_datasets(
        datastore: &DataStore,
        id: ZpoolUuid,
    ) -> Vec<Uuid> {
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        use nexus_db_schema::schema::crucible_dataset::dsl;
        dsl::crucible_dataset
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::pool_id.eq(id.into_untyped_uuid()))
            .select(dsl::id)
            .load_async(&*conn)
            .await
            .unwrap()
    }

    async fn get_local_storage_datasets(
        datastore: &DataStore,
        id: ZpoolUuid,
    ) -> Vec<Uuid> {
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        use nexus_db_schema::schema::rendezvous_local_storage_dataset::dsl;
        dsl::rendezvous_local_storage_dataset
            .filter(dsl::time_tombstoned.is_null())
            .filter(dsl::pool_id.eq(id.into_untyped_uuid()))
            .select(dsl::id)
            .load_async(&*conn)
            .await
            .unwrap()
    }

    async fn get_regions(datastore: &DataStore, id: Uuid) -> Vec<Uuid> {
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        use nexus_db_schema::schema::region::dsl;
        dsl::region
            .filter(dsl::dataset_id.eq(id.into_untyped_uuid()))
            .select(dsl::id)
            .load_async(&*conn)
            .await
            .unwrap()
    }

    #[nexus_test]
    async fn test_decommission_expunged_disks(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let sled_id = SledUuid::from_untyped_uuid(
            Uuid::from_str(&SLED_AGENT_UUID).unwrap(),
        );

        // Create a couple disks in the database
        let disks = [
            make_disk_in_db(&datastore, &opctx, 0, sled_id).await,
            make_disk_in_db(&datastore, &opctx, 1, sled_id).await,
        ];

        // Add a zpool, dataset, and region to each disk.
        for disk_id in disks {
            add_zpool_dataset_and_region(&datastore, &opctx, disk_id, sled_id)
                .await;
        }

        let disk_to_decommission = disks[0];
        let other_disk = disks[1];

        // Expunge one of the disks
        datastore
            .physical_disk_update_policy(
                &opctx,
                disk_to_decommission,
                PhysicalDiskPolicy::Expunged,
            )
            .await
            .unwrap();

        // Verify that the state of both disks is "active"
        let all_disks = datastore
            .physical_disk_list(
                &opctx,
                &DataPageParams::max_page(),
                DiskFilter::All,
            )
            .await
            .unwrap()
            .into_iter()
            .map(|disk| (disk.id(), disk))
            .collect::<BTreeMap<_, _>>();
        let d = &all_disks[&disk_to_decommission];
        assert_eq!(d.disk_state, PhysicalDiskState::Active);
        assert_eq!(d.disk_policy, PhysicalDiskPolicy::Expunged);
        let d = &all_disks[&other_disk];
        assert_eq!(d.disk_state, PhysicalDiskState::Active);
        assert_eq!(d.disk_policy, PhysicalDiskPolicy::InService);

        super::decommission_expunged_disks_impl(
            &opctx,
            &datastore,
            [(sled_id, disk_to_decommission)].into_iter(),
        )
        .await
        .unwrap();

        // After decommissioning, we see the expunged disk become
        // decommissioned. The other disk remains in-service.
        let all_disks = datastore
            .physical_disk_list(
                &opctx,
                &DataPageParams::max_page(),
                DiskFilter::All,
            )
            .await
            .unwrap()
            .into_iter()
            .map(|disk| (disk.id(), disk))
            .collect::<BTreeMap<_, _>>();
        let d = &all_disks[&disk_to_decommission];
        assert_eq!(d.disk_state, PhysicalDiskState::Decommissioned);
        assert_eq!(d.disk_policy, PhysicalDiskPolicy::Expunged);
        let d = &all_disks[&other_disk];
        assert_eq!(d.disk_state, PhysicalDiskState::Active);
        assert_eq!(d.disk_policy, PhysicalDiskPolicy::InService);

        // Even though we've decommissioned this disk, the pools, datasets, and
        // regions should remain. Refer to the "decommissioned_disk_cleaner"
        // for how these get eventually cleared up.
        let pools = get_pools(&datastore, disk_to_decommission).await;
        assert_eq!(pools.len(), 1);
        let datasets = get_crucible_datasets(&datastore, pools[0]).await;
        assert_eq!(datasets.len(), 1);
        let regions = get_regions(&datastore, datasets[0]).await;
        assert_eq!(regions.len(), 1);

        // Similarly, until instances are torn down and release the child
        // datasets of the local storage parent dataset, these remain too.
        let datasets = get_local_storage_datasets(&datastore, pools[0]).await;
        assert_eq!(datasets.len(), 1);

        // Similarly, the "other disk" should still exist.
        let pools = get_pools(&datastore, other_disk).await;
        assert_eq!(pools.len(), 1);
        let datasets = get_crucible_datasets(&datastore, pools[0]).await;
        assert_eq!(datasets.len(), 1);
        let regions = get_regions(&datastore, datasets[0]).await;
        assert_eq!(regions.len(), 1);
        let datasets = get_local_storage_datasets(&datastore, pools[0]).await;
        assert_eq!(datasets.len(), 1);
    }
}
