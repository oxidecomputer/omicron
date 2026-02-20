// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(test)]
mod tests {
    use crate::db;
    use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
    use crate::db::datastore::test::TestDatasets;
    use crate::db::datastore::volume::ExistingTarget;
    use crate::db::datastore::volume::ReplacementTarget;
    use crate::db::datastore::volume::VolumeReplaceResult;
    use crate::db::datastore::volume::VolumeToDelete;
    use crate::db::datastore::volume::VolumeWithTarget;
    use crate::db::datastore::volume::read_only_target_in_vcr;
    use crate::db::datastore::volume::replace_read_only_target_in_vcr;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::diesel::ExpressionMethods;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use nexus_config::RegionAllocationStrategy;
    use nexus_db_model::Region;
    use nexus_db_model::RegionSnapshot;
    use nexus_db_model::SqlU16;
    use nexus_db_model::VolumeResourceUsage;
    use nexus_db_model::to_db_typed_uuid;
    use nexus_types::external_api::disk::DiskSource;
    use nexus_types::identity::Asset;
    use omicron_common::api::external::ByteCount;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::VolumeUuid;
    use sled_agent_client::CrucibleOpts;
    use sled_agent_client::VolumeConstructionRequest;
    use std::net::SocketAddrV6;
    use uuid::Uuid;

    // Assert that Nexus will not fail to deserialize an old version of
    // CrucibleResources that was serialized before schema update 6.0.0.
    #[tokio::test]
    async fn test_deserialize_old_crucible_resources() {
        let logctx =
            dev::test_setup_log("test_deserialize_old_crucible_resources");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let datastore = db.datastore();

        // Start with a fake volume, doesn't matter if it's empty

        let volume_id = VolumeUuid::new_v4();
        let _volume = datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: *volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        // Add old CrucibleResources json in the `resources_to_clean_up` column
        // - this was before the `deleting` column / field was added to
        // ResourceSnapshot.

        {
            use nexus_db_schema::schema::volume::dsl;

            let conn = datastore.pool_connection_unauthorized().await.unwrap();

            let resources_to_clean_up = r#"{
  "V1": {
    "datasets_and_regions": [],
    "datasets_and_snapshots": [
      [
        {
          "identity": {
            "id": "844ee8d5-7641-4b04-bca8-7521e258028a",
            "time_created": "2023-12-19T21:38:34.000000Z",
            "time_modified": "2023-12-19T21:38:34.000000Z"
          },
          "time_deleted": null,
          "rcgen": 1,
          "pool_id": "81a98506-4a97-4d92-8de5-c21f6fc71649",
          "ip": "fd00:1122:3344:101::1",
          "port": 32345,
          "kind": "Crucible",
          "size_used": 10737418240
        },
        {
          "dataset_id": "b69edd77-1b3e-4f11-978c-194a0a0137d0",
          "region_id": "8d668bf9-68cc-4387-8bc0-b4de7ef9744f",
          "snapshot_id": "f548332c-6026-4eff-8c1c-ba202cd5c834",
          "snapshot_addr": "[fd00:1122:3344:101::2]:19001",
          "volume_references": 0
        }
      ]
    ]
  }
}
"#;

            diesel::update(dsl::volume)
                .filter(dsl::id.eq(to_db_typed_uuid(volume_id)))
                .set((
                    dsl::resources_to_clean_up.eq(resources_to_clean_up),
                    dsl::time_deleted.eq(Utc::now()),
                ))
                .execute_async(&*conn)
                .await
                .unwrap();
        }

        // Soft delete the volume

        let cr = datastore.soft_delete_volume(volume_id).await.unwrap();

        // Assert the contents of the returned CrucibleResources

        let datasets_and_regions =
            datastore.regions_to_delete(&cr).await.unwrap();
        let datasets_and_snapshots =
            datastore.snapshots_to_delete(&cr).await.unwrap();

        assert!(datasets_and_regions.is_empty());
        assert_eq!(datasets_and_snapshots.len(), 1);

        let region_snapshot = &datasets_and_snapshots[0].1;

        assert_eq!(
            region_snapshot.snapshot_id,
            "f548332c-6026-4eff-8c1c-ba202cd5c834".parse::<Uuid>().unwrap()
        );
        assert_eq!(region_snapshot.deleting, false);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_volume_replace_region() {
        let logctx = dev::test_setup_log("test_volume_replace_region");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let opctx = db.opctx();
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let _test_datasets = TestDatasets::create(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        let volume_id = VolumeUuid::new_v4();
        let volume_to_delete_id = VolumeUuid::new_v4();

        let datasets_and_regions = datastore
            .disk_region_allocate(
                &opctx,
                volume_id,
                &DiskSource::Blank { block_size: 512.try_into().unwrap() },
                ByteCount::from_gibibytes_u32(1),
                &&RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
            )
            .await
            .unwrap();

        let mut region_addresses: Vec<SocketAddrV6> =
            Vec::with_capacity(datasets_and_regions.len());

        for (i, (_, region)) in datasets_and_regions.iter().enumerate() {
            // `disk_region_allocate` won't put any ports in, so add fake ones
            // here
            use nexus_db_schema::schema::region::dsl;
            diesel::update(dsl::region)
                .filter(dsl::id.eq(region.id()))
                .set(dsl::port.eq(Some::<SqlU16>((100 + i as u16).into())))
                .execute_async(&*conn)
                .await
                .unwrap();

            let address: SocketAddrV6 =
                datastore.region_addr(region.id()).await.unwrap().unwrap();

            region_addresses.push(address);
        }

        // Manually create a replacement region at the first dataset
        let replacement_region = {
            let (dataset, region) = &datasets_and_regions[0];
            let region = Region::new(
                dataset.id(),
                volume_to_delete_id,
                region.block_size().try_into().unwrap(),
                region.blocks_per_extent(),
                region.extent_count(),
                111,
                false, // read-write
            );

            use nexus_db_schema::schema::region::dsl;
            diesel::insert_into(dsl::region)
                .values(region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();

            region
        };

        let replacement_region_addr: SocketAddrV6 = datastore
            .region_addr(replacement_region.id())
            .await
            .unwrap()
            .unwrap();

        let _volume = datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: *volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        generation: 1,
                        opts: CrucibleOpts {
                            id: *volume_id.as_untyped_uuid(),
                            target: vec![
                                // target to replace
                                region_addresses[0].into(),
                                region_addresses[1].into(),
                                region_addresses[2].into(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: false,
                        },
                    }],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        // Replace one

        let volume_replace_region_result = datastore
            .volume_replace_region(
                /* target */
                db::datastore::volume::VolumeReplacementParams {
                    volume_id,
                    region_id: datasets_and_regions[0].1.id(),
                    region_addr: region_addresses[0],
                },
                /* replacement */
                db::datastore::volume::VolumeReplacementParams {
                    volume_id: volume_to_delete_id,
                    region_id: replacement_region.id(),
                    region_addr: replacement_region_addr,
                },
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_region_result, VolumeReplaceResult::Done);

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        // Ensure the shape of the resulting VCR
        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    generation: 2, // generation number bumped
                    opts: CrucibleOpts {
                        id: *volume_id.as_untyped_uuid(),
                        target: vec![
                            replacement_region_addr.into(), // replaced
                            region_addresses[1].into(),
                            region_addresses[2].into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: None,
            },
        );

        // Now undo the replacement. Note volume ID is not swapped.
        let volume_replace_region_result = datastore
            .volume_replace_region(
                /* target */
                db::datastore::volume::VolumeReplacementParams {
                    volume_id,
                    region_id: replacement_region.id(),
                    region_addr: replacement_region_addr,
                },
                /* replacement */
                db::datastore::volume::VolumeReplacementParams {
                    volume_id: volume_to_delete_id,
                    region_id: datasets_and_regions[0].1.id(),
                    region_addr: region_addresses[0],
                },
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_region_result, VolumeReplaceResult::Done);

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        // Ensure the shape of the resulting VCR
        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    generation: 3, // generation number bumped
                    opts: CrucibleOpts {
                        id: *volume_id.as_untyped_uuid(),
                        target: vec![
                            region_addresses[0].into(), // back to what it was
                            region_addresses[1].into(),
                            region_addresses[2].into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: None,
            },
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_volume_replace_snapshot() {
        let logctx = dev::test_setup_log("test_volume_replace_snapshot");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let opctx = db.opctx();
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let _test_datasets = TestDatasets::create(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        let volume_id = VolumeUuid::new_v4();
        let volume_to_delete_id = VolumeUuid::new_v4();

        let datasets_and_regions = datastore
            .disk_region_allocate(
                &opctx,
                volume_id,
                &DiskSource::Blank { block_size: 512.try_into().unwrap() },
                ByteCount::from_gibibytes_u32(1),
                &&RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
            )
            .await
            .unwrap();

        let mut region_addresses: Vec<SocketAddrV6> =
            Vec::with_capacity(datasets_and_regions.len());

        for (i, (_, region)) in datasets_and_regions.iter().enumerate() {
            // `disk_region_allocate` won't put any ports in, so add fake ones
            // here
            use nexus_db_schema::schema::region::dsl;
            diesel::update(dsl::region)
                .filter(dsl::id.eq(region.id()))
                .set(dsl::port.eq(Some::<SqlU16>((100 + i as u16).into())))
                .execute_async(&*conn)
                .await
                .unwrap();

            let address: SocketAddrV6 =
                datastore.region_addr(region.id()).await.unwrap().unwrap();

            region_addresses.push(address);
        }

        // Manually create a replacement region at the first dataset
        let replacement_region = {
            let (dataset, region) = &datasets_and_regions[0];
            let region = Region::new(
                dataset.id(),
                volume_to_delete_id,
                region.block_size().try_into().unwrap(),
                region.blocks_per_extent(),
                region.extent_count(),
                111,
                true, // read-only
            );

            use nexus_db_schema::schema::region::dsl;
            diesel::insert_into(dsl::region)
                .values(region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();

            region
        };

        let replacement_region_addr: SocketAddrV6 = datastore
            .region_addr(replacement_region.id())
            .await
            .unwrap()
            .unwrap();

        // need to add region snapshot objects to satisfy volume create
        // transaction's search for resources

        let address_1: SocketAddrV6 =
            "[fd00:1122:3344:104::1]:400".parse().unwrap();
        let address_2: SocketAddrV6 =
            "[fd00:1122:3344:105::1]:401".parse().unwrap();
        let address_3: SocketAddrV6 =
            "[fd00:1122:3344:106::1]:402".parse().unwrap();

        let region_snapshots = [
            RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_1.to_string(),
            ),
            RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_2.to_string(),
            ),
            RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_3.to_string(),
            ),
        ];

        datastore
            .region_snapshot_create(region_snapshots[0].clone())
            .await
            .unwrap();
        datastore
            .region_snapshot_create(region_snapshots[1].clone())
            .await
            .unwrap();
        datastore
            .region_snapshot_create(region_snapshots[2].clone())
            .await
            .unwrap();

        // Insert two volumes: one with the target to replace, and one temporary
        // "volume to delete" that's blank. Validate the pre-replacement volume
        // resource usage records.

        let rop_id = Uuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: *volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        generation: 1,
                        opts: CrucibleOpts {
                            id: *volume_id.as_untyped_uuid(),
                            target: vec![
                                region_addresses[0].into(),
                                region_addresses[1].into(),
                                region_addresses[2].into(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: false,
                        },
                    }],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: 10,
                            extent_count: 10,
                            generation: 1,
                            opts: CrucibleOpts {
                                id: rop_id,
                                target: vec![
                                    // target to replace
                                    address_1.into(),
                                    address_2.into(),
                                    address_3.into(),
                                ],
                                lossy: false,
                                flush_timeout: None,
                                key: None,
                                cert_pem: None,
                                key_pem: None,
                                root_cert_pem: None,
                                control: None,
                                read_only: true,
                            },
                        },
                    )),
                },
            )
            .await
            .unwrap();

        for region_snapshot in &region_snapshots {
            let usage = datastore
                .volume_usage_records_for_resource(
                    VolumeResourceUsage::RegionSnapshot {
                        dataset_id: region_snapshot.dataset_id(),
                        region_id: region_snapshot.region_id,
                        snapshot_id: region_snapshot.snapshot_id,
                    },
                )
                .await
                .unwrap();

            assert_eq!(usage.len(), 1);
            assert_eq!(usage[0].volume_id(), volume_id);
        }

        datastore
            .volume_create(
                volume_to_delete_id,
                VolumeConstructionRequest::Volume {
                    id: *volume_to_delete_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        // `volume_create` above was called with a blank volume, so no usage
        // record will have been created for the read-only region

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: replacement_region.id(),
                },
            )
            .await
            .unwrap();

        assert!(usage.is_empty());

        // Do the replacement

        let volume_replace_snapshot_result = datastore
            .volume_replace_snapshot(
                VolumeWithTarget(volume_id),
                ExistingTarget(address_1),
                ReplacementTarget(replacement_region_addr),
                VolumeToDelete(volume_to_delete_id),
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_snapshot_result, VolumeReplaceResult::Done);

        // Ensure the shape of the resulting VCRs

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    generation: 1,
                    opts: CrucibleOpts {
                        id: *volume_id.as_untyped_uuid(),
                        target: vec![
                            region_addresses[0].into(),
                            region_addresses[1].into(),
                            region_addresses[2].into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        generation: 1,
                        opts: CrucibleOpts {
                            id: rop_id,
                            target: vec![
                                // target replaced
                                replacement_region_addr.into(),
                                address_2.into(),
                                address_3.into(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    }
                )),
            },
        );

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore
                .volume_get(volume_to_delete_id)
                .await
                .unwrap()
                .unwrap()
                .data(),
        )
        .unwrap();

        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_to_delete_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    generation: 1,
                    opts: CrucibleOpts {
                        id: *volume_to_delete_id.as_untyped_uuid(),
                        target: vec![
                            // replaced target stashed here
                            address_1.into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                }],
                read_only_parent: None,
            },
        );

        // Validate the post-replacement volume resource usage records

        for (i, region_snapshot) in region_snapshots.iter().enumerate() {
            let usage = datastore
                .volume_usage_records_for_resource(
                    VolumeResourceUsage::RegionSnapshot {
                        dataset_id: region_snapshot.dataset_id(),
                        region_id: region_snapshot.region_id,
                        snapshot_id: region_snapshot.snapshot_id,
                    },
                )
                .await
                .unwrap();

            assert_eq!(usage.len(), 1);

            match i {
                0 => {
                    assert_eq!(usage[0].volume_id(), volume_to_delete_id);
                }

                1 | 2 => {
                    assert_eq!(usage[0].volume_id(), volume_id);
                }

                _ => panic!("out of range"),
            }
        }

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: replacement_region.id(),
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].volume_id(), volume_id);

        // Now undo the replacement. Note volume ID is not swapped.

        let volume_replace_snapshot_result = datastore
            .volume_replace_snapshot(
                VolumeWithTarget(volume_id),
                ExistingTarget(replacement_region_addr),
                ReplacementTarget(address_1),
                VolumeToDelete(volume_to_delete_id),
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_snapshot_result, VolumeReplaceResult::Done,);

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        // Ensure the shape of the resulting VCR
        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    generation: 1,
                    opts: CrucibleOpts {
                        id: *volume_id.as_untyped_uuid(),
                        target: vec![
                            region_addresses[0].into(),
                            region_addresses[1].into(),
                            region_addresses[2].into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        generation: 1,
                        opts: CrucibleOpts {
                            id: *rop_id.as_untyped_uuid(),
                            target: vec![
                                // back to what it was
                                address_1.into(),
                                address_2.into(),
                                address_3.into(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    }
                )),
            },
        );

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore
                .volume_get(volume_to_delete_id)
                .await
                .unwrap()
                .unwrap()
                .data(),
        )
        .unwrap();

        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_to_delete_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    generation: 1,
                    opts: CrucibleOpts {
                        id: *volume_to_delete_id.as_untyped_uuid(),
                        target: vec![
                            // replacement stashed here
                            replacement_region_addr.into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                }],
                read_only_parent: None,
            },
        );

        // Validate the post-post-replacement volume resource usage records

        for region_snapshot in &region_snapshots {
            let usage = datastore
                .volume_usage_records_for_resource(
                    VolumeResourceUsage::RegionSnapshot {
                        dataset_id: region_snapshot.dataset_id(),
                        region_id: region_snapshot.region_id,
                        snapshot_id: region_snapshot.snapshot_id,
                    },
                )
                .await
                .unwrap();

            assert_eq!(usage.len(), 1);
            assert_eq!(usage[0].volume_id(), volume_id);
        }

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: replacement_region.id(),
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].volume_id(), volume_to_delete_id);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_find_volumes_referencing_socket_addr() {
        let logctx =
            dev::test_setup_log("test_find_volumes_referencing_socket_addr");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let volume_id = VolumeUuid::new_v4();

        // need to add region snapshot objects to satisfy volume create
        // transaction's search for resources

        let address_1: SocketAddrV6 =
            "[fd00:1122:3344:104::1]:400".parse().unwrap();
        let address_2: SocketAddrV6 =
            "[fd00:1122:3344:105::1]:401".parse().unwrap();
        let address_3: SocketAddrV6 =
            "[fd00:1122:3344:106::1]:402".parse().unwrap();

        datastore
            .region_snapshot_create(RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_1.to_string(),
            ))
            .await
            .unwrap();
        datastore
            .region_snapshot_create(RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_2.to_string(),
            ))
            .await
            .unwrap();
        datastore
            .region_snapshot_create(RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_3.to_string(),
            ))
            .await
            .unwrap();

        // case where the needle is found

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: *volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: 10,
                            extent_count: 10,
                            generation: 1,
                            opts: CrucibleOpts {
                                id: Uuid::new_v4(),
                                target: vec![
                                    address_1.into(),
                                    address_2.into(),
                                    address_3.into(),
                                ],
                                lossy: false,
                                flush_timeout: None,
                                key: None,
                                cert_pem: None,
                                key_pem: None,
                                root_cert_pem: None,
                                control: None,
                                read_only: true,
                            },
                        },
                    )),
                },
            )
            .await
            .unwrap();

        let volumes = datastore
            .find_volumes_referencing_socket_addr(&opctx, address_1.into())
            .await
            .unwrap();

        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].id(), volume_id);

        // case where the needle is missing

        let volumes = datastore
            .find_volumes_referencing_socket_addr(
                &opctx,
                "[fd55:1122:3344:104::1]:400".parse().unwrap(),
            )
            .await
            .unwrap();

        assert!(volumes.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn test_read_only_target_in_vcr() {
        // read_only_target_in_vcr should find read-only targets

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    generation: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![
                            "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                            "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                            "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                },
            )),
        };

        assert!(
            read_only_target_in_vcr(
                &vcr,
                &"[fd00:1122:3344:104::1]:400".parse().unwrap(),
            )
            .unwrap()
        );

        // read_only_target_in_vcr should _not_ find read-write targets

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 10,
                extent_count: 10,
                generation: 1,
                opts: CrucibleOpts {
                    id: Uuid::new_v4(),
                    target: vec![
                        "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                        "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                        "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                    ],
                    lossy: false,
                    flush_timeout: None,
                    key: None,
                    cert_pem: None,
                    key_pem: None,
                    root_cert_pem: None,
                    control: None,
                    read_only: false,
                },
            }],
            read_only_parent: None,
        };

        assert!(
            !read_only_target_in_vcr(
                &vcr,
                &"[fd00:1122:3344:104::1]:400".parse().unwrap(),
            )
            .unwrap()
        );

        // read_only_target_in_vcr should bail on incorrect VCRs (currently it
        // only detects a read/write region under a read-only parent)

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    generation: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![
                            "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                            "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                            "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false, // invalid!
                    },
                },
            )),
        };

        read_only_target_in_vcr(
            &vcr,
            &"[fd00:1122:3344:104::1]:400".parse().unwrap(),
        )
        .unwrap_err();
    }

    #[test]
    fn test_replace_read_only_target_in_vcr() {
        // replace_read_only_target_in_vcr should perform a replacement in a
        // read-only parent

        let volume_id = Uuid::new_v4();

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: 512,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    generation: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                            "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                            "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                },
            )),
        };

        let old_target =
            ExistingTarget("[fd00:1122:3344:105::1]:401".parse().unwrap());
        let new_target =
            ReplacementTarget("[fd99:1122:3344:105::1]:12345".parse().unwrap());

        let (new_vcr, replacements) =
            replace_read_only_target_in_vcr(&vcr, old_target, new_target)
                .unwrap();

        assert_eq!(replacements, 1);
        assert_eq!(
            &new_vcr,
            &VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        generation: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                                new_target.0.into(),
                                "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        }
                    }
                ))
            }
        );

        // replace_read_only_target_in_vcr should perform a replacement in a
        // read-only parent in a sub-volume

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    generation: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            "[fd55:1122:3344:204::1]:1000".parse().unwrap(),
                            "[fd55:1122:3344:205::1]:1001".parse().unwrap(),
                            "[fd55:1122:3344:206::1]:1002".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        generation: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                "[fd33:1122:3344:304::1]:2000".parse().unwrap(),
                                "[fd33:1122:3344:305::1]:2001".parse().unwrap(),
                                "[fd33:1122:3344:306::1]:2002".parse().unwrap(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    },
                )),
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    generation: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                            "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                            "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                },
            )),
        };

        let old_target =
            ExistingTarget("[fd33:1122:3344:306::1]:2002".parse().unwrap());
        let new_target =
            ReplacementTarget("[fd99:1122:3344:105::1]:12345".parse().unwrap());

        let (new_vcr, replacements) =
            replace_read_only_target_in_vcr(&vcr, old_target, new_target)
                .unwrap();

        assert_eq!(replacements, 1);
        assert_eq!(
            &new_vcr,
            &VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Volume {
                    id: volume_id,
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        generation: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                "[fd55:1122:3344:204::1]:1000".parse().unwrap(),
                                "[fd55:1122:3344:205::1]:1001".parse().unwrap(),
                                "[fd55:1122:3344:206::1]:1002".parse().unwrap(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: false,
                        }
                    }],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: 10,
                            extent_count: 10,
                            generation: 1,
                            opts: CrucibleOpts {
                                id: volume_id,
                                target: vec![
                                    "[fd33:1122:3344:304::1]:2000"
                                        .parse()
                                        .unwrap(),
                                    "[fd33:1122:3344:305::1]:2001"
                                        .parse()
                                        .unwrap(),
                                    new_target.0.into(),
                                ],
                                lossy: false,
                                flush_timeout: None,
                                key: None,
                                cert_pem: None,
                                key_pem: None,
                                root_cert_pem: None,
                                control: None,
                                read_only: true,
                            }
                        }
                    )),
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        generation: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                                "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                                "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        }
                    }
                ))
            }
        );

        // replace_read_only_target_in_vcr should perform multiple replacements
        // if necessary (even if this is dubious!) - the caller will decide if
        // this should be legal or not

        let rop = VolumeConstructionRequest::Region {
            block_size: 512,
            blocks_per_extent: 10,
            extent_count: 10,
            generation: 1,
            opts: CrucibleOpts {
                id: volume_id,
                target: vec![
                    "[fd33:1122:3344:304::1]:2000".parse().unwrap(),
                    "[fd33:1122:3344:305::1]:2001".parse().unwrap(),
                    "[fd33:1122:3344:306::1]:2002".parse().unwrap(),
                ],
                lossy: false,
                flush_timeout: None,
                key: None,
                cert_pem: None,
                key_pem: None,
                root_cert_pem: None,
                control: None,
                read_only: true,
            },
        };

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    generation: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            "[fd55:1122:3344:204::1]:1000".parse().unwrap(),
                            "[fd55:1122:3344:205::1]:1001".parse().unwrap(),
                            "[fd55:1122:3344:206::1]:1002".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: Some(Box::new(rop.clone())),
            }],
            read_only_parent: Some(Box::new(rop)),
        };

        let old_target =
            ExistingTarget("[fd33:1122:3344:304::1]:2000".parse().unwrap());
        let new_target =
            ReplacementTarget("[fd99:1122:3344:105::1]:12345".parse().unwrap());

        let (new_vcr, replacements) =
            replace_read_only_target_in_vcr(&vcr, old_target, new_target)
                .unwrap();

        assert_eq!(replacements, 2);

        let rop = VolumeConstructionRequest::Region {
            block_size: 512,
            blocks_per_extent: 10,
            extent_count: 10,
            generation: 1,
            opts: CrucibleOpts {
                id: volume_id,
                target: vec![
                    new_target.0.into(),
                    "[fd33:1122:3344:305::1]:2001".parse().unwrap(),
                    "[fd33:1122:3344:306::1]:2002".parse().unwrap(),
                ],
                lossy: false,
                flush_timeout: None,
                key: None,
                cert_pem: None,
                key_pem: None,
                root_cert_pem: None,
                control: None,
                read_only: true,
            },
        };

        assert_eq!(
            &new_vcr,
            &VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Volume {
                    id: volume_id,
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        generation: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                "[fd55:1122:3344:204::1]:1000".parse().unwrap(),
                                "[fd55:1122:3344:205::1]:1001".parse().unwrap(),
                                "[fd55:1122:3344:206::1]:1002".parse().unwrap(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: false,
                        }
                    }],
                    read_only_parent: Some(Box::new(rop.clone())),
                }],
                read_only_parent: Some(Box::new(rop)),
            }
        );
    }

    /// Assert that there are no "deleted" r/w regions found when the associated
    /// volume hasn't been created yet.
    #[tokio::test]
    async fn test_no_find_deleted_region_for_no_volume() {
        let logctx =
            dev::test_setup_log("test_no_find_deleted_region_for_no_volume");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let _test_datasets = TestDatasets::create(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        let volume_id = VolumeUuid::new_v4();

        // Assert that allocating regions without creating the volume does not
        // cause them to be returned as "deleted" regions, as this can cause
        // sagas that allocate regions to race with the volume delete saga and
        // cause premature region deletion.

        let _datasets_and_regions = datastore
            .disk_region_allocate(
                &opctx,
                volume_id,
                &DiskSource::Blank { block_size: 512.try_into().unwrap() },
                ByteCount::from_gibibytes_u32(1),
                &&RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
            )
            .await
            .unwrap();

        let deleted_regions = datastore
            .find_deleted_volume_regions()
            .await
            .expect("find_deleted_volume_regions");

        assert!(deleted_regions.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
