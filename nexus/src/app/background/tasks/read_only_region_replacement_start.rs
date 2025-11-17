// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting read-only regions that need replacing and
//! beginning that process.
//!
//! This task's responsibility is to create replacement requests for read-only
//! regions when physical disks are expunged. The corresponding region snapshot
//! replacement start saga will be triggered by the 'region snapshot replacement
//! start' background task.

use crate::app::background::BackgroundTask;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::ReadOnlyRegionReplacementStartStatus;
use omicron_common::api::external::Error;
use serde_json::json;
use std::sync::Arc;

pub struct ReadOnlyRegionReplacementDetector {
    datastore: Arc<DataStore>,
}

impl ReadOnlyRegionReplacementDetector {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        ReadOnlyRegionReplacementDetector { datastore }
    }

    /// Find read-only regions on expunged physical disks and create replacement
    /// requests for them.
    async fn create_requests_for_read_only_regions_on_expunged_disks(
        &self,
        opctx: &OpContext,
        status: &mut ReadOnlyRegionReplacementStartStatus,
    ) {
        let log = &opctx.log;

        // Find read-only regions on expunged physical disks
        let regions_to_be_replaced = match self
            .datastore
            .find_read_only_regions_on_expunged_physical_disks(opctx)
            .await
        {
            Ok(regions) => regions,

            Err(e) => {
                let s = format!(
                    "find_read_only_regions_on_expunged_physical_disks \
                        failed: {e}",
                );

                error!(&log, "{s}");
                status.errors.push(s);
                return;
            }
        };

        for region in regions_to_be_replaced {
            let region_id = region.id();

            // If no request exists yet, create one.
            let existing_request = match self
                .datastore
                .lookup_read_only_region_replacement_request(opctx, region_id)
                .await
            {
                Ok(existing_request) => existing_request,

                Err(e) => {
                    let s =
                        format!("error looking up replacement request: {e}");

                    error!(
                        &log,
                        "{s}";
                        "region_id" => %region_id,
                    );
                    status.errors.push(s);
                    continue;
                }
            };

            if existing_request.is_none() {
                match self
                    .datastore
                    .create_read_only_region_replacement_request(
                        opctx, region_id,
                    )
                    .await
                {
                    Ok(request_id) => {
                        let s = format!(
                            "created region snapshot replacement request \
                            {request_id}"
                        );

                        info!(
                            &log,
                            "{s}";
                            "region_id" => %region_id,
                        );
                        status.requests_created_ok.push(s);
                    }

                    Err(e) => {
                        match e {
                            Error::Conflict { message }
                                if message.external_message()
                                    == "volume repair lock" =>
                            {
                                // This is not a fatal error! If there are
                                // competing region replacement and region
                                // snapshot replacements, then they are both
                                // attempting to lock volumes.
                            }

                            _ => {
                                let s = format!(
                                    "error creating replacement request: {e}"
                                );

                                error!(
                                    &log,
                                    "{s}";
                                    "region_id" => %region_id,
                                );

                                status.errors.push(s);
                            }
                        }
                    }
                }
            }
        }
    }
}

impl BackgroundTask for ReadOnlyRegionReplacementDetector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let mut status = ReadOnlyRegionReplacementStartStatus::default();

            self.create_requests_for_read_only_regions_on_expunged_disks(
                opctx,
                &mut status,
            )
            .await;

            json!(status)
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::MIN_DISK_SIZE_BYTES;
    use crate::app::RegionAllocationStrategy;
    use crate::external_api::params;
    use chrono::Utc;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::BlockSize;
    use nexus_db_model::Generation;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::ReadOnlyTargetReplacement;
    use nexus_db_model::Snapshot;
    use nexus_db_model::SnapshotIdentity;
    use nexus_db_model::SnapshotState;
    use nexus_db_queries::authz;
    use nexus_db_queries::db::datastore::RegionAllocationFor;
    use nexus_db_queries::db::datastore::RegionAllocationParameters;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external;
    use omicron_uuid_kinds::DatasetUuid;

    use omicron_uuid_kinds::VolumeUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_agent_client::VolumeConstructionRequest;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;
    type DiskTest<'a> =
        nexus_test_utils::resource_helpers::DiskTest<'a, crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_expunge_disk_causes_read_only_region_replacement_request(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let disk_test = DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let project = create_project(&client, "testing").await;
        let project_id = project.identity.id;

        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Record which crucible datasets map to which zpools for later

        let mut dataset_to_zpool: BTreeMap<ZpoolUuid, DatasetUuid> =
            BTreeMap::default();

        for zpool in disk_test.zpools() {
            let dataset = zpool.crucible_dataset();
            dataset_to_zpool.insert(zpool.id, dataset.id);
        }

        let mut task =
            ReadOnlyRegionReplacementDetector::new(datastore.clone());

        // Noop test
        let result: ReadOnlyRegionReplacementStartStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, ReadOnlyRegionReplacementStartStatus::default());

        // Add three read-only regions

        let volume_id = VolumeUuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let datasets_and_regions = datastore
            .arbitrary_region_allocate(
                &opctx,
                RegionAllocationFor::SnapshotVolume { volume_id, snapshot_id },
                RegionAllocationParameters::FromDiskSource {
                    disk_source: &params::DiskSource::Blank {
                        block_size: params::BlockSize::try_from(512).unwrap(),
                    },
                    size: external::ByteCount::from_gibibytes_u32(1),
                },
                &RegionAllocationStrategy::Random { seed: None },
                3,
            )
            .await
            .unwrap();

        assert_eq!(datasets_and_regions.len(), 3);

        for (_, region) in &datasets_and_regions {
            assert!(region.read_only());
        }

        // Create the fake snapshot

        let (.., authz_project) = LookupPath::new(&opctx, datastore)
            .project_id(project_id)
            .lookup_for(authz::Action::CreateChild)
            .await
            .unwrap();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        datastore
            .project_ensure_snapshot(
                &opctx,
                &authz_project,
                Snapshot {
                    identity: SnapshotIdentity {
                        id: snapshot_id,
                        name: external::Name::try_from("snapshot".to_string())
                            .unwrap()
                            .into(),
                        description: "snapshot".into(),

                        time_created: Utc::now(),
                        time_modified: Utc::now(),
                        time_deleted: None,
                    },

                    project_id,
                    disk_id: Uuid::new_v4(),

                    volume_id: volume_id.into(),
                    destination_volume_id: VolumeUuid::new_v4().into(),

                    generation: Generation::new(),
                    state: SnapshotState::Creating,
                    block_size: BlockSize::Traditional,

                    size: external::ByteCount::try_from(MIN_DISK_SIZE_BYTES)
                        .unwrap()
                        .into(),
                },
            )
            .await
            .unwrap();

        // Expunge one of the physical disks

        let first_zpool =
            disk_test.zpools().next().expect("Expected at least one zpool");

        let (_, db_zpool) = LookupPath::new(&opctx, datastore)
            .zpool_id(first_zpool.id)
            .fetch()
            .await
            .unwrap();

        datastore
            .physical_disk_update_policy(
                &opctx,
                db_zpool.physical_disk_id(),
                PhysicalDiskPolicy::Expunged,
            )
            .await
            .unwrap();

        // Activate the task - it should pick that up and create a replacement
        // request for the read-only region on that expunged disk

        let result: ReadOnlyRegionReplacementStartStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        eprintln!("{:?}", &result);

        assert_eq!(result.requests_created_ok.len(), 1);
        assert!(result.errors.is_empty());

        // The last part of the message is the replacement request id
        let request_created_uuid: Uuid = result.requests_created_ok[0]
            .split(" ")
            .last()
            .unwrap()
            .parse()
            .unwrap();

        let request = datastore
            .get_region_snapshot_replacement_request_by_id(
                &opctx,
                request_created_uuid,
            )
            .await
            .unwrap();

        let ReadOnlyTargetReplacement::ReadOnlyRegion {
            region_id: replacement_region_id,
        } = request.replacement_type()
        else {
            panic!("wrong type!");
        };

        let expunged_dataset_id =
            dataset_to_zpool.get(&first_zpool.id).unwrap();
        let region = datastore.get_region(replacement_region_id).await.unwrap();
        assert_eq!(*expunged_dataset_id, region.dataset_id());
    }
}
