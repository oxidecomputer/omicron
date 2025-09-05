// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting region snapshots that need replacing and
//! beginning that process
//!
//! This task's responsibility is to create region snapshot replacement requests
//! when physical disks are expunged, and trigger the region snapshot
//! replacement start saga for any requests that are in state "Requested". See
//! the documentation in that saga's docstring for more information.

use crate::app::RegionAllocationStrategy;
use crate::app::authn;
use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas;
use crate::app::sagas::NexusSaga;
use crate::app::sagas::region_snapshot_replacement_start::*;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_model::ReadOnlyTargetReplacement;
use nexus_db_model::RegionSnapshotReplacement;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::RegionSnapshotReplacementStartStatus;
use omicron_common::api::external::Error;
use serde_json::json;
use std::sync::Arc;

pub struct RegionSnapshotReplacementDetector {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

impl RegionSnapshotReplacementDetector {
    pub fn new(datastore: Arc<DataStore>, sagas: Arc<dyn StartSaga>) -> Self {
        RegionSnapshotReplacementDetector { datastore, sagas }
    }

    async fn send_start_request(
        &self,
        serialized_authn: authn::saga::Serialized,
        request: RegionSnapshotReplacement,
    ) -> Result<(), Error> {
        let params = sagas::region_snapshot_replacement_start::Params {
            serialized_authn,
            request,
            allocation_strategy:
                RegionAllocationStrategy::RandomWithDistinctSleds { seed: None },
        };

        let saga_dag = SagaRegionSnapshotReplacementStart::prepare(&params)?;
        // We only care that the saga was started, and don't wish to wait for it
        // to complete, so use `StartSaga::saga_start`, rather than `saga_run`.
        self.sagas.saga_start(saga_dag).await?;
        Ok(())
    }

    /// Find region snapshots on expunged physical disks and create region
    /// snapshot replacement requests for them.
    async fn create_requests_for_region_snapshots_on_expunged_disks(
        &self,
        opctx: &OpContext,
        status: &mut RegionSnapshotReplacementStartStatus,
    ) {
        let log = &opctx.log;

        // Find region snapshots on expunged physical disks
        let region_snapshots_to_be_replaced = match self
            .datastore
            .find_region_snapshots_on_expunged_physical_disks(opctx)
            .await
        {
            Ok(region_snapshots) => region_snapshots,

            Err(e) => {
                let s = format!(
                    "find_region_snapshots_on_expunged_physical_disks \
                        failed: {e}",
                );

                error!(&log, "{s}");
                status.errors.push(s);
                return;
            }
        };

        for region_snapshot in region_snapshots_to_be_replaced {
            let replacement = ReadOnlyTargetReplacement::RegionSnapshot {
                dataset_id: region_snapshot.dataset_id,
                region_id: region_snapshot.region_id,
                snapshot_id: region_snapshot.snapshot_id,
            };

            // If no request exists yet, create one.
            let existing_request = match self
                .datastore
                .lookup_region_snapshot_replacement_request(
                    opctx,
                    &region_snapshot,
                )
                .await
            {
                Ok(existing_request) => existing_request,

                Err(e) => {
                    let s =
                        format!("error looking up replacement request: {e}");

                    error!(&log, "{s}"; replacement);
                    status.errors.push(s);
                    continue;
                }
            };

            if existing_request.is_none() {
                match self
                    .datastore
                    .create_region_snapshot_replacement_request(
                        opctx,
                        &region_snapshot,
                    )
                    .await
                {
                    Ok(request_id) => {
                        let s = format!(
                            "created region snapshot replacement request \
                            {request_id}"
                        );

                        info!(&log, "{s}"; replacement);
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

                                error!(&log, "{s}"; replacement);
                                status.errors.push(s);
                            }
                        }
                    }
                }
            }
        }
    }

    /// For each region snapshot replacement request in state "Requested", run
    /// the start saga.
    async fn start_requested_region_snapshot_replacements(
        &self,
        opctx: &OpContext,
        status: &mut RegionSnapshotReplacementStartStatus,
    ) {
        let log = &opctx.log;

        let requests = match self
            .datastore
            .get_requested_region_snapshot_replacements(opctx)
            .await
        {
            Ok(requests) => requests,

            Err(e) => {
                let s = format!(
                    "query for region snapshot replacement requests failed: {e}"
                );

                error!(&log, "{s}");
                status.errors.push(s);
                return;
            }
        };

        for request in requests {
            let request_id = request.id;
            let replacement = request.replacement_type();

            // If the region snapshot or read-only region is gone, then there
            // are no more references in any volume, and the whole region
            // snapshot replacement can be fast-tracked to Complete.

            let deleted =
                match self.datastore.read_only_target_deleted(&request).await {
                    Ok(deleted) => deleted,

                    Err(e) => {
                        let s = format!(
                            "error querying for read-only target deletion: {e}",
                        );
                        error!(
                            &log,
                            "{s}";
                            "request_id" => %request_id,
                            replacement
                        );
                        status.errors.push(s);
                        continue;
                    }
                };

            if deleted {
                match self
                    .datastore
                    .set_region_snapshot_replacement_complete_from_requested(
                        &opctx, request.id,
                    )
                    .await
                {
                    Ok(()) => {
                        let s = format!(
                            "region snapshot replacement {request_id} \
                                completed ok"
                        );
                        info!(&log, "{s}"; replacement);
                        status.requests_completed_ok.push(s);
                    }

                    Err(e) => {
                        let s = format!(
                            "query to set region snapshot request {request_id} \
                            state to complete failed: {e}"
                        );

                        error!(&log, "{s}"; replacement);

                        status.errors.push(s);
                    }
                }

                continue;
            }

            // Do not check if the volume is deleted here: if the read-only
            // target is _not_ deleted (checked above), then even if the
            // snapshot volume is hard deleted there are still other references
            // in other volumes to those read-only targets that need replacing.
            // The start saga can handle if the snapshot volume is hard deleted,
            // so proceed with invoking it here.

            let result = self
                .send_start_request(
                    authn::saga::Serialized::for_opctx(opctx),
                    request,
                )
                .await;

            match result {
                Ok(()) => {
                    let s = format!(
                        "region snapshot replacement start invoked ok for \
                        {request_id}"
                    );

                    info!(&log, "{s}"; replacement);
                    status.start_invoked_ok.push(s);
                }

                Err(e) => {
                    let s = format!(
                        "invoking region snapshot replacement start for \
                        {request_id} failed: {e}",
                    );

                    error!(&log, "{s}"; replacement);
                    status.errors.push(s);
                }
            }
        }
    }
}

impl BackgroundTask for RegionSnapshotReplacementDetector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let mut status = RegionSnapshotReplacementStartStatus::default();

            self.create_requests_for_region_snapshots_on_expunged_disks(
                opctx,
                &mut status,
            )
            .await;

            self.start_requested_region_snapshot_replacements(
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
    use crate::app::background::init::test::NoopStartSaga;
    use chrono::Utc;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::BlockSize;
    use nexus_db_model::Generation;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::ReadOnlyTargetReplacement;
    use nexus_db_model::RegionSnapshot;
    use nexus_db_model::RegionSnapshotReplacement;
    use nexus_db_model::Snapshot;
    use nexus_db_model::SnapshotIdentity;
    use nexus_db_model::SnapshotState;
    use nexus_db_model::VolumeResourceUsage;
    use nexus_db_queries::authz;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::VolumeUuid;
    use sled_agent_client::CrucibleOpts;
    use sled_agent_client::VolumeConstructionRequest;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;
    type DiskTest<'a> =
        nexus_test_utils::resource_helpers::DiskTest<'a, crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_add_region_snapshot_replacement_causes_start(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let starter = Arc::new(NoopStartSaga::new());
        let mut task = RegionSnapshotReplacementDetector::new(
            datastore.clone(),
            starter.clone(),
        );

        // Noop test
        let result: RegionSnapshotReplacementStartStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, RegionSnapshotReplacementStartStatus::default());
        assert_eq!(starter.count_reset(), 0);

        // Add a region snapshot replacement request for a fake region snapshot

        let dataset_id = DatasetUuid::new_v4();
        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let region_snapshot = RegionSnapshot::new(
            dataset_id,
            region_id,
            snapshot_id,
            "[::]:12345".to_string(),
        );

        datastore.region_snapshot_create(region_snapshot).await.unwrap();

        let request = RegionSnapshotReplacement::new_from_region_snapshot(
            dataset_id,
            region_id,
            snapshot_id,
        );

        let request_id = request.id;

        let volume_id = VolumeUuid::new_v4();

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx, request, volume_id,
            )
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the
        // region snapshot replacement start saga
        let result: RegionSnapshotReplacementStartStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        assert_eq!(
            result,
            RegionSnapshotReplacementStartStatus {
                requests_created_ok: vec![],
                start_invoked_ok: vec![format!(
                    "region snapshot replacement start invoked ok for \
                    {request_id}"
                )],
                requests_completed_ok: vec![],
                errors: vec![],
            },
        );

        assert_eq!(starter.count_reset(), 1);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_expunge_disk_causes_region_snapshot_replacement_start(
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

        let starter = Arc::new(NoopStartSaga::new());
        let mut task = RegionSnapshotReplacementDetector::new(
            datastore.clone(),
            starter.clone(),
        );

        // Noop test
        let result: RegionSnapshotReplacementStartStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, RegionSnapshotReplacementStartStatus::default());
        assert_eq!(starter.count_reset(), 0);

        // Add three region snapshots for each dataset

        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();
        let mut dataset_to_zpool: BTreeMap<String, String> =
            BTreeMap::default();

        for zpool in disk_test.zpools() {
            let dataset = zpool.crucible_dataset();
            dataset_to_zpool
                .insert(zpool.id.to_string(), dataset.id.to_string());

            datastore
                .region_snapshot_create(RegionSnapshot::new(
                    dataset.id,
                    region_id,
                    snapshot_id,
                    String::from("[fd00:1122:3344::101]:12345"),
                ))
                .await
                .unwrap();
        }

        // Create the fake snapshot

        let (.., authz_project) = LookupPath::new(&opctx, datastore)
            .project_id(project_id)
            .lookup_for(authz::Action::CreateChild)
            .await
            .unwrap();

        let volume_id = VolumeUuid::new_v4();

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

                    gen: Generation::new(),
                    state: SnapshotState::Creating,
                    block_size: BlockSize::AdvancedFormat,

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
            .zpool_id(first_zpool.id.into_untyped_uuid())
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

        // Activate the task - it should pick that up and try to run the region
        // snapshot replacement start saga for the region snapshot on that
        // expunged disk

        let result: RegionSnapshotReplacementStartStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        eprintln!("{:?}", &result);

        assert_eq!(result.requests_created_ok.len(), 1);
        assert_eq!(result.start_invoked_ok.len(), 1);
        assert!(result.errors.is_empty());

        // The last part of the message is the region snapshot replacement
        // request id
        let request_created_uuid: Uuid = result.requests_created_ok[0]
            .split(" ")
            .last()
            .unwrap()
            .parse()
            .unwrap();
        let request_started_uuid: Uuid = result.start_invoked_ok[0]
            .split(" ")
            .last()
            .unwrap()
            .parse()
            .unwrap();

        assert_eq!(request_created_uuid, request_started_uuid);

        assert_eq!(starter.count_reset(), 1);

        let request = datastore
            .get_region_snapshot_replacement_request_by_id(
                &opctx,
                request_created_uuid,
            )
            .await
            .unwrap();

        let ReadOnlyTargetReplacement::RegionSnapshot {
            dataset_id: replacement_dataset_id,
            region_id: replacement_region_id,
            snapshot_id: replacement_snapshot_id,
        } = request.replacement_type()
        else {
            panic!("wrong type!");
        };

        assert_eq!(replacement_snapshot_id, snapshot_id);
        assert_eq!(replacement_region_id, region_id);

        let dataset_id =
            dataset_to_zpool.get(&first_zpool.id.to_string()).unwrap();
        assert_eq!(&replacement_dataset_id.to_string(), dataset_id);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_delete_region_snapshot_replacement_volume_causes_complete(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let starter = Arc::new(NoopStartSaga::new());
        let mut task = RegionSnapshotReplacementDetector::new(
            datastore.clone(),
            starter.clone(),
        );

        // Noop test
        let result: RegionSnapshotReplacementStartStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, RegionSnapshotReplacementStartStatus::default());
        assert_eq!(starter.count_reset(), 0);

        // The volume reference counting machinery needs a fake dataset to exist
        // (region snapshots are joined with the dataset table when creating the
        // CrucibleResources object)

        let disk_test = DiskTest::new(cptestctx).await;

        let dataset_id =
            disk_test.zpools().next().unwrap().crucible_dataset().id;

        // Add a region snapshot replacement request for a fake region snapshot

        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let region_snapshot = RegionSnapshot::new(
            dataset_id,
            region_id,
            snapshot_id,
            "[::1]:12345".to_string(),
        );

        datastore
            .region_snapshot_create(region_snapshot.clone())
            .await
            .unwrap();

        let request = RegionSnapshotReplacement::new_from_region_snapshot(
            dataset_id,
            region_id,
            snapshot_id,
        );

        let request_id = request.id;

        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: 1,
                            extent_count: 1,
                            gen: 1,
                            opts: CrucibleOpts {
                                id: Uuid::new_v4(),
                                target: vec!["[::1]:12345".parse().unwrap()],
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

        // Assert usage

        let records = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id: region_snapshot.dataset_id.into(),
                    region_id: region_snapshot.region_id,
                    snapshot_id: region_snapshot.snapshot_id,
                },
            )
            .await
            .unwrap();

        assert!(!records.is_empty());
        assert_eq!(records[0].volume_id, volume_id.into());

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx, request, volume_id,
            )
            .await
            .unwrap();

        // Before the task starts, soft-delete the volume, and delete the
        // region snapshot (like the volume delete saga would do).

        let crucible_resources =
            datastore.soft_delete_volume(volume_id).await.unwrap();

        // Assert no more usage

        let records = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id: region_snapshot.dataset_id.into(),
                    region_id: region_snapshot.region_id,
                    snapshot_id: region_snapshot.snapshot_id,
                },
            )
            .await
            .unwrap();

        assert!(records.is_empty());

        // The region snapshot should have been returned for deletion

        let datasets_and_snapshots =
            datastore.snapshots_to_delete(&crucible_resources).await.unwrap();

        assert!(!datasets_and_snapshots.is_empty());

        let region_snapshot_to_delete = &datasets_and_snapshots[0].1;

        assert_eq!(
            region_snapshot_to_delete.dataset_id,
            region_snapshot.dataset_id,
        );
        assert_eq!(
            region_snapshot_to_delete.region_id,
            region_snapshot.region_id,
        );
        assert_eq!(
            region_snapshot_to_delete.snapshot_id,
            region_snapshot.snapshot_id,
        );

        // So delete it!

        datastore
            .region_snapshot_remove(
                region_snapshot_to_delete.dataset_id.into(),
                region_snapshot_to_delete.region_id,
                region_snapshot_to_delete.snapshot_id,
            )
            .await
            .unwrap();

        // Activate the task - it should pick the request up but not attempt to
        // run the start saga

        let result: RegionSnapshotReplacementStartStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        assert_eq!(
            result,
            RegionSnapshotReplacementStartStatus {
                requests_created_ok: vec![],
                start_invoked_ok: vec![],
                requests_completed_ok: vec![format!(
                    "region snapshot replacement {request_id} completed ok"
                )],
                errors: vec![],
            },
        );

        // Assert start saga not invoked
        assert_eq!(starter.count_reset(), 0);
    }
}
