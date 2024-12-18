// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests related to region and region snapshot replacement

use dropshot::test_util::ClientTestContext;
use nexus_client::types::LastResult;
use nexus_db_model::PhysicalDiskPolicy;
use nexus_db_model::RegionReplacementState;
use nexus_db_model::RegionSnapshotReplacementState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::region_snapshot_replacement::*;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use nexus_test_utils::background::*;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_disk_from_snapshot;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_snapshot;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::*;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use omicron_uuid_kinds::GenericUuid;
use slog::Logger;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

type DiskTest<'a> =
    nexus_test_utils::resource_helpers::DiskTest<'a, omicron_nexus::Server>;

type DiskTestBuilder<'a> = nexus_test_utils::resource_helpers::DiskTestBuilder<
    'a,
    omicron_nexus::Server,
>;

const PROJECT_NAME: &str = "now-this-is-pod-racing";

fn get_disk_url(disk_name: &str) -> String {
    format!("/v1/disks/{disk_name}?project={}", PROJECT_NAME)
}

fn get_disks_url() -> String {
    format!("/v1/disks?project={}", PROJECT_NAME)
}

fn get_snapshot_url(snapshot_name: &str) -> String {
    format!("/v1/snapshots/{snapshot_name}?project={}", PROJECT_NAME)
}

fn get_snapshots_url() -> String {
    format!("/v1/snapshots?project={}", PROJECT_NAME)
}

async fn create_project_and_pool(client: &ClientTestContext) -> Uuid {
    create_default_ip_pool(client).await;
    let project = create_project(client, PROJECT_NAME).await;
    project.identity.id
}

async fn collection_list<T>(
    client: &ClientTestContext,
    list_url: &str,
) -> Vec<T>
where
    T: Clone + serde::de::DeserializeOwned,
{
    NexusRequest::iter_collection_authn(client, list_url, "", None)
        .await
        .expect("failed to list")
        .all_items
}

/// Assert that the first part of region replacement does not create a freed
/// crucible region (that would be picked up by a volume delete saga)
#[nexus_test]
async fn test_region_replacement_does_not_create_freed_region(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create four zpools, each with one dataset. This is required for region
    // and region snapshot replacement to have somewhere to move the data.
    let sled_id = cptestctx.first_sled();
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(sled_id)
        .with_zpool_count(4)
        .build()
        .await;

    // Create a disk
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    // Before expunging the physical disk, save the DB model
    let (.., db_disk) = LookupPath::new(&opctx, &datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_disk.id(), disk.identity.id);

    // Next, expunge a physical disk that contains a region

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id).await.unwrap();
    let (dataset, _) = &disk_allocated_regions[0];
    let zpool = disk_test
        .zpools()
        .find(|x| *x.id.as_untyped_uuid() == dataset.pool_id)
        .expect("Expected at least one zpool");

    let (_, db_zpool) = LookupPath::new(&opctx, datastore)
        .zpool_id(zpool.id.into_untyped_uuid())
        .fetch()
        .await
        .unwrap();

    datastore
        .physical_disk_update_policy(
            &opctx,
            db_zpool.physical_disk_id.into(),
            PhysicalDiskPolicy::Expunged,
        )
        .await
        .unwrap();

    // Now, run the first part of region replacement: this will move the deleted
    // region into a temporary volume.

    let internal_client = &cptestctx.internal_client;

    let _ =
        activate_background_task(&internal_client, "region_replacement").await;

    // Assert there are no freed crucible regions that result from that
    assert!(datastore.find_deleted_volume_regions().await.unwrap().is_empty());
}

mod region_replacement {
    use super::*;

    #[derive(Debug)]
    struct ExpectedEndState(pub RegionReplacementState);

    #[derive(Debug)]
    struct ExpectedIntermediateState(pub RegionReplacementState);

    #[derive(Debug)]
    struct ExpectedStartState(pub RegionReplacementState);

    pub(super) struct DeletedVolumeTest<'a> {
        log: Logger,
        datastore: Arc<DataStore>,
        disk_test: DiskTest<'a>,
        client: ClientTestContext,
        internal_client: ClientTestContext,
        replacement_request_id: Uuid,
    }

    impl<'a> DeletedVolumeTest<'a> {
        pub async fn new(cptestctx: &'a ControlPlaneTestContext) -> Self {
            let nexus = &cptestctx.server.server_context().nexus;

            // Create four zpools, each with one dataset. This is required for
            // region and region snapshot replacement to have somewhere to move
            // the data.
            let disk_test = DiskTestBuilder::new(&cptestctx)
                .on_specific_sled(cptestctx.first_sled())
                .with_zpool_count(4)
                .build()
                .await;

            let client = &cptestctx.external_client;
            let internal_client = &cptestctx.internal_client;
            let datastore = nexus.datastore().clone();

            let opctx = OpContext::for_tests(
                cptestctx.logctx.log.new(o!()),
                datastore.clone(),
            );

            // Create a disk
            let _project_id = create_project_and_pool(client).await;

            let disk = create_disk(&client, PROJECT_NAME, "disk").await;

            // Manually create the region replacement request for the first
            // allocated region of that disk

            let (.., db_disk) = LookupPath::new(&opctx, &datastore)
                .disk_id(disk.identity.id)
                .fetch()
                .await
                .unwrap();

            assert_eq!(db_disk.id(), disk.identity.id);

            let disk_allocated_regions = datastore
                .get_allocated_regions(db_disk.volume_id)
                .await
                .unwrap();
            let (_, region) = &disk_allocated_regions[0];

            let replacement_request_id = datastore
                .create_region_replacement_request_for_region(&opctx, &region)
                .await
                .unwrap();

            // Assert the request is in state Requested

            let region_replacement = datastore
                .get_region_replacement_request_by_id(
                    &opctx,
                    replacement_request_id,
                )
                .await
                .unwrap();

            assert_eq!(
                region_replacement.replacement_state,
                RegionReplacementState::Requested,
            );

            DeletedVolumeTest {
                log: cptestctx.logctx.log.new(o!()),
                datastore,
                disk_test,
                client: client.clone(),
                internal_client: internal_client.clone(),
                replacement_request_id,
            }
        }

        pub fn opctx(&self) -> OpContext {
            OpContext::for_tests(self.log.clone(), self.datastore.clone())
        }

        pub async fn delete_the_disk(&self) {
            let disk_url = get_disk_url("disk");
            NexusRequest::object_delete(&self.client, &disk_url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .expect("failed to delete disk");
        }

        /// Make sure:
        ///
        /// - all region replacement related background tasks run to completion
        /// - this harness' region replacement request has transitioned to
        ///   Complete
        /// - no Crucible resources are leaked
        pub async fn finish_test(&self) {
            // Make sure that all the background tasks can run to completion.

            run_replacement_tasks_to_completion(&self.internal_client).await;

            // Assert the request is in state Complete

            let region_replacement = self
                .datastore
                .get_region_replacement_request_by_id(
                    &self.opctx(),
                    self.replacement_request_id,
                )
                .await
                .unwrap();

            assert_eq!(
                region_replacement.replacement_state,
                RegionReplacementState::Complete,
            );

            // Assert there are no more Crucible resources

            assert!(self.disk_test.crucible_resources_deleted().await);
        }

        async fn wait_for_request_state(
            &self,
            expected_end_state: ExpectedEndState,
            expected_intermediate_state: ExpectedIntermediateState,
            expected_start_state: ExpectedStartState,
        ) {
            wait_for_condition(
                || {
                    let datastore = self.datastore.clone();
                    let opctx = self.opctx();
                    let replacement_request_id = self.replacement_request_id;

                    async move {
                        let region_replacement = datastore
                            .get_region_replacement_request_by_id(
                                &opctx,
                                replacement_request_id,
                            )
                            .await
                            .unwrap();

                        let state = region_replacement.replacement_state;

                        if state == expected_end_state.0 {
                            // The saga transitioned the request ok
                            Ok(())
                        } else if state == expected_intermediate_state.0 {
                            // The saga is still running
                            Err(CondCheckError::<()>::NotYet)
                        // If the expected start and end state are the same,
                        // then it's impossible to determine when the saga
                        // starts and stops based on the state.
                        } else if expected_end_state.0 != expected_start_state.0
                            && state == expected_start_state.0
                        {
                            // The saga hasn't started yet
                            Err(CondCheckError::<()>::NotYet)
                        } else {
                            // Any other state is not expected
                            panic!("unexpected state {state:?}!");
                        }
                    }
                },
                &std::time::Duration::from_millis(500),
                &std::time::Duration::from_secs(60),
            )
            .await
            .expect("request transitioned to expected state");

            // Assert the request state

            let region_replacement = self
                .datastore
                .get_region_replacement_request_by_id(
                    &self.opctx(),
                    self.replacement_request_id,
                )
                .await
                .unwrap();

            assert_eq!(
                region_replacement.replacement_state,
                expected_end_state.0
            );
        }

        /// Run the "region replacement" task to transition the request to
        /// Running.
        pub async fn transition_request_to_running(&self) {
            // Activate the "region replacement" background task

            run_region_replacement(&self.internal_client).await;

            // The activation above could only have started the associated saga,
            // so wait until the request is in state Running.

            self.wait_for_request_state(
                ExpectedEndState(RegionReplacementState::Running),
                ExpectedIntermediateState(RegionReplacementState::Allocating),
                ExpectedStartState(RegionReplacementState::Requested),
            )
            .await;
        }

        /// Call the region replacement drive task to attach the associated volume
        /// to the simulated pantry, ostensibly for reconciliation
        pub async fn attach_request_volume_to_pantry(&self) {
            // Run the "region replacement driver" task to attach the associated
            // volume to the simulated pantry.

            run_region_replacement_driver(&self.internal_client).await;

            // The activation above could only have started the associated saga,
            // so wait until the request is in the expected end state.

            self.wait_for_request_state(
                ExpectedEndState(RegionReplacementState::Running),
                ExpectedIntermediateState(RegionReplacementState::Driving),
                ExpectedStartState(RegionReplacementState::Running),
            )
            .await;

            // Additionally, assert that the drive saga recorded that it sent
            // the attachment request to the simulated pantry.
            //
            // If `wait_for_request_state` has the same expected start and end
            // state (as it does above), it's possible to exit that function
            // having not yet started the saga yet, and this requires an
            // additional `wait_for_condition` to wait for the expected recorded
            // step.

            let most_recent_step = wait_for_condition(
                || {
                    let datastore = self.datastore.clone();
                    let opctx = self.opctx();
                    let replacement_request_id = self.replacement_request_id;

                    async move {
                        match datastore
                            .current_region_replacement_request_step(
                                &opctx,
                                replacement_request_id,
                            )
                            .await
                            .unwrap()
                        {
                            Some(step) => Ok(step),

                            None => {
                                // The saga either has not started yet or is
                                // still running - see the comment before this
                                // check for mroe info.
                                Err(CondCheckError::<()>::NotYet)
                            }
                        }
                    }
                },
                &std::time::Duration::from_millis(500),
                &std::time::Duration::from_secs(10),
            )
            .await
            .expect("most recent step");

            assert!(most_recent_step.pantry_address().is_some());
        }

        /// Manually activate the background attachment for the request volume
        pub async fn manually_activate_attached_volume(
            &self,
            cptestctx: &'a ControlPlaneTestContext,
        ) {
            let pantry = cptestctx
                .sled_agent
                .pantry_server
                .as_ref()
                .unwrap()
                .pantry
                .clone();

            let region_replacement = self
                .datastore
                .get_region_replacement_request_by_id(
                    &self.opctx(),
                    self.replacement_request_id,
                )
                .await
                .unwrap();

            pantry
                .activate_background_attachment(
                    region_replacement.volume_id.to_string(),
                )
                .await
                .unwrap();
        }

        /// Transition request to ReplacementDone via the region replacement
        /// drive saga
        pub async fn transition_request_to_replacement_done(&self) {
            // Run the "region replacement driver" task

            run_region_replacement_driver(&self.internal_client).await;

            // The activation above could only have started the associated saga,
            // so wait until the request is in the expected end state.

            self.wait_for_request_state(
                ExpectedEndState(RegionReplacementState::ReplacementDone),
                ExpectedIntermediateState(RegionReplacementState::Driving),
                ExpectedStartState(RegionReplacementState::Running),
            )
            .await;
        }
    }
}

/// Assert that a region replacement request in state "Requested" can have its
/// volume deleted and still transition to Complete
#[nexus_test]
async fn test_delete_volume_region_replacement_state_requested(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the `new` function in state Requested: delete the
    // disk, then finish the test.

    test_harness.delete_the_disk().await;

    test_harness.finish_test().await;
}

/// Assert that a region replacement request in state "Running" can have its
/// volume deleted and still transition to Complete
#[nexus_test]
async fn test_delete_volume_region_replacement_state_running(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the `new` function in state Requested:
    // - transition the request to "Running"
    // - delete the disk, then finish the test.

    test_harness.transition_request_to_running().await;

    test_harness.delete_the_disk().await;

    test_harness.finish_test().await;
}

/// Assert that a region replacement request in state "Running" that has
/// additionally had its volume attached to a Pantry can have its volume deleted
/// and still transition to Complete
#[nexus_test]
async fn test_delete_volume_region_replacement_state_running_on_pantry(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the `new` function in state Requested:
    // - transition the request to "Running"
    // - call the drive task to attach the volume to the simulated pantry
    // - delete the disk, then finish the test.

    test_harness.transition_request_to_running().await;

    test_harness.attach_request_volume_to_pantry().await;

    test_harness.delete_the_disk().await;

    test_harness.finish_test().await;
}

/// Assert that a region replacement request in state "ReplacementDone" can have
/// its volume deleted and still transition to Complete
#[nexus_test]
async fn test_delete_volume_region_replacement_state_replacement_done(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the `new` function in state Requested:
    // - transition the request to "Running"
    // - call the drive task to attach the volume to the simulated pantry
    // - simulate that the volume activated ok
    // - call the drive task again, which will observe that activation and
    //   transition the request to "ReplacementDone"
    // - delete the disk, then finish the test.

    test_harness.transition_request_to_running().await;

    test_harness.attach_request_volume_to_pantry().await;

    test_harness.manually_activate_attached_volume(&cptestctx).await;

    test_harness.transition_request_to_replacement_done().await;

    test_harness.delete_the_disk().await;

    test_harness.finish_test().await;
}

/// Assert that the problem experienced in issue 6353 is fixed
#[nexus_test]
async fn test_racing_replacements_for_soft_deleted_disk_volume(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create four zpools, each with one dataset. This is required for region
    // and region snapshot replacement to have somewhere to move the data.
    let sled_id = cptestctx.first_sled();
    let mut disk_test = DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(sled_id)
        .with_zpool_count(4)
        .build()
        .await;

    // Create a disk, then a snapshot of that disk
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "snapshot".parse().unwrap(),
                description: String::from("a snapshot"),
            },
            disk: disk.identity.name.into(),
        },
    )
    .await;

    // Before deleting the disk, save the DB model
    let (.., db_disk) = LookupPath::new(&opctx, &datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_disk.id(), disk.identity.id);

    // Next, expunge a physical disk that contains a region snapshot (which
    // means it'll have the region too)

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id).await.unwrap();
    let (dataset, region) = &disk_allocated_regions[0];
    let zpool = disk_test
        .zpools()
        .find(|x| *x.id.as_untyped_uuid() == dataset.pool_id)
        .expect("Expected at least one zpool");

    let (_, db_zpool) = LookupPath::new(&opctx, datastore)
        .zpool_id(zpool.id.into_untyped_uuid())
        .fetch()
        .await
        .unwrap();

    datastore
        .physical_disk_update_policy(
            &opctx,
            db_zpool.physical_disk_id.into(),
            PhysicalDiskPolicy::Expunged,
        )
        .await
        .unwrap();

    // Only one region snapshot should be been returned by the following call
    // due to the allocation policy.

    let expunged_region_snapshots = datastore
        .find_region_snapshots_on_expunged_physical_disks(&opctx)
        .await
        .unwrap();

    assert_eq!(expunged_region_snapshots.len(), 1);

    for expunged_region_snapshot in expunged_region_snapshots {
        assert_eq!(expunged_region_snapshot.snapshot_id, snapshot.identity.id);
    }

    // Either one or two regions can be returned, depending on if the snapshot
    // destination volume was allocated on to the physical disk that was
    // expunged.

    let expunged_regions = datastore
        .find_regions_on_expunged_physical_disks(&opctx)
        .await
        .unwrap();

    match expunged_regions.len() {
        1 => {
            assert_eq!(expunged_regions[0].id(), region.id());
        }

        2 => {
            assert!(expunged_regions.iter().any(|r| r.id() == region.id()));

            let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
                .snapshot_id(snapshot.identity.id)
                .fetch()
                .await
                .unwrap();

            let snapshot_allocated_datasets_and_regions = datastore
                .get_allocated_regions(db_snapshot.destination_volume_id)
                .await
                .unwrap();

            let snapshot_allocated_regions: Vec<Uuid> =
                snapshot_allocated_datasets_and_regions
                    .into_iter()
                    .map(|(_, r)| r.id())
                    .collect();

            assert!(expunged_regions.iter().any(|region| {
                snapshot_allocated_regions.contains(&region.id())
            }));
        }

        _ => {
            panic!("unexpected number of expunged regions!");
        }
    }

    // Now, race the region replacement with the region snapshot replacement:
    //
    // 1) region replacement will allocate a new region and swap it into the
    //    disk volume.

    let internal_client = &cptestctx.internal_client;

    let _ =
        activate_background_task(&internal_client, "region_replacement").await;

    // After that task invocation, there should be one running region
    // replacement for the disk's region. Filter out the replacement request for
    // the snapshot destination volume if it's there. The above background task
    // only starts the associated saga, so wait for it to complete.

    wait_for_condition(
        || {
            let opctx = OpContext::for_tests(
                cptestctx.logctx.log.new(o!()),
                datastore.clone(),
            );

            async move {
                let region_replacements: Vec<_> = datastore
                    .get_running_region_replacements(&opctx)
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(|x| x.old_region_id == region.id())
                    .collect();

                if region_replacements.len() == 1 {
                    // The saga transitioned the request ok
                    Ok(())
                } else {
                    // The saga is still running
                    Err(CondCheckError::<()>::NotYet)
                }
            }
        },
        &std::time::Duration::from_millis(500),
        &std::time::Duration::from_secs(20),
    )
    .await
    .expect("request transitioned to expected state");

    let region_replacements: Vec<_> = datastore
        .get_running_region_replacements(&opctx)
        .await
        .unwrap()
        .into_iter()
        .filter(|x| x.old_region_id == region.id())
        .collect();

    assert_eq!(region_replacements.len(), 1);

    // 2) region snapshot replacement start will replace the region snapshot in
    //    the snapshot volume

    let _ = activate_background_task(
        &internal_client,
        "region_snapshot_replacement_start",
    )
    .await;

    // After that, there should be one "replacement done" region snapshot
    // replacement for the associated region snapshot. The above background task
    // only starts the associated saga, so wait for it to complete.
    wait_for_condition(
        || {
            let opctx = OpContext::for_tests(
                cptestctx.logctx.log.new(o!()),
                datastore.clone(),
            );

            async move {
                let region_snapshot_replacements = datastore
                    .get_replacement_done_region_snapshot_replacements(&opctx)
                    .await
                    .unwrap();

                if region_snapshot_replacements.len() == 1 {
                    // The saga transitioned the request ok
                    Ok(())
                } else {
                    // The saga is still running
                    Err(CondCheckError::<()>::NotYet)
                }
            }
        },
        &std::time::Duration::from_millis(500),
        &std::time::Duration::from_secs(20),
    )
    .await
    .expect("request transitioned to expected state");

    let region_snapshot_replacements = datastore
        .get_replacement_done_region_snapshot_replacements(&opctx)
        .await
        .unwrap();

    assert_eq!(region_snapshot_replacements.len(), 1);
    assert_eq!(
        region_snapshot_replacements[0].old_dataset_id,
        dataset.id().into()
    );
    assert_eq!(region_snapshot_replacements[0].old_region_id, region.id());
    assert_eq!(
        region_snapshot_replacements[0].old_snapshot_id,
        snapshot.identity.id
    );
    assert_eq!(
        region_snapshot_replacements[0].replacement_state,
        RegionSnapshotReplacementState::ReplacementDone,
    );

    assert!(datastore.find_deleted_volume_regions().await.unwrap().is_empty());

    // 3) Delete the disk
    let disk_url = get_disk_url("disk");
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // The volume should be soft-deleted now. The region snapshot replacement
    // swapped out the region snapshot from the snapshot volume to the temporary
    // volume for later deletion, but has not actually deleted that temporary
    // volume yet, so the count will not have gone to 0.

    let volume = datastore.volume_get(db_disk.volume_id).await.unwrap();
    assert!(volume.is_some());
    assert!(volume.unwrap().time_deleted.is_some());

    // 4) region snapshot replacement garbage collect will delete the temporary
    //    volume with the stashed reference to the region snapshot, bringing the
    //    reference count to zero.

    let _ = activate_background_task(
        &internal_client,
        "region_snapshot_replacement_garbage_collection",
    )
    .await;

    // Assert the region snapshot was deleted.
    assert!(datastore
        .region_snapshot_get(dataset.id(), region.id(), snapshot.identity.id)
        .await
        .unwrap()
        .is_none());

    // Assert that the disk's volume is still only soft-deleted, because the two
    // other associated region snapshots still exist.
    let volume = datastore.volume_get(db_disk.volume_id).await.unwrap();
    assert!(volume.is_some());

    // Check on the old region id - it should not be deleted
    let maybe_region =
        datastore.get_region_optional(region.id()).await.unwrap();

    eprintln!("old_region_id: {:?}", &maybe_region);
    assert!(maybe_region.is_some());

    // But the new region id will be!
    let maybe_region = datastore
        .get_region_optional(region_replacements[0].new_region_id.unwrap())
        .await
        .unwrap();

    eprintln!("new region id: {:?}", &maybe_region);
    assert!(maybe_region.is_none());

    // The region_replacement drive task should invoke the drive saga now, which
    // will skip over all notification steps and transition the request to
    // ReplacementDone

    let last_background_task =
        activate_background_task(&internal_client, "region_replacement_driver")
            .await;

    assert!(match last_background_task.last {
        LastResult::Completed(last_result_completed) => {
            match serde_json::from_value::<RegionReplacementDriverStatus>(
                last_result_completed.details,
            ) {
                Err(e) => {
                    eprintln!("{e}");
                    false
                }

                Ok(v) => !v.drive_invoked_ok.is_empty(),
            }
        }

        _ => {
            false
        }
    });

    // wait for the drive saga to complete here
    wait_for_condition(
        || {
            let opctx = OpContext::for_tests(
                cptestctx.logctx.log.new(o!()),
                datastore.clone(),
            );
            let replacement_request_id = region_replacements[0].id;

            async move {
                let region_replacement = datastore
                    .get_region_replacement_request_by_id(
                        &opctx,
                        replacement_request_id,
                    )
                    .await
                    .unwrap();

                let state = region_replacement.replacement_state;

                if state == RegionReplacementState::ReplacementDone {
                    // The saga transitioned the request ok
                    Ok(())
                } else if state == RegionReplacementState::Driving {
                    // The saga is still running
                    Err(CondCheckError::<()>::NotYet)
                } else if state == RegionReplacementState::Completing {
                    // The saga transitioned the request ok, and it's now being
                    // finished by the region replacement finish saga
                    Ok(())
                } else {
                    // Any other state is not expected
                    panic!("unexpected state {state:?}!");
                }
            }
        },
        &std::time::Duration::from_millis(500),
        &std::time::Duration::from_secs(60),
    )
    .await
    .expect("request transitioned to expected state");

    // After the region snapshot replacement process runs to completion, there
    // should be no more crucible resources left. Run the "region snapshot
    // replacement step" background task until there's nothing left, then the
    // "region snapshot replacement finish", then make sure there are no
    // crucible resources left.

    let mut count = 0;
    loop {
        let actions_taken =
            run_region_snapshot_replacement_step(&internal_client).await;

        if actions_taken == 0 {
            break;
        }

        count += 1;

        if count > 20 {
            assert!(false);
        }
    }

    let _ = activate_background_task(
        &internal_client,
        "region_snapshot_replacement_finish",
    )
    .await;

    // Ensure the region snapshot replacement request went to Complete

    wait_for_condition(
        || {
            let opctx = OpContext::for_tests(
                cptestctx.logctx.log.new(o!()),
                datastore.clone(),
            );
            let request_id = region_snapshot_replacements[0].id;

            async move {
                let region_snapshot_replacement = datastore
                    .get_region_snapshot_replacement_request_by_id(
                        &opctx, request_id,
                    )
                    .await
                    .unwrap();

                let state = region_snapshot_replacement.replacement_state;

                if state == RegionSnapshotReplacementState::Complete {
                    Ok(())
                } else {
                    // Any other state is not expected
                    Err(CondCheckError::<()>::NotYet)
                }
            }
        },
        &std::time::Duration::from_millis(500),
        &std::time::Duration::from_secs(60),
    )
    .await
    .expect("request transitioned to expected state");

    // Delete the snapshot

    let snapshot_url = get_snapshot_url("snapshot");
    NexusRequest::object_delete(client, &snapshot_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    // and now there should be no higher level resources left

    let disks_url = get_disks_url();
    assert_eq!(
        collection_list::<external::Disk>(&client, &disks_url).await.len(),
        0
    );

    let snapshots_url = get_snapshots_url();
    assert_eq!(
        collection_list::<views::Snapshot>(&client, &snapshots_url).await.len(),
        0
    );

    // Make sure that all the background tasks can run to completion.

    run_replacement_tasks_to_completion(&internal_client).await;

    // The disk volume should be deleted by the snapshot delete: wait until this
    // happens

    wait_for_condition(
        || {
            let datastore = datastore.clone();
            let volume_id = db_disk.volume_id;

            async move {
                let volume = datastore.volume_get(volume_id).await.unwrap();
                if volume.is_none() {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            }
        },
        &std::time::Duration::from_millis(500),
        &std::time::Duration::from_secs(10),
    )
    .await
    .expect("disk volume deleted");

    // There should be no more crucible resources left. Don't just check for
    // `crucible_resources_deleted` here! We have set one of the physical disk
    // policies to expunged, so Nexus will not attempt to clean up any resources
    // on that physical disk.

    disk_test.remove_zpool(db_zpool.id()).await;

    // Now, assert that all crucible resources are cleaned up

    assert!(disk_test.crucible_resources_deleted().await);
}

mod region_snapshot_replacement {
    use super::*;

    #[derive(Debug)]
    struct ExpectedEndState(pub RegionSnapshotReplacementState);

    #[derive(Debug)]
    struct ExpectedIntermediateState(pub RegionSnapshotReplacementState);

    #[derive(Debug)]
    struct ExpectedStartState(pub RegionSnapshotReplacementState);

    pub(super) struct DeletedVolumeTest<'a> {
        log: Logger,
        datastore: Arc<DataStore>,
        disk_test: DiskTest<'a>,
        client: ClientTestContext,
        internal_client: ClientTestContext,
        replacement_request_id: Uuid,
        snapshot_socket_addr: SocketAddr,
    }

    impl<'a> DeletedVolumeTest<'a> {
        pub async fn new(cptestctx: &'a ControlPlaneTestContext) -> Self {
            let nexus = &cptestctx.server.server_context().nexus;

            // Create four zpools, each with one dataset. This is required for
            // region and region snapshot replacement to have somewhere to move
            // the data.
            let disk_test = DiskTestBuilder::new(&cptestctx)
                .on_specific_sled(cptestctx.first_sled())
                .with_zpool_count(4)
                .build()
                .await;

            let client = &cptestctx.external_client;
            let internal_client = &cptestctx.internal_client;
            let datastore = nexus.datastore().clone();

            let opctx = OpContext::for_tests(
                cptestctx.logctx.log.new(o!()),
                datastore.clone(),
            );

            // Create a disk, a snapshot of that disk, and a disk from that
            // snapshot
            let _project_id = create_project_and_pool(client).await;

            let disk = create_disk(&client, PROJECT_NAME, "disk").await;

            let snapshot =
                create_snapshot(&client, PROJECT_NAME, "disk", "snapshot")
                    .await;

            let disk_from_snapshot = create_disk_from_snapshot(
                &client,
                PROJECT_NAME,
                "disk-from-snapshot",
                snapshot.identity.id,
            )
            .await;

            // Manually create the region snapshot replacement request for the
            // first allocated region of that disk

            let (.., db_disk) = LookupPath::new(&opctx, &datastore)
                .disk_id(disk.identity.id)
                .fetch()
                .await
                .unwrap();

            assert_eq!(db_disk.id(), disk.identity.id);

            let disk_allocated_regions = datastore
                .get_allocated_regions(db_disk.volume_id)
                .await
                .unwrap();
            let (_, region) = &disk_allocated_regions[0];

            let region_snapshot = datastore
                .region_snapshot_get(
                    region.dataset_id(),
                    region.id(),
                    snapshot.identity.id,
                )
                .await
                .expect("found region snapshot without error")
                .unwrap();

            let replacement_request_id = datastore
                .create_region_snapshot_replacement_request(
                    &opctx,
                    &region_snapshot,
                )
                .await
                .unwrap();

            // Assert the request is in state Requested

            let region_snapshot_replacement = datastore
                .get_region_snapshot_replacement_request_by_id(
                    &opctx,
                    replacement_request_id,
                )
                .await
                .unwrap();

            assert_eq!(
                region_snapshot_replacement.replacement_state,
                RegionSnapshotReplacementState::Requested,
            );

            // Assert two volumes reference the snapshot addr

            let snapshot_socket_addr =
                region_snapshot.snapshot_addr.parse().unwrap();

            let volumes = datastore
                .find_volumes_referencing_socket_addr(
                    &opctx,
                    snapshot_socket_addr,
                )
                .await
                .unwrap();

            assert_eq!(volumes.len(), 2);

            // Validate that they are snapshot and disk from snapshot

            let volumes_set: HashSet<Uuid> =
                volumes.into_iter().map(|v| v.id()).collect();

            let (.., db_snapshot) = LookupPath::new(&opctx, &datastore)
                .snapshot_id(snapshot.identity.id)
                .fetch()
                .await
                .unwrap();

            let (.., db_disk_from_snapshot) =
                LookupPath::new(&opctx, &datastore)
                    .disk_id(disk_from_snapshot.identity.id)
                    .fetch()
                    .await
                    .unwrap();

            assert!(volumes_set.contains(&db_snapshot.volume_id));
            assert!(volumes_set.contains(&db_disk_from_snapshot.volume_id));

            DeletedVolumeTest {
                log: cptestctx.logctx.log.new(o!()),
                datastore,
                disk_test,
                client: client.clone(),
                internal_client: internal_client.clone(),
                replacement_request_id,
                snapshot_socket_addr,
            }
        }

        pub fn opctx(&self) -> OpContext {
            OpContext::for_tests(self.log.clone(), self.datastore.clone())
        }

        pub async fn delete_the_disk(&self) {
            let disk_url = get_disk_url("disk");
            NexusRequest::object_delete(&self.client, &disk_url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .expect("failed to delete disk");
        }

        pub async fn delete_the_snapshot(&self) {
            let snapshot_url = get_snapshot_url("snapshot");
            NexusRequest::object_delete(&self.client, &snapshot_url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .expect("failed to delete snapshot");
        }

        pub async fn delete_the_disk_from_snapshot(&self) {
            let disk_url = get_disk_url("disk-from-snapshot");
            NexusRequest::object_delete(&self.client, &disk_url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .expect("failed to delete disk-from-snapshot");
        }

        /// Make sure:
        ///
        /// - all region snapshot replacement related background tasks run to
        ///   completion
        /// - this harness' region snapshot replacement request has transitioned
        ///   to Complete
        /// - there are no more volumes that reference the request's region
        ///   snapshot
        pub async fn finish_test(&self) {
            // Make sure that all the background tasks can run to completion.

            run_replacement_tasks_to_completion(&self.internal_client).await;

            // Assert the request is in state Complete

            wait_for_condition(
                || {
                    let datastore = self.datastore.clone();
                    let opctx = self.opctx();
                    let replacement_request_id = self.replacement_request_id;

                    async move {
                        let region_replacement = datastore
                            .get_region_snapshot_replacement_request_by_id(
                                &opctx,
                                replacement_request_id,
                            )
                            .await
                            .unwrap();

                        let state = region_replacement.replacement_state;

                        if state == RegionSnapshotReplacementState::Complete {
                            // The saga transitioned the request ok
                            Ok(())
                        } else {
                            // The saga is still running
                            Err(CondCheckError::<()>::NotYet)
                        }
                    }
                },
                &std::time::Duration::from_millis(500),
                &std::time::Duration::from_secs(10),
            )
            .await
            .expect("request transitioned to expected state");

            let region_snapshot_replacement = self
                .datastore
                .get_region_snapshot_replacement_request_by_id(
                    &self.opctx(),
                    self.replacement_request_id,
                )
                .await
                .unwrap();

            assert_eq!(
                region_snapshot_replacement.replacement_state,
                RegionSnapshotReplacementState::Complete,
            );

            // Assert no volumes are referencing the snapshot address

            let volumes = self
                .datastore
                .find_volumes_referencing_socket_addr(
                    &self.opctx(),
                    self.snapshot_socket_addr,
                )
                .await
                .unwrap();

            if !volumes.is_empty() {
                eprintln!("{:?}", volumes);
            }

            assert!(volumes.is_empty());
        }

        /// Assert no Crucible resources are leaked
        pub async fn assert_no_crucible_resources_leaked(&self) {
            assert!(self.disk_test.crucible_resources_deleted().await);
        }

        async fn wait_for_request_state(
            &self,
            expected_end_state: ExpectedEndState,
            expected_intermediate_state: ExpectedIntermediateState,
            expected_start_state: ExpectedStartState,
        ) {
            wait_for_condition(
                || {
                    let datastore = self.datastore.clone();
                    let opctx = self.opctx();
                    let replacement_request_id = self.replacement_request_id;

                    async move {
                        let request = datastore
                            .get_region_snapshot_replacement_request_by_id(
                                &opctx,
                                replacement_request_id,
                            )
                            .await
                            .unwrap();

                        let state = request.replacement_state;

                        if state == expected_end_state.0 {
                            // The saga transitioned the request ok
                            Ok(())
                        } else if state == expected_intermediate_state.0 {
                            // The saga is still running
                            Err(CondCheckError::<()>::NotYet)
                        } else if state == expected_start_state.0 {
                            // The saga hasn't started yet
                            Err(CondCheckError::<()>::NotYet)
                        } else {
                            // Any other state is not expected
                            panic!("unexpected state {state:?}!");
                        }
                    }
                },
                &std::time::Duration::from_millis(500),
                &std::time::Duration::from_secs(60),
            )
            .await
            .expect("request transitioned to expected state");

            // Assert the request state

            let region_snapshot_replacement = self
                .datastore
                .get_region_snapshot_replacement_request_by_id(
                    &self.opctx(),
                    self.replacement_request_id,
                )
                .await
                .unwrap();

            assert_eq!(
                region_snapshot_replacement.replacement_state,
                expected_end_state.0,
            );
        }

        /// Run the "region snapshot replacement" task to transition the request
        /// to ReplacementDone.
        pub async fn transition_request_to_replacement_done(&self) {
            // Activate the "region snapshot replacement start" background task

            run_region_snapshot_replacement_start(&self.internal_client).await;

            // The activation above could only have started the associated saga,
            // so wait until the request is in state Running.

            self.wait_for_request_state(
                ExpectedEndState(
                    RegionSnapshotReplacementState::ReplacementDone,
                ),
                ExpectedIntermediateState(
                    RegionSnapshotReplacementState::Allocating,
                ),
                ExpectedStartState(RegionSnapshotReplacementState::Requested),
            )
            .await;
        }

        /// Run the "region snapshot replacement garbage collection" task to
        /// transition the request to Running.
        pub async fn transition_request_to_running(&self) {
            // Activate the "region snapshot replacement garbage collection"
            // background task

            run_region_snapshot_replacement_garbage_collection(
                &self.internal_client,
            )
            .await;

            // The activation above could only have started the associated saga,
            // so wait until the request is in state Running.

            self.wait_for_request_state(
                ExpectedEndState(RegionSnapshotReplacementState::Running),
                ExpectedIntermediateState(
                    RegionSnapshotReplacementState::DeletingOldVolume,
                ),
                ExpectedStartState(
                    RegionSnapshotReplacementState::ReplacementDone,
                ),
            )
            .await;
        }

        /// Manually create a region snapshot replacement step for the disk
        /// created from the snapshot
        pub async fn create_manual_region_snapshot_replacement_step(&self) {
            let disk_url = get_disk_url("disk-from-snapshot");

            let disk_from_snapshot: external::Disk =
                NexusRequest::object_get(&self.client, &disk_url)
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .unwrap()
                    .parsed_body()
                    .unwrap();

            let (.., db_disk_from_snapshot) =
                LookupPath::new(&self.opctx(), &self.datastore)
                    .disk_id(disk_from_snapshot.identity.id)
                    .fetch()
                    .await
                    .unwrap();

            let result = self
                .datastore
                .create_region_snapshot_replacement_step(
                    &self.opctx(),
                    self.replacement_request_id,
                    db_disk_from_snapshot.volume_id,
                )
                .await
                .unwrap();

            match result {
                InsertStepResult::Inserted { .. } => {}

                _ => {
                    assert!(false, "bad result from create_region_snapshot_replacement_step");
                }
            }
        }
    }
}

/// Assert that a region snapshot replacement request in state "Requested" can
/// have its snapshot deleted and still transition to Complete
#[nexus_test]
async fn test_delete_volume_region_snapshot_replacement_state_requested(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_snapshot_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the above `new` function in state Requested: delete
    // the snapshot, then finish the test.

    test_harness.delete_the_snapshot().await;

    test_harness.finish_test().await;

    // Delete all the non-deleted resources

    test_harness.delete_the_disk().await;
    test_harness.delete_the_disk_from_snapshot().await;

    // Assert there are no more Crucible resources

    test_harness.assert_no_crucible_resources_leaked().await;
}

/// Assert that a region snapshot replacement request in state "Requested" can
/// have its snapshot deleted, and the snapshot's source disk can be deleted,
/// and the request will still transition to Complete
#[nexus_test]
async fn test_delete_volume_region_snapshot_replacement_state_requested_2(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_snapshot_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the above `new` function in state Requested:
    // - delete the snapshot
    // - delete the snapshot's source disk
    // - the only thing that will remain is the disk-from-snap that was created
    // - finally, call finish_test

    test_harness.delete_the_snapshot().await;
    test_harness.delete_the_disk().await;

    test_harness.finish_test().await;

    // Delete all the non-deleted resources

    test_harness.delete_the_disk_from_snapshot().await;

    // Assert there are no more Crucible resources

    test_harness.assert_no_crucible_resources_leaked().await;
}

/// Assert that a region snapshot replacement request in state "Requested" can
/// have everything be deleted, and the request will still transition to
/// Complete
#[nexus_test]
async fn test_delete_volume_region_snapshot_replacement_state_requested_3(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_snapshot_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the above `new` function in state Requested:
    // - delete the snapshot
    // - delete the snapshot's source disk
    // - delete the disk created from the snapshot
    // - finally, call finish_test

    test_harness.delete_the_snapshot().await;
    test_harness.delete_the_disk().await;
    test_harness.delete_the_disk_from_snapshot().await;

    test_harness.finish_test().await;

    // Assert there are no more Crucible resources

    test_harness.assert_no_crucible_resources_leaked().await;
}

/// Assert that a region snapshot replacement request in state "ReplacementDone"
/// can have its snapshot deleted, and the request will still transition to
/// Complete
#[nexus_test]
async fn test_delete_volume_region_snapshot_replacement_state_replacement_done(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_snapshot_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the above `new` function in state Requested:
    // - transition the request to "ReplacementDone"
    // - delete the snapshot
    // - finally, call finish_test

    test_harness.transition_request_to_replacement_done().await;

    test_harness.delete_the_snapshot().await;
    test_harness.delete_the_disk().await;

    test_harness.finish_test().await;

    // Delete all the non-deleted resources

    test_harness.delete_the_disk_from_snapshot().await;

    // Assert there are no more Crucible resources

    test_harness.assert_no_crucible_resources_leaked().await;
}

/// Assert that a region snapshot replacement request in state "Running"
/// can have its snapshot deleted, and the request will still transition to
/// Complete
#[nexus_test]
async fn test_delete_volume_region_snapshot_replacement_state_running(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_snapshot_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the above `new` function in state Requested:
    // - transition the request to "ReplacementDone"
    // - transition the request to "Running"
    // - delete the snapshot
    // - finally, call finish_test

    test_harness.transition_request_to_replacement_done().await;
    test_harness.transition_request_to_running().await;

    test_harness.delete_the_snapshot().await;
    test_harness.delete_the_disk().await;

    test_harness.finish_test().await;

    // Delete all the non-deleted resources

    test_harness.delete_the_disk_from_snapshot().await;

    // Assert there are no more Crucible resources

    test_harness.assert_no_crucible_resources_leaked().await;
}

/// Assert that a region snapshot replacement step can have its associated
/// volume deleted and still transition to VolumeDeleted
#[nexus_test]
async fn test_delete_volume_region_snapshot_replacement_step(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_snapshot_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the above `new` function in state Requested:
    // - transition the request to "ReplacementDone"
    // - transition the request to "Running"
    // - manually create a region snapshot replacement step for the disk created
    //   from the snapshot
    // - delete the disk created from the snapshot
    // - finally, call finish_test

    test_harness.transition_request_to_replacement_done().await;
    test_harness.transition_request_to_running().await;

    test_harness.create_manual_region_snapshot_replacement_step().await;
    test_harness.delete_the_disk_from_snapshot().await;

    test_harness.finish_test().await;

    // Delete all the non-deleted resources

    test_harness.delete_the_disk().await;
    test_harness.delete_the_snapshot().await;

    // Assert there are no more Crucible resources

    test_harness.assert_no_crucible_resources_leaked().await;
}
