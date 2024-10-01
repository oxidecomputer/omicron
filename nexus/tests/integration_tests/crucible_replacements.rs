// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests related to region and region snapshot replacement

use dropshot::test_util::ClientTestContext;
use nexus_db_model::PhysicalDiskPolicy;
use nexus_db_model::RegionReplacementState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use nexus_test_utils::background::*;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils_macros::nexus_test;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use omicron_uuid_kinds::GenericUuid;
use slog::Logger;
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

async fn create_project_and_pool(client: &ClientTestContext) -> Uuid {
    create_default_ip_pool(client).await;
    let project = create_project(client, PROJECT_NAME).await;
    project.identity.id
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
            db_zpool.physical_disk_id,
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

struct RegionReplacementDeletedVolumeTest<'a> {
    log: Logger,
    datastore: Arc<DataStore>,
    disk_test: DiskTest<'a>,
    client: ClientTestContext,
    internal_client: ClientTestContext,
    replacement_request_id: Uuid,
}

#[derive(Debug)]
struct ExpectedEndState(pub RegionReplacementState);

#[derive(Debug)]
struct ExpectedIntermediateState(pub RegionReplacementState);

impl<'a> RegionReplacementDeletedVolumeTest<'a> {
    pub async fn new(cptestctx: &'a ControlPlaneTestContext) -> Self {
        let nexus = &cptestctx.server.server_context().nexus;

        // Create four zpools, each with one dataset. This is required for
        // region and region snapshot replacement to have somewhere to move the
        // data.
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

        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id).await.unwrap();
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

        RegionReplacementDeletedVolumeTest {
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
    /// - this harness' region replacement request has transitioned to Complete
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

        assert_eq!(region_replacement.replacement_state, expected_end_state.0);
    }

    /// Run the "region replacement" task to transition the request to Running.
    pub async fn transition_request_to_running(&self) {
        // Activate the "region replacement" background task

        run_region_replacement(&self.internal_client).await;

        // The activation above could only have started the associated saga, so
        // wait until the request is in state Running.

        self.wait_for_request_state(
            ExpectedEndState(RegionReplacementState::Running),
            ExpectedIntermediateState(RegionReplacementState::Allocating),
        )
        .await;
    }

    /// Call the region replacement drive task to attach the associated volume
    /// to the simulated pantry, ostensibly for reconciliation
    pub async fn attach_request_volume_to_pantry(&self) {
        // Run the "region replacement driver" task to attach the associated
        // volume to the simulated pantry.

        run_region_replacement_driver(&self.internal_client).await;

        // The activation above could only have started the associated saga, so
        // wait until the request is in the expected end state.

        self.wait_for_request_state(
            ExpectedEndState(RegionReplacementState::Running),
            ExpectedIntermediateState(RegionReplacementState::Driving),
        )
        .await;

        // Additionally, assert that the drive saga recorded that it sent the
        // attachment request to the simulated pantry

        let most_recent_step = self
            .datastore
            .current_region_replacement_request_step(
                &self.opctx(),
                self.replacement_request_id,
            )
            .await
            .unwrap()
            .unwrap();

        assert!(most_recent_step.pantry_address().is_some());
    }

    /// Manually activate the background attachment for the request volume
    pub async fn manually_activate_attached_volume(
        &self,
        cptestctx: &'a ControlPlaneTestContext,
    ) {
        let pantry =
            cptestctx.sled_agent.pantry_server.as_ref().unwrap().pantry.clone();

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

    /// Transition request to ReplacementDone via the region replacement drive
    /// saga
    pub async fn transition_request_to_replacement_done(&self) {
        // Run the "region replacement driver" task

        run_region_replacement_driver(&self.internal_client).await;

        // The activation above could only have started the associated saga, so
        // wait until the request is in the expected end state.

        self.wait_for_request_state(
            ExpectedEndState(RegionReplacementState::ReplacementDone),
            ExpectedIntermediateState(RegionReplacementState::Driving),
        )
        .await;
    }
}

/// Assert that a region replacement request in state "Requested" can have its
/// volume deleted and still transition to Complete
#[nexus_test]
async fn test_delete_volume_region_replacement_state_requested(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness = RegionReplacementDeletedVolumeTest::new(cptestctx).await;

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
    let test_harness = RegionReplacementDeletedVolumeTest::new(cptestctx).await;

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
    let test_harness = RegionReplacementDeletedVolumeTest::new(cptestctx).await;

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
    let test_harness = RegionReplacementDeletedVolumeTest::new(cptestctx).await;

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
