// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests related to region and region snapshot replacement

use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use dropshot::test_util::ClientTestContext;
use nexus_client::types::LastResult;
use nexus_db_lookup::LookupPath;
use nexus_db_model::PhysicalDiskPolicy;
use nexus_db_model::ReadOnlyTargetReplacement;
use nexus_db_model::RegionReplacementState;
use nexus_db_model::RegionSnapshotReplacementState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::region_snapshot_replacement::*;
use nexus_test_utils::background::*;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_disk_from_snapshot;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_project_image_from_snapshot;
use nexus_test_utils::resource_helpers::create_snapshot;
use nexus_test_utils::resource_helpers::delete_snapshot;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::*;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::VolumeUuid;
use slog::Logger;
use slog::info;
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

pub(crate) async fn wait_for_all_replacements(
    datastore: &Arc<DataStore>,
    internal_client: &ClientTestContext,
) {
    wait_for_condition(
        || {
            let datastore = datastore.clone();
            let opctx = OpContext::for_tests(
                internal_client.client_log.new(o!()),
                datastore.clone(),
            );

            async move {
                // Trigger all the replacement related background tasks. These
                // will add replacement requests to the database and trigger the
                // associated sagas to push the replacements forward. Bail out
                // of this loop only when there's no more resources on expunged
                // physical disks.
                //
                // Be careful not to check if the background tasks performed any
                // actions, or if there are in-progress replacement requests:
                // the fixed point that we're waiting for is for all Crucible
                // resources to be moved to non-expunged physical disks.
                // Detecting whether or not there's an in-progress replacement
                // can tell you that something is _currently_ moving but not
                // that all work is done.

                run_all_crucible_replacement_tasks(internal_client).await;

                let ro_left_to_do = datastore
                    .find_read_only_regions_on_expunged_physical_disks(&opctx)
                    .await
                    .unwrap()
                    .len();

                let rw_left_to_do = datastore
                    .find_read_write_regions_on_expunged_physical_disks(&opctx)
                    .await
                    .unwrap()
                    .len();

                let rs_left_to_do = datastore
                    .find_region_snapshots_on_expunged_physical_disks(&opctx)
                    .await
                    .unwrap()
                    .len();

                if ro_left_to_do + rw_left_to_do + rs_left_to_do > 0 {
                    info!(
                        &internal_client.client_log,
                        "wait_for_all_replacements: ro {} rw {} rs {}",
                        ro_left_to_do,
                        rw_left_to_do,
                        rs_left_to_do,
                    );

                    return Err(CondCheckError::<()>::NotYet);
                }

                // Now that all resources have been moved, wait for the requests
                // to all transition to complete.

                let conn = datastore.pool_connection_for_tests().await.unwrap();

                let region_replacement_left = {
                    use nexus_db_schema::schema::region_replacement::dsl;

                    dsl::region_replacement
                        .filter(
                            dsl::replacement_state
                                .ne(RegionReplacementState::Complete),
                        )
                        .select(diesel::dsl::count_star())
                        .first_async::<i64>(&*conn)
                        .await
                        .unwrap()
                };

                let region_snapshot_replacement_left = {
                    use nexus_db_schema::schema::region_snapshot_replacement::dsl;

                    dsl::region_snapshot_replacement
                        .filter(
                            dsl::replacement_state
                                .ne(RegionSnapshotReplacementState::Complete),
                        )
                        .select(diesel::dsl::count_star())
                        .first_async::<i64>(&*conn)
                        .await
                        .unwrap()
                };

                if region_replacement_left + region_snapshot_replacement_left
                    > 0
                {
                    info!(
                        &internal_client.client_log,
                        "wait_for_all_replacements: rr {} rsr {}",
                        region_replacement_left,
                        region_snapshot_replacement_left,
                    );

                    return Err(CondCheckError::<()>::NotYet);
                }

                Ok(())
            }
        },
        &std::time::Duration::from_millis(50),
        &std::time::Duration::from_secs(260),
    )
    .await
    .expect("all replacements finished");
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
    let sled_id = cptestctx.first_sled_id();
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
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_disk.id(), disk.identity.id);

    // Next, expunge a physical disk that contains a region

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
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

            // Create one zpool per sled, each with one dataset. This is
            // required for region and region snapshot replacement to have
            // somewhere to move the data.
            let disk_test = DiskTestBuilder::new(&cptestctx)
                .on_all_sleds()
                .with_zpool_count(1)
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
                .get_allocated_regions(db_disk.volume_id())
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
            eprintln!("Delete this disk: {:?}", disk_url);
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

            wait_for_all_replacements(&self.datastore, &self.internal_client)
                .await;

            // Assert the request is in state Complete

            eprintln!(
                "Waited for all replacements, including  {:?}",
                self.replacement_request_id
            );
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

                        // If the expected start and end state are the same
                        // (i.e. there's a back edge in the associated request's
                        // state machine), then it's impossible to determine
                        // when the saga starts and stops based on the state.
                        if expected_end_state.0 == expected_start_state.0 {
                            if state == expected_end_state.0 {
                                // The saga transitioned the request ok, or
                                // hasn't started yet. Either way we have to
                                // return here, and the call site should perform
                                // an additional check for some associated
                                // expected result.
                                Ok(())
                            } else if state == expected_intermediate_state.0 {
                                // The saga is still running
                                Err(CondCheckError::<()>::NotYet)
                            } else {
                                // Any other state is not expected
                                panic!("unexpected state {state:?}!");
                            }
                        } else {
                            if state == expected_end_state.0 {
                                // The saga transitioned the request ok
                                Ok(())
                            } else if state == expected_intermediate_state.0
                                || state == expected_start_state.0
                            {
                                // The saga is still running, or hasn't started
                                // yet.
                                Err(CondCheckError::<()>::NotYet)
                            } else {
                                // Any other state is not expected
                                panic!("unexpected state {state:?}!");
                            }
                        }
                    }
                },
                &std::time::Duration::from_millis(50),
                &std::time::Duration::from_secs(60),
            )
            .await
            .expect("request transitioned to expected state");

            // Assert the request state (if the expected start state is
            // different from the expected end state)

            if expected_start_state.0 != expected_end_state.0 {
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
                                // check for more info.
                                Err(CondCheckError::<()>::NotYet)
                            }
                        }
                    }
                },
                &std::time::Duration::from_millis(50),
                &std::time::Duration::from_secs(30),
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
                .first_sim_server()
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
#[nexus_test(extra_sled_agents = 3)]
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
#[nexus_test(extra_sled_agents = 3)]
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
#[nexus_test(extra_sled_agents = 3)]
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
#[nexus_test(extra_sled_agents = 3)]
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
#[nexus_test(extra_sled_agents = 3)]
async fn test_racing_replacements_for_soft_deleted_disk_volume(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool per sled, each with one dataset. This is required for
    // region and region snapshot replacement to have somewhere to move the
    // data.
    let mut disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
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
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_disk.id(), disk.identity.id);

    // Next, expunge a physical disk that contains a region snapshot (which
    // means it'll have the region too)

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
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

    // Either one or two read/write regions will be returned:
    //
    // - one for the disk, and
    // - one for the snapshot destination volume, depending on if it was
    //   allocated on to the physical disk that was expunged.

    let expunged_regions = datastore
        .find_read_write_regions_on_expunged_physical_disks(&opctx)
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
                .get_allocated_regions(db_snapshot.destination_volume_id())
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
        &std::time::Duration::from_millis(50),
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
        &std::time::Duration::from_millis(50),
        &std::time::Duration::from_secs(20),
    )
    .await
    .expect("request transitioned to expected state");

    let region_snapshot_replacements = datastore
        .get_replacement_done_region_snapshot_replacements(&opctx)
        .await
        .unwrap();

    assert_eq!(region_snapshot_replacements.len(), 1);

    let ReadOnlyTargetReplacement::RegionSnapshot {
        dataset_id: replacement_dataset_id,
        region_id: replacement_region_id,
        snapshot_id: replacement_snapshot_id,
    } = region_snapshot_replacements[0].replacement_type()
    else {
        panic!("wrong replacement type!");
    };

    assert_eq!(replacement_dataset_id, dataset.id().into());
    assert_eq!(replacement_region_id, region.id());
    assert_eq!(replacement_snapshot_id, snapshot.identity.id);
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

    let volume = datastore.volume_get(db_disk.volume_id()).await.unwrap();
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
    wait_for_condition(
        || {
            let dataset_id = dataset.id();
            let region_id = region.id();
            let snapshot_id = snapshot.identity.id;

            async move {
                let region_snapshot = datastore
                    .region_snapshot_get(dataset_id, region_id, snapshot_id)
                    .await
                    .unwrap();

                match region_snapshot {
                    Some(_) => {
                        // Region snapshot not garbage collected yet
                        Err(CondCheckError::<()>::NotYet)
                    }

                    None => {
                        // Region snapshot garbage collected ok
                        Ok(())
                    }
                }
            }
        },
        &std::time::Duration::from_millis(50),
        &std::time::Duration::from_secs(60),
    )
    .await
    .expect("region snapshot garbage collected");

    // Assert that the disk's volume is still only soft-deleted, because the two
    // other associated region snapshots still exist.
    let volume = datastore.volume_get(db_disk.volume_id()).await.unwrap();
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

    let res = match last_background_task.last {
        LastResult::Completed(last_result_completed) => {
            match serde_json::from_value::<RegionReplacementDriverStatus>(
                last_result_completed.details,
            ) {
                Err(e) => {
                    eprintln!("Json not what we expected");
                    eprintln!("{e}");
                    false
                }

                Ok(v) => {
                    if !v.drive_invoked_ok.is_empty() {
                        true
                    } else {
                        eprintln!("v.drive_ok: {:?}", v.drive_invoked_ok);
                        false
                    }
                }
            }
        }
        x => {
            eprintln!("Unexpected result here: {:?}", x);
            false
        }
    };
    assert!(res);

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
                    // The drive saga is still running
                    Err(CondCheckError::<()>::NotYet)
                } else if state == RegionReplacementState::Running {
                    // The drive saga hasn't started yet
                    Err(CondCheckError::<()>::NotYet)
                } else if state == RegionReplacementState::Completing {
                    // The saga transitioned the request ok, and it's now being
                    // finished by the region replacement finish saga
                    Ok(())
                } else if state == RegionReplacementState::Complete {
                    // The saga transitioned the request ok, and it was finished
                    // by the region replacement finish saga
                    Ok(())
                } else {
                    // Any other state is not expected
                    panic!("unexpected state {state:?}!");
                }
            }
        },
        &std::time::Duration::from_millis(50),
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
        &std::time::Duration::from_millis(50),
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

    wait_for_all_replacements(datastore, &internal_client).await;

    // The disk volume should be deleted by the snapshot delete: wait until this
    // happens

    wait_for_condition(
        || {
            let datastore = datastore.clone();
            let volume_id = db_disk.volume_id();

            async move {
                let volume = datastore.volume_get(volume_id).await.unwrap();
                if volume.is_none() {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            }
        },
        &std::time::Duration::from_millis(50),
        &std::time::Duration::from_secs(30),
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

            // Create one zpool per sled, each with one dataset. This is
            // required for region and region snapshot replacement to have
            // somewhere to move the data.
            let disk_test = DiskTestBuilder::new(&cptestctx)
                .on_all_sleds()
                .with_zpool_count(1)
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
                .get_allocated_regions(db_disk.volume_id())
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

            let volumes_set: HashSet<VolumeUuid> =
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

            assert!(volumes_set.contains(&db_snapshot.volume_id()));
            assert!(volumes_set.contains(&db_disk_from_snapshot.volume_id()));

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

            wait_for_all_replacements(&self.datastore, &self.internal_client)
                .await;

            // Assert the request is in state Complete

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

            let mut counter = 1;
            loop {
                let volumes = self
                    .datastore
                    .find_volumes_referencing_socket_addr(
                        &self.opctx(),
                        self.snapshot_socket_addr,
                    )
                    .await
                    .unwrap();

                if !volumes.is_empty() {
                    eprintln!("Volume should be gone, try {counter} {:?}", volumes);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    counter += 1;
                } else {
                    break;
                }
            }
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
                &std::time::Duration::from_millis(50),
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
                    db_disk_from_snapshot.volume_id(),
                )
                .await
                .unwrap();

            // ZZZ: is "AlreadyHandled" an error here?
            // Could that be a valid result if some other actor put the
            // replacement step into place?
            // We get back:
            // bad result: AlreadyHandled { existing_step_id: 83e38140-f238-4fed-8cef-58121d507a49
            // }
            // Can we dump an existing ID and get more info from it?
            match result {
                InsertStepResult::Inserted { .. } => {}

                InsertStepResult::AlreadyHandled { existing_step_id } => {
                    let region_snapshot_replace_request = self
                        .datastore
                        .get_region_snapshot_replacement_request_by_id(
                            &self.opctx(),
                            existing_step_id,
                        )
                        .await
                        .unwrap();
                    eprintln!(
                        "we were suppose to create this: {:?}",
                        region_snapshot_replace_request
                    );
                    panic!("Something else created our replacement");
                }
            }
        }

        pub async fn assert_read_only_target_gone(&self) {
            let mut failed = false;
            eprintln!(
                "NOW1 starting, replace_request_id: {:?}",
                self.replacement_request_id
            );
            let mut i = 1;
            loop {
                let region_snapshot_replace_request = self
                    .datastore
                    .get_region_snapshot_replacement_request_by_id(
                        &self.opctx(),
                        self.replacement_request_id,
                    )
                    .await
                    .unwrap();
                eprintln!(
                    "NOW2 rs_replace_request: {:?}",
                    region_snapshot_replace_request
                );

                let res = self
                    .datastore
                    .read_only_target_addr(&region_snapshot_replace_request)
                    .await
                    .unwrap();

                eprintln!("NOW3 target that should be gone: {:?}", res);
                if res.is_none() {
                    // test pass, move on
                    break;
                }
                failed = true;
                eprintln!("loop {i}, snapshot that should be gone: {:?}", res);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                i += 1;
            }

            if failed {
                panic!("failed {i} times checking for target gone");
            }
        }
        pub async fn pre_assert_read_only_target_gone(&self) {
            eprintln!(
                "PRE1 replace_request_id: {:?}",
                self.replacement_request_id
            );
            let region_snapshot_replace_request = self
                .datastore
                .get_region_snapshot_replacement_request_by_id(
                    &self.opctx(),
                    self.replacement_request_id,
                )
                .await
                .unwrap();

            eprintln!(
                "PRE2 rs_replace_request: {:?}",
                region_snapshot_replace_request
            );
            let res = self
                .datastore
                .read_only_target_addr(&region_snapshot_replace_request)
                .await
                .unwrap();

            eprintln!("PRE3 target that should be gone: {:?}", res);
        }

        pub async fn remove_disk_from_snapshot_rop(&self) {
            let disk_url = get_disk_url("disk-from-snapshot");

            eprintln!("NOW Remove disk from snapshot for disk {:?}", disk_url);
            let disk_from_snapshot: external::Disk =
                NexusRequest::object_get(&self.client, &disk_url)
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .unwrap()
                    .parsed_body()
                    .unwrap();

            let disk_id = disk_from_snapshot.identity.id;

            eprintln!("NOW Remove disk id {:?}", disk_id);
            // Note: `make_request` needs a type here, otherwise rustc cannot
            // figure out the type of the `request_body` parameter
            self.internal_client
                .make_request::<u32>(
                    http::Method::POST,
                    &format!("/disk/{disk_id}/remove-read-only-parent"),
                    None,
                    http::StatusCode::NO_CONTENT,
                )
                .await
                .unwrap();
        }
    }
}

/// Assert that a region snapshot replacement request in state "Requested" can
/// have its snapshot deleted and still transition to Complete
#[nexus_test(extra_sled_agents = 3)]
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
#[nexus_test(extra_sled_agents = 3)]
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
#[nexus_test(extra_sled_agents = 3)]
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
#[nexus_test(extra_sled_agents = 3)]
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
#[nexus_test(extra_sled_agents = 3)]
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
#[nexus_test(extra_sled_agents = 3)]
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

/// Assert that a region snapshot replacement step can still transition to
/// VolumeDeleted if the volume-remove-read-only-parent fires _after_  the step
/// was created.
#[nexus_test(extra_sled_agents = 3)]
async fn test_region_snapshot_replacement_step_after_rop_remove(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_snapshot_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the above `new` function in state Requested:
    // - transition the request to "ReplacementDone"
    // - transition the request to "Running"
    // - manually create a region snapshot replacement step for the disk created
    //   from the snapshot
    // - invoke the volume-remove-read-only-parent saga, which will delete the
    //   region snapshot
    // - finally, call finish_test

    test_harness.transition_request_to_replacement_done().await;
    test_harness.transition_request_to_running().await;

    test_harness.create_manual_region_snapshot_replacement_step().await;

    // Remove the ROP of the disk created from the snapshot
    test_harness.remove_disk_from_snapshot_rop().await;

    // Now the volume doesn't reference the read-only target but the step
    // request still exists.
    test_harness.finish_test().await;

    // Delete the resources, and assert there are no more Crucible resources
    test_harness.delete_the_disk().await;
    test_harness.delete_the_snapshot().await;
    test_harness.delete_the_disk_from_snapshot().await;

    test_harness.assert_no_crucible_resources_leaked().await;
}

/// Assert that a region snapshot replacement step can still transition to
/// VolumeDeleted if the volume-remove-read-only-parent fires _after_  the step
/// was created and the read-only target is gone.
#[nexus_test(extra_sled_agents = 3)]
async fn test_region_snapshot_replacement_step_after_rop_remove_target_gone(
    cptestctx: &ControlPlaneTestContext,
) {
    let test_harness =
        region_snapshot_replacement::DeletedVolumeTest::new(cptestctx).await;

    // The request leaves the above `new` function in state Requested:
    // - transition the request to "ReplacementDone"
    // - transition the request to "Running"
    // - manually create a region snapshot replacement step for the disk created
    //   from the snapshot
    // - delete the disk and snapshot
    // - invoke the volume-remove-read-only-parent saga, which will delete the
    //   region snapshot
    // - finally, call finish_test

    test_harness.transition_request_to_replacement_done().await;
    test_harness.transition_request_to_running().await;

    test_harness.pre_assert_read_only_target_gone().await;
    test_harness.create_manual_region_snapshot_replacement_step().await;
    test_harness.pre_assert_read_only_target_gone().await;
    test_harness.delete_the_disk().await;
    test_harness.pre_assert_read_only_target_gone().await;
    test_harness.delete_the_snapshot().await;
    test_harness.pre_assert_read_only_target_gone().await;

    // Remove the ROP of the disk created from the snapshot
    test_harness.remove_disk_from_snapshot_rop().await;
    test_harness.assert_read_only_target_gone().await;

    // Now the region snapshot is gone but the step request still exists.

    test_harness.finish_test().await;

    // Delete the final resource, and assert there are no more Crucible
    // resources
    test_harness.delete_the_disk_from_snapshot().await;
    test_harness.assert_no_crucible_resources_leaked().await;
}

/// Tests that replacement can occur until completion
#[nexus_test(extra_sled_agents = 3)]
async fn test_replacement_sanity(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool per sled, each with one dataset. This is required for
    // region and region snapshot replacement to have somewhere to move the
    // data.
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // Create a disk and a snapshot and a disk from that snapshot
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;
    let snapshot = create_snapshot(&client, PROJECT_NAME, "disk", "snap").await;
    let _disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snap",
        snapshot.identity.id,
    )
    .await;

    // Before expunging the physical disk, save the DB model
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_disk.id(), disk.identity.id);

    // Next, expunge a physical disk that contains a region

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
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

    // Any volumes sent to the Pantry for reconciliation should return active
    // for this test

    cptestctx
        .first_sim_server()
        .pantry_server
        .as_ref()
        .unwrap()
        .pantry
        .set_auto_activate_volumes();

    // Now, run all replacement tasks to completion
    let internal_client = &cptestctx.internal_client;
    wait_for_all_replacements(&datastore, &internal_client).await;

    // Validate all regions are on non-expunged physical disks
    assert!(
        datastore
            .find_read_write_regions_on_expunged_physical_disks(&opctx)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        datastore
            .find_read_only_regions_on_expunged_physical_disks(&opctx)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Tests that multiple replacements can occur until completion
#[nexus_test(extra_sled_agents = 5)]
async fn test_region_replacement_triple_sanity(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool, each with one dataset, on each of the 6 sleds. This is
    // required for region and region snapshot replacement to have somewhere to
    // move the data, and for this test we're doing two expungements.
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // Any volumes sent to the Pantry for reconciliation should return active
    // for this test

    cptestctx
        .first_sim_server()
        .pantry_server
        .as_ref()
        .unwrap()
        .pantry
        .set_auto_activate_volumes();

    // Create a disk and a snapshot and a disk from that snapshot
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;
    let snapshot = create_snapshot(&client, PROJECT_NAME, "disk", "snap").await;
    let _disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snap",
        snapshot.identity.id,
    )
    .await;

    // Before expunging any physical disk, save some DB models
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap();

    let internal_client = &cptestctx.internal_client;

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
    let snapshot_allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(disk_allocated_regions.len(), 3);
    assert_eq!(snapshot_allocated_regions.len(), 0);

    for i in disk_allocated_regions {
        let (dataset, _) = &i;

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

        // Now, run all replacement tasks to completion
        wait_for_all_replacements(&datastore, &internal_client).await;
    }

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
    let snapshot_allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(disk_allocated_regions.len(), 3);
    assert!(disk_allocated_regions.iter().all(|(_, r)| !r.read_only()));

    // Assert region snapshots replaced with three read-only regions
    assert_eq!(snapshot_allocated_regions.len(), 3);
    assert!(snapshot_allocated_regions.iter().all(|(_, r)| r.read_only()));

    // Validate all regions are on non-expunged physical disks
    assert!(
        datastore
            .find_read_write_regions_on_expunged_physical_disks(&opctx)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        datastore
            .find_read_only_regions_on_expunged_physical_disks(&opctx)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Tests that multiple replacements can occur until completion, after expunging
/// two physical disks before any replacements occur (aka we can lose two
/// physical disks and still recover)
#[nexus_test(extra_sled_agents = 5)]
async fn test_region_replacement_triple_sanity_2(
    cptestctx: &ControlPlaneTestContext,
) {
    let log = &cptestctx.logctx.log;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool, each with one dataset, on each of the 6 sleds. This is
    // required for region and region snapshot replacement to have somewhere to
    // move the data, and for this test we're doing two expungements.
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // Any volumes sent to the Pantry for reconciliation should return active
    // for this test

    cptestctx
        .first_sim_server()
        .pantry_server
        .as_ref()
        .unwrap()
        .pantry
        .set_auto_activate_volumes();

    // Create a disk and a snapshot and a disk from that snapshot
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;
    let snapshot = create_snapshot(&client, PROJECT_NAME, "disk", "snap").await;
    let _disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snap",
        snapshot.identity.id,
    )
    .await;

    // Before expunging any physical disk, save some DB models
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap();

    let internal_client = &cptestctx.internal_client;

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
    let snapshot_allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(disk_allocated_regions.len(), 3);
    assert_eq!(snapshot_allocated_regions.len(), 0);

    // Expunge two physical disks before any replacements occur
    for i in [0, 1] {
        let (dataset, _) = &disk_allocated_regions[i];

        let zpool = disk_test
            .zpools()
            .find(|x| *x.id.as_untyped_uuid() == dataset.pool_id)
            .expect("Expected at least one zpool");

        let (_, db_zpool) = LookupPath::new(&opctx, datastore)
            .zpool_id(zpool.id.into_untyped_uuid())
            .fetch()
            .await
            .unwrap();

        info!(log, "expunging physical disk {}", db_zpool.physical_disk_id);

        datastore
            .physical_disk_update_policy(
                &opctx,
                db_zpool.physical_disk_id.into(),
                PhysicalDiskPolicy::Expunged,
            )
            .await
            .unwrap();
    }

    info!(log, "waiting for all replacements");

    // Now, run all replacement tasks to completion
    wait_for_all_replacements(&datastore, &internal_client).await;

    // Expunge the last physical disk
    {
        let (dataset, _) = &disk_allocated_regions[2];

        let zpool = disk_test
            .zpools()
            .find(|x| *x.id.as_untyped_uuid() == dataset.pool_id)
            .expect("Expected at least one zpool");

        let (_, db_zpool) = LookupPath::new(&opctx, datastore)
            .zpool_id(zpool.id.into_untyped_uuid())
            .fetch()
            .await
            .unwrap();

        info!(log, "expunging physical disk {}", db_zpool.physical_disk_id);

        datastore
            .physical_disk_update_policy(
                &opctx,
                db_zpool.physical_disk_id.into(),
                PhysicalDiskPolicy::Expunged,
            )
            .await
            .unwrap();
    }

    info!(log, "waiting for all replacements");

    // Now, run all replacement tasks to completion
    wait_for_all_replacements(&datastore, &internal_client).await;

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
    let snapshot_allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(disk_allocated_regions.len(), 3);
    assert!(disk_allocated_regions.iter().all(|(_, r)| !r.read_only()));

    // Assert region snapshots replaced with three read-only regions
    assert_eq!(snapshot_allocated_regions.len(), 3);
    assert!(snapshot_allocated_regions.iter().all(|(_, r)| r.read_only()));

    // Validate all regions are on non-expunged physical disks
    assert!(
        datastore
            .find_read_write_regions_on_expunged_physical_disks(&opctx)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        datastore
            .find_read_only_regions_on_expunged_physical_disks(&opctx)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Tests that replacement can occur until completion twice - meaning region
/// snapshots are replaced with read-only regions, and then read-only regions
/// are replaced with other read-only regions.
#[nexus_test(extra_sled_agents = 3)]
async fn test_replacement_sanity_twice(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let internal_client = &cptestctx.internal_client;

    // Create one zpool per sled, each with one dataset. This is required for
    // region and region snapshot replacement to have somewhere to move the
    // data.
    DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // Any volumes sent to the Pantry for reconciliation should return active
    // for this test
    cptestctx
        .first_sim_server()
        .pantry_server
        .as_ref()
        .unwrap()
        .pantry
        .set_auto_activate_volumes();

    // Create a disk and a snapshot and a disk from that snapshot
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;
    let snapshot = create_snapshot(&client, PROJECT_NAME, "disk", "snap").await;
    let _disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snap",
        snapshot.identity.id,
    )
    .await;

    // Manually create region snapshot replacement requests for each region
    // snapshot.

    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_disk.id(), disk.identity.id);

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    for (_, region) in &disk_allocated_regions {
        let region_snapshot = datastore
            .region_snapshot_get(
                region.dataset_id(),
                region.id(),
                snapshot.identity.id,
            )
            .await
            .expect("found region snapshot")
            .unwrap();

        datastore
            .create_region_snapshot_replacement_request(
                &opctx,
                &region_snapshot,
            )
            .await
            .unwrap();

        wait_for_all_replacements(&datastore, &internal_client).await;
    }

    // Now, do it again, except this time specifying the read-only regions

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_snapshot.id(), snapshot.identity.id);

    let snapshot_allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    for (_, region) in &snapshot_allocated_regions {
        let region =
            datastore.get_region(region.id()).await.expect("found region");

        datastore
            .create_read_only_region_replacement_request(&opctx, region.id())
            .await
            .unwrap();

        wait_for_all_replacements(&datastore, &internal_client).await;
    }
}

/// Tests that expunging a sled with read-only regions will lead to them being
/// replaced
#[nexus_test(extra_sled_agents = 3)]
async fn test_read_only_replacement_sanity(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let internal_client = &cptestctx.internal_client;

    // Create one zpool per sled, each with one dataset. This is required for
    // region and region snapshot replacement to have somewhere to move the
    // data.
    DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // Any volumes sent to the Pantry for reconciliation should return active
    // for this test
    cptestctx
        .first_sim_server()
        .pantry_server
        .as_ref()
        .unwrap()
        .pantry
        .set_auto_activate_volumes();

    // Create a disk and a snapshot and a disk from that snapshot
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;
    let snapshot = create_snapshot(&client, PROJECT_NAME, "disk", "snap").await;
    let _disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snap",
        snapshot.identity.id,
    )
    .await;

    // Manually create region snapshot replacement requests for each region
    // snapshot.

    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_disk.id(), disk.identity.id);

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    for (_, region) in &disk_allocated_regions {
        let region_snapshot = datastore
            .region_snapshot_get(
                region.dataset_id(),
                region.id(),
                snapshot.identity.id,
            )
            .await
            .expect("found region snapshot")
            .unwrap();

        datastore
            .create_region_snapshot_replacement_request(
                &opctx,
                &region_snapshot,
            )
            .await
            .unwrap();

        wait_for_all_replacements(&datastore, &internal_client).await;
    }

    // Now expunge a sled with read-only regions on it.

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_snapshot.id(), snapshot.identity.id);

    let snapshot_allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    let (dataset, region) = &snapshot_allocated_regions[0];
    assert!(region.read_only());

    let (_, db_zpool) = LookupPath::new(&opctx, datastore)
        .zpool_id(dataset.pool_id.into_untyped_uuid())
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

    wait_for_all_replacements(&datastore, &internal_client).await;

    // Validate all regions are on non-expunged physical disks
    assert!(
        datastore
            .find_read_write_regions_on_expunged_physical_disks(&opctx)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        datastore
            .find_read_only_regions_on_expunged_physical_disks(&opctx)
            .await
            .unwrap()
            .is_empty()
    );
}

/// `test_replacement_sanity_twice`, but delete the snapshot before doing the
/// expungements
#[nexus_test(extra_sled_agents = 3)]
async fn test_replacement_sanity_twice_after_snapshot_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let internal_client = &cptestctx.internal_client;

    // Create one zpool per sled, each with one dataset. This is required for
    // region and region snapshot replacement to have somewhere to move the
    // data.
    DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // Any volumes sent to the Pantry for reconciliation should return active
    // for this test
    cptestctx
        .first_sim_server()
        .pantry_server
        .as_ref()
        .unwrap()
        .pantry
        .set_auto_activate_volumes();

    // Create a disk and a snapshot and a disk from that snapshot
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;
    let snapshot = create_snapshot(&client, PROJECT_NAME, "disk", "snap").await;

    create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "snap-disk-1",
        snapshot.identity.id,
    )
    .await;
    create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "snap-disk-2",
        snapshot.identity.id,
    )
    .await;
    create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "snap-disk-3",
        snapshot.identity.id,
    )
    .await;

    create_project_image_from_snapshot(
        &client,
        PROJECT_NAME,
        "image",
        snapshot.identity.id,
    )
    .await;

    // Delete the snapshot
    delete_snapshot(&client, PROJECT_NAME, "snap").await;

    // Assert snapshot volume is gone
    let db_snapshot = datastore
        .snapshot_get(&opctx, snapshot.identity.id)
        .await
        .unwrap()
        .unwrap();

    assert!(
        datastore.volume_get(db_snapshot.volume_id()).await.unwrap().is_none()
    );

    // Manually create region snapshot replacement requests for each region
    // snapshot.

    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_disk.id(), disk.identity.id);

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    for (_, region) in &disk_allocated_regions {
        let region_snapshot = datastore
            .region_snapshot_get(
                region.dataset_id(),
                region.id(),
                snapshot.identity.id,
            )
            .await
            .expect("found region snapshot")
            .unwrap();

        datastore
            .create_region_snapshot_replacement_request(
                &opctx,
                &region_snapshot,
            )
            .await
            .unwrap();

        wait_for_all_replacements(&datastore, &internal_client).await;
    }

    // Now, do it again, except this time specifying the read-only regions

    let db_snapshot = datastore
        .snapshot_get(&opctx, snapshot.identity.id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(db_snapshot.id(), snapshot.identity.id);

    let snapshot_allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    for (_, region) in &snapshot_allocated_regions {
        let region =
            datastore.get_region(region.id()).await.expect("found region");

        datastore
            .create_read_only_region_replacement_request(&opctx, region.id())
            .await
            .unwrap();

        wait_for_all_replacements(&datastore, &internal_client).await;
    }
}
