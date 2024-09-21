// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting volumes affected by a region snapshot
//! replacement, creating records for those, and triggering the "step" saga for
//! them.
//!
//! After the region snapshot replacement start saga finishes, the snapshot's
//! volume is no longer in a degraded state: the requested read-only region was
//! cloned to a new region, and the reference was replaced in the construction
//! request.  Any disk that is now created using the snapshot as a source will
//! work without issues.
//!
//! The problem now is volumes that still reference the replaced read-only
//! region, and any Upstairs constructed from a VCR that references that region.
//! This task's responsibility is to find all volumes that reference the
//! replaced read-only region, create a record for them, and trigger the region
//! snapshot replacement step saga. This is a much less involved process than
//! region replacement: no continuous monitoring and driving is required. See
//! the "region snapshot replacement step" saga's docstring for more
//! information.

use crate::app::authn;
use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas;
use crate::app::sagas::region_snapshot_replacement_step::*;
use crate::app::sagas::region_snapshot_replacement_step_garbage_collect::*;
use crate::app::sagas::NexusSaga;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::RegionSnapshotReplacementStep;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::region_snapshot_replacement;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::RegionSnapshotReplacementStepStatus;
use serde_json::json;
use std::sync::Arc;

pub struct RegionSnapshotReplacementFindAffected {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

impl RegionSnapshotReplacementFindAffected {
    pub fn new(datastore: Arc<DataStore>, sagas: Arc<dyn StartSaga>) -> Self {
        RegionSnapshotReplacementFindAffected { datastore, sagas }
    }

    async fn send_start_request(
        &self,
        opctx: &OpContext,
        request: RegionSnapshotReplacementStep,
    ) -> Result<(), omicron_common::api::external::Error> {
        let params = sagas::region_snapshot_replacement_step::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            request,
        };

        let saga_dag = SagaRegionSnapshotReplacementStep::prepare(&params)?;
        self.sagas.saga_start(saga_dag).await
    }

    async fn send_garbage_collect_request(
        &self,
        opctx: &OpContext,
        request: RegionSnapshotReplacementStep,
    ) -> Result<(), omicron_common::api::external::Error> {
        let Some(old_snapshot_volume_id) = request.old_snapshot_volume_id
        else {
            // This state is illegal!
            let s = format!(
                "request {} old snapshot volume id is None!",
                request.id,
            );

            return Err(omicron_common::api::external::Error::internal_error(
                &s,
            ));
        };

        let params =
            sagas::region_snapshot_replacement_step_garbage_collect::Params {
                serialized_authn: authn::saga::Serialized::for_opctx(opctx),
                old_snapshot_volume_id,
                request,
            };

        let saga_dag =
            SagaRegionSnapshotReplacementStepGarbageCollect::prepare(&params)?;
        self.sagas.saga_start(saga_dag).await
    }

    async fn clean_up_region_snapshot_replacement_step_volumes(
        &self,
        opctx: &OpContext,
        status: &mut RegionSnapshotReplacementStepStatus,
    ) {
        let log = &opctx.log;

        let requests = match self
            .datastore
            .region_snapshot_replacement_steps_requiring_garbage_collection(
                opctx,
            )
            .await
        {
            Ok(requests) => requests,

            Err(e) => {
                let s = format!("querying for steps to collect failed! {e}");
                error!(&log, "{s}");
                status.errors.push(s);
                return;
            }
        };

        for request in requests {
            let request_id = request.id;

            let result =
                self.send_garbage_collect_request(opctx, request.clone()).await;

            match result {
                Ok(()) => {
                    let s = format!(
                        "region snapshot replacement step garbage \
                    collect request ok for {request_id}"
                    );

                    info!(
                        &log,
                        "{s}";
                        "request.volume_id" => %request.volume_id,
                        "request.old_snapshot_volume_id" => ?request.old_snapshot_volume_id,
                    );
                    status.step_garbage_collect_invoked_ok.push(s);
                }

                Err(e) => {
                    let s = format!(
                        "sending region snapshot replacement step garbage \
                        collect request failed: {e}",
                    );
                    error!(
                        &log,
                        "{s}";
                        "request.volume_id" => %request.volume_id,
                        "request.old_snapshot_volume_id" => ?request.old_snapshot_volume_id,
                    );
                    status.errors.push(s);
                }
            }
        }
    }

    // Any request in state Running means that the target replacement has
    // occurred already, meaning the region snapshot being replaced is not
    // present as a target in the snapshot's volume construction request
    // anymore. Any future usage of that snapshot (as a source for a disk or
    // otherwise) will get a volume construction request that references the
    // replacement read-only region.
    //
    // "step" records are created here for each volume found that still
    // references the replaced region snapshot, most likely having been created
    // by copying the snapshot's volume construction request before the target
    // replacement occurred. These volumes also need to have target replacement
    // performed, and this is captured in this "step" record.
    async fn create_step_records_for_affected_volumes(
        &self,
        opctx: &OpContext,
        status: &mut RegionSnapshotReplacementStepStatus,
    ) {
        let log = &opctx.log;

        // Find all region snapshot replacement requests in state "Running"
        let requests = match self
            .datastore
            .get_running_region_snapshot_replacements(opctx)
            .await
        {
            Ok(requests) => requests,

            Err(e) => {
                let s = format!(
                    "get_running_region_snapshot_replacements failed: {e}",
                );

                error!(&log, "{s}");
                status.errors.push(s);
                return;
            }
        };

        for request in requests {
            // Find all volumes that reference the replaced snapshot
            let region_snapshot = match self
                .datastore
                .region_snapshot_get(
                    request.old_dataset_id,
                    request.old_region_id,
                    request.old_snapshot_id,
                )
                .await
            {
                Ok(Some(region_snapshot)) => region_snapshot,

                Ok(None) => {
                    let s = format!(
                        "region snapshot {} {} {} not found!",
                        request.old_dataset_id,
                        request.old_region_id,
                        request.old_snapshot_id,
                    );
                    error!(&log, "{s}");
                    status.errors.push(s);

                    continue;
                }

                Err(e) => {
                    let s = format!(
                        "error querying for region snapshot {} {} {}: {e}",
                        request.old_dataset_id,
                        request.old_region_id,
                        request.old_snapshot_id,
                    );
                    error!(&log, "{s}");
                    status.errors.push(s);

                    continue;
                }
            };

            let snapshot_addr = match region_snapshot.snapshot_addr.parse() {
                Ok(addr) => addr,

                Err(e) => {
                    let s = format!(
                        "region snapshot addr {} could not be parsed: {e}",
                        region_snapshot.snapshot_addr,
                    );
                    error!(&log, "{s}");
                    status.errors.push(s);

                    continue;
                }
            };

            let volumes = match self
                .datastore
                .find_volumes_referencing_socket_addr(&opctx, snapshot_addr)
                .await
            {
                Ok(volumes) => volumes,

                Err(e) => {
                    let s = format!("error finding referenced volumes: {e}");
                    error!(
                        log,
                        "{s}";
                        "request id" => ?request.id,
                    );
                    status.errors.push(s);

                    continue;
                }
            };

            for volume in volumes {
                // Any volume referencing the old socket addr needs to be
                // replaced. Create a "step" record for this.
                //
                // Note: this function returns a conflict error if there already
                // exists a step record referencing this volume ID because a
                // volume repair record is also created using that volume ID,
                // and only one of those can exist for a given volume at a time.
                //
                // Also note: this function returns a conflict error if another
                // step record references this volume id in the "old snapshot
                // volume id" column - this is ok! Region snapshot replacement
                // step records are created for some volume id, and a null old
                // snapshot volume id:
                //
                //   volume_id: references snapshot_addr
                //   old_snapshot_volume_id: null
                //
                // The region snapshot replacement step saga will create a
                // volume to stash the reference to snapshot_addr, and then call
                // `volume_replace_snapshot`. This will swap snapshot_addr
                // reference into the old snapshot volume for later deletion:
                //
                //   volume_id: does _not_ reference snapshot_addr anymore
                //   old_snapshot_volume_id: now references snapshot_addr
                //
                // If `find_volumes_referencing_socket_addr` is executed before
                // that volume is deleted, it will return the old snapshot
                // volume id above, and then this for loop tries to make a
                // region snapshot replacement step record for it!
                //
                // Allowing a region snapshot replacement step record to be
                // created in this case would mean that (depending on when the
                // functions execute), an indefinite amount of work would be
                // created, continually "moving" the snapshot_addr from
                // temporary volume to temporary volume.

                match self
                    .datastore
                    .create_region_snapshot_replacement_step(
                        opctx,
                        request.id,
                        volume.id(),
                    )
                    .await
                {
                    Ok(insertion_result) => match insertion_result {
                        region_snapshot_replacement::InsertStepResult::Inserted { step_id } => {
                            let s = format!("created {step_id}");
                            info!(
                                log,
                                "{s}";
                                "request id" => ?request.id,
                                "volume id" => ?volume.id(),
                            );
                            status.step_records_created_ok.push(s);
                        }

                        region_snapshot_replacement::InsertStepResult::AlreadyHandled { .. } => {
                            info!(
                                log,
                                "step already exists for volume id";
                                "request id" => ?request.id,
                                "volume id" => ?volume.id(),
                            );
                        }
                    }

                    Err(e) => {
                        let s = format!("error creating step request: {e}");
                        error!(
                            log,
                            "{s}";
                            "request id" => ?request.id,
                            "volume id" => ?volume.id(),
                        );
                        status.errors.push(s);
                    }
                }
            }
        }
    }

    async fn invoke_step_saga_for_affected_volumes(
        &self,
        opctx: &OpContext,
        status: &mut RegionSnapshotReplacementStepStatus,
    ) {
        let log = &opctx.log;

        // Once all region snapshot replacement step records have been created,
        // trigger sagas as appropriate.

        let step_requests = match self
            .datastore
            .get_requested_region_snapshot_replacement_steps(opctx)
            .await
        {
            Ok(step_requests) => step_requests,

            Err(e) => {
                let s = format!(
                    "query for requested region snapshot replacement step \
                    requests failed: {e}"
                );
                error!(&log, "{s}");
                status.errors.push(s);

                return;
            }
        };

        for request in step_requests {
            let request_id = request.id;

            match self.send_start_request(opctx, request.clone()).await {
                Ok(()) => {
                    let s = format!(
                        "region snapshot replacement step saga invoked ok for \
                        {request_id}"
                    );

                    info!(
                        &log,
                        "{s}";
                        "request.request_id" => %request.request_id,
                        "request.volume_id" => %request.volume_id,
                    );
                    status.step_invoked_ok.push(s);
                }

                Err(e) => {
                    let s = format!(
                        "invoking region snapshot replacement step saga for \
                        {request_id} failed: {e}"
                    );

                    error!(
                        &log,
                        "{s}";
                        "request.request_id" => %request.request_id,
                        "request.volume_id" => %request.volume_id,
                    );
                    status.errors.push(s);
                }
            };
        }
    }
}

impl BackgroundTask for RegionSnapshotReplacementFindAffected {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            let mut status = RegionSnapshotReplacementStepStatus::default();

            // Importantly, clean old steps up before finding affected volumes!
            // Otherwise, will continue to find the snapshot in volumes to
            // delete, and will continue to see conflicts in next function.
            self.clean_up_region_snapshot_replacement_step_volumes(
                opctx,
                &mut status,
            )
            .await;

            self.create_step_records_for_affected_volumes(opctx, &mut status)
                .await;

            self.invoke_step_saga_for_affected_volumes(opctx, &mut status)
                .await;

            json!(status)
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::init::test::NoopStartSaga;
    use nexus_db_model::RegionSnapshot;
    use nexus_db_model::RegionSnapshotReplacement;
    use nexus_db_model::RegionSnapshotReplacementStep;
    use nexus_db_model::RegionSnapshotReplacementStepState;
    use nexus_db_model::Volume;
    use nexus_test_utils_macros::nexus_test;
    use sled_agent_client::types::CrucibleOpts;
    use sled_agent_client::types::VolumeConstructionRequest;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    async fn add_fake_volume_for_snapshot_addr(
        datastore: &DataStore,
        snapshot_addr: String,
    ) -> Uuid {
        let new_volume_id = Uuid::new_v4();

        let volume_construction_request = VolumeConstructionRequest::Volume {
            id: new_volume_id,
            block_size: 0,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 0,
                    blocks_per_extent: 0,
                    extent_count: 0,
                    gen: 0,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![snapshot_addr],
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

        let volume_data =
            serde_json::to_string(&volume_construction_request).unwrap();

        let volume = Volume::new(new_volume_id, volume_data);

        datastore.volume_create(volume).await.unwrap();

        new_volume_id
    }

    #[nexus_test(server = crate::Server)]
    async fn test_region_snapshot_replacement_step_task(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let starter = Arc::new(NoopStartSaga::new());
        let mut task = RegionSnapshotReplacementFindAffected::new(
            datastore.clone(),
            starter.clone(),
        );

        // Noop test
        let result: RegionSnapshotReplacementStepStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, RegionSnapshotReplacementStepStatus::default());
        assert_eq!(starter.count_reset(), 0);

        // Add a region snapshot replacement request for a fake region snapshot.

        let dataset_id = Uuid::new_v4();
        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();
        let snapshot_addr = String::from("[fd00:1122:3344::101]:9876");

        let fake_region_snapshot = RegionSnapshot::new(
            dataset_id,
            region_id,
            snapshot_id,
            snapshot_addr.clone(),
        );

        datastore.region_snapshot_create(fake_region_snapshot).await.unwrap();

        let request =
            RegionSnapshotReplacement::new(dataset_id, region_id, snapshot_id);

        let request_id = request.id;

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx,
                request,
                Uuid::new_v4(),
            )
            .await
            .unwrap();

        // Transition that to Allocating -> ReplacementDone -> DeletingOldVolume
        // -> Running

        let operating_saga_id = Uuid::new_v4();

        datastore
            .set_region_snapshot_replacement_allocating(
                &opctx,
                request_id,
                operating_saga_id,
            )
            .await
            .unwrap();

        let new_region_id = Uuid::new_v4();
        let old_snapshot_volume_id = Uuid::new_v4();

        datastore
            .set_region_snapshot_replacement_replacement_done(
                &opctx,
                request_id,
                operating_saga_id,
                new_region_id,
                old_snapshot_volume_id,
            )
            .await
            .unwrap();

        datastore
            .set_region_snapshot_replacement_deleting_old_volume(
                &opctx,
                request_id,
                operating_saga_id,
            )
            .await
            .unwrap();

        datastore
            .set_region_snapshot_replacement_running(
                &opctx,
                request_id,
                operating_saga_id,
            )
            .await
            .unwrap();

        // Add some fake volumes that reference the region snapshot being
        // replaced

        let new_volume_1_id = add_fake_volume_for_snapshot_addr(
            &datastore,
            snapshot_addr.clone(),
        )
        .await;
        let new_volume_2_id = add_fake_volume_for_snapshot_addr(
            &datastore,
            snapshot_addr.clone(),
        )
        .await;

        // Add some fake volumes that do not

        let other_volume_1_id = add_fake_volume_for_snapshot_addr(
            &datastore,
            String::from("[fd00:1122:3344::101]:1000"),
        )
        .await;

        let other_volume_2_id = add_fake_volume_for_snapshot_addr(
            &datastore,
            String::from("[fd12:5544:3344::912]:3901"),
        )
        .await;

        // Activate the task - it should pick the running request up and try to
        // run the region snapshot replacement step saga for the volumes

        let result: RegionSnapshotReplacementStepStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        let requested_region_snapshot_replacement_steps = datastore
            .get_requested_region_snapshot_replacement_steps(&opctx)
            .await
            .unwrap();

        assert_eq!(requested_region_snapshot_replacement_steps.len(), 2);

        for step in &requested_region_snapshot_replacement_steps {
            let s: String = format!("created {}", step.id);
            assert!(result.step_records_created_ok.contains(&s));

            let s: String = format!(
                "region snapshot replacement step saga invoked ok for {}",
                step.id
            );
            assert!(result.step_invoked_ok.contains(&s));

            if step.volume_id == new_volume_1_id
                || step.volume_id == new_volume_2_id
            {
                // ok!
            } else if step.volume_id == other_volume_1_id
                || step.volume_id == other_volume_2_id
            {
                // error!
                assert!(false);
            } else {
                // error!
                assert!(false);
            }
        }

        // No garbage collection would be invoked yet, as the step records are
        // not in state Complete
        assert!(result.step_garbage_collect_invoked_ok.is_empty());

        assert_eq!(result.errors.len(), 0);

        assert_eq!(starter.count_reset(), 2);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_region_snapshot_replacement_step_task_gc(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let starter = Arc::new(NoopStartSaga::new());
        let mut task = RegionSnapshotReplacementFindAffected::new(
            datastore.clone(),
            starter.clone(),
        );

        // Noop test
        let result: RegionSnapshotReplacementStepStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, RegionSnapshotReplacementStepStatus::default());
        assert_eq!(starter.count_reset(), 0);

        // Now, add some Complete records and make sure the garbage collection
        // saga is invoked.

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, {
                let mut record = RegionSnapshotReplacementStep::new(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                );

                record.replacement_state =
                    RegionSnapshotReplacementStepState::Complete;
                record.old_snapshot_volume_id = Some(Uuid::new_v4());

                record
            })
            .await
            .unwrap();

        assert!(matches!(
            result,
            region_snapshot_replacement::InsertStepResult::Inserted { .. }
        ));

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, {
                let mut record = RegionSnapshotReplacementStep::new(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                );

                record.replacement_state =
                    RegionSnapshotReplacementStepState::Complete;
                record.old_snapshot_volume_id = Some(Uuid::new_v4());

                record
            })
            .await
            .unwrap();

        assert!(matches!(
            result,
            region_snapshot_replacement::InsertStepResult::Inserted { .. }
        ));

        // Activate the task - it should pick the complete steps up and try to
        // run the region snapshot replacement step garbage collect saga

        let result: RegionSnapshotReplacementStepStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        let region_snapshot_replacement_steps_requiring_gc = datastore
            .region_snapshot_replacement_steps_requiring_garbage_collection(
                &opctx,
            )
            .await
            .unwrap();

        assert_eq!(region_snapshot_replacement_steps_requiring_gc.len(), 2);

        eprintln!("{:?}", result);

        for step in &region_snapshot_replacement_steps_requiring_gc {
            let s: String = format!(
                "region snapshot replacement step garbage collect request ok \
                for {}",
                step.id
            );
            assert!(result.step_garbage_collect_invoked_ok.contains(&s));
        }

        assert!(result.step_records_created_ok.is_empty());

        assert!(result.step_invoked_ok.is_empty());

        assert_eq!(result.errors.len(), 0);

        assert_eq!(starter.count_reset(), 2);
    }
}
