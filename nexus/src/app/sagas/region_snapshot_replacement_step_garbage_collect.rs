// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Soft-delete the volume that stashes the target replaced during a region
//! snapshot replacement step saga. After that's done, change the region
//! snapshot replacement step's state to "VolumeDeleted".

use super::{ActionRegistry, NexusActionContext, NexusSaga, SagaInitError};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, db};
use nexus_types::saga::saga_action_failed;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

// region snapshot replacement step garbage collect saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    /// The fake volume created for the snapshot that was replaced
    // Note: this is only required in the params to build the volume-delete sub
    // saga
    pub old_snapshot_volume_id: VolumeUuid,
    pub request: db::model::RegionSnapshotReplacementStep,
}

// region snapshot replacement step garbage collect saga: actions

declare_saga_actions! {
    region_snapshot_replacement_step_garbage_collect;
    SOFT_DELETE_VOLUME -> "soft_delete_volume" {
        + srsgs_soft_delete_volume
    }
    UPDATE_REQUEST_RECORD -> "unused_1" {
        + srsgs_update_request_record
    }
}

// region snapshot replacement step garbage collect saga: definition

#[derive(Debug)]
pub(crate) struct SagaRegionSnapshotReplacementStepGarbageCollect;
impl NexusSaga for SagaRegionSnapshotReplacementStepGarbageCollect {
    const NAME: &'static str =
        "region-snapshot-replacement-step-garbage-collect";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        region_snapshot_replacement_step_garbage_collect_register_actions(
            registry,
        );
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(soft_delete_volume_action());
        builder.append(update_request_record_action());

        Ok(builder.build()?)
    }
}

// region snapshot replacement step garbage collect saga: action implementations

async fn srsgs_soft_delete_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();

    osagactx
        .datastore()
        .soft_delete_volume(params.old_snapshot_volume_id)
        .await
        .map_err(|e| {
            saga_action_failed(Error::internal_error(&format!(
                "failed to soft_delete_volume: {:?}",
                e,
            )))
        })?;

    Ok(())
}

async fn srsgs_update_request_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // Now that the region snapshot step volume has been deleted, update the
    // replacement request record to 'VolumeDeleted'. There is no undo step for
    // this, it should succeed idempotently.

    datastore
        .set_region_snapshot_replacement_step_volume_deleted(
            &opctx,
            params.request.id,
        )
        .await
        .map_err(saga_action_failed)?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use crate::app::sagas::region_snapshot_replacement_step_garbage_collect::*;
    use nexus_db_model::RegionSnapshotReplacementStep;
    use nexus_db_model::RegionSnapshotReplacementStepState;
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::datastore::region_snapshot_replacement;
    use nexus_test_utils::background::wait_for_all_volume_deletes;
    use nexus_test_utils_macros::nexus_test;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::VolumeUuid;
    use sled_agent_client::CrucibleOpts;
    use sled_agent_client::VolumeConstructionRequest;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_region_snapshot_replacement_step_garbage_collect_saga(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Manually insert required records
        let old_snapshot_volume_id = VolumeUuid::new_v4();

        let volume_construction_request = VolumeConstructionRequest::Volume {
            id: *old_snapshot_volume_id.as_untyped_uuid(),
            block_size: 0,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 0,
                blocks_per_extent: 0,
                extent_count: 0,
                generation: 0,
                opts: CrucibleOpts {
                    id: *old_snapshot_volume_id.as_untyped_uuid(),
                    target: vec![
                        // if you put something here, you'll need a synthetic
                        // dataset record
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

        datastore
            .volume_create(old_snapshot_volume_id, volume_construction_request)
            .await
            .unwrap();

        let step_volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                step_volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        let mut request =
            RegionSnapshotReplacementStep::new(Uuid::new_v4(), step_volume_id);
        request.replacement_state =
            RegionSnapshotReplacementStepState::Complete;
        request.old_snapshot_volume_id = Some(old_snapshot_volume_id.into());

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, request.clone())
            .await
            .unwrap();

        assert!(matches!(
            result,
            region_snapshot_replacement::InsertStepResult::Inserted { .. }
        ));

        // Run the saga
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            old_snapshot_volume_id,
            request: request.clone(),
        };

        let _output = nexus
            .sagas
            .saga_execute::<SagaRegionSnapshotReplacementStepGarbageCollect>(
                params,
            )
            .await
            .unwrap();

        // Validate the state transition
        let result = datastore
            .get_region_snapshot_replacement_step_by_id(&opctx, request.id)
            .await
            .unwrap();

        assert_eq!(
            result.replacement_state,
            RegionSnapshotReplacementStepState::VolumeDeleted
        );

        // Run the volume delete background task and validate the Volume was
        // deleted.
        wait_for_all_volume_deletes(datastore, &cptestctx.lockstep_client)
            .await;

        assert!(
            datastore
                .volume_get(old_snapshot_volume_id)
                .await
                .unwrap()
                .is_none()
        );
    }
}
