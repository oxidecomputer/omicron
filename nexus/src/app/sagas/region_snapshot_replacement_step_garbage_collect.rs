// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Delete the volume that stashes the target replaced during a region snapshot
//! replacement step saga. After that's done, change the region snapshot
//! replacement step's state to "VolumeDeleted".

use super::{ActionRegistry, NexusActionContext, NexusSaga, SagaInitError};
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::volume_delete;
use crate::app::{authn, db};
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;

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
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        let subsaga_params = volume_delete::Params {
            serialized_authn: params.serialized_authn.clone(),
            volume_id: params.old_snapshot_volume_id,
        };

        let subsaga_dag = {
            let subsaga_builder = steno::DagBuilder::new(steno::SagaName::new(
                volume_delete::SagaVolumeDelete::NAME,
            ));
            volume_delete::SagaVolumeDelete::make_saga_dag(
                &subsaga_params,
                subsaga_builder,
            )?
        };

        builder.append(Node::constant(
            "params_for_volume_delete_subsaga",
            serde_json::to_value(&subsaga_params).map_err(|e| {
                SagaInitError::SerializeError(
                    "params_for_volume_delete_subsaga".to_string(),
                    e,
                )
            })?,
        ));

        builder.append(Node::subsaga(
            "volume_delete_subsaga_no_result",
            subsaga_dag,
            "params_for_volume_delete_subsaga",
        ));

        builder.append(update_request_record_action());

        Ok(builder.build()?)
    }
}

// region snapshot replacement step garbage collect saga: action implementations

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
        .map_err(ActionError::action_failed)?;

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

        // Validate the Volume was deleted
        assert!(
            datastore
                .volume_get(old_snapshot_volume_id)
                .await
                .unwrap()
                .is_none()
        );
    }
}
