// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Soft-delete up the volume that stashes the target replaced during the region
//! snapshot replacement start saga. After that's done, change the region
//! snapshot replacement state to Running. This saga handles the following
//! region snapshot replacement request state transitions:
//!
//! ```text
//!    ReplacementDone  <--
//!                       |
//!          |            |
//!          v            |
//!                       |
//!  DeletingOldVolume  --
//!
//!          |
//!          v
//!
//!       Running
//! ```
//!
//! See the documentation for the "region snapshot replacement step" saga for
//! the next step(s) in the process.

use super::{
    ACTION_GENERATE_ID, ActionRegistry, NexusActionContext, NexusSaga,
    SagaInitError,
};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, db};
use nexus_types::saga::saga_action_failed;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// region snapshot replacement garbage collect saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    /// The fake volume created for the snapshot that was replaced
    // Note: this is only required in the params to build the volume-delete sub
    // saga
    pub old_snapshot_volume_id: VolumeUuid,
    pub request: db::model::RegionSnapshotReplacement,
}

// region snapshot replacement garbage collect saga: actions

declare_saga_actions! {
    region_snapshot_replacement_garbage_collect;
    SET_SAGA_ID -> "unused_1" {
        + rsrgs_set_saga_id
        - rsrgs_set_saga_id_undo
    }
    SOFT_DELETE_VOLUME -> "soft_delete_volume" {
        + rsrgs_soft_delete_volume
    }
    UPDATE_REQUEST_RECORD -> "unused_2" {
        + rsrgs_update_request_record
    }
}

// region snapshot replacement garbage collect saga: definition

#[derive(Debug)]
pub(crate) struct SagaRegionSnapshotReplacementGarbageCollect;
impl NexusSaga for SagaRegionSnapshotReplacementGarbageCollect {
    const NAME: &'static str = "region-snapshot-replacement-garbage-collect";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        region_snapshot_replacement_garbage_collect_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(Node::action(
            "saga_id",
            "GenerateSagaId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(set_saga_id_action());
        builder.append(soft_delete_volume_action());
        builder.append(update_request_record_action());

        Ok(builder.build()?)
    }
}

// region snapshot replacement garbage collect saga: action implementations

async fn rsrgs_set_saga_id(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;

    // Change the request record here to an intermediate "deleting old volume"
    // state to block out other sagas that will be triggered for the same
    // request.
    osagactx
        .datastore()
        .set_region_snapshot_replacement_deleting_old_volume(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await
        .map_err(saga_action_failed)?;

    Ok(())
}

async fn rsrgs_set_saga_id_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;

    osagactx
        .datastore()
        .undo_set_region_snapshot_replacement_deleting_old_volume(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await?;

    Ok(())
}

async fn rsrgs_soft_delete_volume(
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

async fn rsrgs_update_request_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;

    // Now that the snapshot volume has been deleted, update the replacement
    // request record to 'Running'. There is no undo step for this, it should
    // succeed idempotently.

    datastore
        .set_region_snapshot_replacement_running(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await
        .map_err(saga_action_failed)?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use crate::app::sagas::region_snapshot_replacement_garbage_collect::{
        Params, SagaRegionSnapshotReplacementGarbageCollect,
    };
    use nexus_db_model::RegionSnapshotReplacement;
    use nexus_db_model::RegionSnapshotReplacementState;
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::background::wait_for_all_volume_deletes;
    use nexus_test_utils_macros::nexus_test;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::VolumeUuid;
    use sled_agent_client::CrucibleOpts;
    use sled_agent_client::VolumeConstructionRequest;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_region_snapshot_replacement_garbage_collect_saga(
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

        let mut request = RegionSnapshotReplacement::new_from_region_snapshot(
            DatasetUuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        request.replacement_state =
            RegionSnapshotReplacementState::ReplacementDone;
        request.old_snapshot_volume_id = Some(old_snapshot_volume_id.into());

        let volume_id = VolumeUuid::new_v4();

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx,
                request.clone(),
                volume_id,
            )
            .await
            .unwrap();

        // Run the saga
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            old_snapshot_volume_id,
            request: request.clone(),
        };

        let _output = nexus
            .sagas
            .saga_execute::<SagaRegionSnapshotReplacementGarbageCollect>(params)
            .await
            .unwrap();

        // Validate the state transition
        let result = datastore
            .get_region_snapshot_replacement_request_by_id(&opctx, request.id)
            .await
            .unwrap();

        assert_eq!(
            result.replacement_state,
            RegionSnapshotReplacementState::Running
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
