// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! After the change to store a "new region volume" in the region snapshot
//! replacement request, that volume requires garbage collection before the
//! region snapshot replacement transitions to Complete. It's this saga's
//! responsibility to ensure that cleanup. This saga handles the following
//! region snapshot replacement request state transitions:
//!
//! ```text
//!          Running    <--
//!                       |
//!             |         |
//!             v         |
//!                       |
//!         Completing   --
//!
//!             |
//!             v
//!
//!          Complete
//! ```
//!
//! The first thing this saga does is set itself as the "operating saga" for the
//! request, and change the state to "Completing". Then, it performs the volume
//! delete sub-saga for the new region volume. Finally, it updates the region
//! snapshot replacement request by clearing the operating saga id and changing
//! the state to "Complete".
//!
//! Any unwind will place the state back into Running.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::volume_delete;
use crate::app::{authn, db};
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// region snapshot replacement finish saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub request: db::model::RegionSnapshotReplacement,
}

// region snapshot replacement finish saga: actions

declare_saga_actions! {
    region_snapshot_replacement_finish;
    SET_SAGA_ID -> "unused_1" {
        + rsrfs_set_saga_id
        - rsrfs_set_saga_id_undo
    }
    UPDATE_REQUEST_RECORD -> "unused_4" {
        + rsrfs_update_request_record
    }
}

// region snapshot replacement finish saga: definition

#[derive(Debug)]
pub(crate) struct SagaRegionSnapshotReplacementFinish;
impl NexusSaga for SagaRegionSnapshotReplacementFinish {
    const NAME: &'static str = "region-snapshot-replacement-finish";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        region_snapshot_replacement_finish_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(Node::action(
            "saga_id",
            "GenerateSagaId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(set_saga_id_action());

        if let Some(new_region_volume_id) =
            params.request.new_region_volume_id()
        {
            let subsaga_params = volume_delete::Params {
                serialized_authn: params.serialized_authn.clone(),
                volume_id: new_region_volume_id,
            };

            let subsaga_dag = {
                let subsaga_builder = steno::DagBuilder::new(
                    steno::SagaName::new(volume_delete::SagaVolumeDelete::NAME),
                );
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
        }

        builder.append(update_request_record_action());

        Ok(builder.build()?)
    }
}

// region snapshot replacement finish saga: action implementations

async fn rsrfs_set_saga_id(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;

    // Change the request record here to an intermediate "completing" state to
    // block out other sagas that will be triggered for the same request.
    osagactx
        .datastore()
        .set_region_snapshot_replacement_completing(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn rsrfs_set_saga_id_undo(
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
        .undo_set_region_snapshot_replacement_completing(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await?;

    Ok(())
}

async fn rsrfs_update_request_record(
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

    // Update the replacement request record to 'Complete' and clear the
    // operating saga id. There is no undo step for this, it should succeed
    // idempotently.
    datastore
        .set_region_snapshot_replacement_complete(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}
