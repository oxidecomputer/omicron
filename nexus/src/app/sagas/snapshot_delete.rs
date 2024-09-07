// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas;
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::{authn, authz, db};
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub authz_snapshot: authz::Snapshot,
    pub snapshot: db::model::Snapshot,
}

declare_saga_actions! {
    snapshot_delete;
    DELETE_SNAPSHOT_RECORD -> "no_result1" {
        + ssd_delete_snapshot_record
    }
    SPACE_ACCOUNT -> "no_result2" {
        + ssd_account_space
    }
    NOOP -> "no_result3" {
        + ssd_noop
    }
}

#[derive(Debug)]
pub(crate) struct SagaSnapshotDelete;
impl NexusSaga for SagaSnapshotDelete {
    const NAME: &'static str = "snapshot-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        snapshot_delete_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(delete_snapshot_record_action());
        builder.append(space_account_action());

        const DELETE_VOLUME_PARAMS: &'static str = "delete_volume_params";
        const DELETE_VOLUME_DESTINATION_PARAMS: &'static str =
            "delete_volume_destination_params";

        let volume_delete_params = sagas::volume_delete::Params {
            serialized_authn: params.serialized_authn.clone(),
            volume_id: params.snapshot.volume_id,
        };
        builder.append(Node::constant(
            DELETE_VOLUME_PARAMS,
            serde_json::to_value(&volume_delete_params).map_err(|e| {
                super::SagaInitError::SerializeError(
                    String::from("volume_id"),
                    e,
                )
            })?,
        ));

        let volume_delete_params = sagas::volume_delete::Params {
            serialized_authn: params.serialized_authn.clone(),
            volume_id: params.snapshot.destination_volume_id,
        };
        builder.append(Node::constant(
            DELETE_VOLUME_DESTINATION_PARAMS,
            serde_json::to_value(&volume_delete_params).map_err(|e| {
                super::SagaInitError::SerializeError(
                    String::from("destination_volume_id"),
                    e,
                )
            })?,
        ));

        let make_volume_delete_dag = || {
            let subsaga_builder = steno::DagBuilder::new(steno::SagaName::new(
                sagas::volume_delete::SagaVolumeDelete::NAME,
            ));
            sagas::volume_delete::create_dag(subsaga_builder)
        };

        builder.append_parallel(vec![
            steno::Node::subsaga(
                "delete_volume",
                make_volume_delete_dag()?,
                DELETE_VOLUME_PARAMS,
            ),
            steno::Node::subsaga(
                "delete_destination_volume",
                make_volume_delete_dag()?,
                DELETE_VOLUME_DESTINATION_PARAMS,
            ),
        ]);

        builder.append(noop_action());

        Ok(builder.build()?)
    }
}

// snapshot delete saga: action implementations

async fn ssd_delete_snapshot_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .datastore()
        .project_delete_snapshot(
            &opctx,
            &params.authz_snapshot,
            &params.snapshot,
            vec![
                db::model::SnapshotState::Ready,
                db::model::SnapshotState::Faulted,
                db::model::SnapshotState::Destroyed,
            ],
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn ssd_account_space(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_snapshot(
            &opctx,
            params.authz_snapshot.id(),
            params.snapshot.project_id,
            params.snapshot.size,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

// Sagas must end in one node, not parallel
async fn ssd_noop(_sagactx: NexusActionContext) -> Result<(), ActionError> {
    Ok(())
}
