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
            volume_id: params.snapshot.volume_id(),
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
            volume_id: params.snapshot.destination_volume_id(),
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

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    use crate::{
        app::authn::saga::Serialized, app::sagas::test::assert_dag_unchanged,
        app::sagas::test_helpers,
    };
    use chrono::Utc;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external;
    use omicron_common::api::external::LookupType;
    use omicron_uuid_kinds::VolumeUuid;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn assert_saga_dags_unchanged(cptestctx: &ControlPlaneTestContext) {
        let opctx = test_helpers::test_opctx(&cptestctx);

        let silo = authz::Silo::new(
            authz::FLEET,
            Uuid::new_v4(),
            LookupType::ByName("silo".to_string()),
        );

        let project = authz::Project::new(
            silo.clone(),
            Uuid::new_v4(),
            LookupType::ByName("project".to_string()),
        );

        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            authz_snapshot: authz::Snapshot::new(
                project,
                Uuid::new_v4(),
                LookupType::ByName("snapshot".to_string()),
            ),
            snapshot: db::model::Snapshot {
                identity: db::model::SnapshotIdentity {
                    id: Uuid::new_v4(),
                    name: external::Name::try_from("snapshot".to_string())
                        .unwrap()
                        .into(),
                    description: "snapshot".into(),

                    time_created: Utc::now(),
                    time_modified: Utc::now(),
                    time_deleted: None,
                },

                project_id: Uuid::new_v4(),
                disk_id: Uuid::new_v4(),
                volume_id: VolumeUuid::new_v4().into(),
                destination_volume_id: VolumeUuid::new_v4().into(),

                gen: db::model::Generation::new(),
                state: db::model::SnapshotState::Creating,
                block_size: db::model::BlockSize::AdvancedFormat,

                size: external::ByteCount::from_gibibytes_u32(2).into(),
            },
        };

        assert_dag_unchanged::<SagaSnapshotDelete>(
            "snapshot_delete.json",
            params,
        );
    }
}
