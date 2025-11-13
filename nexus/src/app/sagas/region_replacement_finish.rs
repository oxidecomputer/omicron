// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Compared to the rest of the region replacement process, finishing the
//! process is straight forward. This saga is responsible for the following
//! region replacement request state transitions:
//!
//! ```text
//!     ReplacementDone  <--
//!                         |
//!            |            |
//!            v            |
//!                         |
//!        Completing     --
//!
//!            |
//!            v
//!
//!        Completed
//! ```
//!
//! It will set itself as the "operating saga" for a region replacement request,
//! change the state to "Completing", and:
//!
//! 1. Call the Volume delete saga for the fake Volume that points to the old
//!    region.
//!
//! 2. Clear the operating saga id from the request record, and change the state
//!    to Completed.
//!

use super::{
    ACTION_GENERATE_ID, ActionRegistry, NexusActionContext, NexusSaga,
    SagaInitError,
};
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::volume_delete;
use crate::app::{authn, db};
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// region replacement finish saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    /// The fake volume created for the region that was replaced
    // Note: this is only required in the params to build the volume-delete sub
    // saga
    pub region_volume_id: VolumeUuid,
    pub request: db::model::RegionReplacement,
}

// region replacement finish saga: actions

declare_saga_actions! {
    region_replacement_finish;
    SET_SAGA_ID -> "unused_1" {
        + srrf_set_saga_id
        - srrf_set_saga_id_undo
    }
    UPDATE_REQUEST_RECORD -> "unused_2" {
        + srrf_update_request_record
    }
}

// region replacement finish saga: definition

#[derive(Debug)]
pub(crate) struct SagaRegionReplacementFinish;
impl NexusSaga for SagaRegionReplacementFinish {
    const NAME: &'static str = "region-replacement-finish";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        region_replacement_finish_register_actions(registry);
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

        let subsaga_params = volume_delete::Params {
            serialized_authn: params.serialized_authn.clone(),
            volume_id: params.region_volume_id,
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

// region replacement finish saga: action implementations

async fn srrf_set_saga_id(
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
        .set_region_replacement_completing(&opctx, params.request.id, saga_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn srrf_set_saga_id_undo(
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
        .undo_set_region_replacement_completing(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await?;

    Ok(())
}

async fn srrf_update_request_record(
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

    // Now that the region has been deleted, update the replacement request
    // record to 'Complete' and clear the operating saga id. There is no undo
    // step for this, it should succeed idempotently.
    datastore
        .set_region_replacement_complete(&opctx, params.request, saga_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::sagas::region_replacement_finish::Params,
        app::sagas::region_replacement_finish::SagaRegionReplacementFinish,
    };
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use nexus_db_model::Region;
    use nexus_db_model::RegionReplacement;
    use nexus_db_model::RegionReplacementState;
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::context::OpContext;
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
    async fn test_region_replacement_finish_saga(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Manually insert required records
        let old_region_volume_id = VolumeUuid::new_v4();
        let new_volume_id = VolumeUuid::new_v4();

        let replaced_region = {
            let dataset_id = DatasetUuid::new_v4();
            Region::new(
                dataset_id,
                old_region_volume_id,
                512_i64.try_into().unwrap(),
                10,
                10,
                12345,
                false,
            )
        };

        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();

            use nexus_db_schema::schema::region::dsl;
            diesel::insert_into(dsl::region)
                .values(replaced_region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();
        }

        let volume_construction_request = VolumeConstructionRequest::Volume {
            id: *old_region_volume_id.as_untyped_uuid(),
            block_size: 0,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 0,
                blocks_per_extent: 0,
                extent_count: 0,
                generation: 0,
                opts: CrucibleOpts {
                    id: Uuid::new_v4(),
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
            .volume_create(old_region_volume_id, volume_construction_request)
            .await
            .unwrap();

        let request = RegionReplacement {
            id: Uuid::new_v4(),
            request_time: Utc::now(),
            old_region_id: replaced_region.id(),
            volume_id: new_volume_id.into(),
            old_region_volume_id: Some(old_region_volume_id.into()),
            new_region_id: None, // no value needed here
            replacement_state: RegionReplacementState::ReplacementDone,
            operating_saga_id: None,
        };

        datastore
            .volume_create(
                new_volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // Run the region replacement finish saga
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            region_volume_id: old_region_volume_id,
            request: request.clone(),
        };
        let _output = nexus
            .sagas
            .saga_execute::<SagaRegionReplacementFinish>(params)
            .await
            .unwrap();

        // Validate the state transition
        let result = datastore
            .get_region_replacement_request_by_id(&opctx, request.id)
            .await
            .unwrap();
        assert_eq!(result.replacement_state, RegionReplacementState::Complete);
        assert!(result.operating_saga_id.is_none());

        // Validate the Volume was deleted
        assert!(
            datastore.volume_get(old_region_volume_id).await.unwrap().is_none()
        );
    }
}
