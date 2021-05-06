/*!
 * Saga actions, undo actions, and saga constructors used in Nexus.
 */

/*
 * NOTE: We want to be careful about what interfaces we expose to saga actions.
 * In the future, we expect to mock these out for comprehensive testing of
 * correctness, idempotence, etc.  The more constrained this interface is, the
 * easier it will be to test, version, and update in deployed systems.
 */

use crate::saga_interface::SagaContext;
use chrono::Utc;
use omicron_common::model::ApiGeneration;
use omicron_common::model::ApiInstanceCreateParams;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::model::ApiInstanceState;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaType;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub struct ParamsInstanceCreate {
    pub project_id: Uuid,
    pub create_params: ApiInstanceCreateParams,
}

#[derive(Debug)]
pub struct SagaInstanceCreate;
impl SagaType for SagaInstanceCreate {
    type SagaParamsType = Arc<ParamsInstanceCreate>;
    type ExecContextType = Arc<SagaContext>;
}

pub fn saga_instance_create() -> SagaTemplate<SagaInstanceCreate> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "instance_id",
        "GenerateInstanceId",
        new_action_noop_undo(sic_generate_instance_id),
    );

    template_builder.append(
        "server_id",
        "AllocServer",
        // TODO-robustness This still needs an undo action, and we should really
        // keep track of resources and reservations, etc.  See the comment on
        // SagaContext::alloc_server()
        new_action_noop_undo(sic_alloc_server),
    );

    template_builder.append(
        "initial_runtime",
        "CreateInstanceRecord",
        new_action_noop_undo(sic_create_instance_record),
    );

    template_builder.append(
        "instance_ensure",
        "InstanceEnsure",
        new_action_noop_undo(sic_instance_ensure),
    );

    template_builder.build()
}

async fn sic_generate_instance_id(
    _: ActionContext<SagaInstanceCreate>,
) -> Result<Uuid, ActionError> {
    Ok(Uuid::new_v4())
}

async fn sic_alloc_server(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<Uuid, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    osagactx
        .alloc_server(&params.create_params)
        .await
        .map_err(ActionError::action_failed)
}

async fn sic_create_instance_record(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<ApiInstanceRuntimeState, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let sled_uuid = sagactx.lookup::<Uuid>("server_id");
    let instance_id = sagactx.lookup::<Uuid>("instance_id");

    let runtime = ApiInstanceRuntimeState {
        run_state: ApiInstanceState::Creating,
        reboot_in_progress: false,
        sled_uuid: sled_uuid?,
        gen: ApiGeneration::new(),
        time_updated: Utc::now(),
    };

    let instance = osagactx
        .datastore()
        .project_create_instance(
            &instance_id?,
            &params.project_id,
            &params.create_params,
            &runtime,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(instance.runtime)
}

async fn sic_instance_ensure(
    sagactx: ActionContext<SagaInstanceCreate>,
) -> Result<(), ActionError> {
    /*
     * TODO-correctness is this idempotent?
     */
    let osagactx = sagactx.user_data();
    let runtime_params = ApiInstanceRuntimeStateRequested {
        run_state: ApiInstanceState::Running,
        reboot_wanted: false,
    };
    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
    let sled_uuid = sagactx.lookup::<Uuid>("server_id")?;
    let initial_runtime =
        sagactx.lookup::<ApiInstanceRuntimeState>("initial_runtime")?;
    let sa = osagactx
        .sled_client(&sled_uuid)
        .await
        .map_err(ActionError::action_failed)?;

    /*
     * Ask the sled agent to begin the state change.  Then update the database
     * to reflect the new intermediate state.  If this update is not the newest
     * one, that's fine.  That might just mean the sled agent beat us to it.
     */
    let new_runtime_state = sa
        .instance_ensure(instance_id, initial_runtime, runtime_params)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime_state)
        .await
        .map(|_| ())
        .map_err(ActionError::action_failed)
}
