/*!
 * Saga actions, undo actions, and saga constructors used in the controller.
 */

/*
 * NOTE: We want to be careful about what interfaces we expose to saga actions.
 * In the future, we expect to mock these out for comprehensive testing of
 * correctness, idempotence, etc.  The more constrained this interface is, the
 * easier it will be to test, version, and update in deployed systems.
 */

use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiInstanceRuntimeState;
use crate::api_model::ApiInstanceRuntimeStateRequested;
use crate::api_model::ApiInstanceState;
use crate::api_model::ApiName;
use crate::controller::saga_interface::OxcSagaContext;
use chrono::Utc;
use core::future::ready;
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
    pub project_name: ApiName,
    pub create_params: ApiInstanceCreateParams,
}

pub struct OxcSagaInstanceCreate;
impl SagaType for OxcSagaInstanceCreate {
    type SagaParamsType = Arc<ParamsInstanceCreate>;
    type ExecContextType = Arc<OxcSagaContext>;
}

pub fn saga_instance_create() -> SagaTemplate<OxcSagaInstanceCreate> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "instance_id",
        "GenerateInstanceId",
        new_action_noop_undo(|_| ready(Ok(Uuid::new_v4()))),
    );

    template_builder.append(
        "server_id",
        "AllocServer",
        // TODO-robustness This still needs an undo action, and we should really
        // keep track of resources and reservations, etc.  See the comment on
        // OxcSagaContext::alloc_server()
        new_action_noop_undo(
            move |sagactx: ActionContext<OxcSagaInstanceCreate>| {
                let osagactx = sagactx.user_data().clone();
                let params = sagactx.saga_params().clone();
                async move {
                    let project = osagactx
                        .datastore()
                        .project_lookup(&params.project_name)
                        .await
                        .map_err(ActionError::action_failed)?;
                    osagactx
                        .alloc_server(&project, &params.create_params)
                        .await
                        .map_err(ActionError::action_failed)
                }
            },
        ),
    );

    template_builder.append(
        "create_instance_record",
        "CreateInstanceRecord",
        new_action_noop_undo(
            move |sagactx: ActionContext<OxcSagaInstanceCreate>| {
                let osagactx = sagactx.user_data().clone();
                let params = sagactx.saga_params().clone();
                let sled_uuid = sagactx.lookup::<Uuid>("server_id");
                let instance_id = sagactx.lookup::<Uuid>("instance_id");

                async move {
                    let runtime = ApiInstanceRuntimeState {
                        run_state: ApiInstanceState::Creating,
                        reboot_in_progress: false,
                        sled_uuid: sled_uuid?,
                        gen: 1,
                        time_updated: Utc::now(),
                    };

                    /*
                     * TODO-correctness We want to have resolved the project
                     * name to an id in an earlier step.  To do that, we need to
                     * fix a bunch of the datastore issues.
                     * TODO-correctness needs to handle the case where the
                     * record already exists and looks similar vs. different
                     */
                    osagactx
                        .datastore()
                        .project_create_instance(
                            &instance_id?,
                            &params.project_name,
                            &params.create_params,
                            &runtime,
                        )
                        .await
                        .map_err(ActionError::action_failed)?;
                    Ok(())
                }
            },
        ),
    );

    template_builder.append(
        "instance_ensure",
        "InstanceEnsure",
        new_action_noop_undo(
            /*
             * TODO-correctness is this idempotent?
             */
            move |sagactx: ActionContext<OxcSagaInstanceCreate>| {
                let osagactx = sagactx.user_data().clone();
                let runtime_params = ApiInstanceRuntimeStateRequested {
                    run_state: ApiInstanceState::Running,
                    reboot_wanted: false,
                };
                async move {
                    let instance_id = sagactx.lookup::<Uuid>("instance_id")?;
                    let sled_uuid = sagactx.lookup::<Uuid>("server_id")?;
                    let sa = osagactx
                        .sled_client(&sled_uuid)
                        .await
                        .map_err(ActionError::action_failed)?;
                    /*
                     * TODO-datastore This should be cached from the previous
                     * stage once we figure out how best to pass this
                     * information between saga actions.
                     */
                    let mut instance = osagactx
                        .datastore()
                        .instance_lookup_by_id(&instance_id)
                        .await
                        .map_err(ActionError::action_failed)?;

                    /*
                     * Ask the SA to begin the state change.  Then update the
                     * database to reflect the new intermediate state.
                     */
                    let new_runtime_state = sa
                        .instance_ensure(
                            instance.identity.id,
                            instance.runtime.clone(),
                            runtime_params,
                        )
                        .await
                        .map_err(ActionError::action_failed)?;

                    let instance_ref = Arc::make_mut(&mut instance);
                    instance_ref.runtime = new_runtime_state.clone();
                    osagactx
                        .datastore()
                        .instance_update(Arc::clone(&instance))
                        .await
                        .map_err(ActionError::action_failed)?;
                    Ok(())
                }
            },
        ),
    );

    template_builder.build()
}
