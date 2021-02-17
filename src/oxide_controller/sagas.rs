/*!
 * Saga actions, undo actions, and saga constructors used in the controller.
 */

/*
 * NOTE: We want to be careful about what interfaces we expose to saga actions.
 * In the future, we expect to mock these out for comprehensive testing of
 * correctness, idempotence, etc.  The more constrained this interface is, the
 * easier it will be to test, version, and update in deployed systems.
 */

/*
 * XXX thoughts for steno:
 * - may want a first-class way to pass parameters into the saga?
 * - may want a cache of in-memory-only data?  Not sure (e.g., for storing
 *   SledAgentClient)
 */

use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiInstanceRuntimeState;
use crate::api_model::ApiInstanceRuntimeStateRequested;
use crate::api_model::ApiInstanceState;
use crate::api_model::ApiName;
use crate::oxide_controller::saga_interface::OxcSagaContext;
use chrono::Utc;
use core::future::ready;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::SagaActionError;
use steno::SagaContext;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use uuid::Uuid;

pub fn saga_instance_create(
    project_name: &ApiName,
    params: &ApiInstanceCreateParams,
) -> SagaTemplate<Arc<OxcSagaContext>> {
    let mut template_builder = SagaTemplateBuilder::new();

    /*
     * TODO-cleanup This first action that just emits parameters is a bit icky.
     * It would be better if Steno first-classed the idea of a saga's parameter,
     * adding a type parameter for it on SagaTemplate, SagaExecutor, etc.  This
     * would allow us to load SagaTemplates once, at startup, and create new
     * executors with a new set of parameters when we need to run them again.
     * Making this change requires modifying the SagaLog to record the
     * parameters and to recover these as well.
     */
    struct SagaParamsInstanceCreate {
        project_name: ApiName,
        create_params: ApiInstanceCreateParams,
    }

    let saga_params = SagaParamsInstanceCreate {
        project_name: project_name.clone(),
        create_params: params.clone(),
    };

    template_builder.append(
        "params",
        "EmitParams",
        new_action_noop_undo(|_| ready(Ok(saga_params))),
    );

    template_builder.append(
        "instance_id",
        "GenerateInstanceId",
        new_action_noop_undo(|_| ready(Ok(Uuid::new_v4()))),
    );

    // XXX Compare this to what was previously in instance_create() -- and in
    // particular the block comment there explaining the order of operations and
    // crash-safety.
    template_builder.append(
        "server_id",
        "AllocServer",
        // XXX needs an undo action, and we should really keep track of
        // resources and reservations, etc.
        new_action_noop_undo(
            move |sagactx: SagaContext<Arc<OxcSagaContext>>| {
                let osagactx = sagactx.context().clone();
                let params =
                    sagactx.lookup::<SagaParamsInstanceCreate>("params");
                async move {
                    let project = osagactx
                        .datastore()
                        .project_lookup(&params.project_name)
                        .await
                        .map_err(SagaActionError::action_failed)?;
                    let sa = osagactx
                        .alloc_server(&project, &params.create_params)
                        .await
                        .map_err(SagaActionError::action_failed)?;
                    Ok(sa.id)
                }
            },
        ),
    );

    template_builder.append(
        "create_instance_record",
        "CreateInstanceRecord",
        new_action_noop_undo(
            move |sagactx: SagaContext<Arc<OxcSagaContext>>| {
                let osagactx = sagactx.context().clone();
                let params =
                    sagactx.lookup::<SagaParamsInstanceCreate>("params");
                let sled_uuid = sagactx.lookup::<Uuid>("server_id");
                let instance_id = sagactx.lookup::<Uuid>("instance_id");
                let runtime = ApiInstanceRuntimeState {
                    run_state: ApiInstanceState::Creating,
                    reboot_in_progress: false,
                    sled_uuid,
                    gen: 1,
                    time_updated: Utc::now(),
                };

                async move {
                    /*
                     * XXX I think we want to have resolved the project name to
                     * an id in an earlier step.
                     * XXX needs to handle the case where the record already
                     * exists and looks similar vs. different
                     */
                    osagactx
                        .datastore()
                        .project_create_instance(
                            &instance_id,
                            &params.project_name,
                            &params.create_params,
                            &runtime,
                        )
                        .await
                        .map_err(SagaActionError::action_failed)?;
                    Ok(())
                }
            },
        ),
    );

    template_builder.append(
        "instance_ensure",
        "InstanceEnsure",
        new_action_noop_undo(
            move |sagactx: SagaContext<Arc<OxcSagaContext>>| {
                let osagactx = sagactx.context().clone();
                let runtime_params = ApiInstanceRuntimeStateRequested {
                    run_state: ApiInstanceState::Running,
                    reboot_wanted: false,
                };
                let instance_id = sagactx.lookup::<Uuid>("instance_id");
                let sled_uuid = sagactx.lookup::<Uuid>("server_id");
                async move {
                    let sa = osagactx
                        .sled_client(&sled_uuid)
                        .await
                        .map_err(SagaActionError::action_failed)?;
                    // XXX Should this be cached from the previous stage?
                    let mut instance = osagactx
                        .datastore()
                        .instance_lookup_by_id(&instance_id)
                        .await
                        .map_err(SagaActionError::action_failed)?;

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
                        .map_err(SagaActionError::action_failed)?;

                    let instance_ref = Arc::make_mut(&mut instance);
                    instance_ref.runtime = new_runtime_state.clone();
                    osagactx
                        .datastore()
                        .instance_update(Arc::clone(&instance))
                        .await
                        .map_err(SagaActionError::action_failed)?;
                    Ok(())
                }
            },
        ),
    );

    /*
     * XXX what does the interface back to the controller look like for:
     * - accessing a server agent
     * - accessing the data store
     */

    template_builder.build()
}
