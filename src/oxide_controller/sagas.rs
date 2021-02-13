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
 * - may want a first-class way to pass context into each function (e.g., our
 *   OxcSagaContext)
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
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use uuid::Uuid;

pub fn saga_instance_create(
    osagactx: Arc<OxcSagaContext>,
    project_name: &ApiName,
    params: &ApiInstanceCreateParams,
) -> SagaTemplate {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "instance_id",
        "GenerateInstanceId",
        new_action_noop_undo(|_| ready(Ok(Uuid::new_v4()))),
    );

    // TODO-cleanup These clones should not be necessary.  But we need them in
    // various saga actions, and those are currently defined to be 'static
    // because in principle the caller is supposed to be able to rerun the saga
    // lots of times.  It might be better if there were a "params" interface in
    // steno.
    // XXX Compare this to what was previously in instance_create() -- and in
    // particular the block comment there explaining the order of operations and
    // crash-safety.
    let project_name_clone = project_name.clone();
    let params_clone = params.clone();
    let osaga_clone = Arc::clone(&osagactx);
    template_builder.append(
        "server_id",
        "AllocServer",
        // XXX needs an undo action, and we should really keep track of
        // resources and reservations, etc.
        new_action_noop_undo(move |_| {
            // XXX clones -- see above
            let osagactx = Arc::clone(&osaga_clone);
            let project_name = project_name_clone.clone();
            let params = params_clone.clone();
            async move {
                let project = osagactx
                    .datastore()
                    .project_lookup(&project_name)
                    .await
                    .map_err(SagaActionError::action_failed)?;
                let sa = osagactx
                    .alloc_server(&project, &params)
                    .await
                    .map_err(SagaActionError::action_failed)?;
                Ok(sa.id)
            }
        }),
    );

    // XXX clones -- see above
    let project_name_clone = project_name.clone();
    let params_clone = params.clone();
    let osaga_clone = Arc::clone(&osagactx);
    template_builder.append(
        "create_instance_record",
        "CreateInstanceRecord",
        new_action_noop_undo(move |sagactx| {
            // XXX clones -- see above
            let osagactx = Arc::clone(&osaga_clone);
            let project_name = project_name_clone.clone();
            let params = params_clone.clone();
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
                 * XXX I think we want to have resolved the project name to an
                 * id in an earlier step.
                 * XXX needs to use the instance id we allocated earlier
                 * XXX needs to handle the case where the record already exists
                 * and looks similar vs. different
                 */
                osagactx
                    .datastore()
                    .project_create_instance(
                        &instance_id,
                        &project_name,
                        &params,
                        &runtime,
                    )
                    .await
                    .map_err(SagaActionError::action_failed)?;
                Ok(())
            }
        }),
    );

    let osaga_clone = Arc::clone(&osagactx);
    template_builder.append(
        "instance_ensure",
        "InstanceEnsure",
        new_action_noop_undo(move |sagactx| {
            // XXX clones -- see above
            let osagactx = Arc::clone(&osaga_clone);
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
        }),
    );

    /*
     * XXX what does the interface back to the controller look like for:
     * - accessing a server agent
     * - accessing the data store
     */

    template_builder.build()
}
