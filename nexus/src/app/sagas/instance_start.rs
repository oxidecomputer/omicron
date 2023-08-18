// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implements a saga that starts an instance.

use super::{NexusActionContext, NexusSaga, SagaInitError};
use crate::{
    app::{
        instance::WriteBackUpdatedInstance,
        sagas::{declare_saga_actions, retry_until_known_result},
    },
    authn, authz,
    db::{self, identity::Resource, lookup::LookupPath},
};
use omicron_common::api::external::{Error, InstanceState};
use serde::{Deserialize, Serialize};
use sled_agent_client::types::InstanceStateRequested;
use slog::info;
use steno::ActionError;

/// Parameters to the instance start saga.
#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub instance: db::model::Instance,

    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,

    /// True if the saga should configure Dendrite and OPTE configuration for
    /// this instance. This allows the instance create saga to do this work
    /// prior to invoking the instance start saga as a subsaga without repeating
    /// these steps.
    pub ensure_network: bool,
}

declare_saga_actions! {
    instance_start;

    MARK_AS_STARTING -> "starting_state" {
        + sis_move_to_starting
        - sis_move_to_starting_undo
    }

    // TODO(#3879) This can be replaced with an action that triggers the NAT RPW
    // once such an RPW is available.
    DPD_ENSURE -> "dpd_ensure" {
        + sis_dpd_ensure
        - sis_dpd_ensure_undo
    }

    V2P_ENSURE -> "v2p_ensure" {
        + sis_v2p_ensure
        - sis_v2p_ensure_undo
    }

    ENSURE_REGISTERED -> "ensure_registered" {
        + sis_ensure_registered
        - sis_ensure_registered_undo
    }

    ENSURE_RUNNING -> "ensure_running" {
        + sis_ensure_running
    }
}

#[derive(Debug)]
pub struct SagaInstanceStart;
impl NexusSaga for SagaInstanceStart {
    const NAME: &'static str = "instance-start";
    type Params = Params;

    fn register_actions(registry: &mut super::ActionRegistry) {
        instance_start_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(mark_as_starting_action());
        builder.append(dpd_ensure_action());
        builder.append(v2p_ensure_action());
        builder.append(ensure_registered_action());
        builder.append(ensure_running_action());
        Ok(builder.build()?)
    }
}

async fn sis_move_to_starting(
    sagactx: NexusActionContext,
) -> Result<db::model::InstanceRuntimeState, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let instance_id = params.instance.id();
    info!(osagactx.log(), "moving instance to Starting state via saga";
          "instance_id" => %instance_id);

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // The saga invoker needs to supply a prior state in which the instance can
    // legally be started. This action will try to transition the instance to
    // the Starting state; once this succeeds, the instance can't be deleted, so
    // it is safe to program its network configuration (if required) and then
    // try to start it.
    //
    // This interlock is not sufficient to handle multiple concurrent instance
    // creation sagas. See below.
    if !matches!(
        params.instance.runtime_state.state.0,
        InstanceState::Creating | InstanceState::Stopped,
    ) {
        return Err(ActionError::action_failed(Error::conflict(&format!(
            "instance is in state {}, but must be one of {} or {} to be started",
            params.instance.runtime_state.state.0,
            InstanceState::Creating,
            InstanceState::Stopped
        ))));
    }

    let new_runtime = db::model::InstanceRuntimeState {
        state: db::model::InstanceState::new(InstanceState::Starting),
        gen: params.instance.runtime_state.gen.next().into(),
        ..params.instance.runtime_state
    };

    if !osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime)
        .await
        .map_err(ActionError::action_failed)?
    {
        // If the update was not applied, but the desired state is already
        // what's in the database, proceed anyway.
        //
        // TODO(#2315) This logic is not completely correct. It provides
        // idempotency in the case where this action moved the instance to
        // Starting, but the action was then replayed. It does not handle the
        // case where the conflict occurred because a different instance of this
        // saga won the race to set the instance to Starting; this will lead to
        // two sagas concurrently trying to start the instance.
        //
        // The correct way to handle this case is to use saga-generated Propolis
        // IDs to distinguish between saga executions: the ID must be NULL in
        // order to start the instance; if multiple saga executions race, only
        // one will write its chosen ID to the record, allowing the sagas to
        // determine a winner.
        let (.., new_instance) = LookupPath::new(&opctx, &osagactx.datastore())
            .instance_id(instance_id)
            .fetch()
            .await
            .map_err(ActionError::action_failed)?;

        if new_instance.runtime_state.gen != new_runtime.gen
            || !matches!(
                new_instance.runtime_state.state.0,
                InstanceState::Starting
            )
        {
            return Err(ActionError::action_failed(Error::conflict(
                "instance changed state before it could be started",
            )));
        }

        info!(osagactx.log(), "start saga: instance was already starting";
              "instance_id" => %instance_id);
    }

    Ok(new_runtime)
}

async fn sis_move_to_starting_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    info!(osagactx.log(), "start saga failed; returning instance to Stopped";
          "instance_id" => %params.instance.id());

    let runtime_state =
        sagactx.lookup::<db::model::InstanceRuntimeState>("starting_state")?;

    // Don't just restore the old state; if the instance was being created, and
    // starting it failed, the instance is now stopped, not creating.
    let new_runtime = db::model::InstanceRuntimeState {
        state: db::model::InstanceState::new(InstanceState::Stopped),
        gen: runtime_state.gen.next().into(),
        ..runtime_state
    };

    if !osagactx
        .datastore()
        .instance_update_runtime(&params.instance.id(), &new_runtime)
        .await?
    {
        info!(osagactx.log(),
              "did not return instance to Stopped: old generation number";
              "instance_id" => %params.instance.id());
    }

    Ok(())
}

async fn sis_dpd_ensure(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    if !params.ensure_network {
        info!(osagactx.log(), "start saga: skipping dpd_ensure by request";
              "instance_id" => %params.instance.id());

        return Ok(());
    }

    info!(osagactx.log(), "start saga: ensuring instance dpd configuration";
          "instance_id" => %params.instance.id());

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let datastore = osagactx.datastore();
    let runtime_state =
        sagactx.lookup::<db::model::InstanceRuntimeState>("starting_state")?;

    let sled_uuid = runtime_state.sled_id;
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, &datastore)
        .sled_id(sled_uuid)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    let boundary_switches = osagactx
        .nexus()
        .boundary_switches(&opctx)
        .await
        .map_err(ActionError::action_failed)?;

    for switch in boundary_switches {
        let dpd_client =
            osagactx.nexus().dpd_clients.get(&switch).ok_or_else(|| {
                ActionError::action_failed(Error::internal_error(&format!(
                    "unable to find client for switch {switch}"
                )))
            })?;

        osagactx
            .nexus()
            .instance_ensure_dpd_config(
                &opctx,
                params.instance.id(),
                &sled.address(),
                None,
                dpd_client,
            )
            .await
            .map_err(ActionError::action_failed)?;
    }

    Ok(())
}

async fn sis_dpd_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    if !params.ensure_network {
        info!(log,
              "start saga: didn't ensure dpd configuration, nothing to undo";
              "instance_id" => %params.instance.id());

        return Ok(());
    }

    info!(log, "start saga: undoing dpd configuration";
          "instance_id" => %params.instance.id());

    let datastore = &osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let target_ips = &datastore
        .instance_lookup_external_ips(&opctx, params.instance.id())
        .await?;

    let boundary_switches = osagactx.nexus().boundary_switches(&opctx).await?;
    for switch in boundary_switches {
        let dpd_client =
            osagactx.nexus().dpd_clients.get(&switch).ok_or_else(|| {
                ActionError::action_failed(Error::internal_error(&format!(
                    "unable to find client for switch {switch}"
                )))
            })?;

        for ip in target_ips {
            let result = retry_until_known_result(log, || async {
                dpd_client
                    .ensure_nat_entry_deleted(log, ip.ip, *ip.first_port)
                    .await
            })
            .await;

            match result {
                Ok(_) => {
                    debug!(log, "successfully deleted nat entry for {ip:#?}");
                    Ok(())
                }
                Err(e) => Err(Error::internal_error(&format!(
                    "failed to delete nat entry for {ip:#?} via dpd: {e}"
                ))),
            }?;
        }
    }

    Ok(())
}

async fn sis_v2p_ensure(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    if !params.ensure_network {
        info!(osagactx.log(), "start saga: skipping v2p_ensure by request";
              "instance_id" => %params.instance.id());

        return Ok(());
    }

    info!(osagactx.log(), "start saga: ensuring v2p mappings are configured";
          "instance_id" => %params.instance.id());

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let runtime_state =
        sagactx.lookup::<db::model::InstanceRuntimeState>("starting_state")?;

    let sled_uuid = runtime_state.sled_id;
    osagactx
        .nexus()
        .create_instance_v2p_mappings(&opctx, params.instance.id(), sled_uuid)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sis_v2p_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    if !params.ensure_network {
        info!(osagactx.log(),
              "start saga: didn't ensure v2p configuration, nothing to undo";
              "instance_id" => %params.instance.id());

        return Ok(());
    }

    let instance_id = params.instance.id();
    info!(osagactx.log(), "start saga: undoing v2p configuration";
          "instance_id" => %instance_id);

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .nexus()
        .delete_instance_v2p_mappings(&opctx, instance_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sis_ensure_registered(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let osagactx = sagactx.user_data();

    info!(osagactx.log(), "start saga: ensuring instance is registered on sled";
          "instance_id" => %params.instance.id(),
          "sled_id" => %params.instance.runtime().sled_id);

    let (.., authz_instance, mut db_instance) =
        LookupPath::new(&opctx, &osagactx.datastore())
            .instance_id(params.instance.id())
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    // The instance is not really being "created" (it already exists from
    // the caller's perspective), but if it does not exist on its sled, the
    // target sled agent will populate its instance manager with the
    // contents of this modified record, and that record needs to allow a
    // transition to the Starting state.
    db_instance.runtime_state.state =
        nexus_db_model::InstanceState(InstanceState::Creating);

    osagactx
        .nexus()
        .instance_ensure_registered(&opctx, &authz_instance, &db_instance)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sis_ensure_registered_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let datastore = osagactx.datastore();
    let instance_id = params.instance.id();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    info!(osagactx.log(), "start saga: unregistering instance from sled";
          "instance_id" => %instance_id,
          "sled_id" => %params.instance.runtime().sled_id);

    let (.., authz_instance, db_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_unregistered(
            &opctx,
            &authz_instance,
            &db_instance,
            WriteBackUpdatedInstance::WriteBack,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sis_ensure_running(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    info!(osagactx.log(), "start saga: ensuring instance is running";
          "instance_id" => %params.instance.id());

    let (.., authz_instance, db_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(params.instance.id())
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_request_state(
            &opctx,
            &authz_instance,
            &db_instance,
            InstanceStateRequested::Running,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::external_api::params;
    use crate::{
        app::{saga::create_saga_dag, sagas::test_helpers},
        authn, Nexus, TestInterfaces as _,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_test_utils::resource_helpers::{
        create_project, object_create, populate_ip_pool,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::{
        ByteCount, IdentityMetadataCreateParams, InstanceCpuCount,
    };
    use sled_agent_client::TestInterfaces as _;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use uuid::Uuid;

    use super::*;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "test-project";
    const INSTANCE_NAME: &str = "test-instance";

    async fn setup_test_project(client: &ClientTestContext) -> Uuid {
        populate_ip_pool(&client, "default", None).await;
        let project = create_project(&client, PROJECT_NAME).await;
        project.identity.id
    }

    async fn create_instance(
        client: &ClientTestContext,
    ) -> omicron_common::api::external::Instance {
        let instances_url = format!("/v1/instances?project={}", PROJECT_NAME);
        object_create(
            client,
            &instances_url,
            &params::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: INSTANCE_NAME.parse().unwrap(),
                    description: format!("instance {:?}", INSTANCE_NAME),
                },
                ncpus: InstanceCpuCount(2),
                memory: ByteCount::from_gibibytes_u32(2),
                hostname: String::from(INSTANCE_NAME),
                user_data: b"#cloud-config".to_vec(),
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::None,
                external_ips: vec![],
                disks: vec![],
                start: false,
            },
        )
        .await
    }

    async fn fetch_db_instance(
        cptestctx: &ControlPlaneTestContext,
        opctx: &nexus_db_queries::context::OpContext,
        id: Uuid,
    ) -> nexus_db_model::Instance {
        let datastore = cptestctx.server.apictx().nexus.datastore().clone();
        let (.., db_instance) = LookupPath::new(&opctx, &datastore)
            .instance_id(id)
            .fetch()
            .await
            .expect("test instance should be present in datastore");

        info!(&cptestctx.logctx.log, "refetched instance from db";
              "instance" => ?db_instance);

        db_instance
    }

    async fn instance_simulate(
        cptestctx: &ControlPlaneTestContext,
        nexus: &Arc<Nexus>,
        instance_id: &Uuid,
    ) {
        info!(&cptestctx.logctx.log, "Poking simulated instance";
              "instance_id" => %instance_id);
        let sa = nexus.instance_sled_by_id(instance_id).await.unwrap();
        sa.instance_finish_transition(*instance_id).await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let _project_id = setup_test_project(&client).await;
        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let db_instance =
            fetch_db_instance(cptestctx, &opctx, instance.identity.id).await;

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            instance: db_instance,
            ensure_network: true,
        };

        let dag = create_saga_dag::<SagaInstanceStart>(params).unwrap();
        let saga = nexus.create_runnable_saga(dag).await.unwrap();
        nexus.run_saga(saga).await.expect("Start saga should succeed");

        instance_simulate(cptestctx, nexus, &instance.identity.id).await;
        let db_instance =
            fetch_db_instance(cptestctx, &opctx, instance.identity.id).await;
        assert_eq!(db_instance.runtime().state.0, InstanceState::Running);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let _project_id = setup_test_project(&client).await;
        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;

        // Fetch enough state to be able to reason about how many nodes are in
        // the saga.
        let db_instance =
            fetch_db_instance(cptestctx, &opctx, instance.identity.id).await;
        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            instance: db_instance,
            ensure_network: true,
        };

        test_helpers::action_failure_can_unwind::<
            SagaInstanceStart,
            _,
            _,
        >(
            nexus,
            params,
            || {
                Box::pin({
                    async {
                        let db_instance = fetch_db_instance(
                            cptestctx,
                            &opctx,
                            instance.identity.id,
                        )
                        .await;

                        Params {
                            serialized_authn:
                                authn::saga::Serialized::for_opctx(&opctx),
                            instance: db_instance,
                            ensure_network: true,
                        }
                    }
                })
            },
            || {
                Box::pin({
                    async {
                        let new_db_instance = fetch_db_instance(
                            cptestctx,
                            &opctx,
                            instance.identity.id,
                        )
                        .await;

                        info!(log,
                              "fetched instance runtime state after saga execution";
                              "instance_id" => %instance.identity.id,
                              "instance_runtime" => ?new_db_instance.runtime());

                        assert_eq!(
                            new_db_instance.runtime().state.0,
                            InstanceState::Stopped
                        );
                    }
                })
            },
            log,
        ).await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let _project_id = setup_test_project(&client).await;
        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let db_instance =
            fetch_db_instance(cptestctx, &opctx, instance.identity.id).await;

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            instance: db_instance,
            ensure_network: true,
        };

        let dag = create_saga_dag::<SagaInstanceStart>(params).unwrap();
        let runnable_saga =
            nexus.create_runnable_saga(dag.clone()).await.unwrap();

        for node in dag.get_nodes() {
            nexus
                .sec()
                .saga_inject_repeat(
                    runnable_saga.id(),
                    node.index(),
                    steno::RepeatInjected {
                        action: NonZeroU32::new(2).unwrap(),
                        undo: NonZeroU32::new(1).unwrap(),
                    },
                )
                .await
                .unwrap();
        }

        nexus
            .run_saga(runnable_saga)
            .await
            .expect("Saga should have succeeded");

        instance_simulate(cptestctx, nexus, &instance.identity.id).await;
        let new_db_instance =
            fetch_db_instance(cptestctx, &opctx, instance.identity.id).await;

        assert_eq!(new_db_instance.runtime().state.0, InstanceState::Running);
    }
}
