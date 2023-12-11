// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implements a saga that starts an instance.

use std::net::Ipv6Addr;

use super::{
    instance_common::allocate_sled_ipv6, NexusActionContext, NexusSaga,
    SagaInitError, ACTION_GENERATE_ID,
};
use crate::app::sagas::declare_saga_actions;
use chrono::Utc;
use nexus_db_queries::db::{identity::Resource, lookup::LookupPath};
use nexus_db_queries::{authn, authz, db};
use omicron_common::api::external::{Error, InstanceState};
use serde::{Deserialize, Serialize};
use slog::info;
use steno::{ActionError, Node};
use uuid::Uuid;

/// Parameters to the instance start saga.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub db_instance: db::model::Instance,

    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

declare_saga_actions! {
    instance_start;

    ALLOC_SERVER -> "sled_id" {
        + sis_alloc_server
        - sis_alloc_server_undo
    }

    ALLOC_PROPOLIS_IP -> "propolis_ip" {
        + sis_alloc_propolis_ip
    }

    CREATE_VMM_RECORD -> "vmm_record" {
        + sis_create_vmm_record
        - sis_destroy_vmm_record
    }

    MARK_AS_STARTING -> "started_record" {
        + sis_move_to_starting
        - sis_move_to_starting_undo
    }

    ADD_VIRTUAL_RESOURCES -> "virtual_resources" {
        + sis_account_virtual_resources
        - sis_account_virtual_resources_undo
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
pub(crate) struct SagaInstanceStart;
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
        builder.append(Node::action(
            "propolis_id",
            "GeneratePropolisId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(alloc_server_action());
        builder.append(alloc_propolis_ip_action());
        builder.append(create_vmm_record_action());
        builder.append(mark_as_starting_action());
        builder.append(add_virtual_resources_action());
        builder.append(dpd_ensure_action());
        builder.append(v2p_ensure_action());
        builder.append(ensure_registered_action());
        builder.append(ensure_running_action());
        Ok(builder.build()?)
    }
}

async fn sis_alloc_server(
    sagactx: NexusActionContext,
) -> Result<Uuid, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let hardware_threads = params.db_instance.ncpus.0;
    let reservoir_ram = params.db_instance.memory;
    let propolis_id = sagactx.lookup::<Uuid>("propolis_id")?;

    let resource = super::instance_common::reserve_vmm_resources(
        osagactx.nexus(),
        propolis_id,
        hardware_threads.0 as u32,
        reservoir_ram,
        db::model::SledReservationConstraints::none(),
    )
    .await?;

    Ok(resource.sled_id)
}

async fn sis_alloc_server_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let propolis_id = sagactx.lookup::<Uuid>("propolis_id")?;

    osagactx.nexus().delete_sled_reservation(propolis_id).await?;
    Ok(())
}

async fn sis_alloc_propolis_ip(
    sagactx: NexusActionContext,
) -> Result<Ipv6Addr, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let sled_uuid = sagactx.lookup::<Uuid>("sled_id")?;
    allocate_sled_ipv6(&opctx, sagactx.user_data().datastore(), sled_uuid).await
}

async fn sis_create_vmm_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Vmm, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let instance_id = params.db_instance.id();
    let propolis_id = sagactx.lookup::<Uuid>("propolis_id")?;
    let sled_id = sagactx.lookup::<Uuid>("sled_id")?;
    let propolis_ip = sagactx.lookup::<Ipv6Addr>("propolis_ip")?;

    super::instance_common::create_and_insert_vmm_record(
        osagactx.datastore(),
        &opctx,
        instance_id,
        propolis_id,
        sled_id,
        propolis_ip,
        nexus_db_model::VmmInitialState::Starting,
    )
    .await
}

async fn sis_destroy_vmm_record(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let vmm = sagactx.lookup::<db::model::Vmm>("vmm_record")?;
    super::instance_common::destroy_vmm_record(
        osagactx.datastore(),
        &opctx,
        &vmm,
    )
    .await
}

async fn sis_move_to_starting(
    sagactx: NexusActionContext,
) -> Result<db::model::Instance, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let instance_id = params.db_instance.id();
    let propolis_id = sagactx.lookup::<Uuid>("propolis_id")?;
    info!(osagactx.log(), "moving instance to Starting state via saga";
          "instance_id" => %instance_id,
          "propolis_id" => %propolis_id);

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // For idempotency, refetch the instance to see if this step already applied
    // its desired update.
    let (.., db_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .fetch_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    match db_instance.runtime().propolis_id {
        // If this saga's Propolis ID is already written to the record, then
        // this step must have completed already and is being retried, so
        // proceed.
        Some(db_id) if db_id == propolis_id => {
            info!(osagactx.log(), "start saga: Propolis ID already set";
                  "instance_id" => %instance_id);

            Ok(db_instance)
        }

        // If the instance has a different Propolis ID, a competing start saga
        // must have started the instance already, so unwind.
        Some(_) => {
            return Err(ActionError::action_failed(Error::conflict(
                "instance changed state before it could be started",
            )));
        }

        // If the instance has no Propolis ID, try to write this saga's chosen
        // ID into the instance and put it in the Running state. (While the
        // instance is still technically starting up, writing the Propolis ID at
        // this point causes the VMM's state, which is Starting, to supersede
        // the instance's state, so this won't cause the instance to appear to
        // be running before Propolis thinks it has started.)
        None => {
            let new_runtime = db::model::InstanceRuntimeState {
                nexus_state: db::model::InstanceState::new(
                    InstanceState::Running,
                ),
                propolis_id: Some(propolis_id),
                time_updated: Utc::now(),
                gen: db_instance.runtime().gen.next().into(),
                ..db_instance.runtime_state
            };

            // Bail if another actor managed to update the instance's state in
            // the meantime.
            if !osagactx
                .datastore()
                .instance_update_runtime(&instance_id, &new_runtime)
                .await
                .map_err(ActionError::action_failed)?
            {
                return Err(ActionError::action_failed(Error::conflict(
                    "instance changed state before it could be started",
                )));
            }

            let mut new_record = db_instance.clone();
            new_record.runtime_state = new_runtime;
            Ok(new_record)
        }
    }
}

async fn sis_move_to_starting_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let db_instance =
        sagactx.lookup::<db::model::Instance>("started_record")?;
    let instance_id = db_instance.id();
    info!(osagactx.log(), "start saga failed; returning instance to Stopped";
          "instance_id" => %instance_id);

    let new_runtime = db::model::InstanceRuntimeState {
        nexus_state: db::model::InstanceState::new(InstanceState::Stopped),
        propolis_id: None,
        gen: db_instance.runtime_state.gen.next().into(),
        ..db_instance.runtime_state
    };

    if !osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime)
        .await?
    {
        info!(osagactx.log(),
              "did not return instance to Stopped: old generation number";
              "instance_id" => %instance_id);
    }

    Ok(())
}

async fn sis_account_virtual_resources(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let instance_id = params.db_instance.id();

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .datastore()
        .virtual_provisioning_collection_insert_instance(
            &opctx,
            instance_id,
            params.db_instance.project_id,
            i64::from(params.db_instance.ncpus.0 .0),
            nexus_db_model::ByteCount(*params.db_instance.memory),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sis_account_virtual_resources_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let instance_id = params.db_instance.id();

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let started_record =
        sagactx.lookup::<db::model::Instance>("started_record")?;

    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            instance_id,
            params.db_instance.project_id,
            i64::from(params.db_instance.ncpus.0 .0),
            nexus_db_model::ByteCount(*params.db_instance.memory),
            // Use the next instance generation number as the generation limit
            // to ensure the provisioning counters are released. (The "mark as
            // starting" undo step will "publish" this new state generation when
            // it moves the instance back to Stopped.)
            (&started_record.runtime().gen.next()).into(),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sis_dpd_ensure(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let db_instance =
        sagactx.lookup::<db::model::Instance>("started_record")?;
    let instance_id = db_instance.id();

    info!(osagactx.log(), "start saga: ensuring instance dpd configuration";
          "instance_id" => %instance_id);

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let datastore = osagactx.datastore();

    // Querying sleds requires fleet access; use the instance allocator context
    // for this.
    let sled_uuid = sagactx.lookup::<Uuid>("sled_id")?;
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, &datastore)
        .sled_id(sled_uuid)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    // Querying boundary switches also requires fleet access and the use of the
    // instance allocator context.
    let boundary_switches = osagactx
        .nexus()
        .boundary_switches(&osagactx.nexus().opctx_alloc)
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
                instance_id,
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
    let instance_id = params.db_instance.id();
    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    info!(log, "start saga: undoing dpd configuration";
          "instance_id" => %instance_id);

    let (.., authz_instance) = LookupPath::new(&opctx, &osagactx.datastore())
        .instance_id(instance_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_delete_dpd_config(&opctx, &authz_instance)
        .await?;

    Ok(())
}

async fn sis_v2p_ensure(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let instance_id = params.db_instance.id();

    info!(osagactx.log(), "start saga: ensuring v2p mappings are configured";
          "instance_id" => %instance_id);

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let sled_uuid = sagactx.lookup::<Uuid>("sled_id")?;
    osagactx
        .nexus()
        .create_instance_v2p_mappings(&opctx, instance_id, sled_uuid)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sis_v2p_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let instance_id = params.db_instance.id();
    let sled_id = sagactx.lookup::<Uuid>("sled_id")?;
    info!(osagactx.log(), "start saga: undoing v2p configuration";
          "instance_id" => %instance_id,
          "sled_id" => %sled_id);

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
    let db_instance =
        sagactx.lookup::<db::model::Instance>("started_record")?;
    let instance_id = db_instance.id();
    let sled_id = sagactx.lookup::<Uuid>("sled_id")?;
    let vmm_record = sagactx.lookup::<db::model::Vmm>("vmm_record")?;
    let propolis_id = sagactx.lookup::<Uuid>("propolis_id")?;

    info!(osagactx.log(), "start saga: ensuring instance is registered on sled";
          "instance_id" => %instance_id,
          "sled_id" => %sled_id);

    let (.., authz_instance) = LookupPath::new(&opctx, &osagactx.datastore())
        .instance_id(instance_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_registered(
            &opctx,
            &authz_instance,
            &db_instance,
            &propolis_id,
            &vmm_record,
        )
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
    let instance_id = params.db_instance.id();
    let sled_id = sagactx.lookup::<Uuid>("sled_id")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    info!(osagactx.log(), "start saga: unregistering instance from sled";
          "instance_id" => %instance_id,
          "sled_id" => %sled_id);

    // Fetch the latest record so that this callee can drive the instance into
    // a Failed state if the unregister call fails.
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
            &sled_id,
            db_instance.runtime(),
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

    let db_instance =
        sagactx.lookup::<db::model::Instance>("started_record")?;
    let db_vmm = sagactx.lookup::<db::model::Vmm>("vmm_record")?;
    let instance_id = params.db_instance.id();
    let sled_id = sagactx.lookup::<Uuid>("sled_id")?;
    info!(osagactx.log(), "start saga: ensuring instance is running";
          "instance_id" => %instance_id,
          "sled_id" => %sled_id);

    let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_request_state(
            &opctx,
            &authz_instance,
            &db_instance,
            &Some(db_vmm),
            crate::app::instance::InstanceStateChangeRequest::Run,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::app::{saga::create_saga_dag, sagas::test_helpers};
    use crate::external_api::params;
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::authn;
    use nexus_test_utils::resource_helpers::{
        create_project, object_create, populate_ip_pool,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::{
        ByteCount, IdentityMetadataCreateParams, InstanceCpuCount,
    };
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
            test_helpers::instance_fetch(cptestctx, instance.identity.id)
                .await
                .instance()
                .clone();

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            db_instance,
        };

        let dag = create_saga_dag::<SagaInstanceStart>(params).unwrap();
        let saga = nexus.create_runnable_saga(dag).await.unwrap();
        nexus.run_saga(saga).await.expect("Start saga should succeed");

        test_helpers::instance_simulate(cptestctx, &instance.identity.id).await;
        let vmm_state =
            test_helpers::instance_fetch(cptestctx, instance.identity.id)
                .await
                .vmm()
                .as_ref()
                .expect("running instance should have a vmm")
                .runtime
                .state
                .0;

        assert_eq!(vmm_state, InstanceState::Running);
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

        test_helpers::action_failure_can_unwind::<
            SagaInstanceStart,
            _,
            _,
        >(
            nexus,
            || {
                Box::pin({
                    async {
                        let db_instance = test_helpers::instance_fetch(
                            cptestctx,
                            instance.identity.id,
                        )
                        .await.instance().clone();

                        Params {
                            serialized_authn:
                                authn::saga::Serialized::for_opctx(&opctx),
                                db_instance,
                        }
                    }
                })
            },
            || {
                Box::pin({
                    async {
                        let new_db_instance = test_helpers::instance_fetch(
                            cptestctx,
                            instance.identity.id,
                        )
                        .await.instance().clone();

                        info!(log,
                              "fetched instance runtime state after saga execution";
                              "instance_id" => %instance.identity.id,
                              "instance_runtime" => ?new_db_instance.runtime());

                        assert!(new_db_instance.runtime().propolis_id.is_none());
                        assert_eq!(
                            new_db_instance.runtime().nexus_state.0,
                            InstanceState::Stopped
                        );

                        assert!(test_helpers::no_virtual_provisioning_resource_records_exist(cptestctx).await);
                        assert!(test_helpers::no_virtual_provisioning_collection_records_using_instances(cptestctx).await);
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
            test_helpers::instance_fetch(cptestctx, instance.identity.id)
                .await
                .instance()
                .clone();

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            db_instance,
        };

        let dag = create_saga_dag::<SagaInstanceStart>(params).unwrap();
        test_helpers::actions_succeed_idempotently(nexus, dag).await;
        test_helpers::instance_simulate(cptestctx, &instance.identity.id).await;
        let vmm_state =
            test_helpers::instance_fetch(cptestctx, instance.identity.id)
                .await
                .vmm()
                .as_ref()
                .expect("running instance should have a vmm")
                .runtime
                .state
                .0;

        assert_eq!(vmm_state, InstanceState::Running);
    }
}
