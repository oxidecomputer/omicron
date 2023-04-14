// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::declare_saga_actions;
use crate::db;
use crate::{authn, authz};
use nexus_types::identity::Resource;
use omicron_common::api::external::{Error, ResourceType};
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

// instance delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub authz_instance: authz::Instance,
    pub instance: db::model::Instance,
}

// instance delete saga: actions

declare_saga_actions! {
    instance_delete;
    V2P_ENSURE_UNDO -> "v2p_ensure_undo" {
        + sid_noop
        - sid_v2p_ensure_undo
    }
    V2P_ENSURE -> "v2p_ensure" {
        + sid_v2p_ensure
    }
    INSTANCE_DELETE_RECORD -> "no_result1" {
        + sid_delete_instance_record
    }
    DELETE_ASIC_CONFIGURATION -> "delete_asic_configuration" {
        + sid_delete_network_config
    }
    DELETE_NETWORK_INTERFACES -> "no_result2" {
        + sid_delete_network_interfaces
    }
    DEALLOCATE_EXTERNAL_IP -> "no_result3" {
        + sid_deallocate_external_ip
    }
    VIRTUAL_RESOURCES_ACCOUNT -> "no_result4" {
        + sid_account_virtual_resources
    }
    SLED_RESOURCES_ACCOUNT -> "no_result5" {
        + sid_account_sled_resources
    }
}

// instance delete saga: definition

#[derive(Debug)]
pub struct SagaInstanceDelete;
impl NexusSaga for SagaInstanceDelete {
    const NAME: &'static str = "instance-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_delete_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(v2p_ensure_undo_action());
        builder.append(v2p_ensure_action());
        builder.append(delete_asic_configuration_action());
        builder.append(instance_delete_record_action());
        builder.append(delete_network_interfaces_action());
        builder.append(deallocate_external_ip_action());
        builder.append(virtual_resources_account_action());
        builder.append(sled_resources_account_action());
        Ok(builder.build()?)
    }
}

// instance delete saga: action implementations

async fn sid_noop(_sagactx: NexusActionContext) -> Result<(), ActionError> {
    Ok(())
}

/// Ensure that the v2p mappings for this instance are deleted
async fn sid_v2p_ensure(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .nexus()
        .delete_instance_v2p_mappings(&opctx, params.authz_instance.id())
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

/// During unwind, ensure that v2p mappings are created again
async fn sid_v2p_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .nexus()
        .create_instance_v2p_mappings(&opctx, params.authz_instance.id())
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sid_delete_network_config(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let osagactx = sagactx.user_data();
    let dpd_client = &osagactx.nexus().dpd_client;
    let datastore = &osagactx.datastore();
    let log = sagactx.user_data().log();

    debug!(log, "fetching external ip addresses");

    let external_ips = &datastore
        .instance_lookup_external_ips(&opctx, params.authz_instance.id())
        .await
        .map_err(ActionError::action_failed)?;

    let mut errors: Vec<ActionError> = vec![];

    // Here we are attempting to delete every existing NAT entry while deferring
    // any error handling. If we don't defer error handling, we might end up
    // bailing out before we've attempted deletion of all entries.
    for entry in external_ips {
        debug!(log, "deleting nat mapping for entry: {entry:#?}");
        let result = match entry.ip {
            ipnetwork::IpNetwork::V4(network) => {
                dpd_client
                    .nat_ipv4_delete(&network.ip(), *entry.first_port)
                    .await
            }
            ipnetwork::IpNetwork::V6(network) => {
                dpd_client
                    .nat_ipv6_delete(&network.ip(), *entry.first_port)
                    .await
            }
        };

        match result {
            Ok(_) => {
                debug!(log, "deletion of nat entry successful for: {entry:#?}");
            }
            Err(e) => {
                if e.status() == Some(http::StatusCode::NOT_FOUND) {
                    debug!(log, "no nat entry found for: {entry:#?}");
                } else {
                    let new_error =
                        ActionError::action_failed(Error::internal_error(
                            &format!("failed to delete nat entry via dpd: {e}"),
                        ));
                    error!(log, "{new_error:#?}");
                    errors.push(new_error);
                }
            }
        }
    }

    if let Some(error) = errors.first() {
        return Err(error.clone());
    }

    Ok(())
}

async fn sid_delete_instance_record(
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
        .project_delete_instance(&opctx, &params.authz_instance)
        .await
        .or_else(|err| {
            // Necessary for idempotency
            match err {
                Error::ObjectNotFound {
                    type_name: ResourceType::Instance,
                    lookup_type: _,
                } => Ok(()),
                _ => Err(err),
            }
        })
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sid_delete_network_interfaces(
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
        .instance_delete_all_network_interfaces(&opctx, &params.authz_instance)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sid_deallocate_external_ip(
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
        .deallocate_external_ip_by_instance_id(
            &opctx,
            params.authz_instance.id(),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sid_account_virtual_resources(
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
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            params.instance.id(),
            params.instance.project_id,
            i64::from(params.instance.runtime_state.ncpus.0 .0),
            params.instance.runtime_state.memory,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sid_account_sled_resources(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // Fetch the previously-deleted instance record to get its Propolis ID. It
    // is safe to fetch the ID at this point because the instance is already
    // deleted and so cannot change anymore.
    //
    // TODO(#2315): This prevents the garbage collection of soft-deleted
    // instance records. A better method is to remove a Propolis's reservation
    // once an instance no longer refers to it (e.g. when it has stopped or
    // been removed from the instance's migration information) and then make
    // this saga check that the instance has no active Propolises before it is
    // deleted. This logic should be part of the logic needed to stop an
    // instance and release its Propolis reservation; when that is added this
    // step can be removed.
    let instance = osagactx
        .datastore()
        .instance_fetch_deleted(&opctx, &params.authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .datastore()
        .sled_reservation_delete(&opctx, instance.runtime().propolis_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::{
        app::saga::create_saga_dag,
        app::sagas::instance_create::test::verify_clean_slate,
        app::sagas::instance_delete::Params,
        app::sagas::instance_delete::SagaInstanceDelete,
        authn::saga::Serialized, db, db::lookup::LookupPath,
        external_api::params,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::populate_ip_pool;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::identity::Resource;
    use omicron_common::api::external::{
        ByteCount, IdentityMetadataCreateParams, InstanceCpuCount,
    };
    use std::num::NonZeroU32;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const INSTANCE_NAME: &str = "my-instance";
    const PROJECT_NAME: &str = "springfield-squidport";
    const DISK_NAME: &str = "my-disk";

    async fn create_org_project_and_disk(client: &ClientTestContext) -> Uuid {
        populate_ip_pool(&client, "default", None).await;
        let project = create_project(client, PROJECT_NAME).await;
        create_disk(&client, PROJECT_NAME, DISK_NAME).await;
        project.identity.id
    }

    async fn new_test_params(
        cptestctx: &ControlPlaneTestContext,
        instance_id: Uuid,
    ) -> Params {
        let opctx = test_opctx(&cptestctx);
        let datastore = cptestctx.server.apictx().nexus.datastore();

        let (.., authz_instance, instance) =
            LookupPath::new(&opctx, &datastore)
                .instance_id(instance_id)
                .fetch()
                .await
                .expect("Failed to lookup instance");
        Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            authz_instance,
            instance,
        }
    }

    // Helper for creating instance create parameters
    fn new_instance_create_params() -> params::InstanceCreate {
        params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: INSTANCE_NAME.parse().unwrap(),
                description: "My instance".to_string(),
            },
            ncpus: InstanceCpuCount::try_from(2).unwrap(),
            memory: ByteCount::from_gibibytes_u32(4),
            hostname: String::from("inst"),
            user_data: vec![],
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::Default,
            external_ips: vec![params::ExternalIpCreate::Ephemeral {
                pool_name: None,
            }],
            disks: vec![params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach { name: DISK_NAME.parse().unwrap() },
            )],
            start: false,
        }
    }

    pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.apictx().nexus.datastore().clone(),
        )
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let dag = create_saga_dag::<SagaInstanceDelete>(
            new_test_params(
                &cptestctx,
                create_instance(&cptestctx, new_instance_create_params())
                    .await
                    .id(),
            )
            .await,
        )
        .unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();

        // Actually run the saga
        nexus
            .run_saga(runnable_saga)
            .await
            .expect("Saga should have succeeded");
    }

    async fn create_instance(
        cptestctx: &ControlPlaneTestContext,
        params: params::InstanceCreate,
    ) -> db::model::Instance {
        let nexus = &cptestctx.server.apictx().nexus;
        let opctx = test_opctx(&cptestctx);

        let project_selector = params::ProjectSelector {
            project: PROJECT_NAME.to_string().try_into().unwrap(),
        };
        let project_lookup =
            nexus.project_lookup(&opctx, project_selector).unwrap();
        nexus
            .project_create_instance(&opctx, &project_lookup, &params)
            .await
            .unwrap()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters
        let dag = create_saga_dag::<SagaInstanceDelete>(
            new_test_params(
                &cptestctx,
                create_instance(&cptestctx, new_instance_create_params())
                    .await
                    .id(),
            )
            .await,
        )
        .unwrap();

        let runnable_saga =
            nexus.create_runnable_saga(dag.clone()).await.unwrap();

        // Cause all actions to run twice. The saga should succeed regardless!
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

        // Verify that the saga's execution succeeded.
        nexus
            .run_saga(runnable_saga)
            .await
            .expect("Saga should have succeeded");

        verify_clean_slate(&cptestctx).await;
    }
}
