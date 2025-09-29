// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashSet;

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::declare_saga_actions;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::{authn, authz, db};
use omicron_common::api::internal::shared::SwitchLocation;
use serde::Deserialize;
use serde::Serialize;
use slog::info;
use steno::ActionError;

// instance delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub authz_instance: authz::Instance,
    pub instance: db::model::Instance,
    pub boundary_switches: HashSet<SwitchLocation>,
}

// instance delete saga: actions

declare_saga_actions! {
    instance_delete;

    INSTANCE_DELETE_RECORD -> "no_result1" {
        + sid_delete_instance_record
    }
    DELETE_NETWORK_INTERFACES -> "no_result2" {
        + sid_delete_network_interfaces
    }
    DEALLOCATE_EXTERNAL_IP -> "no_result3" {
        + sid_deallocate_external_ip
    }
    LEAVE_MULTICAST_GROUPS -> "no_result4" {
        + sid_leave_multicast_groups
    }
    INSTANCE_DELETE_NAT -> "no_result5" {
        + sid_delete_nat
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
        builder.append(instance_delete_nat_action());
        builder.append(instance_delete_record_action());
        builder.append(delete_network_interfaces_action());
        builder.append(deallocate_external_ip_action());
        builder.append(leave_multicast_groups_action());
        Ok(builder.build()?)
    }
}

// instance delete saga: action implementations

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
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sid_delete_network_interfaces(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
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
    nexus.background_tasks.activate(&nexus.background_tasks.task_v2p_manager);
    Ok(())
}

async fn sid_delete_nat(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let instance_id = params.authz_instance.id();
    let osagactx = sagactx.user_data();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_instance) = LookupPath::new(&opctx, osagactx.datastore())
        .instance_id(instance_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_delete_dpd_config(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sid_leave_multicast_groups(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let instance_id = params.authz_instance.id();

    // Check if multicast is enabled - if not, no members exist to remove
    if !osagactx.nexus().multicast_enabled() {
        debug!(osagactx.log(),
               "multicast not enabled, skipping multicast group member removal";
               "instance_id" => %instance_id);
        return Ok(());
    }

    // Mark all multicast group memberships for this instance as deleted
    datastore
        .multicast_group_members_mark_for_removal(&opctx, instance_id)
        .await
        .map_err(ActionError::action_failed)?;

    info!(
        osagactx.log(),
        "Marked multicast members for removal";
        "instance_id" => %instance_id
    );

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
    osagactx
        .datastore()
        .detach_floating_ips_by_instance_id(&opctx, params.authz_instance.id())
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
        app::sagas::instance_delete::SagaInstanceDelete, external_api::params,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_lookup::LookupPath;
    use nexus_db_queries::{authn::saga::Serialized, context::OpContext, db};
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils::resource_helpers::create_default_ip_pool;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::identity::Resource;
    use omicron_common::api::external::{
        ByteCount, IdentityMetadataCreateParams, InstanceCpuCount,
    };
    use omicron_common::api::internal::shared::SwitchLocation;
    use std::collections::HashSet;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const INSTANCE_NAME: &str = "my-instance";
    const PROJECT_NAME: &str = "springfield-squidport";
    const DISK_NAME: &str = "my-disk";

    async fn create_org_project_and_disk(client: &ClientTestContext) -> Uuid {
        create_default_ip_pool(&client).await;
        let project = create_project(client, PROJECT_NAME).await;
        create_disk(&client, PROJECT_NAME, DISK_NAME).await;
        project.identity.id
    }

    async fn new_test_params(
        cptestctx: &ControlPlaneTestContext,
        instance_id: Uuid,
    ) -> Params {
        let opctx = test_opctx(&cptestctx);
        let datastore = cptestctx.server.server_context().nexus.datastore();

        let (.., authz_instance, instance) = LookupPath::new(&opctx, datastore)
            .instance_id(instance_id)
            .fetch()
            .await
            .expect("Failed to lookup instance");
        Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            authz_instance,
            instance,
            boundary_switches: HashSet::from([SwitchLocation::Switch0]),
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
            hostname: "inst".parse().unwrap(),
            user_data: vec![],
            ssh_public_keys: Some(Vec::new()),
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::Default,
            external_ips: vec![params::ExternalIpCreate::Ephemeral {
                pool: None,
            }],
            boot_disk: Some(params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach { name: DISK_NAME.parse().unwrap() },
            )),
            cpu_platform: None,
            disks: Vec::new(),
            start: false,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
            multicast_groups: Vec::new(),
        }
    }

    pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.server_context().nexus.datastore().clone(),
        )
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        create_org_project_and_disk(&client).await;

        // Build the saga DAG with the provided test parameters and run it.
        let params = new_test_params(
            &cptestctx,
            create_instance(&cptestctx, new_instance_create_params())
                .await
                .id(),
        )
        .await;
        nexus
            .sagas
            .saga_execute::<SagaInstanceDelete>(params)
            .await
            .expect("Saga should have succeeded");
    }

    async fn create_instance(
        cptestctx: &ControlPlaneTestContext,
        params: params::InstanceCreate,
    ) -> db::model::Instance {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_opctx(&cptestctx);

        let project_selector = params::ProjectSelector {
            project: PROJECT_NAME.to_string().try_into().unwrap(),
        };
        let project_lookup =
            nexus.project_lookup(&opctx, project_selector).unwrap();

        let instance_state = nexus
            .project_create_instance(&opctx, &project_lookup, &params)
            .await
            .unwrap();

        let datastore =
            cptestctx.server.server_context().nexus.datastore().clone();
        let (.., db_instance) = LookupPath::new(&opctx, &datastore)
            .instance_id(instance_state.instance().id())
            .fetch()
            .await
            .expect("test instance should be present in datastore");

        db_instance
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
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

        crate::app::sagas::test_helpers::actions_succeed_idempotently(
            nexus, dag,
        )
        .await;

        verify_clean_slate(&cptestctx).await;
    }
}
