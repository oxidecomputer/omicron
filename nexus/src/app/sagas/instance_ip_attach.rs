// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::instance_common::{
    instance_ip_add_nat, instance_ip_add_opte, instance_ip_get_instance_state,
    instance_ip_move_state, instance_ip_remove_nat, instance_ip_remove_opte,
    InstanceStateForIp,
};
use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use nexus_db_model::{ExternalIp, IpAttachState};
use nexus_db_queries::db::lookup::LookupPath;
use nexus_types::external_api::views;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use uuid::Uuid;

// The IP attach/detach sagas do some resource locking -- because we
// allow them to be called in [Running, Stopped], they must contend
// with each other/themselves, instance start, instance delete, and
// the instance stop action (noting the latter is not a saga.
//
// The main means of access control here is an external IP's `state`.
// Entering either saga begins with an atomic swap from Attached/Detached
// to Attaching/Detaching. This prevents concurrent attach/detach on the
// same EIP, and prevents instance start from executing with an
// Error::unavail.
//
// Overlap with stop is handled by treating comms failures with
// sled-agent as temporary errors and unwinding. For the delete case, we
// allow the attach/detach completion to have a missing record.
// See `instance_common::instance_ip_get_instance_state` for more info.

declare_saga_actions! {
    instance_ip_attach;
    ATTACH_EXTERNAL_IP -> "target_ip" {
        + siia_begin_attach_ip
        - siia_begin_attach_ip_undo
    }

    INSTANCE_STATE -> "instance_state" {
        + siia_get_instance_state
    }

    REGISTER_NAT -> "no_result0" {
        + siia_nat
        - siia_nat_undo
    }

    ENSURE_OPTE_PORT -> "no_result1" {
        + siia_update_opte
        - siia_update_opte_undo
    }

    COMPLETE_ATTACH -> "output" {
        + siia_complete_attach
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub create_params: params::ExternalIpCreate,
    pub authz_instance: authz::Instance,
    pub project_id: Uuid,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

// TODO: factor this out for attach, detach, and instance create
//       to share an impl.

async fn siia_begin_attach_ip(
    sagactx: NexusActionContext,
) -> Result<ExternalIp, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    match params.create_params {
        // Allocate a new IP address from the target, possibly default, pool
        params::ExternalIpCreate::Ephemeral { ref pool_name } => {
            let pool_name =
                pool_name.as_ref().map(|name| db::model::Name(name.clone()));
            datastore
                .allocate_instance_ephemeral_ip(
                    &opctx,
                    Uuid::new_v4(),
                    params.authz_instance.id(),
                    pool_name,
                )
                .await
                .map_err(ActionError::action_failed)
        }
        // Set the parent of an existing floating IP to the new instance's ID.
        params::ExternalIpCreate::Floating { ref floating_ip_name } => {
            let floating_ip_name = db::model::Name(floating_ip_name.clone());
            let (.., authz_fip) = LookupPath::new(&opctx, &datastore)
                .project_id(params.project_id)
                .floating_ip_name(&floating_ip_name)
                .lookup_for(authz::Action::Modify)
                .await
                .map_err(ActionError::action_failed)?;

            datastore
                .floating_ip_begin_attach(
                    &opctx,
                    &authz_fip,
                    params.authz_instance.id(),
                    false,
                )
                .await
                .map_err(ActionError::action_failed)
        }
    }
}

async fn siia_begin_attach_ip_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    warn!(log, "siia_begin_attach_ip_undo: Reverting detached->attaching");
    let params = sagactx.saga_params::<Params>()?;
    if !instance_ip_move_state(
        &sagactx,
        &params.serialized_authn,
        IpAttachState::Attaching,
        IpAttachState::Detached,
    )
    .await?
    {
        error!(log, "siia_begin_attach_ip_undo: external IP was deleted")
    }

    Ok(())
}

async fn siia_get_instance_state(
    sagactx: NexusActionContext,
) -> Result<InstanceStateForIp, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_get_instance_state(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
        "attach",
    )
    .await
}

async fn siia_nat(sagactx: NexusActionContext) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_add_nat(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
    )
    .await
}

async fn siia_nat_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_remove_nat(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
    )
    .await?;

    Ok(())
}

async fn siia_update_opte(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_add_opte(&sagactx, &params.authz_instance).await
}

async fn siia_update_opte_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_remove_opte(&sagactx, &params.authz_instance).await?;
    Ok(())
}

async fn siia_complete_attach(
    sagactx: NexusActionContext,
) -> Result<views::ExternalIp, ActionError> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;
    let target_ip = sagactx.lookup::<ExternalIp>("target_ip")?;

    if !instance_ip_move_state(
        &sagactx,
        &params.serialized_authn,
        IpAttachState::Attaching,
        IpAttachState::Attached,
    )
    .await?
    {
        warn!(
            log,
            "siia_complete_attach: external IP was deleted or call was idempotent"
        )
    }

    target_ip.try_into().map_err(ActionError::action_failed)
}

// TODO: backout changes if run state changed illegally?

#[derive(Debug)]
pub struct SagaInstanceIpAttach;
impl NexusSaga for SagaInstanceIpAttach {
    const NAME: &'static str = "external-ip-attach";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_ip_attach_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(attach_external_ip_action());
        builder.append(instance_state_action());
        builder.append(register_nat_action());
        builder.append(ensure_opte_port_action());
        builder.append(complete_attach_action());
        Ok(builder.build()?)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::app::sagas::test_helpers;

    use super::*;
    use dropshot::test_util::ClientTestContext;
    use nexus_db_model::Name;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::{populate_ip_pool, create_project, create_disk, create_floating_ip, object_create};
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::{SimpleIdentity, IdentityMetadataCreateParams};

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "cafe";
    const INSTANCE_NAME: &str = "menu";
    const FIP_NAME: &str = "affogato";
    const DISK_NAME: &str = "my-disk";

    // Test matrix:
    // - instance started/stopped
    // - fip vs ephemeral

    // async fn create_instance(
    //     client: &ClientTestContext,
    // ) -> omicron_common::api::external::Instance {
    //     let instances_url = format!("/v1/instances?project={}", PROJECT_NAME);
    //     object_create(
    //         client,
    //         &instances_url,
    //         &params::InstanceCreate {
    //             identity: IdentityMetadataCreateParams {
    //                 name: INSTANCE_NAME.parse().unwrap(),
    //                 description: format!("instance {:?}", INSTANCE_NAME),
    //             },
    //             ncpus: InstanceCpuCount(2),
    //             memory: ByteCount::from_gibibytes_u32(2),
    //             hostname: String::from(INSTANCE_NAME),
    //             user_data: b"#cloud-config".to_vec(),
    //             network_interfaces:
    //                 params::InstanceNetworkInterfaceAttachment::None,
    //             external_ips: vec![],
    //             disks: vec![],
    //             start: false,
    //         },
    //     )
    //     .await
    // }

    pub async fn ip_manip_test_setup(client: &ClientTestContext) -> Uuid {
        populate_ip_pool(&client, "default", None).await;
        let project = create_project(client, PROJECT_NAME).await;
        create_disk(&client, PROJECT_NAME, DISK_NAME).await;
        create_floating_ip(
            client,
            FIP_NAME,
            &project.identity.id.to_string(),
            None,
            None,
        )
        .await;


        project.id()
    }

    async fn new_test_params(opctx: &OpContext, datastore: &db::DataStore, project_id: Uuid, use_floating: bool) -> Params {
        let create_params = if use_floating {
            params::ExternalIpCreate::Floating { floating_ip_name: FIP_NAME.parse().unwrap() }
        } else {
            params::ExternalIpCreate::Ephemeral { pool_name: None }
        };

        let (.., authz_instance) = LookupPath::new(opctx, datastore).project_id(project_id)
        .instance_name(&Name(INSTANCE_NAME.parse().unwrap())).lookup_for(authz::Action::Modify).await.unwrap();
        Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id,
            create_params,
            authz_instance,
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let db_instance =
            test_helpers::instance_fetch(cptestctx, instance.identity.id)
                .await
                .instance()
                .clone();
        let project_id = ip_manip_test_setup(&client);
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        _cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        _cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        _cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }
}
