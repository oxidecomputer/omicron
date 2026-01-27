// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use super::instance_common::VmmAndSledIds;
use super::instance_common::networking_resource_instance_state;
use crate::app::authn;
use crate::app::authz;
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::instance_common::delete_subnet_attachment_from_dpd;
use crate::app::sagas::instance_common::delete_subnet_attachment_from_opte;
use crate::app::sagas::instance_common::send_subnet_attachment_to_dpd;
use crate::app::sagas::instance_common::send_subnet_attachment_to_opte;
use anyhow::Context as _;
use nexus_db_model::IpAttachState;
use nexus_db_queries::db::datastore::ExternalSubnetBeginAttachResult;
use nexus_db_queries::db::datastore::ExternalSubnetCompleteAttachResult;
use nexus_types::external_api::views;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

declare_saga_actions! {
    subnet_detach;
    DETACH_SUBNET -> "begin_detach_result" {
        + ssd_begin_detach_subnet
        - ssd_begin_detach_subnet_undo
    }

    INSTANCE_STATE -> "instance_state" {
        + ssd_get_instance_state
    }

    NOTIFY_DPD -> "no_result0" {
        + ssd_notify_dpd
        - ssd_notify_dpd_undo
    }

    NOTIFY_OPTE -> "no_result1" {
        + ssd_notify_opte
        - ssd_notify_opte_undo
    }

    COMPLETE_DETACH -> "output" {
        + ssd_complete_detach
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Params {
    pub authz_instance: authz::Instance,
    pub authz_subnet: authz::ExternalSubnet,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

async fn ssd_begin_detach_subnet(
    sagactx: NexusActionContext,
) -> Result<ExternalSubnetBeginAttachResult, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    datastore
        .begin_detach_subnet(
            &opctx,
            &params.authz_instance,
            &params.authz_subnet,
        )
        .await
        .map_err(ActionError::action_failed)
}

async fn ssd_begin_detach_subnet_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    let datastore = osagactx.datastore();
    warn!(log, "ssd_begin_detach_subnet_undo: Reverting attached->detaching");
    let params = sagactx.saga_params::<Params>()?;
    let ExternalSubnetBeginAttachResult { subnet, do_saga: _ } =
        sagactx
            .lookup::<ExternalSubnetBeginAttachResult>("begin_detach_result")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    match datastore
        .external_subnet_complete_op(
            &opctx,
            subnet.identity.id.into(),
            IpAttachState::Detaching,
            IpAttachState::Attached,
        )
        .await
    {
        Ok(ExternalSubnetCompleteAttachResult::Modified(_)) => Ok(()),
        Ok(ExternalSubnetCompleteAttachResult::NoChanges) => {
            warn!(log, "subnet is deleted, could not reattach");
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!("failed to reattach subnet: {e}")),
    }
}

async fn ssd_get_instance_state(
    sagactx: NexusActionContext,
) -> Result<Option<VmmAndSledIds>, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    networking_resource_instance_state(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
        "detach",
    )
    .await
}

async fn ssd_notify_dpd(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let ExternalSubnetBeginAttachResult { subnet, do_saga } =
        sagactx
            .lookup::<ExternalSubnetBeginAttachResult>("begin_detach_result")?;
    if !do_saga {
        return Ok(());
    }
    delete_subnet_attachment_from_dpd(&sagactx, subnet.subnet)
        .await
        .map_err(ActionError::action_failed)
}

async fn ssd_notify_dpd_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    let sled_id = sagactx
        .lookup::<Option<VmmAndSledIds>>("instance_state")?
        .map(|ids| ids.sled_id);
    let subnet = sagactx
        .lookup::<ExternalSubnetBeginAttachResult>("begin_detach_result")?;
    let ip_subnet = subnet.subnet.subnet;
    send_subnet_attachment_to_dpd(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
        sled_id,
        subnet,
    )
    .await
    .map(|_| ())
    .with_context(|| {
        format!(
            "ssd_notify_dpd_undo: sending attached subnet back to \
            dendrite for reattachment, subnet={}",
            ip_subnet,
        )
    })
}

async fn ssd_notify_opte(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let ids = sagactx.lookup::<Option<VmmAndSledIds>>("instance_state")?;
    let subnet = sagactx
        .lookup::<ExternalSubnetBeginAttachResult>("begin_detach_result")?;
    delete_subnet_attachment_from_opte(&sagactx, ids, subnet)
        .await
        .map_err(ActionError::action_failed)
}

async fn ssd_notify_opte_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let ids = sagactx.lookup::<Option<VmmAndSledIds>>("instance_state")?;
    let subnet = sagactx
        .lookup::<ExternalSubnetBeginAttachResult>("begin_detach_result")?;
    let ip_subnet = subnet.subnet.subnet;
    send_subnet_attachment_to_opte(&sagactx, ids, subnet).await.with_context(
        || {
            format!(
                "ssd_notify_opte_undo: sending attached subnet back to \
            OPTE for reattachment, subnet={}",
                ip_subnet,
            )
        },
    )
}

async fn ssd_complete_detach(
    sagactx: NexusActionContext,
) -> Result<views::ExternalSubnet, ActionError> {
    let log = sagactx.user_data().log();
    let datastore = sagactx.user_data().datastore();
    let params = sagactx.saga_params::<Params>()?;
    let ExternalSubnetBeginAttachResult { subnet, do_saga } =
        sagactx
            .lookup::<ExternalSubnetBeginAttachResult>("begin_detach_result")?;
    if !do_saga {
        return Ok(subnet.into());
    }
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    match datastore
        .external_subnet_complete_op(
            &opctx,
            subnet.identity.id.into(),
            IpAttachState::Detaching,
            IpAttachState::Detached,
        )
        .await
    {
        Ok(ExternalSubnetCompleteAttachResult::Modified(subnet)) => {
            Ok(subnet.into())
        }
        Ok(ExternalSubnetCompleteAttachResult::NoChanges) => {
            warn!(log, "ssd_complete_detach ran more than once");
            Ok(subnet.into())
        }
        Err(e) => Err(ActionError::action_failed(e)),
    }
}

#[derive(Debug)]
pub struct SagaSubnetDetach;
impl NexusSaga for SagaSubnetDetach {
    const NAME: &'static str = "subnet-detach";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        subnet_detach_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(detach_subnet_action());
        builder.append(instance_state_action());
        builder.append(notify_dpd_action());
        builder.append(notify_opte_action());
        builder.append(complete_detach_action());
        Ok(builder.build()?)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::app::saga::create_saga_dag;
    use crate::app::sagas::subnet_attach::SagaSubnetAttach;
    use crate::app::sagas::test_helpers;
    use nexus_db_lookup::LookupPath;
    use nexus_test_utils::resource_helpers::create_default_ip_pools;
    use nexus_test_utils::resource_helpers::create_external_subnet_in_pool;
    use nexus_test_utils::resource_helpers::create_instance;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::create_subnet_pool;
    use nexus_test_utils::resource_helpers::create_subnet_pool_member;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::views::ExternalSubnet;
    use nexus_types::external_api::views::Project;
    use nexus_types::external_api::views::SubnetPool;
    use nexus_types::external_api::views::SubnetPoolMember;
    use omicron_common::address::IpVersion;
    use omicron_common::api::external::LookupType;
    use omicron_common::api::external::SimpleIdentityOrName;
    use omicron_uuid_kinds::ExternalSubnetUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::InstanceUuid;
    use oxnet::IpNet;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "cafe";
    const INSTANCE_NAME: &str = "menu";
    const SUBNET_POOL_NAME: &str = "coffee";
    const EXTERNAL_SUBNET_NAME: &str = "espresso";

    struct Context {
        _subnet_pool: SubnetPool,
        _member: SubnetPoolMember,
        subnet: ExternalSubnet,
        _project: Project,
        authz_instance: authz::Instance,
        detach_params: Params,
    }

    async fn setup_test(cptestctx: &ControlPlaneTestContext) -> Context {
        let client = &cptestctx.external_client;
        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = cptestctx.server.server_context().nexus.datastore();

        let subnet_pool =
            create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4).await;
        let member = create_subnet_pool_member(
            client,
            SUBNET_POOL_NAME,
            "192.0.2.0/24".parse().unwrap(),
        )
        .await;
        // Need an IPv4 and IPv6 pool.
        let _ = create_default_ip_pools(client).await;
        let project = create_project(client, PROJECT_NAME).await;

        // Lookup the project since we need that to make the authz stuff.
        let (.., authz_project, _db_project) =
            LookupPath::new(&opctx, datastore)
                .project_id(project.identity.id)
                .fetch()
                .await
                .unwrap();

        // Create the subnet.
        let subnet = create_external_subnet_in_pool(
            client,
            SUBNET_POOL_NAME,
            PROJECT_NAME,
            EXTERNAL_SUBNET_NAME,
            28,
        )
        .await;
        let authz_subnet = authz::ExternalSubnet::new(
            authz_project.clone(),
            ExternalSubnetUuid::from_untyped_uuid(subnet.identity.id),
            LookupType::ById(subnet.identity.id),
        );

        // Create an instance.
        let instance =
            create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;
        let authz_instance = authz::Instance::new(
            authz_project.clone(),
            instance.identity.id,
            LookupType::ById(instance.identity.id),
        );
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &instance_id,
        )
        .await;

        // Actually attach the subnet.
        let attach_params = crate::app::sagas::subnet_attach::Params {
            authz_subnet: authz_subnet.clone(),
            ip_version: IpVersion::V4.into(),
            authz_instance: authz_instance.clone(),
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
        };
        cptestctx
            .server
            .server_context()
            .nexus
            .sagas
            .saga_execute::<SagaSubnetAttach>(attach_params)
            .await
            .expect("subnet attach saga should succeed");

        Context {
            _subnet_pool: subnet_pool,
            _member: member,
            subnet,
            _project: project,
            authz_instance: authz_instance.clone(),
            detach_params: Params {
                authz_instance,
                authz_subnet,
                serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            },
        }
    }

    async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
        instance_id: InstanceUuid,
        subnet: IpNet,
    ) {
        let sled_agent = cptestctx.first_sled_agent();
        let datastore = cptestctx.server.server_context().nexus.datastore();
        let opctx = test_helpers::test_opctx(cptestctx);

        // We should be back to having the subnet attached in the DB.
        let subnets =
            datastore.list_all_attached_subnets_batched(&opctx).await.unwrap();
        assert_eq!(subnets.len(), 1);
        assert_eq!(subnets[0].instance_id, instance_id);
        assert_eq!(subnets[0].subnet, subnet);

        // And the sled-agent should know about it too.
        let VmmAndSledIds { vmm_id, .. } =
            test_helpers::instance_fetch_vmm_and_sled_ids(
                cptestctx,
                &instance_id,
            )
            .await;
        let on_sled_agent = sled_agent
            .attached_subnets
            .lock()
            .unwrap()
            .get(&vmm_id)
            .expect("sled agent should have entry for this instance")
            .get(&subnet)
            .copied()
            .unwrap_or_else(|| {
                panic!(
                    "sled agent should have an entry for the subnet attached \
                in the saga: {}",
                    subnet,
                )
            });
        assert!(
            on_sled_agent.is_external,
            "All attached subnets should be external at this point"
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let sled_agent = cptestctx.first_sled_agent();
        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = &nexus.db_datastore;
        let context = setup_test(&cptestctx).await;

        // Run the detach saga itself.
        nexus
            .sagas
            .saga_execute::<SagaSubnetDetach>(context.detach_params.clone())
            .await
            .expect("subnet detach saga should succeed");

        // Now the sled agent should not have a record of this subnet.
        let instance_id =
            InstanceUuid::from_untyped_uuid(context.authz_instance.id());
        let VmmAndSledIds { vmm_id, .. } =
            test_helpers::instance_fetch_vmm_and_sled_ids(
                cptestctx,
                &instance_id,
            )
            .await;
        assert!(
            !sled_agent
                .attached_subnets
                .lock()
                .unwrap()
                .get(&vmm_id)
                .expect("sled agent should have entry for this instance")
                .contains_key(&context.subnet.subnet),
            "sled agent should not have an entry for this subnet \
            after running the detach saga"
        );

        // The database records should also indicate it's now detached.
        let subnets = datastore
            .instance_lookup_attached_external_subnets(&opctx, instance_id)
            .await
            .unwrap();
        assert!(subnets.is_empty());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let context = setup_test(cptestctx).await;
        let instance_id =
            InstanceUuid::from_untyped_uuid(context.authz_instance.id());
        let ip_subnet = context.subnet.subnet;
        test_helpers::action_failure_can_unwind::<SagaSubnetDetach, _, _>(
            nexus,
            || Box::pin(async { context.detach_params.clone() }),
            || Box::pin(verify_clean_slate(&cptestctx, instance_id, ip_subnet)),
            log,
        )
        .await
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let context = setup_test(cptestctx).await;
        let instance_id =
            InstanceUuid::from_untyped_uuid(context.authz_instance.id());
        let ip_subnet = context.subnet.subnet;
        test_helpers::action_failure_can_unwind_idempotently::<
            SagaSubnetDetach,
            _,
            _,
        >(
            nexus,
            || Box::pin(async { context.detach_params.clone() }),
            || Box::pin(verify_clean_slate(&cptestctx, instance_id, ip_subnet)),
            log,
        )
        .await
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let context = setup_test(cptestctx).await;
        let dag =
            create_saga_dag::<SagaSubnetDetach>(context.detach_params.clone())
                .unwrap();
        test_helpers::actions_succeed_idempotently(nexus, dag).await;
    }
}
