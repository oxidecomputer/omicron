// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus saga to attach a subnet to an instance.

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
use nexus_db_model::IpNet;
use nexus_db_queries::db::datastore::ExternalSubnetAttachResult;
use nexus_db_queries::db::datastore::ExternalSubnetBeginAttachResult;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

declare_saga_actions! {
    subnet_attach;
    ATTACH_SUBNET -> "subnet" {
        + ssa_begin_attach_subnet
        - ssa_begin_attach_subnet_undo
    }

    INSTANCE_STATE -> "instance_state" {
        + ssa_get_instance_state
    }

    NOTIFY_DPD -> "pushed_subnet" {
        + ssa_notify_dpd
        - ssa_notify_dpd_undo
    }

    ENSURE_OPTE_PORT -> "no_result2" {
        + ssa_update_opte
        - ssa_update_opte_undo
    }

    COMPLETE_ATTACH -> "output" {
        + ssa_complete_attach
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub authz_subnet: authz::ExternalSubnet,
    pub authz_instance: authz::Instance,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

// Mark the external subnet record as "attaching" to the provided instance.
async fn ssa_begin_attach_subnet(
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
        .begin_attach_subnet(
            &opctx,
            &params.authz_instance,
            &params.authz_subnet,
        )
        .await
        .map_err(ActionError::action_failed)
}

async fn ssa_begin_attach_subnet_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    let datastore = osagactx.datastore();

    debug!(log, "ensuring subnet is detached");
    let params = sagactx.saga_params::<Params>()?;
    let ExternalSubnetBeginAttachResult { subnet, do_saga } =
        sagactx.lookup::<ExternalSubnetBeginAttachResult>("subnet")?;
    if !do_saga {
        return Ok(());
    }
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    match datastore
        .ensure_external_subnet_final_attach_state(
            &opctx,
            subnet.id(),
            IpAttachState::Attaching,
            IpAttachState::Detached,
        )
        .await
        .map_err(ActionError::action_failed)
    {
        Ok(ExternalSubnetAttachResult::Modified) => Ok(()),
        Ok(ExternalSubnetAttachResult::NoChanges) => {
            warn!(log, "subnet is deleted, could not fully detach",);
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!("failed to fully detach subnet: {e}",)),
    }
}

async fn ssa_get_instance_state(
    sagactx: NexusActionContext,
) -> Result<Option<VmmAndSledIds>, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    networking_resource_instance_state(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
        "attach",
    )
    .await
}

async fn ssa_notify_dpd(
    sagactx: NexusActionContext,
) -> Result<Option<IpNet>, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let sled_id = sagactx
        .lookup::<Option<VmmAndSledIds>>("instance_state")?
        .map(|ids| ids.sled_id);
    let subnet = sagactx.lookup::<ExternalSubnetBeginAttachResult>("subnet")?;
    send_subnet_attachment_to_dpd(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
        sled_id,
        subnet,
    )
    .await
}

async fn ssa_notify_dpd_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let Some(subnet) = sagactx.lookup::<Option<IpNet>>("pushed_subnet")? else {
        // Never sent the subnet to Dendrite, nothing to undo.
        return Ok(());
    };
    delete_subnet_attachment_from_dpd(&sagactx, subnet)
        .await
        .context("deleting attached subnet from Dendrite")
}

async fn ssa_update_opte(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let ids = sagactx.lookup::<Option<VmmAndSledIds>>("instance_state")?;
    let subnet = sagactx.lookup::<ExternalSubnetBeginAttachResult>("subnet")?;
    send_subnet_attachment_to_opte(&sagactx, ids, subnet).await
}

async fn ssa_update_opte_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let ids = sagactx.lookup::<Option<VmmAndSledIds>>("instance_state")?;
    let subnet = sagactx.lookup::<ExternalSubnetBeginAttachResult>("subnet")?;
    delete_subnet_attachment_from_opte(&sagactx, ids, subnet)
        .await
        .context("deleting attached subnet from OPTE")
}

async fn ssa_complete_attach(
    sagactx: NexusActionContext,
) -> Result<views::ExternalSubnet, ActionError> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    let datastore = osagactx.datastore();
    debug!(log, "finalizing subnet attachment");
    let params = sagactx.saga_params::<Params>()?;
    let ExternalSubnetBeginAttachResult { subnet, do_saga } =
        sagactx.lookup::<ExternalSubnetBeginAttachResult>("subnet")?;
    if !do_saga {
        return Ok(subnet.into());
    }
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    match datastore
        .ensure_external_subnet_final_attach_state(
            &opctx,
            subnet.id(),
            IpAttachState::Attaching,
            IpAttachState::Attached,
        )
        .await
    {
        Ok(ExternalSubnetAttachResult::Modified) => Ok(subnet.into()),
        Ok(ExternalSubnetAttachResult::NoChanges) => {
            warn!(log, "subnet is deleted, could not complete attach");
            Err(ActionError::action_failed(Error::Gone))
        }
        Err(e) => Err(ActionError::action_failed(e)),
    }
}

#[derive(Debug)]
pub struct SagaSubnetAttach;
impl NexusSaga for SagaSubnetAttach {
    const NAME: &'static str = "subnet-attach";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        subnet_attach_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(attach_subnet_action());
        builder.append(instance_state_action());
        builder.append(notify_dpd_action());
        builder.append(ensure_opte_port_action());
        builder.append(complete_attach_action());
        Ok(builder.build()?)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::app::db;
    use crate::app::sagas::test_helpers;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::SelectableHelper;
    use diesel::QueryDsl;
    use diesel::OptionalExtension;
    use dropshot::test_util::ClientTestContext;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::ExternalIp;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::create_external_subnet;
    use nexus_test_utils::resource_helpers::create_floating_ip;
    use nexus_test_utils::resource_helpers::create_default_ip_pools;
    use nexus_test_utils::resource_helpers::create_subnet_pool_member;
    use nexus_test_utils::resource_helpers::create_subnet_pool;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::create_instance;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::params;
    use nexus_types::external_api::views::ExternalSubnet;
    use nexus_types::external_api::views::Project;
    use nexus_types::external_api::views::SubnetPool;
    use nexus_types::external_api::views::SubnetPoolMember;
    use omicron_common::api::external::SimpleIdentityOrName;
    use omicron_common::address::IpVersion;
    use omicron_uuid_kinds::InstanceUuid;
    use omicron_uuid_kinds::GenericUuid as _;
    use sled_agent_types::instance::InstanceExternalIpBody;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "cafe";
    const INSTANCE_NAME: &str = "menu";
    const POOL_NAME: &str = "coffee";
    const EXTERNAL_SUBNET_NAME: &str = "espresso";

    struct Context {
        _pool: SubnetPool,
        _member: SubnetPoolMember,
        subnet: ExternalSubnet,
        _project: Project,
    }

    async fn setup_test(client: &ClientTestContext) -> Context {
        let pool = create_subnet_pool(client, POOL_NAME, IpVersion::V6).await;
        let member = create_subnet_pool_member(client, POOL_NAME, "fd00::/48".parse().unwrap()).await;
        let subnet = create_external_subnet(client, POOL_NAME, EXTERNAL_SUBNET_NAME, 56).await;
        let project = create_project(client, PROJECT_NAME).await;
        Context {
            _pool: pool,
            _member: member,
            subnet,
            _project: project,
        }
    }

    pub async fn new_test_params(
        opctx: &OpContext,
        datastore: &db::DataStore,
    ) -> Params {
        let project_name = db::model::Name(PROJECT_NAME.parse().unwrap());
        let (.., authz_subnet, _) = LookupPath::new(opctx, datastore)
            .project_name(&project_name)
            .external_subnet_name(&db::model::Name(EXTERNAL_SUBNET_NAME.parse().unwrap()))
            .fetch_for(authz::Action::Modify)
            .await
            .unwrap();
        let (.., authz_instance) =
            LookupPath::new(opctx, datastore)
                .project_name(&project_name)
                .instance_name(&db::model::Name(INSTANCE_NAME.parse().unwrap()))
                .lookup_for(authz::Action::Modify)
                .await
                .unwrap();
        Params {
            authz_subnet,
            authz_instance,
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let sled_agent = cptestctx.first_sled_agent();
        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = &nexus.db_datastore;
        let context = setup_test(&client).await;
        let instance = create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &instance_id,
        )
        .await;

        // Run the saga itself.
        let params = new_test_params(&opctx, datastore).await;
        nexus
            .sagas
            .saga_execute::<SagaSubnetAttach>(params)
            .await
            .expect("subnet attach saga should succeed");

        // The sled agent should now know about these attached subnets.
        let VmmAndSledIds { vmm_id, .. } =
            test_helpers::instance_fetch_vmm_and_sled_ids(
                cptestctx,
                &instance_id,
            )
            .await;
        assert!(
            sled_agent
            .attached_subnets
            .lock()
            .unwrap()
            .get(&vmm_id)
            .expect("sled agent should have entry for this instance")
            .contains_key(&context.subnet.subnet),
            "sled agent should have an entry for the subnet attached \
            in the saga: {}",
            context.subnet.subnet,
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
        let client = &cptestctx.external_client;
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = &nexus.db_datastore;
        let context = setup_test(&client).await;
        let instance = create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &instance_id,
        )
        .await;
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
        let client = &cptestctx.external_client;
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = &nexus.db_datastore;
        let context = setup_test(&client).await;
        let instance = create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &instance_id,
        )
        .await;
        todo!();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = &nexus.db_datastore;
        let context = setup_test(&client).await;
        let instance = create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;
        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &InstanceUuid::from_untyped_uuid(instance.identity.id),
        )
        .await;
        todo!()
    }
}
