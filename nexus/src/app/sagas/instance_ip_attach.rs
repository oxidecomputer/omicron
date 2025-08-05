// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::instance_common::{
    ExternalIpAttach, ModifyStateForExternalIp, VmmAndSledIds,
    instance_ip_add_nat, instance_ip_add_opte, instance_ip_get_instance_state,
    instance_ip_move_state, instance_ip_remove_opte,
};
use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz};
use nexus_db_model::{IpAttachState, NatEntry};
use nexus_types::external_api::views;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use uuid::Uuid;

// The IP attach/detach sagas do some resource locking -- because we
// allow them to be called in [Running, Stopped], they must contend
// with each other/themselves, instance start, instance delete, and
// the instance stop action (noting the latter is not a saga).
//
// The main means of access control here is an external IP's `state`.
// Entering either saga begins with an atomic swap from Attached/Detached
// to Attaching/Detaching. This prevents concurrent attach/detach on the
// same EIP, and prevents instance start and migrate from completing with an
// Error::unavail via instance_ensure_registered and/or DPD.
//
// Overlap with stop is handled by treating comms failures with
// sled-agent as temporary errors and unwinding. For the delete case, we
// allow the detach completion to have a missing record -- both instance delete
// and detach will leave NAT in the correct state. For attach, if we make it
// to completion and an IP is `detached`, we unwind as a precaution.
// See `instance_common::instance_ip_get_instance_state` for more info.
//
// One more consequence of sled state being able to change beneath us
// is that the central undo actions (DPD/OPTE state) *must* be best-effort.
// This is not bad per-se: instance stop does not itself remove NAT routing
// rules. The only reason these should fail is because an instance has stopped,
// or DPD has died.

declare_saga_actions! {
    instance_ip_attach;
    ATTACH_EXTERNAL_IP -> "target_ip" {
        + siia_begin_attach_ip
        - siia_begin_attach_ip_undo
    }

    INSTANCE_STATE -> "instance_state" {
        + siia_get_instance_state
    }

    REGISTER_NAT -> "nat_entry" {
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
    pub create_params: ExternalIpAttach,
    pub authz_instance: authz::Instance,
    pub project_id: Uuid,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

async fn siia_begin_attach_ip(
    sagactx: NexusActionContext,
) -> Result<ModifyStateForExternalIp, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let instance_id =
        InstanceUuid::from_untyped_uuid(params.authz_instance.id());
    match &params.create_params {
        // Allocate a new IP address from the target, possibly default, pool
        ExternalIpAttach::Ephemeral { pool } => {
            let pool = if let Some(name_or_id) = pool {
                Some(
                    osagactx
                        .nexus()
                        .ip_pool_lookup(&opctx, name_or_id)
                        .map_err(ActionError::action_failed)?
                        .lookup_for(authz::Action::CreateChild)
                        .await
                        .map_err(ActionError::action_failed)?
                        .0,
                )
            } else {
                None
            };

            datastore
                .allocate_instance_ephemeral_ip(
                    &opctx,
                    Uuid::new_v4(),
                    instance_id,
                    pool,
                    false,
                )
                .await
                .map_err(ActionError::action_failed)
                .map(|(external_ip, do_saga)| ModifyStateForExternalIp {
                    external_ip: Some(external_ip),
                    do_saga,
                })
        }
        // Set the parent of an existing floating IP to the new instance's ID.
        ExternalIpAttach::Floating { floating_ip } => datastore
            .floating_ip_begin_attach(&opctx, &floating_ip, instance_id, false)
            .await
            .map_err(ActionError::action_failed)
            .map(|(external_ip, do_saga)| ModifyStateForExternalIp {
                external_ip: Some(external_ip),
                do_saga,
            }),
    }
}

async fn siia_begin_attach_ip_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    warn!(log, "siia_begin_attach_ip_undo: Reverting detached->attaching");
    let params = sagactx.saga_params::<Params>()?;
    let new_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;
    if !instance_ip_move_state(
        &sagactx,
        &params.serialized_authn,
        IpAttachState::Attaching,
        IpAttachState::Detached,
        &new_ip,
    )
    .await?
    {
        error!(log, "siia_begin_attach_ip_undo: external IP was deleted")
    }

    Ok(())
}

async fn siia_get_instance_state(
    sagactx: NexusActionContext,
) -> Result<Option<VmmAndSledIds>, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_get_instance_state(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
        "attach",
    )
    .await
}

// XXX: Need to abstract over v4 and v6 NAT entries when the time comes.
async fn siia_nat(
    sagactx: NexusActionContext,
) -> Result<Option<NatEntry>, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let sled_id = sagactx
        .lookup::<Option<VmmAndSledIds>>("instance_state")?
        .map(|ids| ids.sled_id);

    let target_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;
    instance_ip_add_nat(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
        sled_id,
        target_ip,
    )
    .await
}

async fn siia_nat_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nat_entry = sagactx.lookup::<Option<NatEntry>>("nat_entry")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let Some(nat_entry) = nat_entry else {
        // Seeing `None` here means that we never pushed DPD state in
        // the first instance. Nothing to undo.
        return Ok(());
    };

    // This requires some explanation in one case, where we can fail because an
    // instance may have moved running -> stopped -> deleted.
    // An instance delete will cause us to unwind and return to this stage *but*
    // the ExternalIp will no longer have a useful parent (or even a
    // different parent!).
    //
    // Internally, we delete the NAT entry *without* checking its instance state because
    // it may either be `None`, or another instance may have attached. The
    // first case is fine, but we need to consider NAT RPW semantics for the second:
    // * The NAT entry table will ensure uniqueness on (external IP, low_port,
    //   high_port) for non-deleted rows.
    // * Instance start and IP attach on a running instance will try to insert such
    //   a row, fail, and then delete this row before moving forwards.
    //   - Until either side deletes the row, we're polluting switch NAT.
    //   - We can't guarantee quick reuse to remove this rule via attach.
    //   - This will lead to a *new* NAT entry we need to protect, so we need to be careful
    //     that we only remove *our* incarnation. This is likelier to be hit
    //     if an ephemeral IP is deallocated, reallocated, and reused in a short timeframe.
    // * Instance create will successfully set parent, since it won't attempt to ensure
    //   DPD has correct NAT state unless set to `start: true`.
    // So it is safe/necessary to remove using the old entry here to target the
    // exact row we created..

    if let Err(e) = osagactx
        .nexus()
        .delete_dpd_config_by_entry(&opctx, &nat_entry)
        .await
        .map_err(ActionError::action_failed)
    {
        error!(log, "siia_nat_undo: failed to notify DPD: {e}");
    }

    Ok(())
}

async fn siia_update_opte(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let ids = sagactx.lookup::<Option<VmmAndSledIds>>("instance_state")?;
    let target_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;
    instance_ip_add_opte(&sagactx, ids, target_ip).await
}

async fn siia_update_opte_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let ids = sagactx.lookup::<Option<VmmAndSledIds>>("instance_state")?;
    let target_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;
    if let Err(e) = instance_ip_remove_opte(&sagactx, ids, target_ip).await {
        error!(log, "siia_update_opte_undo: failed to notify sled-agent: {e}");
    }
    Ok(())
}

async fn siia_complete_attach(
    sagactx: NexusActionContext,
) -> Result<views::ExternalIp, ActionError> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;
    let target_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;

    // There is a clause in `external_ip_complete_op` which specifically
    // causes an unwind here if the instance delete saga fires and an IP is either
    // detached or deleted.
    if !instance_ip_move_state(
        &sagactx,
        &params.serialized_authn,
        IpAttachState::Attaching,
        IpAttachState::Attached,
        &target_ip,
    )
    .await?
    {
        warn!(log, "siia_complete_attach: call was idempotent")
    }

    target_ip
        .external_ip
        .ok_or_else(|| {
            Error::internal_error(
                "must always have a defined external IP during instance attach",
            )
        })
        .and_then(TryInto::try_into)
        .map_err(ActionError::action_failed)
}

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
    use super::*;
    use crate::app::{db, saga::create_saga_dag, sagas::test_helpers};
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::{ExternalIp, IpKind};
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::{
        create_default_ip_pool, create_floating_ip, create_instance,
        create_project,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::SimpleIdentityOrName;
    use sled_agent_types::instance::InstanceExternalIpBody;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "cafe";
    const INSTANCE_NAME: &str = "menu";
    const FIP_NAME: &str = "affogato";

    pub async fn ip_manip_test_setup(client: &ClientTestContext) -> Uuid {
        create_default_ip_pool(&client).await;
        let project = create_project(client, PROJECT_NAME).await;
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

    pub async fn new_test_params(
        opctx: &OpContext,
        datastore: &db::DataStore,
        use_floating: bool,
    ) -> Params {
        let project_name = db::model::Name(PROJECT_NAME.parse().unwrap());
        let create_params = if use_floating {
            let (.., floating_ip) = LookupPath::new(opctx, datastore)
                .project_name(&project_name)
                .floating_ip_name(&db::model::Name(FIP_NAME.parse().unwrap()))
                .lookup_for(authz::Action::Modify)
                .await
                .unwrap();
            ExternalIpAttach::Floating { floating_ip }
        } else {
            ExternalIpAttach::Ephemeral { pool: None }
        };

        let (.., authz_project, authz_instance) =
            LookupPath::new(opctx, datastore)
                .project_name(&project_name)
                .instance_name(&db::model::Name(INSTANCE_NAME.parse().unwrap()))
                .lookup_for(authz::Action::Modify)
                .await
                .unwrap();

        Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: authz_project.id(),
            create_params,
            authz_instance,
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
        let _project_id = ip_manip_test_setup(&client).await;
        let instance =
            create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;

        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &instance_id,
        )
        .await;

        for use_float in [false, true] {
            let params = new_test_params(&opctx, datastore, use_float).await;
            nexus
                .sagas
                .saga_execute::<SagaInstanceIpAttach>(params)
                .await
                .expect("Attach saga should succeed");
        }

        // Sled agent has a record of the new external IPs.
        let VmmAndSledIds { vmm_id, .. } =
            crate::app::sagas::test_helpers::instance_fetch_vmm_and_sled_ids(
                cptestctx,
                &instance_id,
            )
            .await;
        {
            let mut eips = sled_agent.external_ips.lock().unwrap();
            let my_eips = eips.entry(vmm_id).or_default();
            assert!(
                my_eips
                    .iter()
                    .any(|v| matches!(v, InstanceExternalIpBody::Floating(_)))
            );
            assert!(
                my_eips
                    .iter()
                    .any(|v| matches!(v, InstanceExternalIpBody::Ephemeral(_)))
            );
        }

        // DB has records for SNAT plus the new IPs.
        let db_eips = datastore
            .instance_lookup_external_ips(&opctx, instance_id)
            .await
            .unwrap();
        assert_eq!(db_eips.len(), 3);
        assert!(db_eips.iter().any(|v| v.kind == IpKind::Ephemeral));
        assert!(db_eips.iter().any(|v| v.kind == IpKind::Floating));
        assert!(db_eips.iter().any(|v| v.kind == IpKind::SNat));
    }

    pub(crate) async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
        instance_id: InstanceUuid,
    ) {
        use nexus_db_schema::schema::external_ip::dsl;

        let sled_agent = cptestctx.first_sled_agent();
        let datastore = cptestctx.server.server_context().nexus.datastore();

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // No Floating IPs exist in states other than 'detached'.
        assert!(
            dsl::external_ip
                .filter(dsl::kind.eq(IpKind::Floating))
                .filter(dsl::time_deleted.is_null())
                .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
                .filter(dsl::state.ne(IpAttachState::Detached))
                .select(ExternalIp::as_select())
                .first_async::<ExternalIp>(&*conn)
                .await
                .optional()
                .unwrap()
                .is_none()
        );

        // All ephemeral IPs are removed.
        assert!(
            dsl::external_ip
                .filter(dsl::kind.eq(IpKind::Ephemeral))
                .filter(dsl::time_deleted.is_null())
                .select(ExternalIp::as_select())
                .first_async::<ExternalIp>(&*conn)
                .await
                .optional()
                .unwrap()
                .is_none()
        );

        // No IP bindings remain on sled-agent.
        let VmmAndSledIds { vmm_id, .. } =
            crate::app::sagas::test_helpers::instance_fetch_vmm_and_sled_ids(
                cptestctx,
                &instance_id,
            )
            .await;
        let mut eips = sled_agent.external_ips.lock().unwrap();
        let my_eips = eips.entry(vmm_id).or_default();
        assert!(my_eips.is_empty());
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
        let _project_id = ip_manip_test_setup(&client).await;
        let instance =
            create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;

        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &instance_id,
        )
        .await;

        for use_float in [false, true] {
            test_helpers::action_failure_can_unwind::<SagaInstanceIpAttach, _, _>(
                nexus,
                || Box::pin(new_test_params(&opctx, datastore, use_float) ),
                || Box::pin(verify_clean_slate(&cptestctx, instance_id)),
                log,
            )
            .await;
        }
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
        let _project_id = ip_manip_test_setup(&client).await;
        let instance =
            create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;

        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &instance_id,
        )
        .await;

        for use_float in [false, true] {
            test_helpers::action_failure_can_unwind_idempotently::<
                SagaInstanceIpAttach,
                _,
                _,
            >(
                nexus,
                || Box::pin(new_test_params(&opctx, datastore, use_float)),
                || Box::pin(verify_clean_slate(&cptestctx, instance_id)),
                log,
            )
            .await;
        }
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
        let _project_id = ip_manip_test_setup(&client).await;
        let instance =
            create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;

        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &InstanceUuid::from_untyped_uuid(instance.identity.id),
        )
        .await;

        for use_float in [false, true] {
            let params = new_test_params(&opctx, datastore, use_float).await;
            let dag = create_saga_dag::<SagaInstanceIpAttach>(params).unwrap();
            test_helpers::actions_succeed_idempotently(nexus, dag).await;
        }
    }
}
