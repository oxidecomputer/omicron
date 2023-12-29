// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::instance_common::{
    instance_ip_add_nat, instance_ip_add_opte, instance_ip_get_instance_state,
    instance_ip_move_state, instance_ip_remove_nat, instance_ip_remove_opte,
    ModifyStateForExternalIp,
};
use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use nexus_db_model::IpAttachState;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_types::external_api::views;
use omicron_common::api::external::Error;
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
// same EIP, and prevents instance start and migrate from completing with an
// Error::unavail via instance_ensure_registered and/or DPD.
//
// Overlap with stop is handled by treating comms failures with
// sled-agent as temporary errors and unwinding. For the delete case, we
// allow the attach/detach completion to have a missing record.
// See `instance_common::instance_ip_get_instance_state` for more info.
//
// One more consequence of sled state being able to change beneath us
// is that the central undo actions (DPD/OPTE state) *must* be best-effort.
// This is not bad per-se: instance stop does not itself remove NAT routing
// rules. The only reason these should fail is because an instance has stopped,
// at which point there's no good in e.g. adding another entry to a non-existent
// sled-agent regardless.

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
                .map(|(external_ip, do_saga)| ModifyStateForExternalIp {
                    external_ip: Some(external_ip),
                    do_saga,
                })
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
) -> Result<Option<Uuid>, ActionError> {
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
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;
    if let Err(e) = instance_ip_remove_nat(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
    )
    .await
    {
        error!(log, "siia_nat_undo: failed to notify DPD: {e}");
    }

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
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;
    if let Err(e) =
        instance_ip_remove_opte(&sagactx, &params.authz_instance).await
    {
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
    use crate::app::{saga::create_saga_dag, sagas::test_helpers};
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_model::{ExternalIp, IpKind, Name};
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::{
        create_floating_ip, create_instance, create_project, populate_ip_pool,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::SimpleIdentity;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "cafe";
    const INSTANCE_NAME: &str = "menu";
    const FIP_NAME: &str = "affogato";

    pub async fn ip_manip_test_setup(client: &ClientTestContext) -> Uuid {
        populate_ip_pool(&client, "default", None).await;
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
        let create_params = if use_floating {
            params::ExternalIpCreate::Floating {
                floating_ip_name: FIP_NAME.parse().unwrap(),
            }
        } else {
            params::ExternalIpCreate::Ephemeral { pool_name: None }
        };

        let (.., authz_project, authz_instance) =
            LookupPath::new(opctx, datastore)
                .project_name(&Name(PROJECT_NAME.parse().unwrap()))
                .instance_name(&Name(INSTANCE_NAME.parse().unwrap()))
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
        let apictx = &cptestctx.server.apictx();
        let nexus = &apictx.nexus;
        let sled_agent = &cptestctx.sled_agent.sled_agent;

        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = &nexus.db_datastore;
        let _project_id = ip_manip_test_setup(&client).await;
        let instance =
            create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;

        for use_float in [false, true] {
            let params = new_test_params(&opctx, datastore, use_float).await;

            let dag = create_saga_dag::<SagaInstanceIpAttach>(params).unwrap();
            let saga = nexus.create_runnable_saga(dag).await.unwrap();
            nexus.run_saga(saga).await.expect("Attach saga should succeed");
        }

        let instance_id = instance.id();

        // Sled agent has a record of the new external IPs.
        let mut eips = sled_agent.external_ips.lock().await;
        let my_eips = eips.entry(instance_id).or_default();
        assert!(my_eips.iter().any(|v| matches!(
            v,
            omicron_sled_agent::params::InstanceExternalIpBody::Floating(_)
        )));
        assert!(my_eips.iter().any(|v| matches!(
            v,
            omicron_sled_agent::params::InstanceExternalIpBody::Ephemeral(_)
        )));

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
        instance_id: Uuid,
    ) {
        use nexus_db_queries::db::schema::external_ip::dsl;

        let sled_agent = &cptestctx.sled_agent.sled_agent;
        let datastore = cptestctx.server.apictx().nexus.datastore();

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // No Floating IPs exist in states other than 'detached'.
        assert!(dsl::external_ip
            .filter(dsl::kind.eq(IpKind::Floating))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::state.ne(IpAttachState::Detached))
            .select(ExternalIp::as_select())
            .first_async::<ExternalIp>(&*conn,)
            .await
            .optional()
            .unwrap()
            .is_none());

        // All ephemeral IPs are removed.
        assert!(dsl::external_ip
            .filter(dsl::kind.eq(IpKind::Ephemeral))
            .filter(dsl::time_deleted.is_null())
            .select(ExternalIp::as_select())
            .first_async::<ExternalIp>(&*conn,)
            .await
            .optional()
            .unwrap()
            .is_none());

        // No IP bindings remain on sled-agent.
        let mut eips = sled_agent.external_ips.lock().await;
        let my_eips = eips.entry(instance_id).or_default();
        assert!(my_eips.is_empty());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
        let client = &cptestctx.external_client;
        let apictx = &cptestctx.server.apictx();
        let nexus = &apictx.nexus;

        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = &nexus.db_datastore;
        let _project_id = ip_manip_test_setup(&client).await;
        let instance =
            create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;

        for use_float in [false, true] {
            test_helpers::action_failure_can_unwind::<SagaInstanceIpAttach, _, _>(
                nexus,
                || Box::pin(new_test_params(&opctx, datastore, use_float) ),
                || Box::pin(verify_clean_slate(&cptestctx, instance.id())),
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
        let apictx = &cptestctx.server.apictx();
        let nexus = &apictx.nexus;

        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = &nexus.db_datastore;
        let _project_id = ip_manip_test_setup(&client).await;
        let instance =
            create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;

        for use_float in [false, true] {
            test_helpers::action_failure_can_unwind_idempotently::<
                SagaInstanceIpAttach,
                _,
                _,
            >(
                nexus,
                || Box::pin(new_test_params(&opctx, datastore, use_float)),
                || Box::pin(verify_clean_slate(&cptestctx, instance.id())),
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
        let apictx = &cptestctx.server.apictx();
        let nexus = &apictx.nexus;

        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = &nexus.db_datastore;
        let _project_id = ip_manip_test_setup(&client).await;
        let _instance =
            create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;

        for use_float in [false, true] {
            let params = new_test_params(&opctx, datastore, use_float).await;
            let dag = create_saga_dag::<SagaInstanceIpAttach>(params).unwrap();
            test_helpers::actions_succeed_idempotently(nexus, dag).await;
        }
    }
}
