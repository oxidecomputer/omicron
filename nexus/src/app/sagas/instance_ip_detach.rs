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
use omicron_common::api::external::NameOrId;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, SledUuid};
use ref_cast::RefCast;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use uuid::Uuid;

// This runs on similar logic to instance IP attach: see its head
// comment for an explanation of the structure wrt. other sagas.

declare_saga_actions! {
    instance_ip_detach;
    DETACH_EXTERNAL_IP -> "target_ip" {
        + siid_begin_detach_ip
        - siid_begin_detach_ip_undo
    }

    INSTANCE_STATE -> "instance_state" {
        + siid_get_instance_state
    }

    REMOVE_NAT -> "no_result0" {
        + siid_nat
        - siid_nat_undo
    }

    REMOVE_OPTE_PORT -> "no_result1" {
        + siid_update_opte
        - siid_update_opte_undo
    }

    COMPLETE_DETACH -> "output" {
        + siid_complete_detach
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub delete_params: params::ExternalIpDetach,
    pub authz_instance: authz::Instance,
    pub project_id: Uuid,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

async fn siid_begin_detach_ip(
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
    match &params.delete_params {
        params::ExternalIpDetach::Ephemeral => {
            let eip = datastore
                .instance_lookup_ephemeral_ip(&opctx, instance_id)
                .await
                .map_err(ActionError::action_failed)?;

            if let Some(eph_ip) = eip {
                datastore
                    .begin_deallocate_ephemeral_ip(
                        &opctx,
                        eph_ip.id,
                        instance_id,
                    )
                    .await
                    .map_err(ActionError::action_failed)
                    .map(|external_ip| ModifyStateForExternalIp {
                        do_saga: external_ip.is_some(),
                        external_ip,
                    })
            } else {
                Ok(ModifyStateForExternalIp {
                    do_saga: false,
                    external_ip: None,
                })
            }
        }
        params::ExternalIpDetach::Floating { floating_ip } => {
            let (.., authz_fip) = match floating_ip {
                NameOrId::Name(name) => LookupPath::new(&opctx, datastore)
                    .project_id(params.project_id)
                    .floating_ip_name(db::model::Name::ref_cast(name)),
                NameOrId::Id(id) => {
                    LookupPath::new(&opctx, datastore).floating_ip_id(*id)
                }
            }
            .lookup_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

            datastore
                .floating_ip_begin_detach(
                    &opctx,
                    &authz_fip,
                    instance_id,
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

async fn siid_begin_detach_ip_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    warn!(log, "siid_begin_detach_ip_undo: Reverting attached->detaching");
    let params = sagactx.saga_params::<Params>()?;
    let new_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;
    if !instance_ip_move_state(
        &sagactx,
        &params.serialized_authn,
        IpAttachState::Detaching,
        IpAttachState::Attached,
        &new_ip,
    )
    .await?
    {
        error!(log, "siid_begin_detach_ip_undo: external IP was deleted")
    }

    Ok(())
}

async fn siid_get_instance_state(
    sagactx: NexusActionContext,
) -> Result<Option<SledUuid>, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_get_instance_state(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
        "detach",
    )
    .await
}

async fn siid_nat(sagactx: NexusActionContext) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let sled_id = sagactx.lookup::<Option<SledUuid>>("instance_state")?;
    let target_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;
    instance_ip_remove_nat(
        &sagactx,
        &params.serialized_authn,
        sled_id,
        target_ip,
    )
    .await
}

async fn siid_nat_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;
    let sled_id = sagactx.lookup::<Option<SledUuid>>("instance_state")?;
    let target_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;
    if let Err(e) = instance_ip_add_nat(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
        sled_id,
        target_ip,
    )
    .await
    {
        error!(log, "siid_nat_undo: failed to notify DPD: {e}");
    }

    Ok(())
}

async fn siid_update_opte(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let sled_id = sagactx.lookup::<Option<SledUuid>>("instance_state")?;
    let target_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;
    instance_ip_remove_opte(
        &sagactx,
        &params.authz_instance,
        sled_id,
        target_ip,
    )
    .await
}

async fn siid_update_opte_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;
    let sled_id = sagactx.lookup::<Option<SledUuid>>("instance_state")?;
    let target_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;
    if let Err(e) = instance_ip_add_opte(
        &sagactx,
        &params.authz_instance,
        sled_id,
        target_ip,
    )
    .await
    {
        error!(log, "siid_update_opte_undo: failed to notify sled-agent: {e}");
    }
    Ok(())
}

async fn siid_complete_detach(
    sagactx: NexusActionContext,
) -> Result<Option<views::ExternalIp>, ActionError> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;
    let target_ip = sagactx.lookup::<ModifyStateForExternalIp>("target_ip")?;

    if !instance_ip_move_state(
        &sagactx,
        &params.serialized_authn,
        IpAttachState::Detaching,
        IpAttachState::Detached,
        &target_ip,
    )
    .await?
    {
        warn!(
            log,
            "siid_complete_detach: external IP was deleted or call was idempotent"
        )
    }

    target_ip
        .external_ip
        .map(TryInto::try_into)
        .transpose()
        .map_err(ActionError::action_failed)
}

#[derive(Debug)]
pub struct SagaInstanceIpDetach;
impl NexusSaga for SagaInstanceIpDetach {
    const NAME: &'static str = "external-ip-detach";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_ip_detach_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(detach_external_ip_action());
        builder.append(instance_state_action());
        builder.append(remove_nat_action());
        builder.append(remove_opte_port_action());
        builder.append(complete_detach_action());
        Ok(builder.build()?)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::{
        app::{
            saga::create_saga_dag,
            sagas::{
                instance_ip_attach::{self, test::ip_manip_test_setup},
                test_helpers,
            },
        },
        Nexus,
    };
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use nexus_db_model::{ExternalIp, IpKind};
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::create_instance;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::{Name, SimpleIdentity};
    use std::sync::Arc;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "cafe";
    const INSTANCE_NAME: &str = "menu";
    const FIP_NAME: &str = "affogato";

    async fn new_test_params(
        opctx: &OpContext,
        datastore: &db::DataStore,
        use_floating: bool,
    ) -> Params {
        let delete_params = if use_floating {
            params::ExternalIpDetach::Floating {
                floating_ip: FIP_NAME.parse::<Name>().unwrap().into(),
            }
        } else {
            params::ExternalIpDetach::Ephemeral
        };

        let (.., authz_project, authz_instance) =
            LookupPath::new(opctx, datastore)
                .project_name(&db::model::Name(PROJECT_NAME.parse().unwrap()))
                .instance_name(&db::model::Name(INSTANCE_NAME.parse().unwrap()))
                .lookup_for(authz::Action::Modify)
                .await
                .unwrap();

        Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: authz_project.id(),
            delete_params,
            authz_instance,
        }
    }

    async fn attach_instance_ips(nexus: &Arc<Nexus>, opctx: &OpContext) {
        let datastore = &nexus.db_datastore;

        let proj_name = db::model::Name(PROJECT_NAME.parse().unwrap());
        let inst_name = db::model::Name(INSTANCE_NAME.parse().unwrap());
        let lookup = LookupPath::new(opctx, datastore)
            .project_name(&proj_name)
            .instance_name(&inst_name);

        let (.., authz_proj, authz_instance, _) = lookup.fetch().await.unwrap();

        for use_float in [false, true] {
            let params = instance_ip_attach::test::new_test_params(
                opctx, datastore, use_float,
            )
            .await;
            nexus
                .instance_attach_external_ip(
                    opctx,
                    authz_instance.clone(),
                    authz_proj.id(),
                    params.create_params,
                )
                .await
                .unwrap();
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let sled_agent = &cptestctx.sled_agent.sled_agent;

        let opctx = test_helpers::test_opctx(cptestctx);
        let datastore = &nexus.db_datastore;
        let _ = ip_manip_test_setup(&client).await;
        let instance =
            create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());

        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &instance_id,
        )
        .await;

        attach_instance_ips(nexus, &opctx).await;

        for use_float in [false, true] {
            let params = new_test_params(&opctx, datastore, use_float).await;
            nexus
                .sagas
                .saga_execute::<SagaInstanceIpDetach>(params)
                .await
                .expect("Detach saga should succeed");
        }

        // Sled agent has removed its records of the external IPs.
        let mut eips = sled_agent.external_ips.lock().await;
        let my_eips = eips.entry(instance_id.into_untyped_uuid()).or_default();
        assert!(my_eips.is_empty());

        // DB only has record for SNAT.
        let db_eips = datastore
            .instance_lookup_external_ips(&opctx, instance_id)
            .await
            .unwrap();
        assert_eq!(db_eips.len(), 1);
        assert!(db_eips.iter().any(|v| v.kind == IpKind::SNat));
    }

    pub(crate) async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
        instance_id: Uuid,
    ) {
        use nexus_db_queries::db::schema::external_ip::dsl;

        let opctx = test_helpers::test_opctx(cptestctx);
        let sled_agent = &cptestctx.sled_agent.sled_agent;
        let datastore = cptestctx.server.server_context().nexus.datastore();

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // No IPs in transitional states w/ current instance.
        assert!(dsl::external_ip
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::state.ne(IpAttachState::Attached))
            .select(ExternalIp::as_select())
            .first_async::<ExternalIp>(&*conn)
            .await
            .optional()
            .unwrap()
            .is_none());

        // No external IPs in detached state.
        assert!(dsl::external_ip
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.eq(IpAttachState::Detached))
            .select(ExternalIp::as_select())
            .first_async::<ExternalIp>(&*conn)
            .await
            .optional()
            .unwrap()
            .is_none());

        // Instance still has one Ephemeral IP, and one Floating IP.
        let db_eips = datastore
            .instance_lookup_external_ips(
                &opctx,
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .unwrap();
        assert_eq!(db_eips.len(), 3);
        assert!(db_eips.iter().any(|v| v.kind == IpKind::Ephemeral));
        assert!(db_eips.iter().any(|v| v.kind == IpKind::Floating));
        assert!(db_eips.iter().any(|v| v.kind == IpKind::SNat));

        // No IP bindings remain on sled-agent.
        let eips = &*sled_agent.external_ips.lock().await;
        for (_nic_id, eip_set) in eips {
            assert_eq!(eip_set.len(), 2);
        }
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

        crate::app::sagas::test_helpers::instance_simulate(
            cptestctx,
            &InstanceUuid::from_untyped_uuid(instance.identity.id),
        )
        .await;

        attach_instance_ips(nexus, &opctx).await;

        for use_float in [false, true] {
            test_helpers::action_failure_can_unwind::<SagaInstanceIpDetach, _, _>(
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

        attach_instance_ips(nexus, &opctx).await;

        for use_float in [false, true] {
            test_helpers::action_failure_can_unwind_idempotently::<
                SagaInstanceIpDetach,
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

        attach_instance_ips(nexus, &opctx).await;

        for use_float in [false, true] {
            let params = new_test_params(&opctx, datastore, use_float).await;
            let dag = create_saga_dag::<SagaInstanceIpDetach>(params).unwrap();
            test_helpers::actions_succeed_idempotently(nexus, dag).await;
        }
    }
}
