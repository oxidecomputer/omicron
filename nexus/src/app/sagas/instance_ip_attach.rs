// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    common_storage::{
        call_pantry_attach_for_disk, call_pantry_detach_for_disk,
        delete_crucible_regions, ensure_all_datasets_and_regions,
        get_pantry_address,
    },
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use futures::TryFutureExt;
use nexus_db_queries::db::identity::{Asset, Resource};
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::{CrucibleOpts, VolumeConstructionRequest};
use std::convert::TryFrom;
use std::net::IpAddr;
use std::net::SocketAddrV6;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

use sled_agent_client::types::InstanceExternalIpBody;

#[derive(Debug, Deserialize, Serialize)]
enum ExternalIp {
    Ephemeral(IpAddr, Uuid),
    Floating(IpAddr, Uuid),
}

impl From<ExternalIp> for InstanceExternalIpBody {
    fn from(value: ExternalIp) -> Self {
        match value {
            ExternalIp::Ephemeral(ip, _) => {
                InstanceExternalIpBody::Ephemeral(ip)
            }
            ExternalIp::Floating(ip, _) => InstanceExternalIpBody::Floating(ip),
        }
    }
}

impl From<ExternalIp> for Uuid {
    fn from(value: ExternalIp) -> Self {
        match value {
            ExternalIp::Ephemeral(_, id) => id,
            ExternalIp::Floating(_, id) => id,
        }
    }
}

// rough sequence of evts:
// - take temp ownership of instance while interacting w/ sled agent
//  -> mark instance migration id as Some(0) if None
// - Attach+alloc EIP to instance
// - Register routes
//  -> ensure_dpd...
//  -> must precede OPTE: host may change its sending
//     behaviour prematurely
// - Register addr in OPTE
//  -> Put addr in sled-agent endpoint
// - free up migration_id of instance.
//  -> mark instance migration id as None

declare_saga_actions! {
    instance_ip_attach;
    LOCK_MIGRATION -> "sled_id" {
        + siia_migration_lock
        - siia_migration_lock_undo
    }

    ATTACH_EXTERNAL_IP -> "new_ip" {
        + siia_attach_ip
        - siia_attach_ip_undo
    }

    REGISTER_NAT -> "no_result3" {
        + siia_nat
        - siia_nat
    }

    ENSURE_OPTE_PORT -> "no_result4" {
        + siia_update_opte
        - siia_update_opte_undo
    }

    UNLOCK_MIGRATION -> "no_result1" {
        + siia_migration_unlock
        - siia_migration_unlock_undo
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub create_params: params::ExternalIpCreate,
    pub authz_instance: authz::Instance,
    pub instance: db::model::Instance,
    pub ephemeral_ip_id: Uuid,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
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
        builder.append(lock_migration_action());
        builder.append(attach_external_ip_action());
        builder.append(register_nat_action());
        builder.append(ensure_opte_port_action());
        builder.append(unlock_migration_action());
        Ok(builder.build()?)
    }
}

async fn siia_migration_lock(
    sagactx: NexusActionContext,
) -> Result<Option<Uuid>, ActionError> {
    // TODO: do this.
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;

    let inst_and_vmm = datastore
        .instance_fetch_with_vmm(
            &osagactx.nexus().opctx_alloc,
            &params.authz_instance,
        )
        .await
        .map_err(ActionError::action_failed)?;

    // TODO: actually lock?
    // TODO: fail out in a user-friendly way if migrating?

    Ok(inst_and_vmm.vmm().as_ref().map(|v| v.sled_id))
}

async fn siia_migration_lock_undo(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    // TODO: do this iff. we implement migration lock.
    Ok(())
}

// TODO: factor this out for attach, detach, and instance create
//       to share an impl.

async fn siia_attach_ip(
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
            let eip = datastore
                .allocate_instance_ephemeral_ip(
                    &opctx,
                    params.ephemeral_ip_id,
                    params.instance.id(),
                    pool_name,
                )
                .await
                .map_err(ActionError::action_failed)?;

            Ok(ExternalIp::Ephemeral(eip.ip.ip(), params.ephemeral_ip_id))
        }
        // Set the parent of an existing floating IP to the new instance's ID.
        params::ExternalIpCreate::Floating { ref floating_ip_name } => {
            let floating_ip_name = db::model::Name(floating_ip_name.clone());
            let (.., authz_fip, db_fip) = LookupPath::new(&opctx, &datastore)
                .project_id(params.instance.project_id)
                .floating_ip_name(&floating_ip_name)
                .fetch_for(authz::Action::Modify)
                .await
                .map_err(ActionError::action_failed)?;

            let eip = datastore
                .floating_ip_attach(
                    &opctx,
                    &authz_fip,
                    &db_fip,
                    params.instance.id(),
                )
                .await
                .map_err(ActionError::action_failed)?;

            Ok(ExternalIp::Floating(eip.ip.ip(), db_fip.id()))
        }
    }
}

async fn siia_attach_ip_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // TODO: should not be looking up by name here for FIP.
    match params.create_params {
        params::ExternalIpCreate::Ephemeral { .. } => {
            datastore
                .deallocate_external_ip(&opctx, params.ephemeral_ip_id)
                .await?;
        }
        params::ExternalIpCreate::Floating { floating_ip_name } => {
            let floating_ip_name = db::model::Name(floating_ip_name.clone());
            let (.., authz_fip, db_fip) = LookupPath::new(&opctx, &datastore)
                .project_id(params.instance.project_id)
                .floating_ip_name(&floating_ip_name)
                .fetch_for(authz::Action::Modify)
                .await?;

            datastore
                .floating_ip_detach(
                    &opctx,
                    &authz_fip,
                    &db_fip,
                    Some(params.instance.id()),
                )
                .await?;
        }
    }
    Ok(())
}

async fn siia_nat(sagactx: NexusActionContext) -> Result<(), ActionError> {
    // NOTE: on undo we want to do this after unbind.
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // NOTE: mostly copied from instance_start.

    // No physical sled? Don't push NAT.
    let Some(sled_uuid) = sagactx.lookup::<Option<Uuid>>("sled_id")? else {
        return Ok(());
    };

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;

    // Querying sleds requires fleet access; use the instance allocator context
    // for this.
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
                params.instance.id(),
                &sled.address(),
                Some(new_ip.into()),
                dpd_client,
            )
            .await
            .map_err(ActionError::action_failed)?;
    }

    Ok(())
}

async fn siia_update_opte(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // No physical sled? Don't inform OPTE.
    let Some(sled_uuid) = sagactx.lookup::<Option<Uuid>>("sled_id")? else {
        return Ok(());
    };

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;

    // TODO: disambiguate the various sled agent errors etc.
    osagactx
        .nexus()
        .sled_client(&sled_uuid)
        .await
        .map_err(ActionError::action_failed)?
        .instance_put_external_ip(&params.instance.id(), &new_ip.into())
        .await
        .map_err(|_| {
            ActionError::action_failed(Error::invalid_request("hmm"))
        })?;

    Ok(())
}

async fn siia_update_opte_undo(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    todo!()
}

// TODO
async fn siia_todo(sagactx: NexusActionContext) -> Result<(), ActionError> {
    todo!()
}

async fn siia_migration_unlock(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    todo!()
}

async fn siia_migration_unlock_undo(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    todo!()
}

// TODO: backout changes if run state changed illegally?

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::saga::create_saga_dag, app::sagas::disk_create::Params,
        app::sagas::disk_create::SagaDiskCreate, external_api::params,
    };
    use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::{authn::saga::Serialized, db::datastore::DataStore};
    use nexus_test_utils::resource_helpers::create_ip_pool;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use omicron_sled_agent::sim::SledAgent;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }
}
