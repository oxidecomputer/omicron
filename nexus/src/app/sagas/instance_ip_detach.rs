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
use nexus_db_queries::db::identity::{Asset, Resource};
use nexus_db_queries::db::lookup::LookupPath;
use nexus_types::external_api::views;
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

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
enum ExternalIp {
    Ephemeral(IpAddr, Uuid),
    Floating(IpAddr, Uuid),
}

impl From<ExternalIp> for views::ExternalIp {
    fn from(value: ExternalIp) -> Self {
        match value {
            ExternalIp::Ephemeral(ip, _) => views::ExternalIp {
                ip,
                kind: nexus_types::external_api::shared::IpKind::Ephemeral,
            },
            ExternalIp::Floating(ip, _) => views::ExternalIp {
                ip,
                kind: nexus_types::external_api::shared::IpKind::Floating,
            },
        }
    }
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
// - Withdraw routes
//  -> ensure_dpd... (?) Do we actually need to?
//  -> must precede OPTE: host may change its sending
//     behaviour prematurely
// - Deregister addr in OPTE
//  -> Put addr in sled-agent endpoint
// - Detach and Delete EIP iff. Ephemeral
//   -> why so late? Risk that we can't recover our IP in an unwind.
// - free up migration_id of instance.
//  -> mark instance migration id as None

declare_saga_actions! {
    instance_ip_detach;
    LOCK_MIGRATION -> "sled_id" {
        + siid_migration_lock
        - siid_migration_lock_undo
    }

    RESOLVE_EXTERNAL_IP -> "new_ip_uuid" {
        + siid_resolve_ip
    }

    REMOVE_NAT -> "no_result0" {
        + siid_nat
        - siid_nat_undo
    }

    REMOVE_OPTE_PORT -> "no_result1" {
        + siid_update_opte
        - siid_update_opte_undo
    }

    DETACH_EXTERNAL_IP -> "new_ip" {
        + siid_detach_ip
        - siid_detach_ip_undo
    }

    UNLOCK_MIGRATION -> "output" {
        + siid_migration_unlock
        - siid_migration_unlock_undo
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub delete_params: params::ExternalIpDelete,
    pub authz_instance: authz::Instance,
    pub instance: db::model::Instance,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

async fn siid_migration_lock(
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

async fn siid_migration_lock_undo(
    _sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // TODO: do this iff. we implement migration lock.
    Ok(())
}

// This is split out to prevent double name lookup in event that we
// need to undo `siid_attach_ip`.
async fn siid_resolve_ip(
    sagactx: NexusActionContext,
) -> Result<Uuid, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    match params.delete_params {
        // Allocate a new IP address from the target, possibly default, pool
        params::ExternalIpDelete::Ephemeral => Ok(Uuid::new_v4()),
        // Set the parent of an existing floating IP to the new instance's ID.
        params::ExternalIpDelete::Floating { ref floating_ip_name } => {
            let floating_ip_name = db::model::Name(floating_ip_name.clone());
            let (.., authz_fip) = LookupPath::new(&opctx, &datastore)
                .project_id(params.instance.project_id)
                .floating_ip_name(&floating_ip_name)
                .lookup_for(authz::Action::Modify)
                .await
                .map_err(ActionError::action_failed)?;

            Ok(authz_fip.id())
        }
    }
}

async fn siid_nat(sagactx: NexusActionContext) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // No physical sled? Don't push NAT.
    if sagactx.lookup::<Option<Uuid>>("sled_id")?.is_none() {
        return Ok(());
    }

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;
    let ip_id = new_ip.into();

    osagactx
        .nexus()
        .instance_delete_dpd_config(&opctx, &params.authz_instance, Some(ip_id))
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn siid_nat_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // NOTE: mostly copied from instance_start.

    // If we didn't push NAT before, don't undo it.
    let Some(sled_uuid) = sagactx.lookup::<Option<Uuid>>("sled_id")? else {
        return Ok(());
    };

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;
    let ip_id = new_ip.into();

    // Querying sleds requires fleet access; use the instance allocator context
    // for this.
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, &datastore)
        .sled_id(sled_uuid)
        .fetch()
        .await?;

    osagactx
        .nexus()
        .instance_ensure_dpd_config(
            &opctx,
            params.instance.id(),
            &sled.address(),
            Some(ip_id),
        )
        .await?;

    Ok(())
}

async fn siid_update_opte(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

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
        .instance_delete_external_ip(&params.instance.id(), &new_ip.into())
        .await
        .map_err(|_| {
            ActionError::action_failed(Error::invalid_request("hmm"))
        })?;

    Ok(())
}

async fn siid_update_opte_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    // If we didn't push OPTE before, don't undo it.
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
        .await?;

    Ok(())
}

async fn siid_detach_ip(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;
    let new_ip_uuid = sagactx.lookup::<Uuid>("new_ip_uuid")?;

    match params.delete_params {
        params::ExternalIpDelete::Ephemeral => {
            datastore
                .deallocate_external_ip(&opctx, new_ip_uuid)
                .await
                .map_err(ActionError::action_failed)?;
        }
        params::ExternalIpDelete::Floating { .. } => {
            let (.., authz_fip, db_fip) = LookupPath::new(&opctx, &datastore)
                .floating_ip_id(new_ip_uuid)
                .fetch_for(authz::Action::Modify)
                .await
                .map_err(ActionError::action_failed)?;

            datastore
                .floating_ip_detach(
                    &opctx,
                    &authz_fip,
                    &db_fip,
                    Some(params.instance.id()),
                )
                .await
                .map_err(ActionError::action_failed)?;
        }
    }

    Ok(())
}

async fn siid_detach_ip_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let new_ip_uuid = sagactx.lookup::<Uuid>("new_ip_uuid")?;

    match params.delete_params {
        // Allocate a new IP address from the target, possibly default, pool
        params::ExternalIpDelete::Ephemeral => {
            // let pool_name =
            //     pool_name.as_ref().map(|name| db::model::Name(name.clone()));
            // let eip = datastore
            //     .allocate_instance_ephemeral_ip(
            //         &opctx,
            //         new_ip_uuid,
            //         params.instance.id(),
            //         pool_name,
            //     )
            //     .await
            //     .map_err(ActionError::action_failed)?;

            // Ok(ExternalIp::Ephemeral(eip.ip.ip(), new_ip_uuid))

            // TODO:::
            // need to think over... can we even reallocate the same IP?
            // We can try, and fail, and then completely unwind if so.
            // Can we even fail at this point?
            Ok(())
        }
        // Set the parent of an existing floating IP to the new instance's ID.
        params::ExternalIpDelete::Floating { .. } => {
            let (.., authz_fip, db_fip) = LookupPath::new(&opctx, &datastore)
                .floating_ip_id(new_ip_uuid)
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

            Ok(())
        }
    }
}

async fn siid_migration_unlock(
    sagactx: NexusActionContext,
) -> Result<views::ExternalIp, ActionError> {
    // TODO: do this iff. we implement migration lock.
    // TODO: Backtrack if there's an unexpected change to runstate?
    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;

    Ok(new_ip.into())
}

async fn siid_migration_unlock_undo(
    _sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // TODO: do this iff. we implement migration lock.
    Ok(())
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
        builder.append(lock_migration_action());
        builder.append(resolve_external_ip_action());
        builder.append(remove_nat_action());
        builder.append(remove_opte_port_action());
        builder.append(detach_external_ip_action());
        builder.append(unlock_migration_action());
        Ok(builder.build()?)
    }
}

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
