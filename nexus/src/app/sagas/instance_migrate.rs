// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::instance_create::allocate_sled_ipv6;
use super::{impl_authenticated_saga_params, saga_generate_uuid};
use crate::authn;
use crate::context::OpContext;
use crate::db::identity::Resource;
use crate::external_api::params;
use crate::saga_interface::SagaContext;
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::ExternalIp;
use sled_agent_client::types::InstanceEnsureBody;
use sled_agent_client::types::InstanceHardware;
use sled_agent_client::types::InstanceMigrateParams;
use sled_agent_client::types::InstanceRuntimeStateMigrateParams;
use sled_agent_client::types::InstanceRuntimeStateRequested;
use sled_agent_client::types::InstanceStateRequested;
use std::net::Ipv6Addr;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaType;
use uuid::Uuid;

pub const SAGA_NAME: &'static str = "instance-migrate";
lazy_static! {
    pub static ref SAGA_TEMPLATE: Arc<SagaTemplate<SagaInstanceMigrate>> =
        Arc::new(saga_instance_migrate());
}

// "Migrate Instance" saga template
#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub instance_id: Uuid,
    pub migrate_params: params::InstanceMigrate,
}

#[derive(Debug)]
pub struct SagaInstanceMigrate;
impl SagaType for SagaInstanceMigrate {
    type SagaParamsType = Arc<Params>;
    type ExecContextType = Arc<SagaContext>;
}
impl_authenticated_saga_params!(SagaInstanceMigrate);

fn saga_instance_migrate() -> SagaTemplate<SagaInstanceMigrate> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "migrate_id",
        "GenerateMigrateId",
        new_action_noop_undo(saga_generate_uuid),
    );

    template_builder.append(
        "dst_propolis_id",
        "GeneratePropolisId",
        new_action_noop_undo(saga_generate_uuid),
    );

    template_builder.append(
        "dst_propolis_ip",
        "AllocatePropolisIp",
        new_action_noop_undo(sim_allocate_propolis_ip),
    );

    template_builder.append(
        "migrate_instance",
        "MigratePrep",
        new_action_noop_undo(sim_migrate_prep),
    );

    template_builder.append(
        "instance_migrate",
        "InstanceMigrate",
        // TODO robustness: This needs an undo action
        new_action_noop_undo(sim_instance_migrate),
    );

    template_builder.append(
        "cleanup_source",
        "CleanupSource",
        // TODO robustness: This needs an undo action. Is it even possible
        // to undo at this point?
        new_action_noop_undo(sim_cleanup_source),
    );

    template_builder.build()
}

async fn sim_migrate_prep(
    sagactx: ActionContext<SagaInstanceMigrate>,
) -> Result<(Uuid, InstanceRuntimeState), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let migrate_uuid = sagactx.lookup::<Uuid>("migrate_id")?;
    let dst_propolis_uuid = sagactx.lookup::<Uuid>("dst_propolis_id")?;

    // We have sled-agent (via Nexus) attempt to place
    // the instance in a "Migrating" state w/ the given
    // migration id. This will also update the instance
    // state in the db
    let instance = osagactx
        .nexus()
        .instance_start_migrate(
            &opctx,
            params.instance_id,
            migrate_uuid,
            dst_propolis_uuid,
        )
        .await
        .map_err(ActionError::action_failed)?;
    let instance_id = instance.id();

    Ok((instance_id, instance.runtime_state.into()))
}

// Allocate an IP address on the destination sled for the Propolis server.
async fn sim_allocate_propolis_ip(
    sagactx: ActionContext<SagaInstanceMigrate>,
) -> Result<Ipv6Addr, ActionError> {
    allocate_sled_ipv6(sagactx, "dst_sled_uuid").await
}

async fn sim_instance_migrate(
    sagactx: ActionContext<SagaInstanceMigrate>,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    let migration_id = sagactx.lookup::<Uuid>("migrate_id")?;
    let dst_sled_id = params.migrate_params.dst_sled_id;
    let dst_propolis_id = sagactx.lookup::<Uuid>("dst_propolis_id")?;
    let (instance_id, old_runtime) =
        sagactx.lookup::<(Uuid, InstanceRuntimeState)>("migrate_instance")?;

    // Allocate an IP address the destination sled for the new Propolis server.
    let propolis_addr = osagactx
        .datastore()
        .next_ipv6_address(&opctx, dst_sled_id)
        .await
        .map_err(ActionError::action_failed)?;

    let runtime = InstanceRuntimeState {
        sled_id: dst_sled_id,
        propolis_id: dst_propolis_id,
        propolis_addr: Some(std::net::SocketAddr::new(
            propolis_addr.into(),
            12400,
        )),
        ..old_runtime
    };
    let external_ip = osagactx
        .datastore()
        .instance_lookup_external_ip(&opctx, instance_id)
        .await
        .map_err(ActionError::action_failed)
        .map(ExternalIp::from)?;
    let instance_hardware = InstanceHardware {
        runtime: runtime.into(),
        // TODO: populate NICs
        nics: vec![],
        external_ip,
        // TODO: populate disks
        disks: vec![],
        // TODO: populate cloud init bytes
        cloud_init_bytes: None,
    };
    let target = InstanceRuntimeStateRequested {
        run_state: InstanceStateRequested::Migrating,
        migration_params: Some(InstanceRuntimeStateMigrateParams {
            migration_id,
            dst_propolis_id,
        }),
    };

    let src_propolis_id = old_runtime.propolis_id;
    let src_propolis_addr = old_runtime.propolis_addr.ok_or_else(|| {
        ActionError::action_failed(Error::invalid_request(
            "expected source propolis-addr",
        ))
    })?;

    let dst_sa = osagactx
        .sled_client(&dst_sled_id)
        .await
        .map_err(ActionError::action_failed)?;

    let new_runtime_state: InstanceRuntimeState = dst_sa
        .instance_put(
            &instance_id,
            &InstanceEnsureBody {
                initial: instance_hardware,
                target,
                migrate: Some(InstanceMigrateParams {
                    src_propolis_addr: src_propolis_addr.to_string(),
                    src_propolis_id,
                }),
            },
        )
        .await
        .map_err(omicron_common::api::external::Error::from)
        .map_err(ActionError::action_failed)?
        .into_inner()
        .into();

    osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime_state.into())
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sim_cleanup_source(
    _sagactx: ActionContext<SagaInstanceMigrate>,
) -> Result<(), ActionError> {
    // TODO: clean up the previous instance whether it's on the same sled or a different one
    Ok(())
}
