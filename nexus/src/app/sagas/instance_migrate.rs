// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::instance_create::allocate_sled_ipv6;
use super::{NexusActionContext, NexusSaga, ACTION_GENERATE_ID};
use crate::app::sagas::NexusAction;
use crate::authn;
use crate::context::OpContext;
use crate::db::identity::Resource;
use crate::db::model::IpKind;
use crate::external_api::params;
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::InstanceEnsureBody;
use sled_agent_client::types::InstanceHardware;
use sled_agent_client::types::InstanceMigrateParams;
use sled_agent_client::types::InstanceRuntimeStateMigrateParams;
use sled_agent_client::types::InstanceRuntimeStateRequested;
use sled_agent_client::types::InstanceStateRequested;
use sled_agent_client::types::SourceNatConfig;
use std::net::Ipv6Addr;
use std::sync::Arc;
use steno::ActionError;
use steno::{new_action_noop_undo, Node};
use uuid::Uuid;

// instance migrate saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub instance_id: Uuid,
    pub migrate_params: params::InstanceMigrate,
}

// instance migrate saga: actions

lazy_static! {
    static ref ALLOCATE_PROPOLIS_IP: NexusAction = new_action_noop_undo(
        "instance-migrate.allocate-propolis-ip",
        sim_allocate_propolis_ip
    );
    static ref MIGRATE_PREP: NexusAction = new_action_noop_undo(
        "instance-migrate.migrate-prep",
        sim_migrate_prep,
    );
    static ref INSTANCE_MIGRATE: NexusAction = new_action_noop_undo(
        "instance-migrate.instance-migrate",
        // TODO robustness: This needs an undo action
        sim_instance_migrate,
    );
    static ref CLEANUP_SOURCE: NexusAction = new_action_noop_undo(
        "instance-migrate.cleanup-source",
        // TODO robustness: This needs an undo action. Is it even possible
        // to undo at this point?
        sim_cleanup_source,
    );
}

// instance migrate saga: definition

#[derive(Debug)]
pub struct SagaInstanceMigrate;
impl NexusSaga for SagaInstanceMigrate {
    const NAME: &'static str = "instance-migrate";
    type Params = Params;

    fn register_actions(registry: &mut super::ActionRegistry) {
        registry.register(Arc::clone(&*ALLOCATE_PROPOLIS_IP));
        registry.register(Arc::clone(&*MIGRATE_PREP));
        registry.register(Arc::clone(&*INSTANCE_MIGRATE));
        registry.register(Arc::clone(&*CLEANUP_SOURCE));
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(Node::action(
            "migrate_id",
            "GenerateMigrateId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(Node::action(
            "dst_propolis_id",
            "GeneratePropolisId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(Node::action(
            "dst_propolis_ip",
            "AllocatePropolisIp",
            ALLOCATE_PROPOLIS_IP.as_ref(),
        ));

        builder.append(Node::action(
            "migrate_instance",
            "MigratePrep",
            MIGRATE_PREP.as_ref(),
        ));

        builder.append(Node::action(
            "instance_migrate",
            "InstanceMigrate",
            INSTANCE_MIGRATE.as_ref(),
        ));

        builder.append(Node::action(
            "cleanup_source",
            "CleanupSource",
            CLEANUP_SOURCE.as_ref(),
        ));

        Ok(builder.build()?)
    }
}

async fn sim_migrate_prep(
    sagactx: NexusActionContext,
) -> Result<(Uuid, InstanceRuntimeState), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
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
    sagactx: NexusActionContext,
) -> Result<Ipv6Addr, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    allocate_sled_ipv6(&opctx, sagactx, "dst_sled_uuid").await
}

async fn sim_instance_migrate(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
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

    // Collect the external IPs for the instance.
    //  https://github.com/oxidecomputer/omicron/issues/1467
    // TODO-correctness: Handle Floating IPs, see
    //  https://github.com/oxidecomputer/omicron/issues/1334
    let (snat_ip, external_ips): (Vec<_>, Vec<_>) = osagactx
        .datastore()
        .instance_lookup_external_ips(&opctx, instance_id)
        .await
        .map_err(ActionError::action_failed)?
        .into_iter()
        .partition(|ip| ip.kind == IpKind::SNat);

    // Sanity checks on the number and kind of each IP address.
    if external_ips.len() > crate::app::MAX_EXTERNAL_IPS_PER_INSTANCE {
        return Err(ActionError::action_failed(Error::internal_error(
            format!(
                "Expected the number of external IPs to be limited to \
                {}, but found {}",
                crate::app::MAX_EXTERNAL_IPS_PER_INSTANCE,
                external_ips.len(),
            )
            .as_str(),
        )));
    }
    let external_ips =
        external_ips.into_iter().map(|model| model.ip.ip()).collect();
    if snat_ip.len() != 1 {
        return Err(ActionError::action_failed(Error::internal_error(
            "Expected exactly one SNAT IP address for an instance",
        )));
    }
    let source_nat = SourceNatConfig::from(snat_ip.into_iter().next().unwrap());

    let instance_hardware = InstanceHardware {
        runtime: runtime.into(),
        // TODO: populate NICs
        nics: vec![],
        source_nat,
        external_ips,
        // TODO: populate firewall rules
        firewall_rules: vec![],
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
    _sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    // TODO: clean up the previous instance whether it's on the same sled or a
    // different one
    Ok(())
}
