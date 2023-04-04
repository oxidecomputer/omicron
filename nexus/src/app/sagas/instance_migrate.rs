// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::instance_create::allocate_sled_ipv6;
use super::{NexusActionContext, NexusSaga, ACTION_GENERATE_ID};
use crate::app::sagas::declare_saga_actions;
use crate::authn;
use crate::db::identity::Resource;
use crate::external_api::params;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv6Addr;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// instance migrate saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub instance_id: Uuid,
    pub migrate_params: params::InstanceMigrate,
}

// instance migrate saga: actions

declare_saga_actions! {
    instance_migrate;
    ALLOCATE_PROPOLIS_IP -> "dst_propolis_ip" {
        + sim_allocate_propolis_ip
    }
    MIGRATE_PREP -> "migrate_instance" {
        + sim_migrate_prep
    }
    INSTANCE_MIGRATE -> "instance_migrate" {
        // TODO robustness: This needs an undo action
        + sim_instance_migrate
    }
    CLEANUP_SOURCE -> "cleanup_source" {
        // TODO robustness: This needs an undo action. Is it even possible
        // to undo at this point?
        + sim_cleanup_source
    }
}

// instance migrate saga: definition

#[derive(Debug)]
pub struct SagaInstanceMigrate;
impl NexusSaga for SagaInstanceMigrate {
    const NAME: &'static str = "instance-migrate";
    type Params = Params;

    fn register_actions(registry: &mut super::ActionRegistry) {
        instance_migrate_register_actions(registry);
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

        builder.append(allocate_propolis_ip_action());
        builder.append(migrate_prep_action());
        builder.append(instance_migrate_action());
        builder.append(cleanup_source_action());

        Ok(builder.build()?)
    }
}

async fn sim_migrate_prep(
    sagactx: NexusActionContext,
) -> Result<(Uuid, InstanceRuntimeState), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    allocate_sled_ipv6(&opctx, sagactx, "dst_sled_uuid").await
}

async fn sim_instance_migrate(
    _sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    todo!("Migration action not yet implemented");

    /*
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

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

    // The TODO items below are tracked in
    //   https://github.com/oxidecomputer/omicron/issues/1783
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
                migrate: Some(InstanceMigrationTargetParams {
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
        */
}

async fn sim_cleanup_source(
    _sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    // TODO: clean up the previous instance whether it's on the same sled or a
    // different one
    Ok(())
}
