// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implements a saga that starts an instance.

use std::net::Ipv6Addr;

use super::{
    NexusActionContext, NexusSaga, SagaInitError,
    instance_common::allocate_vmm_ipv6,
};
use crate::app::instance::{
    InstanceEnsureRegisteredApiResources, InstanceRegisterReason,
    InstanceStateChangeError,
};
use crate::app::sagas::declare_saga_actions;
use chrono::Utc;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::{authn, authz, db};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, PropolisUuid, SledUuid};
use serde::{Deserialize, Serialize};
use slog::{error, info};
use steno::ActionError;

/// Parameters to the instance start saga.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub db_instance: db::model::Instance,

    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,

    /// Why is this instance being started?
    pub reason: Reason,
}

/// Reasons an instance may be started.
///
/// Currently, this is primarily used to determine whether the instance's
/// auto-restart timestamp must be updated. It's also included in log messages
/// in the start saga.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) enum Reason {
    /// The instance was automatically started upon being created.
    AutoStart,
    /// The instance was started by a user action.
    User,
    /// The instance has failed and is being automatically restarted by the
    /// control plane.
    AutoRestart,
}

declare_saga_actions! {
    instance_start;

    GENERATE_PROPOLIS_ID -> "propolis_id" {
        + sis_generate_propolis_id
    }

    ALLOC_SERVER -> "sled_id" {
        + sis_alloc_server
        - sis_alloc_server_undo
    }

    ALLOC_PROPOLIS_IP -> "propolis_ip" {
        + sis_alloc_propolis_ip
    }

    CREATE_VMM_RECORD -> "vmm_record" {
        + sis_create_vmm_record
        - sis_destroy_vmm_record
    }

    MARK_AS_STARTING -> "started_record" {
        + sis_move_to_starting
        - sis_move_to_starting_undo
    }

    // TODO(#3879) This can be replaced with an action that triggers the NAT RPW
    // once such an RPW is available.
    DPD_ENSURE -> "dpd_ensure" {
        + sis_dpd_ensure
        - sis_dpd_ensure_undo
    }

    V2P_ENSURE -> "v2p_ensure" {
        + sis_v2p_ensure
        - sis_v2p_ensure_undo
    }

    ENSURE_REGISTERED -> "ensure_registered" {
        + sis_ensure_registered
        - sis_ensure_registered_undo
    }

    // Only account for the instance's resource consumption when the saga is on
    // the brink of actually starting it. This allows prior steps' undo actions
    // to change the instance's generation number if warranted (e.g. by moving
    // the instance to the Failed state) without disrupting this step's undo
    // action (which depends on the instance bearing the same generation number
    // at undo time that it had at resource accounting time).
    ADD_VIRTUAL_RESOURCES -> "virtual_resources" {
        + sis_account_virtual_resources
        - sis_account_virtual_resources_undo
    }

    ENSURE_RUNNING -> "ensure_running" {
        + sis_ensure_running
    }

}

/// Node name for looking up the VMM record once it has been registered with the
/// sled-agent by `sis_ensure_registered`. This is necessary as registering the
/// VMM transitions it from the `Creating` state to the `Starting` state,
/// changing its generation.
const REGISTERED_VMM_RECORD: &'static str = "ensure_registered";

#[derive(Debug)]
pub(crate) struct SagaInstanceStart;
impl NexusSaga for SagaInstanceStart {
    const NAME: &'static str = "instance-start";
    type Params = Params;

    fn register_actions(registry: &mut super::ActionRegistry) {
        instance_start_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(generate_propolis_id_action());
        builder.append(alloc_server_action());
        builder.append(alloc_propolis_ip_action());
        builder.append(create_vmm_record_action());
        builder.append(mark_as_starting_action());
        builder.append(dpd_ensure_action());
        builder.append(v2p_ensure_action());
        builder.append(ensure_registered_action());
        builder.append(add_virtual_resources_action());
        builder.append(ensure_running_action());
        Ok(builder.build()?)
    }
}

async fn sis_generate_propolis_id(
    _sagactx: NexusActionContext,
) -> Result<PropolisUuid, ActionError> {
    Ok(PropolisUuid::new_v4())
}

async fn sis_alloc_server(
    sagactx: NexusActionContext,
) -> Result<SledUuid, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let hardware_threads = params.db_instance.ncpus.0;
    let reservoir_ram = params.db_instance.memory;
    let propolis_id = sagactx.lookup::<PropolisUuid>("propolis_id")?;

    let mut constraint_builder =
        db::model::SledReservationConstraintBuilder::new();
    if let Some(cpu_platform) = params.db_instance.cpu_platform.as_ref() {
        constraint_builder = constraint_builder
            .cpu_families(cpu_platform.compatible_sled_cpu_families());
    }

    let resource = super::instance_common::reserve_vmm_resources(
        osagactx.nexus(),
        InstanceUuid::from_untyped_uuid(params.db_instance.id()),
        propolis_id,
        u32::from(hardware_threads.0),
        reservoir_ram,
        constraint_builder.build(),
    )
    .await?;

    Ok(resource.sled_id.into())
}

async fn sis_alloc_server_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let propolis_id = sagactx.lookup::<PropolisUuid>("propolis_id")?;

    osagactx.nexus().delete_sled_reservation(propolis_id).await?;
    Ok(())
}

async fn sis_alloc_propolis_ip(
    sagactx: NexusActionContext,
) -> Result<Ipv6Addr, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let sled_uuid = sagactx.lookup::<SledUuid>("sled_id")?;
    allocate_vmm_ipv6(&opctx, sagactx.user_data().datastore(), sled_uuid).await
}

async fn sis_create_vmm_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Vmm, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let instance_id = InstanceUuid::from_untyped_uuid(params.db_instance.id());
    let propolis_id = sagactx.lookup::<PropolisUuid>("propolis_id")?;
    let sled_id = sagactx.lookup::<SledUuid>("sled_id")?;
    let propolis_ip = sagactx.lookup::<Ipv6Addr>("propolis_ip")?;

    // If the instance supplied a CPU platform, record that as the VMM's
    // required platform, irrespective of what sled was picked. (This allows a
    // VM to land on a "better" sled than its requirement and migrate back to a
    // minimum-allowed-by-requirement sled later.)
    //
    // If the instance didn't supply a CPU platform, select one for this VMM by
    // looking up the chosen sled and selecting the "minimum compatible
    // platform" for sleds of that lineage. This maximizes the number of sleds
    // that can host the VMM if it needs to migrate in the future. Selecting the
    // sled first and then deriving the platform is meant to support
    // heterogeneous deployments: if a deployment contains some sleds with CPUs
    // from vendor A, and some with CPUs from vendor B, then selecting the sled
    // first implicitly chooses a vendor, and then the "minimum compatible"
    // computation selects the most compatible platform that can run on sleds
    // with CPUs from that vendor.
    let cpu_platform =
        if let Some(cpu_platform) = params.db_instance.cpu_platform {
            cpu_platform.into()
        } else {
            let (.., sled) = osagactx
                .nexus()
                .sled_lookup(&osagactx.nexus().opctx_alloc, &sled_id)
                .map_err(ActionError::action_failed)?
                .fetch()
                .await
                .map_err(ActionError::action_failed)?;

            sled.cpu_family.minimum_compatible_platform()
        };

    super::instance_common::create_and_insert_vmm_record(
        osagactx.datastore(),
        &opctx,
        instance_id,
        propolis_id,
        sled_id,
        propolis_ip,
        cpu_platform,
    )
    .await
}

async fn sis_destroy_vmm_record(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let db_instance = params.db_instance;
    let propolis_id = sagactx.lookup::<PropolisUuid>("propolis_id")?;
    info!(
        osagactx.log(),
        "destroying vmm record for start saga unwind";
        "instance_id" => %db_instance.id(),
        "propolis_id" => %propolis_id,
        "start_reason" => ?params.reason,
    );

    osagactx.datastore().vmm_mark_saga_unwound(&opctx, &propolis_id).await?;

    // Now that the VMM record has been marked as `SagaUnwound`, the instance
    // may be permitted to reincarnate. If it is, activate the instance
    // reincarnation background task to help it along.
    let karmic_status =
        db_instance.auto_restart.can_reincarnate(db_instance.runtime());
    if karmic_status == db::model::Reincarnatability::WillReincarnate {
        info!(
            osagactx.log(),
            "start saga unwound; instance may reincarnate";
            "instance_id" => %db_instance.id(),
            "auto_restart_config" => ?db_instance.auto_restart,
            "start_reason" => ?params.reason,
        );
        osagactx
            .nexus()
            .background_tasks
            .task_instance_reincarnation
            .activate();
    } else {
        debug!(
            osagactx.log(),
            "start saga unwound; but instance will not reincarnate";
            "instance_id" => %db_instance.id(),
            "auto_restart_config" => ?db_instance.auto_restart,
            "start_reason" => ?params.reason,
            "karmic_status" => ?karmic_status,
        );
    }

    Ok(())
}

async fn sis_move_to_starting(
    sagactx: NexusActionContext,
) -> Result<db::model::Instance, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let instance_id = InstanceUuid::from_untyped_uuid(params.db_instance.id());
    let propolis_id = sagactx.lookup::<PropolisUuid>("propolis_id")?;
    info!(osagactx.log(), "moving instance to Starting state via saga";
          "instance_id" => %instance_id,
          "propolis_id" => %propolis_id,
          "start_reason" => ?params.reason);

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // For idempotency, refetch the instance to see if this step already applied
    // its desired update.
    let (_, _, authz_instance, ..) = LookupPath::new(&opctx, datastore)
        .instance_id(instance_id.into_untyped_uuid())
        .fetch_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;
    let state = datastore
        .instance_fetch_with_vmm(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    let db_instance = state.instance();

    // If `true`, we have unlinked a Propolis ID left behind by a previous
    // unwinding start saga, and we should activate the activate the abandoned
    // VMM reaper background task once we've written back the instance record.
    let mut abandoned_unwound_vmm = false;
    match state.vmm() {
        // If this saga's Propolis ID is already written to the record, then
        // this step must have completed already and is being retried, so
        // proceed.
        Some(vmm) if vmm.id == propolis_id.into_untyped_uuid() => {
            info!(osagactx.log(), "start saga: Propolis ID already set";
                  "instance_id" => %instance_id,
                  "start_reason" => ?params.reason);

            return Ok(db_instance.clone());
        }

        // If the instance has a Propolis ID, but the Propolis was left behind
        // by a previous start saga unwinding, that's fine, we can just clear it
        // out and proceed as though there was no Propolis ID here.
        Some(vmm) if vmm.runtime.state == db::model::VmmState::SagaUnwound => {
            abandoned_unwound_vmm = true;
        }

        // If the instance has a different Propolis ID, a competing start saga
        // must have started the instance already, so unwind.
        Some(_) => {
            return Err(ActionError::action_failed(Error::conflict(
                "instance changed state before it could be started",
            )));
        }

        // If the instance has no Propolis ID, try to write this saga's chosen
        // ID into the instance and put it in the Running state. (While the
        // instance is still technically starting up, writing the Propolis ID at
        // this point causes the VMM's state, which is Starting, to supersede
        // the instance's state, so this won't cause the instance to appear to
        // be running before Propolis thinks it has started.)
        None => {}
    }

    let new_runtime = {
        // If we are performing an automated restart of a Failed instance,
        // remember to update the timestamp.
        let time_last_auto_restarted = if params.reason == Reason::AutoRestart {
            Some(Utc::now())
        } else {
            db_instance.runtime().time_last_auto_restarted
        };
        db::model::InstanceRuntimeState {
            nexus_state: db::model::InstanceState::Vmm,
            propolis_id: Some(propolis_id.into_untyped_uuid()),
            r#gen: db_instance.runtime().r#gen.next().into(),
            time_last_auto_restarted,
            ..db_instance.runtime_state
        }
    };

    // Bail if another actor managed to update the instance's state in
    // the meantime.
    if !osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime)
        .await
        .map_err(ActionError::action_failed)?
    {
        return Err(ActionError::action_failed(Error::conflict(
            "instance changed state before it could be started",
        )));
    }

    // Don't fear the reaper!
    if abandoned_unwound_vmm {
        osagactx.nexus().background_tasks.task_abandoned_vmm_reaper.activate();
    }

    let mut new_record = db_instance.clone();
    new_record.runtime_state = new_runtime;
    Ok(new_record)
}

async fn sis_move_to_starting_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let db_instance =
        sagactx.lookup::<db::model::Instance>("started_record")?;
    let instance_id = InstanceUuid::from_untyped_uuid(db_instance.id());
    info!(osagactx.log(), "start saga failed; returning instance to Stopped";
          "instance_id" => %instance_id);

    let new_runtime = db::model::InstanceRuntimeState {
        nexus_state: db::model::InstanceState::NoVmm,
        propolis_id: None,
        gen: db_instance.runtime_state.gen.next().into(),
        ..db_instance.runtime_state
    };

    if !osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime)
        .await?
    {
        info!(osagactx.log(),
              "did not return instance to Stopped: old generation number";
              "instance_id" => %instance_id);
    }

    Ok(())
}

async fn sis_account_virtual_resources(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let instance_id = InstanceUuid::from_untyped_uuid(params.db_instance.id());

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .datastore()
        .virtual_provisioning_collection_insert_instance(
            &opctx,
            instance_id,
            params.db_instance.project_id,
            i64::from(params.db_instance.ncpus.0.0),
            nexus_db_model::ByteCount(*params.db_instance.memory),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sis_account_virtual_resources_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let instance_id = InstanceUuid::from_untyped_uuid(params.db_instance.id());

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            instance_id,
            params.db_instance.project_id,
            i64::from(params.db_instance.ncpus.0.0),
            nexus_db_model::ByteCount(*params.db_instance.memory),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sis_dpd_ensure(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let db_instance =
        sagactx.lookup::<db::model::Instance>("started_record")?;
    let instance_id = InstanceUuid::from_untyped_uuid(db_instance.id());

    info!(osagactx.log(), "start saga: ensuring instance dpd configuration";
          "instance_id" => %instance_id,
          "start_reason" => ?params.reason);

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let datastore = osagactx.datastore();

    // Querying sleds requires fleet access; use the instance allocator context
    // for this.
    let sled_uuid = sagactx.lookup::<SledUuid>("sled_id")?;
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, datastore)
        .sled_id(sled_uuid)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_dpd_config(&opctx, instance_id, &sled.address(), None)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sis_dpd_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    let instance_id = params.db_instance.id();
    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    info!(log, "start saga: undoing dpd configuration";
          "instance_id" => %instance_id,
          "start_reason" => ?params.reason);

    let (.., authz_instance) = LookupPath::new(&opctx, osagactx.datastore())
        .instance_id(instance_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_delete_dpd_config(&opctx, &authz_instance)
        .await?;

    Ok(())
}

async fn sis_v2p_ensure(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
    nexus.background_tasks.activate(&nexus.background_tasks.task_v2p_manager);
    Ok(())
}

async fn sis_v2p_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
    nexus.background_tasks.activate(&nexus.background_tasks.task_v2p_manager);
    Ok(())
}

async fn sis_ensure_registered(
    sagactx: NexusActionContext,
) -> Result<db::model::Vmm, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let osagactx = sagactx.user_data();
    let db_instance =
        sagactx.lookup::<db::model::Instance>("started_record")?;
    let instance_id = db_instance.id();
    let sled_id = sagactx.lookup::<SledUuid>("sled_id")?;
    let vmm_record = sagactx.lookup::<db::model::Vmm>("vmm_record")?;
    let propolis_id = sagactx.lookup::<PropolisUuid>("propolis_id")?;

    info!(osagactx.log(), "start saga: ensuring instance is registered on sled";
          "instance_id" => %instance_id,
          "sled_id" => %sled_id,
          "start_reason" => ?params.reason);

    let (authz_silo, authz_project, authz_instance) =
        LookupPath::new(&opctx, osagactx.datastore())
            .instance_id(instance_id)
            .lookup_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    let register_result = osagactx
        .nexus()
        .instance_ensure_registered(
            &opctx,
            &InstanceEnsureRegisteredApiResources {
                authz_silo,
                authz_project,
                authz_instance,
            },
            &db_instance,
            &propolis_id,
            &vmm_record,
            InstanceRegisterReason::Start { vmm_id: propolis_id },
        )
        .await;

    // Handle the result and update multicast members if successful
    let vmm_record = match register_result {
        Ok(vmm_record) => {
            // Update multicast group members with the instance's sled_id now that it's registered
            // Only do this if multicast is enabled - if disabled, no members exist to update
            if osagactx.nexus().multicast_enabled() {
                if let Err(e) = osagactx
                    .datastore()
                    .multicast_group_member_update_sled_id(
                        &opctx,
                        instance_id,
                        Some(sled_id.into()),
                    )
                    .await
                {
                    // Log but don't fail the saga - the reconciler will fix this later
                    info!(osagactx.log(),
                          "start saga: failed to update multicast member sled_id, reconciler will fix";
                          "instance_id" => %instance_id,
                          "sled_id" => %sled_id,
                          "error" => ?e);
                } else {
                    info!(osagactx.log(),
                          "start saga: updated multicast member sled_id";
                          "instance_id" => %instance_id,
                          "sled_id" => %sled_id);
                }
            }
            vmm_record
        }
        Err(err) => {
            return Err(match err {
                InstanceStateChangeError::SledAgent(inner) => {
                    info!(osagactx.log(),
                          "start saga: sled agent failed to register instance";
                          "instance_id" => %instance_id,
                          "sled_id" =>  %sled_id,
                          "error" => ?inner,
                          "start_reason" => ?params.reason);

                    // Don't set the instance to Failed in this case. Instead, allow
                    // the saga to unwind and restore the instance to the Stopped
                    // state (matching what would happen if there were a failure
                    // prior to this point).
                    ActionError::action_failed(Error::from(inner))
                }
                InstanceStateChangeError::Other(inner) => {
                    info!(osagactx.log(),
                          "start saga: internal error registering instance";
                          "instance_id" => %instance_id,
                          "error" => ?inner,
                          "start_reason" => ?params.reason);
                    ActionError::action_failed(inner)
                }
            });
        }
    };

    Ok(vmm_record)
}

async fn sis_ensure_registered_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let datastore = osagactx.datastore();
    let instance_id = InstanceUuid::from_untyped_uuid(params.db_instance.id());
    let propolis_id = sagactx.lookup::<PropolisUuid>("propolis_id")?;
    let sled_id = sagactx.lookup::<SledUuid>("sled_id")?;
    let db_vmm = sagactx.lookup::<db::model::Vmm>(REGISTERED_VMM_RECORD)?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    info!(osagactx.log(), "start saga: unregistering instance from sled";
          "instance_id" => %instance_id,
          "propolis_id" => %propolis_id,
          "sled_id" => %sled_id,
          "start_reason" => ?params.reason);

    // Fetch the latest record so that this callee can drive the instance into
    // a Failed state if the unregister call fails.
    let (.., authz_instance, _) = LookupPath::new(&opctx, datastore)
        .instance_id(instance_id.into_untyped_uuid())
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    // If the sled successfully unregistered the instance, allow the rest of
    // saga unwind to restore the instance record to its prior state (without
    // writing back the state returned from sled agent). Otherwise, try to
    // reason about the next action from the specific kind of error that was
    // returned.
    let unregister_result = osagactx
        .nexus()
        .instance_ensure_unregistered(&propolis_id, &sled_id)
        .await;

    // Handle the unregister result
    if let Err(e) = unregister_result {
        error!(osagactx.log(),
               "start saga: failed to unregister instance from sled";
               "instance_id" => %instance_id,
               "start_reason" => ?params.reason,
               "error" => ?e);

        // If the failure came from talking to sled agent, and the error code
        // indicates the instance or sled might be unhealthy, manual
        // intervention is likely to be needed, so try to mark the instance as
        // Failed and then bail on unwinding.
        //
        // If sled agent is in good shape but just doesn't know about the
        // instance, this saga still owns the instance's state, so allow
        // unwinding to continue.
        //
        // If some other Nexus error occurred, this saga is in bad shape, so
        // return an error indicating that intervention is needed without trying
        // to modify the instance further.
        //
        // TODO(#3238): `instance_unhealthy` does not take an especially nuanced
        // view of the meanings of the error codes sled agent could return, so
        // assuming that an error that isn't `instance_unhealthy` means
        // that everything is hunky-dory and it's OK to continue unwinding may
        // be a bit of a stretch. See the definition of `instance_unhealthy` for
        // more details.
        match e {
            InstanceStateChangeError::SledAgent(inner) if inner.vmm_gone() => {
                error!(osagactx.log(),
                       "start saga: failing instance after unregister failure";
                       "instance_id" => %instance_id,
                       "start_reason" => ?params.reason,
                       "error" => ?inner);

                if let Err(set_failed_error) = osagactx
                    .nexus()
                    .mark_vmm_failed(&opctx, authz_instance, &db_vmm, &inner)
                    .await
                {
                    error!(osagactx.log(),
                           "start saga: failed to mark instance as failed";
                           "instance_id" => %instance_id,
                           "start_reason" => ?params.reason,
                           "error" => ?set_failed_error);

                    Err(set_failed_error.into())
                } else {
                    Err(inner.0.into())
                }
            }
            InstanceStateChangeError::SledAgent(_) => {
                info!(osagactx.log(),
                       "start saga: instance already unregistered from sled";
                       "instance_id" => %instance_id,
                       "start_reason" => ?params.reason);

                Ok(())
            }
            InstanceStateChangeError::Other(inner) => {
                error!(osagactx.log(),
                       "start saga: internal error unregistering instance";
                       "instance_id" => %instance_id,
                       "start_reason" => ?params.reason,
                       "error" => ?inner);

                Err(inner.into())
            }
        }
    } else {
        // Only clear multicast member sled_id if multicast is enabled
        // If disabled, no members exist to clear
        if osagactx.nexus().multicast_enabled() {
            datastore
                .multicast_group_member_update_sled_id(
                    &opctx,
                    instance_id.into_untyped_uuid(),
                    None,
                )
                .await
                .map(|_| {
                    info!(osagactx.log(),
                          "start saga: cleared multicast member sled_id during undo";
                          "instance_id" => %instance_id);
                })
                .map_err(|e| {
                    // The reconciler will fix this later
                    info!(osagactx.log(),
                          "start saga: failed to clear multicast member sled_id during undo, reconciler will fix";
                          "instance_id" => %instance_id,
                          "error" => ?e);
                })
                .ok(); // Ignore the result
        }

        Ok(())
    }
}

async fn sis_ensure_running(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let db_instance =
        sagactx.lookup::<db::model::Instance>("started_record")?;
    let db_vmm = sagactx.lookup::<db::model::Vmm>(REGISTERED_VMM_RECORD)?;
    let instance_id = InstanceUuid::from_untyped_uuid(params.db_instance.id());
    let sled_id = sagactx.lookup::<SledUuid>("sled_id")?;
    info!(osagactx.log(), "start saga: ensuring instance is running";
          "instance_id" => %instance_id,
          "sled_id" => %sled_id,
          "start_reason" => ?params.reason);

    match osagactx
        .nexus()
        .instance_request_state(
            &opctx,
            &db_instance,
            &Some(db_vmm),
            crate::app::instance::InstanceStateChangeRequest::Run,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(InstanceStateChangeError::SledAgent(inner)) => {
            info!(osagactx.log(),
                  "start saga: sled agent failed to set instance to running";
                  "instance_id" => %instance_id,
                  "sled_id" =>  %sled_id,
                  "start_reason" => ?params.reason,
                  "error" => ?inner);

            // Don't set the instance to Failed in this case. Instead, allow
            // the saga to unwind and restore the instance to the Stopped
            // state (matching what would happen if there were a failure
            // prior to this point).
            Err(ActionError::action_failed(Error::from(inner)))
        }
        Err(InstanceStateChangeError::Other(inner)) => {
            info!(osagactx.log(),
                  "start saga: internal error changing instance state";
                  "instance_id" => %instance_id,
                  "start_reason" => ?params.reason,
                  "error" => ?inner);

            Err(ActionError::action_failed(inner))
        }
    }
}

#[cfg(test)]
mod test {
    use core::time::Duration;
    use std::net::SocketAddrV6;

    use crate::app::{saga::create_saga_dag, sagas::test_helpers};
    use crate::external_api::params;
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::authn;
    use nexus_test_utils::resource_helpers::{
        create_default_ip_pool, create_project, object_create,
    };
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::identity::Resource;
    use omicron_common::api::external::{
        ByteCount, DataPageParams, IdentityMetadataCreateParams,
        InstanceCpuCount, Name,
    };
    use omicron_common::api::internal::shared::SwitchLocation;
    use omicron_test_utils::dev::poll;
    use uuid::Uuid;

    use super::*;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "test-project";
    const INSTANCE_NAME: &str = "test-instance";

    async fn setup_test_project(client: &ClientTestContext) -> Uuid {
        create_default_ip_pool(&client).await;
        let project = create_project(&client, PROJECT_NAME).await;
        project.identity.id
    }

    async fn create_instance(
        client: &ClientTestContext,
    ) -> omicron_common::api::external::Instance {
        let instances_url = format!("/v1/instances?project={}", PROJECT_NAME);
        object_create(
            client,
            &instances_url,
            &params::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: INSTANCE_NAME.parse().unwrap(),
                    description: format!("instance {:?}", INSTANCE_NAME),
                },
                ncpus: InstanceCpuCount(2),
                memory: ByteCount::from_gibibytes_u32(2),
                hostname: INSTANCE_NAME.parse().unwrap(),
                user_data: b"#cloud-config".to_vec(),
                ssh_public_keys: Some(Vec::new()),
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::Default,
                external_ips: vec![],
                disks: vec![],
                boot_disk: None,
                cpu_platform: None,
                start: false,
                auto_restart_policy: Default::default(),
                anti_affinity_groups: Vec::new(),
                multicast_groups: Vec::new(),
            },
        )
        .await
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let _project_id = setup_test_project(&client).await;
        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
        let db_instance = test_helpers::instance_fetch(cptestctx, instance_id)
            .await
            .instance()
            .clone();

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            db_instance,
            reason: Reason::User,
        };

        nexus
            .sagas
            .saga_execute::<SagaInstanceStart>(params)
            .await
            .expect("Start saga should succeed");

        test_helpers::instance_simulate(cptestctx, &instance_id).await;
        let vmm_state = test_helpers::instance_fetch(cptestctx, instance_id)
            .await
            .vmm()
            .as_ref()
            .expect("running instance should have a vmm")
            .runtime
            .state;

        assert_eq!(vmm_state, nexus_db_model::VmmState::Running);
    }

    #[tokio::test]
    async fn should_start_with_dead_switch() {
        let cptestctx = nexus_test_utils::test_setup::<crate::Server>(
            "should_start_with_dead_switch",
            3,
        )
        .await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let _project_id = setup_test_project(&client).await;
        let opctx = test_helpers::test_opctx(&cptestctx);
        let instance = create_instance(&client).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
        let db_instance = test_helpers::instance_fetch(&cptestctx, instance_id)
            .await
            .instance()
            .clone();

        let datastore =
            cptestctx.server.server_context().nexus.datastore().clone();

        // Create uplinks
        // We only eagerly populate NAT entries on switches that have
        // uplinks configured, so we need to do some switch configuration
        // before starting the instance in order to test that section of logic
        let mut uplink0_params = params::SwitchPortSettingsCreate::new(
            IdentityMetadataCreateParams {
                name: "test-uplink0".parse().unwrap(),
                description: "test uplink".into(),
            },
        );

        uplink0_params.routes = vec![params::RouteConfig {
            link_name: "phy0".parse().unwrap(),
            routes: vec![params::Route {
                dst: "0.0.0.0/0".parse().unwrap(),
                gw: "1.1.1.1".parse().unwrap(),
                vid: None,
                rib_priority: None,
            }],
        }];

        let mut uplink1_params = params::SwitchPortSettingsCreate::new(
            IdentityMetadataCreateParams {
                name: "test-uplink1".parse().unwrap(),
                description: "test uplink".into(),
            },
        );

        uplink1_params.routes = vec![params::RouteConfig {
            link_name: "phy0".parse().unwrap(),
            routes: vec![params::Route {
                dst: "0.0.0.0/0".parse().unwrap(),
                gw: "2.2.2.2".parse().unwrap(),
                vid: None,
                rib_priority: None,
            }],
        }];

        let uplink0_settings = datastore
            .switch_port_settings_create(&opctx, &uplink0_params, None)
            .await
            .expect("should be able to create configuration for uplink0");

        let uplink1_settings = datastore
            .switch_port_settings_create(&opctx, &uplink1_params, None)
            .await
            .expect("should be able to create configuration for uplink1");

        let rack_id = datastore
            .rack_list(&opctx, &DataPageParams::max_page())
            .await
            .unwrap()
            .pop()
            .unwrap()
            .identity
            .id;

        let uplink0 = datastore
            .switch_port_get_id(
                &opctx,
                rack_id,
                Name::try_from("switch0".to_string()).unwrap().into(),
                Name::try_from("qsfp0".to_string()).unwrap().into(),
            )
            .await
            .expect("there should be a switch port for switch0");

        let uplink1 = datastore
            .switch_port_get_id(
                &opctx,
                rack_id,
                Name::try_from("switch1".to_string()).unwrap().into(),
                Name::try_from("qsfp0".to_string()).unwrap().into(),
            )
            .await
            .expect("there should be a switch port for switch1");

        datastore
            .switch_port_set_settings_id(
                &opctx,
                uplink0,
                Some(uplink0_settings.settings.id()),
                db::datastore::UpdatePrecondition::DontCare,
            )
            .await
            .expect("unable to update switch0 settings");

        datastore
            .switch_port_set_settings_id(
                &opctx,
                uplink1,
                Some(uplink1_settings.settings.id()),
                db::datastore::UpdatePrecondition::DontCare,
            )
            .await
            .expect("unable to update switch1 settings");

        // Shutdown one of the switch daemons
        let port = {
            let mut dendrite = cptestctx.dendrite.write().unwrap();
            let switch0_dpd = dendrite
                .get_mut(&SwitchLocation::Switch0)
                .expect("there should be at least one dendrite running");

            let port = switch0_dpd.port;

            switch0_dpd
                .cleanup()
                .await
                .expect("switch0 process should get cleaned up");

            port
        };

        let log = &opctx.log;

        let client_state = dpd_client::ClientState {
            tag: String::from("nexus"),
            log: log.new(o!(
                "component" => "DpdClient"
            )),
        };

        let addr = std::net::Ipv6Addr::LOCALHOST;

        let switch_0_dpd_client = dpd_client::Client::new(
            &format!("http://[{addr}]:{port}"),
            client_state,
        );

        // calls to switch0's dpd should fail
        assert!(switch_0_dpd_client.dpd_uptime().await.is_err());

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            db_instance,
            reason: Reason::User,
        };

        // Start an instance
        let dag = create_saga_dag::<SagaInstanceStart>(params).unwrap();
        test_helpers::actions_succeed_idempotently(nexus, dag).await;

        test_helpers::instance_simulate(&cptestctx, &instance_id).await;
        let vmm_state = test_helpers::instance_fetch(&cptestctx, instance_id)
            .await
            .vmm()
            .as_ref()
            .expect("running instance should have a vmm")
            .runtime
            .state;

        assert_eq!(vmm_state, nexus_db_model::VmmState::Running);

        let port = cptestctx
            .dendrite
            .read()
            .unwrap()
            .get(&SwitchLocation::Switch1)
            .expect("two dendrites should be present in test context")
            .port;

        let client_state = dpd_client::ClientState {
            tag: String::from("nexus"),
            log: log.new(o!(
                "component" => "DpdClient"
            )),
        };

        let addr = std::net::Ipv6Addr::LOCALHOST;

        let dpd_client = dpd_client::Client::new(
            &format!("http://[{addr}]:{port}"),
            client_state.clone(),
        );

        let log = opctx.log;

        // Check to ensure that the nat entry for the address has made it onto switch1 dendrite
        let nat_entries = dpd_client
            .nat_ipv4_list(&std::net::Ipv4Addr::new(10, 0, 0, 0), None, None)
            .await
            .unwrap()
            .items
            .clone();

        assert_eq!(nat_entries.len(), 1);

        let port = cptestctx
            .dendrite
            .read()
            .unwrap()
            .get(&SwitchLocation::Switch0)
            .expect("two dendrites should be present in test context")
            .port;

        let nexus_address = cptestctx.internal_client.bind_address;
        let mgs = cptestctx.gateway.get(&SwitchLocation::Switch0).unwrap();
        let mgs_address =
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, mgs.port, 0, 0).into();

        // Test fault recovery for nat propogation
        // Start a new dendrite instance for switch0
        let new_switch0 =
            omicron_test_utils::dev::dendrite::DendriteInstance::start(
                port,
                Some(nexus_address),
                Some(mgs_address),
            )
            .await
            .unwrap();

        cptestctx
            .dendrite
            .write()
            .unwrap()
            .insert(SwitchLocation::Switch0, new_switch0);

        // Ensure that the nat entry for the address has made it onto the new switch0 dendrite.
        // This might take some time while the new dendrite comes online.
        let poll_interval = Duration::from_secs(1);
        let poll_max = Duration::from_secs(60);

        poll::wait_for_condition(
            async || {
                let result = dpd_client
                    .nat_ipv4_list(
                        &std::net::Ipv4Addr::new(10, 0, 0, 0),
                        None,
                        None,
                    )
                    .await;

                info!(log, "nat_ipv4_list"; "result" => ?result);

                let data =
                    result.map_err(|_| poll::CondCheckError::<()>::NotYet)?;

                if data.items.is_empty() {
                    error!(
                        log,
                        "we are expecting nat entries but none were found"
                    );
                    Err(poll::CondCheckError::<()>::NotYet)
                } else {
                    Ok(())
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        cptestctx.teardown().await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let _project_id = setup_test_project(&client).await;
        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

        test_helpers::action_failure_can_unwind::<
            SagaInstanceStart,
            _,
            _,
        >(
            nexus,
            || {
                Box::pin({
                    async {
                        let db_instance = test_helpers::instance_fetch(
                            cptestctx,
                            instance_id,
                        )
                        .await.instance().clone();

                        Params {
                            serialized_authn:
                                authn::saga::Serialized::for_opctx(&opctx),
                            db_instance,
                            reason: Reason::User,
                        }
                    }
                })
            },
            || {
                Box::pin(async {
                    let new_db_state = test_helpers::instance_wait_for_state(
                        cptestctx,
                        instance_id,
                        nexus_db_model::InstanceState::NoVmm,
                    ).await;
                    let new_db_instance = new_db_state.instance();

                    info!(log,
                        "fetched instance runtime state after saga execution";
                        "instance_id" => %instance.identity.id,
                        "instance_runtime" => ?new_db_instance.runtime());

                    assert!(new_db_instance.runtime().propolis_id.is_none());

                    assert!(test_helpers::no_virtual_provisioning_resource_records_exist(cptestctx).await);
                    assert!(test_helpers::no_virtual_provisioning_collection_records_using_instances(cptestctx).await);
                })
            },
            log,
        ).await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let _project_id = setup_test_project(&client).await;
        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
        let db_instance = test_helpers::instance_fetch(cptestctx, instance_id)
            .await
            .instance()
            .clone();

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            db_instance,
            reason: Reason::User,
        };

        let dag = create_saga_dag::<SagaInstanceStart>(params).unwrap();
        test_helpers::actions_succeed_idempotently(nexus, dag).await;
        test_helpers::instance_simulate(cptestctx, &instance_id).await;
        let vmm_state = test_helpers::instance_fetch(cptestctx, instance_id)
            .await
            .vmm()
            .as_ref()
            .expect("running instance should have a vmm")
            .runtime
            .state;

        assert_eq!(vmm_state, nexus_db_model::VmmState::Running);
    }

    /// Tests that if a start saga unwinds because sled agent returned failure
    /// from a call to ensure the instance was running, then the system returns
    /// to the correct state.
    ///
    /// This is different from `test_action_failure_can_unwind` because that
    /// test causes saga nodes to "fail" without actually executing anything,
    /// whereas this test injects a failure into the normal operation of the
    /// ensure-running node.
    #[nexus_test(server = crate::Server)]
    async fn test_ensure_running_unwind(cptestctx: &ControlPlaneTestContext) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let _project_id = setup_test_project(&client).await;
        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
        let db_instance = test_helpers::instance_fetch(cptestctx, instance_id)
            .await
            .instance()
            .clone();

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            db_instance,
            reason: Reason::User,
        };

        let dag = create_saga_dag::<SagaInstanceStart>(params).unwrap();

        // The ensure_running node is last in the saga. This should be the node
        // where the failure ultimately occurs.
        let last_node_name = dag
            .get_nodes()
            .last()
            .expect("saga should have at least one node")
            .name()
            .clone();

        // Inject failure at the simulated sled agent level. This allows the
        // ensure-running node to attempt to change the instance's state, but
        // forces this operation to fail and produce whatever side effects
        // result from that failure.
        let sled_agent = cptestctx.first_sled_agent();
        sled_agent
            .set_instance_ensure_state_error(Some(Error::internal_error(
                "injected by test_ensure_running_unwind",
            )))
            .await;

        let runnable_saga = nexus.sagas.saga_prepare(dag).await.unwrap();
        let saga_result = runnable_saga
            .run_to_completion()
            .await
            .expect("saga execution should have started")
            .into_raw_result();
        let saga_error = saga_result
            .kind
            .expect_err("saga should fail due to injected error");

        assert_eq!(saga_error.error_node_name, last_node_name);

        let db_instance =
            test_helpers::instance_fetch(cptestctx, instance_id).await;

        assert_eq!(
            db_instance.instance().runtime_state.nexus_state,
            nexus_db_model::InstanceState::NoVmm
        );
        assert!(db_instance.vmm().is_none());

        assert!(
            test_helpers::no_virtual_provisioning_resource_records_exist(
                cptestctx
            )
            .await
        );
        assert!(test_helpers::no_virtual_provisioning_collection_records_using_instances(cptestctx).await);
    }
}
