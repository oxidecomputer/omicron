// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common helper functions for instance-related sagas.

use std::net::{IpAddr, Ipv6Addr};

use crate::Nexus;
use chrono::Utc;
use nexus_db_model::{
    ByteCount, ExternalIp, IpAttachState, SledReservationConstraints,
    SledResource,
};
use nexus_db_queries::authz;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::queries::external_ip::SAFE_TRANSIENT_INSTANCE_STATES;
use nexus_db_queries::{authn, context::OpContext, db, db::DataStore};
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use serde::{Deserialize, Serialize};
use steno::ActionError;
use uuid::Uuid;

use super::NexusActionContext;

/// Reserves resources for a new VMM whose instance has `ncpus` guest logical
/// processors and `guest_memory` bytes of guest RAM. The selected sled is
/// random within the set of sleds allowed by the supplied `constraints`.
///
/// This function succeeds idempotently if called repeatedly with the same
/// `propolis_id`.
pub async fn reserve_vmm_resources(
    nexus: &Nexus,
    propolis_id: Uuid,
    ncpus: u32,
    guest_memory: ByteCount,
    constraints: SledReservationConstraints,
) -> Result<SledResource, ActionError> {
    // ALLOCATION POLICY
    //
    // NOTE: This policy can - and should! - be changed.
    //
    // See https://rfd.shared.oxide.computer/rfd/0205 for a more complete
    // discussion.
    //
    // Right now, allocate an instance to any random sled agent. This has a few
    // problems:
    //
    // - There's no consideration for "health of the sled" here, other than
    //   "time_deleted = Null". If the sled is rebooting, in a known unhealthy
    //   state, etc, we'd currently provision it here. I don't think this is a
    //   trivial fix, but it's work we'll need to account for eventually.
    //
    // - This is selecting a random sled from all sleds in the cluster. For
    //   multi-rack, this is going to fling the sled to an arbitrary system.
    //   Maybe that's okay, but worth knowing about explicitly.
    //
    // - This doesn't take into account anti-affinity - users will want to
    //   schedule instances that belong to a cluster on different failure
    //   domains. See https://github.com/oxidecomputer/omicron/issues/1705.
    let resources = db::model::Resources::new(
        ncpus,
        ByteCount::try_from(0i64).unwrap(),
        guest_memory,
    );

    let resource = nexus
        .reserve_on_random_sled(
            propolis_id,
            nexus_db_model::SledResourceKind::Instance,
            resources,
            constraints,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(resource)
}

/// Creates a new VMM record from the supplied IDs and stores it in the supplied
/// datastore.
///
/// This function succeeds idempotently if called repeatedly with the same
/// parameters, provided that the VMM record was not mutated by some other actor
/// after the calling saga inserted it.
pub async fn create_and_insert_vmm_record(
    datastore: &DataStore,
    opctx: &OpContext,
    instance_id: Uuid,
    propolis_id: Uuid,
    sled_id: Uuid,
    propolis_ip: Ipv6Addr,
    initial_state: nexus_db_model::VmmInitialState,
) -> Result<db::model::Vmm, ActionError> {
    let vmm = db::model::Vmm::new(
        propolis_id,
        instance_id,
        sled_id,
        IpAddr::V6(propolis_ip).into(),
        initial_state,
    );

    let vmm = datastore
        .vmm_insert(&opctx, vmm)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(vmm)
}

/// Given a previously-inserted VMM record, set its state to Destroyed and then
/// delete it.
///
/// This function succeeds idempotently if called with the same parameters,
/// provided that the VMM record was not changed by some other actor after the
/// calling saga inserted it.
pub async fn destroy_vmm_record(
    datastore: &DataStore,
    opctx: &OpContext,
    prev_record: &db::model::Vmm,
) -> Result<(), anyhow::Error> {
    let new_runtime = db::model::VmmRuntimeState {
        state: db::model::InstanceState(InstanceState::Destroyed),
        time_state_updated: Utc::now(),
        gen: prev_record.runtime.gen.next().into(),
    };

    datastore.vmm_update_runtime(&prev_record.id, &new_runtime).await?;
    datastore.vmm_mark_deleted(&opctx, &prev_record.id).await?;
    Ok(())
}

/// Allocates a new IPv6 address for a service that will run on the supplied
/// sled.
pub(super) async fn allocate_sled_ipv6(
    opctx: &OpContext,
    datastore: &DataStore,
    sled_uuid: Uuid,
) -> Result<Ipv6Addr, ActionError> {
    datastore
        .next_ipv6_address(opctx, sled_uuid)
        .await
        .map_err(ActionError::action_failed)
}

/// Instance state needed for IP attach/detachment.
#[derive(Debug, Deserialize, Serialize)]
pub struct InstanceStateForIp {
    pub sled_id: Option<Uuid>,
    pub state: InstanceState,
}

/// Move an external IP from one state to another as a saga operation,
/// returning `Ok(true)` if the record was successfully moved and `Ok(false)`
/// if the record was lost.
///
/// Returns `Err` if given an illegal state transition or several rows
/// were updated, which are programmer errors.
pub async fn instance_ip_move_state(
    sagactx: &NexusActionContext,
    serialized_authn: &authn::saga::Serialized,
    from: IpAttachState,
    to: IpAttachState,
) -> Result<bool, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    let new_ip = sagactx.lookup::<ExternalIp>("target_ip")?;

    match datastore
        .external_ip_complete_op(&opctx, new_ip.id, new_ip.kind, from, to)
        .await
        .map_err(ActionError::action_failed)?
    {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ActionError::action_failed(Error::internal_error(
            "ip state change affected > 1 row",
        ))),
    }
}

pub async fn instance_ip_get_instance_state(
    sagactx: &NexusActionContext,
    serialized_authn: &authn::saga::Serialized,
    authz_instance: &authz::Instance,
    verb: &str,
) -> Result<InstanceStateForIp, ActionError> {
    // XXX: we can get instance state (but not sled ID) in same transaction
    //      as attach (but not detach) wth current design. We need to re-query
    //      for sled ID anyhow, so keep consistent between attach/detach.
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    let inst_and_vmm = datastore
        .instance_fetch_with_vmm(&opctx, authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    let found_state = inst_and_vmm.instance().runtime_state.nexus_state.0;
    let mut sled_id = inst_and_vmm.sled_id();

    // Arriving here means we started in a correct state (running/stopped).
    // We need to consider how we interact with the other sagas/ops:
    // - starting: our claim on an IP will block it from moving past
    //             DPD_ensure and instance_start will undo. If we complete
    //             before then, it can move past and will fill in routes/opte.
    //             Act as though we have no sled_id.
    // - stopping: this is not sagaized, and the propolis/sled-agent might
    //             go away. Act as though stopped if we catch it here,
    //             otherwise convert OPTE ensure to 'service unavailable'
    //             and undo.
    // - deleting: can only be called from stopped -- we won't push to dpd
    //             or sled-agent, and IP record might be deleted or forcibly
    //             detached. Catch here just in case.
    let state = match found_state {
        InstanceState::Stopped
        | InstanceState::Starting
        | InstanceState::Stopping => {
            sled_id = None;
            InstanceState::Stopped
        }
        InstanceState::Running => InstanceState::Running,
        state if SAFE_TRANSIENT_INSTANCE_STATES.contains(&state.into()) => {
            return Err(ActionError::action_failed(Error::unavail(&format!(
                "can't {verb} in transient state {state}"
            ))))
        }
        InstanceState::Destroyed => {
            return Err(ActionError::action_failed(Error::not_found_by_id(
                omicron_common::api::external::ResourceType::Instance,
                &authz_instance.id(),
            )))
        }
        // Final cases are rebooting/failed.
        _ => {
            return Err(ActionError::action_failed(Error::invalid_request(
                "cannot modify instance IPs, instance is in unhealthy state",
            )))
        }
    };

    Ok(InstanceStateForIp { sled_id, state })
}

pub async fn instance_ip_add_nat(
    sagactx: &NexusActionContext,
    serialized_authn: &authn::saga::Serialized,
    authz_instance: &authz::Instance,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    // No physical sled? Don't push NAT.
    let Some(sled_uuid) =
        sagactx.lookup::<InstanceStateForIp>("instance_state")?.sled_id
    else {
        return Ok(());
    };

    let target_ip = sagactx.lookup::<ExternalIp>("target_ip")?;

    // Querying sleds requires fleet access; use the instance allocator context
    // for this.
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, &datastore)
        .sled_id(sled_uuid)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_dpd_config(
            &opctx,
            authz_instance.id(),
            &sled.address(),
            Some(target_ip.id),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

pub async fn instance_ip_remove_nat(
    sagactx: &NexusActionContext,
    serialized_authn: &authn::saga::Serialized,
    authz_instance: &authz::Instance,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    // No physical sled? Don't push NAT.
    let Some(_) =
        sagactx.lookup::<InstanceStateForIp>("instance_state")?.sled_id
    else {
        return Ok(());
    };

    let target_ip = sagactx.lookup::<ExternalIp>("target_ip")?;

    osagactx
        .nexus()
        .instance_delete_dpd_config(&opctx, authz_instance, Some(target_ip.id))
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

pub async fn instance_ip_add_opte(
    sagactx: &NexusActionContext,
    authz_instance: &authz::Instance,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    // No physical sled? Don't inform OPTE.
    let Some(sled_uuid) =
        sagactx.lookup::<InstanceStateForIp>("instance_state")?.sled_id
    else {
        return Ok(());
    };

    let new_ip = sagactx.lookup::<ExternalIp>("target_ip")?;
    let sled_agent_body =
        new_ip.try_into().map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .sled_client(&sled_uuid)
        .await
        .map_err(|_| {
            ActionError::action_failed(Error::unavail(
                "sled agent client went away mid-attach",
            ))
        })?
        .instance_put_external_ip(&authz_instance.id(), &sled_agent_body)
        .await
        .map_err(|e| {
            ActionError::action_failed(match e {
                progenitor_client::Error::CommunicationError(_) => {
                    Error::unavail("sled agent client went away mid-attach")
                }
                e => Error::internal_error(&format!("{e}")),
            })
        })?;

    Ok(())
}

pub async fn instance_ip_remove_opte(
    sagactx: &NexusActionContext,
    authz_instance: &authz::Instance,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    // If we didn't push OPTE before, don't undo it.
    let Some(sled_uuid) =
        sagactx.lookup::<InstanceStateForIp>("instance_state")?.sled_id
    else {
        return Ok(());
    };

    let new_ip = sagactx.lookup::<ExternalIp>("target_ip")?;
    let sled_agent_body =
        new_ip.try_into().map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .sled_client(&sled_uuid)
        .await
        .map_err(|_| {
            ActionError::action_failed(Error::unavail(
                "sled agent client went away mid-attach",
            ))
        })?
        .instance_put_external_ip(&authz_instance.id(), &sled_agent_body)
        .await
        .map_err(|e| {
            ActionError::action_failed(match e {
                progenitor_client::Error::CommunicationError(_) => {
                    Error::unavail("sled agent client went away mid-attach")
                }
                e => Error::internal_error(&format!("{e}")),
            })
        })?;

    Ok(())
}
