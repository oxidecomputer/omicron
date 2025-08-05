// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common helper functions for instance-related sagas.

use std::net::{IpAddr, Ipv6Addr};

use crate::Nexus;
use nexus_db_lookup::LookupPath;
use nexus_db_model::{
    ByteCount, ExternalIp, InstanceState, IpAttachState, NatEntry,
    SledReservationConstraints, SledResourceVmm, VmmState,
};
use nexus_db_queries::authz;
use nexus_db_queries::{authn, context::OpContext, db, db::DataStore};
use omicron_common::api::external::{Error, NameOrId};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, PropolisUuid, SledUuid};
use serde::{Deserialize, Serialize};
use steno::ActionError;

use super::NexusActionContext;

/// The port propolis-server listens on inside the propolis zone.
const DEFAULT_PROPOLIS_PORT: u16 = 12400;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct VmmAndSledIds {
    pub(super) vmm_id: PropolisUuid,
    pub(super) sled_id: SledUuid,
}

/// Reserves resources for a new VMM whose instance has `ncpus` guest logical
/// processors and `guest_memory` bytes of guest RAM. The selected sled is
/// random within the set of sleds allowed by the supplied `constraints`.
///
/// This function succeeds idempotently if called repeatedly with the same
/// `propolis_id`.
pub async fn reserve_vmm_resources(
    nexus: &Nexus,
    instance_id: InstanceUuid,
    propolis_id: PropolisUuid,
    ncpus: u32,
    guest_memory: ByteCount,
    constraints: SledReservationConstraints,
) -> Result<SledResourceVmm, ActionError> {
    // ALLOCATION POLICY
    //
    // NOTE: This policy can - and should! - be changed.
    //
    // See https://rfd.shared.oxide.computer/rfd/0205 for a more complete
    // discussion.
    //
    // Right now, allocate an instance to any random sled agent, as long as
    // "constraints" and affinity rules are respected. This has a few problems:
    //
    // - There's no consideration for "health of the sled" here, other than
    //   "time_deleted = Null". If the sled is rebooting, in a known unhealthy
    //   state, etc, we'd currently provision it here. I don't think this is a
    //   trivial fix, but it's work we'll need to account for eventually.
    //
    // - This is selecting a random sled from all sleds in the cluster. For
    //   multi-rack, this is going to fling the sled to an arbitrary system.
    //   Maybe that's okay, but worth knowing about explicitly.
    let resources = db::model::Resources::new(
        ncpus,
        ByteCount::try_from(0i64).unwrap(),
        guest_memory,
    );

    let resource = nexus
        .reserve_on_random_sled(
            instance_id,
            propolis_id,
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
    instance_id: InstanceUuid,
    propolis_id: PropolisUuid,
    sled_id: SledUuid,
    propolis_ip: Ipv6Addr,
) -> Result<db::model::Vmm, ActionError> {
    let vmm = db::model::Vmm::new(
        propolis_id,
        instance_id,
        sled_id,
        IpAddr::V6(propolis_ip).into(),
        DEFAULT_PROPOLIS_PORT,
    );

    let vmm = datastore
        .vmm_insert(&opctx, vmm)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(vmm)
}

/// Allocates a new IPv6 address for a propolis instance that will run on the
/// supplied sled.
pub(super) async fn allocate_vmm_ipv6(
    opctx: &OpContext,
    datastore: &DataStore,
    sled_uuid: SledUuid,
) -> Result<Ipv6Addr, ActionError> {
    datastore
        .next_ipv6_address(opctx, sled_uuid)
        .await
        .map_err(ActionError::action_failed)
}

/// External IP state needed for IP attach/detachment.
///
/// This holds a record of the mid-processing external IP, where possible.
/// there are cases where this might not be known (e.g., double detach of an
/// ephemeral IP).
/// In particular we need to explicitly no-op if not `do_saga`, to prevent
/// failures borne from instance state changes from knocking out a valid IP binding.
#[derive(Debug, Deserialize, Serialize)]
pub struct ModifyStateForExternalIp {
    pub external_ip: Option<ExternalIp>,
    pub do_saga: bool,
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
    new_ip: &ModifyStateForExternalIp,
) -> Result<bool, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    if !new_ip.do_saga {
        return Ok(true);
    }
    let Some(new_ip) = new_ip.external_ip.as_ref() else {
        return Err(ActionError::action_failed(Error::internal_error(
            "tried to `do_saga` without valid external IP",
        )));
    };

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

/// Yields the sled on which an instance is found to be running so that IP
/// attachment and detachment operations can be propagated there.
///
/// # Preconditions
///
/// To synchronize correctly with other concurrent operations on an instance,
/// the calling saga must have placed the IP it is attaching or detaching into
/// the Attaching or Detaching state so that concurrent attempts to start the
/// instance will notice that the IP state is in flux and ask the caller to
/// retry.
pub(super) async fn instance_ip_get_instance_state(
    sagactx: &NexusActionContext,
    serialized_authn: &authn::saga::Serialized,
    authz_instance: &authz::Instance,
    verb: &str,
) -> Result<Option<VmmAndSledIds>, ActionError> {
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

    let found_vmm_state =
        inst_and_vmm.vmm().as_ref().map(|vmm| vmm.runtime.state);
    let found_instance_state =
        inst_and_vmm.instance().runtime_state.nexus_state;
    let mut propolis_and_sled_id =
        inst_and_vmm.vmm().as_ref().map(|vmm| VmmAndSledIds {
            vmm_id: PropolisUuid::from_untyped_uuid(vmm.id),
            sled_id: SledUuid::from_untyped_uuid(vmm.sled_id),
        });

    slog::debug!(
        osagactx.log(), "evaluating instance state for IP attach/detach";
        "instance_state" => ?found_instance_state,
        "vmm_state" => ?found_vmm_state
    );

    // Arriving here means we started in a correct state (running/stopped).
    // We need to consider how we interact with the other sagas/ops:
    // - stopping: this is not sagaized, and the propolis/sled-agent might
    //             go away. Act as though stopped if we catch it here,
    //             otherwise convert OPTE ensure to 'service unavailable'
    //             and undo.
    // - deleting: can only be called from stopped -- we won't push to dpd
    //             or sled-agent, and IP record might be deleted or forcibly
    //             detached. Catch here just in case.
    // - starting: see below.
    match (found_instance_state, found_vmm_state) {
        // If there's no VMM, the instance is definitely not on any sled.
        (InstanceState::NoVmm, _) | (_, Some(VmmState::SagaUnwound)) => {
            propolis_and_sled_id = None;
        }

        // If the instance is running normally or rebooting, it's resident on
        // the sled given by its VMM record.
        (
            InstanceState::Vmm,
            Some(VmmState::Running) | Some(VmmState::Rebooting),
        ) => {}

        // If the VMM is in the Creating, Stopping, Migrating, or Starting
        // states, its  sled assignment is in doubt, so report a transient state
        // error and ask the caller to retry.
        //
        // Although an instance with a Starting (or Creating) VMM has a sled
        // assignment, there's no way to tell at this point whether or not
        // there's a  concurrent instance-start saga that has passed the point
        // where it sends IP assignments to the instance's new sled:
        //
        // - If the start saga is still in progress and hasn't pushed any IP
        //   information to the instance's new sled yet, then either of two
        //   things can happen:
        //   - This function's caller can finish modifying IPs before the start
        //     saga propagates IP information to the sled. In this case the
        //     calling saga should do nothing--the start saga will send the
        //     right IP set to the sled.
        //   - If the start saga "wins" the race, it will see that the instance
        //     still has an attaching/detaching IP and bail out.
        //  - If the start saga is already done, and Nexus is just waiting for
        //    the VMM to report that it's Running, the calling saga needs to
        //    send the IP change to the instance's sled.
        //
        // There's no way to distinguish these cases, so if a VMM is Starting,
        // block the attach/detach.
        (
            InstanceState::Vmm,
            Some(state @ VmmState::Starting)
            | Some(state @ VmmState::Migrating)
            | Some(state @ VmmState::Stopping)
            | Some(state @ VmmState::Stopped)
            | Some(state @ VmmState::Creating),
        ) => {
            return Err(ActionError::action_failed(Error::unavail(&format!(
                "can't {verb} in transient state {state}"
            ))));
        }
        (InstanceState::Destroyed, _) => {
            return Err(ActionError::action_failed(Error::not_found_by_id(
                omicron_common::api::external::ResourceType::Instance,
                &authz_instance.id(),
            )));
        }
        (InstanceState::Creating, _) => {
            return Err(ActionError::action_failed(Error::invalid_request(
                "cannot modify instance IPs, instance is still being created",
            )));
        }
        (InstanceState::Failed, _)
        | (InstanceState::Vmm, Some(VmmState::Failed)) => {
            return Err(ActionError::action_failed(Error::invalid_request(
                "cannot modify instance IPs, instance is in unhealthy state",
            )));
        }

        // This case represents an inconsistency in the database. It should
        // never happen, but don't blow up Nexus if it somehow does.
        (InstanceState::Vmm, None) => {
            return Err(ActionError::action_failed(Error::internal_error(
                &format!(
                    "instance {} is in the 'VMM' state but has no VMM ID",
                    authz_instance.id(),
                ),
            )));
        }
        (InstanceState::Vmm, Some(VmmState::Destroyed)) => {
            return Err(ActionError::action_failed(Error::internal_error(
                &format!(
                    "instance {} points to destroyed VMM",
                    authz_instance.id(),
                ),
            )));
        }
    }

    Ok(propolis_and_sled_id)
}

/// Adds a NAT entry to DPD, routing packets bound for `target_ip` to a
/// target sled.
///
/// This call is a no-op if `sled_uuid` is `None` or the saga is explicitly
/// set to be inactive in event of double attach/detach (`!target_ip.do_saga`).
pub async fn instance_ip_add_nat(
    sagactx: &NexusActionContext,
    serialized_authn: &authn::saga::Serialized,
    authz_instance: &authz::Instance,
    sled_uuid: Option<SledUuid>,
    target_ip: ModifyStateForExternalIp,
) -> Result<Option<NatEntry>, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    // No physical sled? Don't push NAT.
    let Some(sled_uuid) = sled_uuid else {
        return Ok(None);
    };

    if !target_ip.do_saga {
        return Ok(None);
    }
    let Some(target_ip) = target_ip.external_ip else {
        return Err(ActionError::action_failed(Error::internal_error(
            "tried to `do_saga` without valid external IP",
        )));
    };

    // Querying sleds requires fleet access; use the instance allocator context
    // for this.
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, datastore)
        .sled_id(sled_uuid.into_untyped_uuid())
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_dpd_config(
            &opctx,
            InstanceUuid::from_untyped_uuid(authz_instance.id()),
            &sled.address(),
            Some(target_ip.id),
        )
        .await
        .and_then(|v| {
            v.into_iter().next().map(Some).ok_or_else(|| {
                Error::internal_error(
                    "NAT RPW failed to return concrete NAT entry",
                )
            })
        })
        .map_err(ActionError::action_failed)
}

/// Remove a single NAT entry from DPD, dropping packets bound for `target_ip`.
///
/// This call is a no-op if `sled_uuid` is `None` or the saga is explicitly
/// set to be inactive in event of double attach/detach (`!target_ip.do_saga`).
pub async fn instance_ip_remove_nat(
    sagactx: &NexusActionContext,
    serialized_authn: &authn::saga::Serialized,
    sled_uuid: Option<SledUuid>,
    target_ip: ModifyStateForExternalIp,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    // No physical sled? Don't push NAT.
    if sled_uuid.is_none() {
        return Ok(());
    };

    if !target_ip.do_saga {
        return Ok(());
    }
    let Some(target_ip) = target_ip.external_ip else {
        return Err(ActionError::action_failed(Error::internal_error(
            "tried to `do_saga` without valid external IP",
        )));
    };

    osagactx
        .nexus()
        .external_ip_delete_dpd_config(&opctx, &target_ip)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

/// Inform the OPTE port for a running instance that it should start
/// sending/receiving traffic on a given IP address.
///
/// This call is a no-op if the instance is not active (`propolis_and_sled` is
/// `None`) or the calling saga is explicitly set to be inactive in the event of
/// a double attach/detach (`!target_ip.do_saga`).
pub(super) async fn instance_ip_add_opte(
    sagactx: &NexusActionContext,
    vmm_and_sled: Option<VmmAndSledIds>,
    target_ip: ModifyStateForExternalIp,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    // No physical sled? Don't inform OPTE.
    let Some(VmmAndSledIds { vmm_id: propolis_id, sled_id }) = vmm_and_sled
    else {
        return Ok(());
    };

    if !target_ip.do_saga {
        return Ok(());
    }
    let Some(target_ip) = target_ip.external_ip else {
        return Err(ActionError::action_failed(Error::internal_error(
            "tried to `do_saga` without valid external IP",
        )));
    };

    let sled_agent_body =
        target_ip.try_into().map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .sled_client(&sled_id)
        .await
        .map_err(|_| {
            ActionError::action_failed(Error::unavail(
                "sled agent client went away mid-attach/detach",
            ))
        })?
        .vmm_put_external_ip(&propolis_id, &sled_agent_body)
        .await
        .map_err(|e| {
            ActionError::action_failed(match e {
                progenitor_client::Error::CommunicationError(_) => {
                    Error::unavail(
                        "sled agent client went away mid-attach/detach",
                    )
                }
                e => Error::internal_error(&format!("{e}")),
            })
        })?;

    Ok(())
}

/// Inform the OPTE port for a running instance that it should cease
/// sending/receiving traffic on a given IP address.
///
/// This call is a no-op if the instance is not active (`propolis_and_sled` is
/// `None`) or the calling saga is explicitly set to be inactive in the event of
/// a double attach/detach (`!target_ip.do_saga`).
pub(super) async fn instance_ip_remove_opte(
    sagactx: &NexusActionContext,
    propolis_and_sled: Option<VmmAndSledIds>,
    target_ip: ModifyStateForExternalIp,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    // No physical sled? Don't inform OPTE.
    let Some(VmmAndSledIds { vmm_id: propolis_id, sled_id }) =
        propolis_and_sled
    else {
        return Ok(());
    };

    if !target_ip.do_saga {
        return Ok(());
    }
    let Some(target_ip) = target_ip.external_ip else {
        return Err(ActionError::action_failed(Error::internal_error(
            "tried to `do_saga` without valid external IP",
        )));
    };

    let sled_agent_body =
        target_ip.try_into().map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .sled_client(&sled_id)
        .await
        .map_err(|_| {
            ActionError::action_failed(Error::unavail(
                "sled agent client went away mid-attach/detach",
            ))
        })?
        .vmm_delete_external_ip(&propolis_id, &sled_agent_body)
        .await
        .map_err(|e| {
            ActionError::action_failed(match e {
                progenitor_client::Error::CommunicationError(_) => {
                    Error::unavail(
                        "sled agent client went away mid-attach/detach",
                    )
                }
                e => Error::internal_error(&format!("{e}")),
            })
        })?;

    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ExternalIpAttach {
    Ephemeral { pool: Option<NameOrId> },
    Floating { floating_ip: authz::FloatingIp },
}
