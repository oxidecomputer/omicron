// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common helper functions for instance-related sagas.

use std::net::{IpAddr, Ipv6Addr};

use super::NexusActionContext;
use super::sled_out_of_service_gone_check;
use crate::Nexus;
use crate::app::instance_network::InstanceNetworkFilters;
use http::StatusCode;
use nexus_db_lookup::LookupPath;
use nexus_db_model::{
    ByteCount, ExternalIp, InstanceState, IpAttachState, IpNet, NatEntry,
    SledReservationConstraints, SledResourceVmm, VmmCpuPlatform, VmmState,
};
use nexus_db_queries::authz;
use nexus_db_queries::db::datastore::ExternalSubnetBeginOpResult;
use nexus_db_queries::db::datastore::sled::SledReservationReason;
use nexus_db_queries::{authn, context::OpContext, db, db::DataStore};
use nexus_types::saga::saga_action_failed;
use omicron_common::api::external::{Error, IpVersion, NameOrId, ResourceType};
use omicron_common::backoff::BackoffError;
use omicron_common::backoff::backon_retry_policy_internal_service;
use omicron_common::backoff::retry_notify_ext;
use omicron_common::backoff::retry_policy_internal_service;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, PropolisUuid, SledUuid};
use progenitor_extras::retry::retry_operation_while_indefinitely;
use serde::{Deserialize, Serialize};
use slog_error_chain::InlineErrorChain;
use steno::ActionError;

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
    reservation_reason: SledReservationReason,
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
            reservation_reason,
        )
        .await
        .map_err(saga_action_failed)?;

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
    cpu_platform: VmmCpuPlatform,
) -> Result<db::model::Vmm, ActionError> {
    let vmm = db::model::Vmm::new(
        propolis_id,
        instance_id,
        sled_id,
        IpAddr::V6(propolis_ip).into(),
        DEFAULT_PROPOLIS_PORT,
        cpu_platform,
    );

    let vmm =
        datastore.vmm_insert(&opctx, vmm).await.map_err(saga_action_failed)?;

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
        .map_err(saga_action_failed)
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
        return Err(saga_action_failed(Error::internal_error(
            "tried to `do_saga` without valid external IP",
        )));
    };

    match datastore
        .external_ip_complete_op(&opctx, new_ip.id, new_ip.kind, from, to)
        .await
        .map_err(saga_action_failed)?
    {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(saga_action_failed(Error::internal_error(
            "ip state change affected > 1 row",
        ))),
    }
}

/// Yields the sled on which an instance is found to be running so that IP and
/// subnet attachment and detachment operations can be propagated there.
///
/// # Preconditions
///
/// To synchronize correctly with other concurrent operations on an instance,
/// the calling saga must have placed the IP or subnet it is attaching or
/// detaching into the Attaching or Detaching state so that concurrent attempts
/// to start the instance will notice that the IP state is in flux and ask the
/// caller to retry.
pub(super) async fn networking_resource_instance_state(
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
        .map_err(saga_action_failed)?;

    let found_vmm_state = inst_and_vmm.vmm().as_ref().map(|vmm| vmm.state);
    let found_instance_state = inst_and_vmm.instance().nexus_state;
    let mut propolis_and_sled_id =
        inst_and_vmm.vmm().as_ref().map(|vmm| VmmAndSledIds {
            vmm_id: PropolisUuid::from_untyped_uuid(vmm.id),
            sled_id: vmm.sled_id(),
        });

    slog::debug!(
        osagactx.log(),
        "evaluating instance state for networking attach/detach";
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
    //             or sled-agent, and IP / subnet record might be deleted
    //             or forcibly detached. Catch here just in case.
    // - starting: see below.
    match (found_instance_state, found_vmm_state) {
        // If there's no VMM, the instance is definitely not on any sled.
        (InstanceState::NoVmm, _)
        | (InstanceState::SledEvacuating, _)
        | (_, Some(VmmState::SagaUnwound)) => {
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
        // where it sends IP / subnet assignments to the instance's new sled:
        //
        // - If the start saga is still in progress and hasn't pushed any
        //   network information to the instance's new sled yet, then either of
        //   two things can happen:
        //   - This function's caller can finish modifying IPs or subnets before
        //     the start saga propagates information to the sled. In this case,
        //     the calling saga should do nothing--the start saga will send the
        //     right IP or subnet set to the sled.
        //   - If the start saga "wins" the race, it will see that the instance
        //     still has an attaching/detaching IP and bail out.
        //  - If the start saga is already done, and Nexus is just waiting for
        //    the VMM to report that it's Running, the calling saga needs to
        //    send the IP or subnet change to the instance's sled.
        //
        // There's no way to distinguish these cases, so if a VMM is Starting,
        // block the attach/detach.
        (
            InstanceState::Vmm,
            Some(state @ VmmState::Starting)
            | Some(state @ VmmState::Migrating)
            | Some(state @ VmmState::Stopping)
            | Some(state @ VmmState::Stopped)
            | Some(state @ VmmState::Creating)
            | Some(state @ VmmState::Cancelling),
        ) => {
            return Err(saga_action_failed(Error::unavail(&format!(
                "can't {verb} in transient state {state}"
            ))));
        }
        (InstanceState::Destroyed, _) => {
            return Err(saga_action_failed(Error::not_found_by_id(
                omicron_common::api::external::ResourceType::Instance,
                &authz_instance.id(),
            )));
        }
        (InstanceState::Creating, _) => {
            return Err(saga_action_failed(Error::invalid_request(
                "cannot modify instance IP or subnets, instance is \
                still being created",
            )));
        }
        (InstanceState::Failed, _)
        | (InstanceState::Vmm, Some(VmmState::Failed)) => {
            return Err(saga_action_failed(Error::invalid_request(
                "cannot modify instance IPs or subnets, instance is \
                in unhealthy state",
            )));
        }

        // This case represents an inconsistency in the database. It should
        // never happen, but don't blow up Nexus if it somehow does.
        (InstanceState::Vmm, None) => {
            return Err(saga_action_failed(Error::internal_error(&format!(
                "instance {} is in the 'VMM' state but has no VMM ID",
                authz_instance.id(),
            ))));
        }
        (InstanceState::Vmm, Some(VmmState::Destroyed)) => {
            return Err(saga_action_failed(Error::internal_error(&format!(
                "instance {} points to destroyed VMM",
                authz_instance.id(),
            ))));
        }
    }

    Ok(propolis_and_sled_id)
}

/// Send details about a new attached subnet to Dendrite.
pub(super) async fn send_subnet_attachment_to_dpd(
    sagactx: &NexusActionContext,
    serialized_authn: &authn::saga::Serialized,
    authz_instance: &authz::Instance,
    sled_uuid: Option<SledUuid>,
    subnet: ExternalSubnetBeginOpResult,
) -> Result<Option<IpNet>, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    // Check that we have a sled and actually want to proceed with the saga.
    let Some(sled_uuid) = sled_uuid else {
        return Ok(None);
    };
    let ExternalSubnetBeginOpResult { subnet, do_saga } = subnet;
    if !do_saga {
        return Ok(None);
    }

    // Querying sleds requires fleet access; use the instance allocator context
    // for this.
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, datastore)
        .sled_id(sled_uuid)
        .fetch()
        .await
        .map_err(saga_action_failed)?;

    // Build the instance target for Dendrite.
    //
    // We first need the primary NIC of the instance, since we're mapping to the
    // inner MAC and VNI.
    let primary_nic = datastore
        .derive_guest_network_interface_info(&opctx, &authz_instance)
        .await
        .map_err(saga_action_failed)?
        .into_iter()
        .find(|nic| nic.primary)
        .ok_or_else(|| {
            saga_action_failed(Error::internal_error(
                "instance does not have a primary NIC, \
                cannot yet send Dendrite attached subnet info",
            ))
        })?;
    let target = dpd_client::types::InstanceTarget {
        inner_mac: dpd_client::types::MacAddr {
            a: primary_nic.mac.into_array(),
        },
        internal_ip: sled.ip.into(),
        vni: dpd_client::types::Vni(primary_nic.vni.into()),
    };
    osagactx
        .nexus()
        .send_attached_subnet_to_dendrite(&[subnet.subnet.into()], target)
        .await
        .map(|_| Some(subnet.subnet))
        .map_err(saga_action_failed)
}

/// Delete an attached subnet from Dendrite.
pub(super) async fn delete_subnet_attachment_from_dpd(
    sagactx: &NexusActionContext,
    subnet: IpNet,
) -> Result<(), ActionError> {
    sagactx
        .user_data()
        .nexus()
        .delete_attached_subnet_from_dendrite(subnet.into())
        .await
        .map_err(saga_action_failed)
}

/// Send details about a new attached subnet to OPTE.
pub(super) async fn send_subnet_attachment_to_opte(
    sagactx: &NexusActionContext,
    vmm_and_sled: Option<VmmAndSledIds>,
    subnet: ExternalSubnetBeginOpResult,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let Some(VmmAndSledIds { vmm_id: propolis_id, sled_id }) = vmm_and_sled
    else {
        return Ok(());
    };
    let ExternalSubnetBeginOpResult { subnet, do_saga } = subnet;
    if !do_saga {
        return Ok(());
    }
    let request = sled_agent_types::attached_subnet::AttachedSubnet {
        // TODO-completeness: Expand this code to handle VPC subnets too. See
        // https://github.com/oxidecomputer/omicron/issues/9580.
        kind: sled_agent_types::attached_subnet::AttachedSubnetKind::External,
        subnet: subnet.subnet.into(),
    };
    let result = osagactx
        .nexus()
        .sled_client(&sled_id)
        .await
        .map_err(|_| {
            saga_action_failed(Error::unavail(
                "sled agent client went away mid-attach/detach",
            ))
        })?
        .vmm_post_attached_subnet(&propolis_id, &request)
        .await;
    let result = match result {
        Ok(_) => Ok(()),
        Err(progenitor_client::Error::ErrorResponse(err))
            if err.status() == StatusCode::CONFLICT =>
        {
            Ok(())
        }
        Err(progenitor_client::Error::CommunicationError(_)) => {
            Err(Error::unavail("sled agent client went away mid-attach/detach"))
        }
        Err(e) => Err(Error::internal_error(e.to_string().as_str())),
    };
    result.map_err(saga_action_failed)
}

/// Delete a single attached subnet from OPTE.
pub(super) async fn delete_subnet_attachment_from_opte(
    sagactx: &NexusActionContext,
    vmm_and_sled: Option<VmmAndSledIds>,
    subnet: ExternalSubnetBeginOpResult,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let Some(VmmAndSledIds { vmm_id: propolis_id, sled_id }) = vmm_and_sled
    else {
        return Ok(());
    };
    let ExternalSubnetBeginOpResult { subnet, do_saga } = subnet;
    if !do_saga {
        return Ok(());
    }
    osagactx
        .nexus()
        .sled_client(&sled_id)
        .await
        .map_err(|_| {
            saga_action_failed(Error::unavail(
                "sled agent client went away mid-attach/detach",
            ))
        })?
        .vmm_delete_attached_subnet(&propolis_id, &subnet.subnet.into())
        .await
        .map(|_| ())
        .map_err(|e| match e {
            progenitor_client::Error::CommunicationError(_) => {
                Error::unavail("sled agent client went away mid-attach/detach")
            }
            e => Error::internal_error(e.to_string().as_str()),
        })
        .map_err(saga_action_failed)
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
        return Err(saga_action_failed(Error::internal_error(
            "tried to `do_saga` without valid external IP",
        )));
    };

    // Querying sleds requires fleet access; use the instance allocator context
    // for this.
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, datastore)
        .sled_id(sled_uuid)
        .fetch()
        .await
        .map_err(saga_action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_dpd_config(
            &opctx,
            InstanceUuid::from_untyped_uuid(authz_instance.id()),
            &sled.address(),
            InstanceNetworkFilters::single_ip(target_ip.id),
        )
        .await
        .and_then(|v| {
            v.into_iter().next().map(Some).ok_or_else(|| {
                Error::internal_error(
                    "NAT RPW failed to return concrete NAT entry",
                )
            })
        })
        .map_err(saga_action_failed)
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
        return Err(saga_action_failed(Error::internal_error(
            "tried to `do_saga` without valid external IP",
        )));
    };

    osagactx
        .nexus()
        .external_ip_delete_dpd_config(&opctx, &target_ip)
        .await
        .map_err(saga_action_failed)?;

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
        return Err(saga_action_failed(Error::internal_error(
            "tried to `do_saga` without valid external IP",
        )));
    };

    let sled_agent_body = target_ip.try_into().map_err(saga_action_failed)?;

    osagactx
        .nexus()
        .sled_client(&sled_id)
        .await
        .map_err(|_| {
            saga_action_failed(Error::unavail(
                "sled agent client went away mid-attach/detach",
            ))
        })?
        .vmm_put_external_ip(&propolis_id, &sled_agent_body)
        .await
        .map_err(|e| {
            saga_action_failed(match e {
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
        return Err(saga_action_failed(Error::internal_error(
            "tried to `do_saga` without valid external IP",
        )));
    };

    let sled_agent_body = target_ip.try_into().map_err(saga_action_failed)?;

    osagactx
        .nexus()
        .sled_client(&sled_id)
        .await
        .map_err(|_| {
            saga_action_failed(Error::unavail(
                "sled agent client went away mid-attach/detach",
            ))
        })?
        .vmm_delete_external_ip(&propolis_id, &sled_agent_body)
        .await
        .map_err(|e| {
            saga_action_failed(match e {
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

/// Ensures that the VMM `propolis_id` is not registered with the sled-agent on
/// `sled_id`, by rudely terminating the Propolis zone if one exists.
///
/// This is intended use in undo actions that unwind sagas which may have
/// created a VMM on a sled (i.e. instance-start and instance-migrate sagas). It
/// returns `Ok(())` only once it has affirmatively determined that no Propolis
/// zone with the requested ID currently exists on the sled, either because:
///
/// - The sled-agent responds successfully to a `vmm_unregister` request. That
///   endpoint is idempotent, and it succeeds if a VMM was destroyed, *or* if
///   no such VMM currently exists on the sled. Therefore, this function is also
///   idempotent.
/// - The sled is no longer in service (or has disappeared from the database
///   entirely). If the sled has been expunged, the control plane must treat it
///   as though it no longer exists.
///
/// # Errors
///
/// This keeps trying forever until the thing is for sure gone, and retries any
/// errors that do not conclusively indicate that it has been erased from
/// reality permanently. This function comes home with its shield, or on it.
///
/// `propolis_id` delendo est.
pub(super) async fn ensure_vmm_unregistered(
    sagactx: &NexusActionContext,
    serialized_authn: &authn::saga::Serialized,
    instance_id: InstanceUuid,
    propolis_id: PropolisUuid,
    sled_id: SledUuid,
) {
    let osagactx = sagactx.user_data();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, &serialized_authn);
    let whoami = sagactx.node_label();
    let log = osagactx.log().new(o!(
        "instance_id" => instance_id.to_string(),
        "propolis_id" => propolis_id.to_string(),
        "sled_id" => sled_id.to_string(),
    ));

    // `ensure_vmm_unregistered` will not retry errors checking if the sled is
    // out of service (which may include potentially transient errors querying
    // the database), nor will it retry errors from the sled-agent that
    // progenitor-extras considers "permanent". Therefore, we wrap the entire
    // thing in a second retry loop, so that the saga doesn't get stuck.
    retry_notify_ext(
        retry_policy_internal_service(),
        || async {
            // First, we must mark the VMM as `Cancelling` so that no one else
            // (say, a stop request or the instance-watcher background task)
            // will attempt to send requests to the sled-agent which we are
            // about to instruct to disown the instance.
            //
            // This is safe to do HERE (and here ONLY) for the following
            // reasons:
            //
            // 1. This undo action will continually retry the sled-agent
            //    request to unregister the VMM indefinitely until we are
            //    certain it is FOR SURE GONE (either because the sled-agent
            //    said so or because the sled itself was erased from our
            //    understanding of the universe).
            //
            //    If we did not do that, a failed call to the sled-agent would
            //    leak the actual Propolis process, leaving it running while the
            //    database would indicate that it does not exist.
            //
            // 2. We are in a saga node that will be re-run if we die between
            //    when we mark the database record as cancelling and when we
            //    try to unregister the VMM. If we did this outside of a saga
            //    context, Nexus could crash between when we put the VMM record
            //    in the "no longer exists" state and when we actually made it
            //    no longer exist, but here, the saga node will be executed
            //    elsewhere if we die between those two steps.
            //
            // 3. Finally, and due to the consequences of (2) above, because
            //    `vmm_mark_saga_unwinding` succeeds idempotently if the VMM
            //    record is already on a state in which it is not considered to
            //    exist on a sled. This could occur either because a previous
            //    execution of this node had already done so, or if the VMM
            //    record was marked as `Cancelling` by an instance stop request
            //    which is what caused this saga to unwind in the first place.
            //
            match osagactx
                .datastore()
                .vmm_mark_saga_unwinding(&opctx, &propolis_id)
                .await
            {
                Ok(true) => {
                    debug!(log, "{whoami}: marked VMM as `Cancelling`");
                }
                // Either we already marked it as nonexistent in a past life, or
                // we never had to in the first place. Carry on!
                Ok(false) => {}
                // If the VMM record has been hard-deleted, then the query that
                // marks it as `Cancelling` is totally fine: we no longer need
                // to ensure it is in a non-existent state, since no other actor
                // that might otherwise try to send requests to the sled-agent
                // about it will ever see any evidence of its existence anyway!
                // We can now safely proceed to telling the sled-agent to disown
                // it.
                Err(Error::ObjectNotFound { .. }) => {}
                // Any other error must result in us retrying the query until
                // the database record no longer indicates that the VMM exists
                // on the sled. We must bail out here and retry before
                // attempting to unregister the VMM from the sled, since the VMM
                // record's state must be changed before we can do that.
                Err(e) => return Err(BackoffError::transient(e)),
            };

            let client = match osagactx.nexus().sled_client(&sled_id).await {
                Ok(client) => client,
                // If the sled no longer exists in the database at all, that's...quite
                // surprising! But, we can hopefully assume that the sled, and any VMMs
                // running on it, no longer exist.
                Err(Error::ObjectNotFound {
                    type_name: ResourceType::Sled,
                    ..
                }) => {
                    slog::warn!(
                        log,
                        "{whoami}: sled no longer exists in the database; \
                         assuming VMM is gone";
                    );
                    return Ok(());
                }
                Err(e) => {
                    return Err(BackoffError::transient(e.internal_context(
                        "failed to look up sled in order to unregister VMM",
                    )));
                }
            };

            // Now, we can safely instruct the sled-agent to unregister the VMM.
            // This will be retried indefinitely until the sled-agent
            // acknowledges the unregistration or is expunged.
            let unregister_result = retry_operation_while_indefinitely(
                backon_retry_policy_internal_service(),
                || client.vmm_unregister(&propolis_id),
                || {
                    sled_out_of_service_gone_check(
                        osagactx.datastore(),
                        &opctx,
                        sled_id,
                    )
                },
                |notification| {
                    slog::warn!(
                        log,
                        "{whoami}: failed to unregister VMM, retrying in {:?}",
                        notification.delay;
                        "attempt" => notification.attempt,
                        InlineErrorChain::new(&notification.error),
                    );
                },
            )
            .await;

            match unregister_result {
                Ok(rsp) => {
                    // The sled-agent only returns an updated runtime state if
                    // there was previously a running Propolis zone to destroy.
                    // Otherwise, the VMM was either already unregistered or was
                    // never there to begin with.
                    if rsp.into_inner().updated_runtime.is_some() {
                        slog::info!(
                            log,
                            "{whoami}: unregistered VMM from sled; continuing \
                             to unwind...";
                        );
                    } else {
                        slog::debug!(
                            log,
                            "{whoami}: VMM was not registered with sled; \
                             continuing to unwind...";
                        );
                    }
                    Ok(())
                }
                // The sled has either been expunged or completely removed from the
                // database. The Propolis zone we were trying to destroy is definitely
                // gone.
                Err(e) if e.is_gone() => {
                    slog::info!(
                        log,
                        "{whoami}: the sled hosting the VMM to unregister is \
                         no longer in service; assuming the VMM is gone";
                    );
                    Ok(())
                }
                // Welp. That's not good...
                Err(e) => {
                    let error = InlineErrorChain::new(&e);
                    slog::error!(
                        log,
                        "{whoami}: failed to unregister VMM from sled";
                        &error
                    );
                    Err(BackoffError::transient(Error::InternalError {
                        internal_message: format!(
                            "failed to unregister VMM from sled: {error}"
                        ),
                    }))
                }
            }
        },
        |error, call_count, total_duration| {
            warn!(
                log,
                "{whoami}: failed to unregister VMM; retrying";
                "call_count" => call_count,
                "total_duration" => ?total_duration,
                InlineErrorChain::new(&error),
            );
        },
    )
    .await
    .expect("this never returns an error");
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ExternalIpAttach {
    Ephemeral { pool: Option<NameOrId>, ip_version: Option<IpVersion> },
    Floating { floating_ip: authz::FloatingIp, ip_version: IpVersion },
}
