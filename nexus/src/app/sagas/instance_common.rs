// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common helper functions for instance-related sagas.

use std::net::{IpAddr, Ipv6Addr};

use crate::Nexus;
use chrono::Utc;
use nexus_db_model::{ByteCount, SledReservationConstraints, SledResource};
use nexus_db_queries::{context::OpContext, db, db::DataStore};
use omicron_common::api::external::InstanceState;
use steno::ActionError;
use uuid::Uuid;

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

/// Allocates a new IPv6 address for a propolis instance that will run on the
/// supplied sled.
pub(super) async fn allocate_vmm_ipv6(
    opctx: &OpContext,
    datastore: &DataStore,
    sled_uuid: Uuid,
) -> Result<Ipv6Addr, ActionError> {
    datastore
        .next_ipv6_address(opctx, sled_uuid)
        .await
        .map_err(ActionError::action_failed)
}
