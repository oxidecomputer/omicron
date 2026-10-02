// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests that VMM placement honors the blueprint's per-sled update disposition
//! as propagated through rendezvous_sled_bp_availability (RFD 666).

use super::instances::create_project_and_pool;
use super::instances::instance_simulate;
use super::instances::instance_wait_for_vmm_registration;
use anyhow::Context;
use nexus_db_model::DbSledBpAvailability;
use nexus_db_model::SledReservationConstraintBuilder;
use nexus_db_model::SledReservationConstraints;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::sled::SledReservationReason;
use nexus_db_queries::db::pub_test_utils::helpers::small_resource_request;
use nexus_test_utils::background::run_blueprint_loader;
use nexus_test_utils::background::run_blueprint_rendezvous;
use nexus_test_utils::resource_helpers::create_instance_with;
use nexus_test_utils_macros::nexus_test;
use nexus_types::deployment::BlueprintSledUpdateDispositionKind;
use nexus_types::deployment::ReconfiguratorDisruptionPolicy;
use nexus_types::external_api::instance;
use omicron_nexus::TestInterfaces as _;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "springfield-squidport";

/// Make a new target blueprint identical to the current one, except setting
/// `sled_id`'s update disposition to `kind`.
async fn set_sled_update_disposition(
    cptestctx: &ControlPlaneTestContext,
    sled_id: SledUuid,
    kind: BlueprintSledUpdateDispositionKind,
) {
    cptestctx
        .blueprint_edit_current_target(|builder| {
            builder
                .sled_set_update_disposition_kind(sled_id, kind)
                .context("setting sled update disposition")?;
            builder.comment(format!(
                "set sled {sled_id} update disposition to {kind}"
            ));
            Ok(())
        })
        .await
        .expect("editing blueprint to set sled update disposition");
}

/// Ensure the target blueprint has been loaded and rendezvous tables have been
/// updated.
async fn propagate_target_blueprint(cptestctx: &ControlPlaneTestContext) {
    // Load the blueprint before running the rendezvous task, since the latter
    // uses the cached blueprint loaded by in the former.
    run_blueprint_loader(&cptestctx.lockstep_client).await;
    run_blueprint_rendezvous(&cptestctx.lockstep_client).await;
}

async fn assert_bp_availability(
    opctx: &OpContext,
    datastore: &DataStore,
    sled_id: SledUuid,
    expected: DbSledBpAvailability,
) {
    let rows = datastore
        .rendezvous_sled_bp_availability_list_all_batched(opctx)
        .await
        .expect("listed rendezvous_sled_bp_availability");
    let row = rows.get(&sled_id).unwrap_or_else(|| {
        panic!("sled {sled_id} has a rendezvous_sled_bp_availability row")
    });
    assert_eq!(
        row.bp_availability(),
        expected,
        "sled {sled_id} blueprint availability"
    );
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_instance_placement_honors_blueprint_availability(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    let evacuating_sled_id = cptestctx.first_sled_id();
    let other_sled_id = cptestctx.second_sled_id();
    assert_ne!(evacuating_sled_id, other_sled_id);

    // Rack initialization causes both sleds to start off as available.
    for sled_id in [evacuating_sled_id, other_sled_id] {
        assert_bp_availability(
            &opctx,
            datastore,
            sled_id,
            DbSledBpAvailability::Available,
        )
        .await;
    }

    create_project_and_pool(client).await;

    // Evacuate the first sled via a new target blueprint, and update the
    // rendezvous table with that.
    set_sled_update_disposition(
        cptestctx,
        evacuating_sled_id,
        BlueprintSledUpdateDispositionKind::Evacuating {
            policy: ReconfiguratorDisruptionPolicy::Terminate,
        },
    )
    .await;
    propagate_target_blueprint(cptestctx).await;
    assert_bp_availability(
        &opctx,
        datastore,
        evacuating_sled_id,
        DbSledBpAvailability::Unavailable,
    )
    .await;
    assert_bp_availability(
        &opctx,
        datastore,
        other_sled_id,
        DbSledBpAvailability::Available,
    )
    .await;

    // Start an instance -- it must land on the other sled.
    let instance = create_instance_with(
        client,
        PROJECT_NAME,
        "bird-ecology",
        &instance::InstanceNetworkInterfaceAttachment::DefaultIpv4,
        Vec::<instance::InstanceDiskAttachment>::new(),
        Vec::<instance::ExternalIpCreate>::new(),
        true,
        Default::default(),
        None,
        Vec::new(),
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Instance create returns before the start saga has registered the VMM, so
    // wait for it before continuing.
    instance_wait_for_vmm_registration(cptestctx, &instance_id).await;
    instance_simulate(nexus, &instance_id).await;
    let sled_info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .expect("looked up active instance")
        .expect("started instance has a sled");
    assert_eq!(
        sled_info.sled_id, other_sled_id,
        "instance was placed on the evacuating sled"
    );

    // It's possible that gating failed (for whatever reason) but the instance
    // randomly landed on the available sled anyway. Create several reservations
    // to show that the evacuating sled is truly excluded.
    for _ in 0..10 {
        let resource = datastore
            .sled_reservation_create(
                &opctx,
                InstanceUuid::new_v4(),
                PropolisUuid::new_v4(),
                small_resource_request(),
                SledReservationConstraints::none(),
                SledReservationReason::Start,
            )
            .await
            .expect("unconstrained reservation succeeds");
        assert_eq!(
            resource.sled_id(),
            other_sled_id,
            "reservation was placed on the evacuating sled"
        );
        datastore
            .sled_reservation_delete(&opctx, resource.id.into())
            .await
            .expect("deleted reservation");
    }

    // While the sled is evacuating, an explicit request for it fails.
    let error = datastore
        .sled_reservation_create(
            &opctx,
            InstanceUuid::new_v4(),
            PropolisUuid::new_v4(),
            small_resource_request(),
            SledReservationConstraintBuilder::new()
                .must_select_from(&[evacuating_sled_id])
                .build(),
            SledReservationReason::Start,
        )
        .await
        .expect_err("reservation on an evacuating sled is refused");
    match error {
        omicron_common::api::external::Error::InsufficientCapacity {
            ..
        } => (),
        other => panic!("expected InsufficientCapacity, got {other:?}"),
    }

    // Mark the sled available again.
    set_sled_update_disposition(
        cptestctx,
        evacuating_sled_id,
        BlueprintSledUpdateDispositionKind::Available,
    )
    .await;
    propagate_target_blueprint(cptestctx).await;
    assert_bp_availability(
        &opctx,
        datastore,
        evacuating_sled_id,
        DbSledBpAvailability::Available,
    )
    .await;

    // And the sled is back and available for reservations.
    let resource = datastore
        .sled_reservation_create(
            &opctx,
            InstanceUuid::new_v4(),
            PropolisUuid::new_v4(),
            small_resource_request(),
            SledReservationConstraintBuilder::new()
                .must_select_from(&[evacuating_sled_id])
                .build(),
            SledReservationReason::Start,
        )
        .await
        .expect("reservation on a re-available sled succeeds");
    assert_eq!(resource.sled_id(), evacuating_sled_id);
}
