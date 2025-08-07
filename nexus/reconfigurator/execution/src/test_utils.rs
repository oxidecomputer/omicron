// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test utilities for reconfigurator execution.

use std::net::Ipv6Addr;

use internal_dns_resolver::Resolver;
use nexus_db_queries::{context::OpContext, db::DataStore};
use nexus_types::{
    deployment::{
        Blueprint, PendingMgsUpdates,
        execution::{EventBuffer, Overridables},
    },
    quiesce::SagaQuiesceHandle,
};
use omicron_uuid_kinds::OmicronZoneUuid;
use update_engine::TerminalKind;

use crate::{RealizeBlueprintOutput, RequiredRealizeArgs};
use tokio::sync::watch;

pub(crate) async fn realize_blueprint_and_expect(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &Resolver,
    blueprint: &Blueprint,
    overrides: &Overridables,
) -> (RealizeBlueprintOutput, EventBuffer) {
    // Large enough to handle incoming messages without stalling.
    let (sender, mut receiver) = tokio::sync::mpsc::channel(128);
    let receiver_task = tokio::spawn(async move {
        let mut buffer = EventBuffer::default();
        while let Some(msg) = receiver.recv().await {
            buffer.add_event(msg);
        }
        buffer
    });

    // This helper function does not support MGS-managed updates.
    let (mgs_updates, _rx) = watch::channel(PendingMgsUpdates::new());
    // This helper function does not mess with quiescing.
    let saga_quiesce = SagaQuiesceHandle::new(opctx.log.clone());
    let nexus_id = OmicronZoneUuid::new_v4();
    let output = crate::realize_blueprint(
        RequiredRealizeArgs {
            opctx,
            datastore,
            resolver,
            creator: nexus_id,
            blueprint,
            sender,
            mgs_updates,
            saga_quiesce,
        }
        .with_overrides(overrides)
        .as_nexus(OmicronZoneUuid::new_v4()),
    )
    .await
    // We expect here rather than in the caller because we want to assert that
    // the result is a `RealizeBlueprintOutput`. Because the latter is
    // `must_use`, the caller may assign it to `_` and miss the `expect` call.
    .expect("failed to execute blueprint");

    let buffer = receiver_task.await.expect("failed to receive events");
    eprintln!("realize_blueprint output: {:#?}", output);

    let status = buffer
        .root_execution_summary()
        .expect("buffer has a root execution")
        .execution_status;
    let terminal_info = status.terminal_info().unwrap_or_else(|| {
        panic!("expected status to be terminal: {:#?}", status)
    });

    assert_eq!(
        terminal_info.kind,
        TerminalKind::Completed,
        "expected completed"
    );

    (output, buffer)
}

/// Generates a set of overrides describing the simulated test environment.
pub fn overridables_for_test(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
) -> Overridables {
    use omicron_common::api::external::SwitchLocation;

    let mut overrides = Overridables::default();
    let scrimlets = [
        (nexus_test_utils::SLED_AGENT_UUID, SwitchLocation::Switch0),
        (nexus_test_utils::SLED_AGENT2_UUID, SwitchLocation::Switch1),
    ];
    for (id_str, switch_location) in scrimlets {
        let sled_id = id_str.parse().unwrap();
        let ip = Ipv6Addr::LOCALHOST;
        let mgs_port = cptestctx
            .gateway
            .get(&switch_location)
            .unwrap()
            .client
            .bind_address
            .port();
        let dendrite_port =
            cptestctx.dendrite.get(&switch_location).unwrap().port;
        let mgd_port = cptestctx.mgd.get(&switch_location).unwrap().port;
        overrides.override_switch_zone_ip(sled_id, ip);
        overrides.override_dendrite_port(sled_id, dendrite_port);
        overrides.override_mgs_port(sled_id, mgs_port);
        overrides.override_mgd_port(sled_id, mgd_port);
    }
    overrides
}
