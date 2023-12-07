// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for wicketd updates.

use std::{collections::BTreeSet, time::Duration};

use super::setup::WicketdTestContext;
use camino_tempfile::Utf8TempDir;
use clap::Parser;
use gateway_messages::SpPort;
use gateway_test_utils::setup as gateway_setup;
use installinator::HOST_PHASE_2_FILE_NAME;
use maplit::btreeset;
use omicron_common::{
    api::internal::nexus::KnownArtifactKind,
    update::{ArtifactHashId, ArtifactKind},
};
use tokio::sync::oneshot;
use update_engine::NestedError;
use uuid::Uuid;
use wicket::OutputKind;
use wicket_common::{
    rack_update::{ClearUpdateStateResponse, SpIdentifier, SpType},
    update_events::{StepEventKind, UpdateComponent},
};
use wicketd::{RunningUpdateState, StartUpdateError};
use wicketd_client::types::{
    GetInventoryParams, GetInventoryResponse, StartUpdateOptions,
    StartUpdateParams,
};

// See documentation for extract_nested_artifact_pair in update_plan.rs for why
// multi_thread is required.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_updates() {
    let gateway = gateway_setup::test_setup("test_updates", SpPort::One).await;
    let wicketd_testctx = WicketdTestContext::setup(gateway).await;
    let log = wicketd_testctx.log();

    let temp_dir = Utf8TempDir::new().expect("temp dir created");
    let archive_path = temp_dir.path().join("archive.zip");

    let args = tufaceous::Args::try_parse_from([
        "tufaceous",
        "assemble",
        "../tufaceous/manifests/fake.toml",
        archive_path.as_str(),
    ])
    .expect("args parsed correctly");

    args.exec(log).await.expect("assemble command completed successfully");

    // Read the archive and upload it to the server.
    let zip_bytes =
        fs_err::read(&archive_path).expect("archive read correctly");
    wicketd_testctx
        .wicketd_client
        .put_repository(zip_bytes)
        .await
        .expect("bytes read and archived");

    // List out the artifacts in the repository.
    let response = wicketd_testctx
        .wicketd_client
        .get_artifacts_and_event_reports()
        .await
        .expect("get_artifacts_and_event_reports succeeded")
        .into_inner();

    // We should have an artifact for every known artifact kind...
    let expected_kinds: BTreeSet<_> =
        KnownArtifactKind::iter().map(ArtifactKind::from).collect();

    // ... and installable artifacts that replace the top level host,
    // trampoline, and RoT with their inner parts (phase1/phase2 for OS images
    // and A/B images for the RoT) during import.
    let mut expected_installable_kinds = expected_kinds.clone();
    for remove in [
        KnownArtifactKind::Host,
        KnownArtifactKind::Trampoline,
        KnownArtifactKind::GimletRot,
        KnownArtifactKind::PscRot,
        KnownArtifactKind::SwitchRot,
    ] {
        assert!(expected_installable_kinds.remove(&remove.into()));
    }
    for add in [
        ArtifactKind::HOST_PHASE_1,
        ArtifactKind::HOST_PHASE_2,
        ArtifactKind::TRAMPOLINE_PHASE_1,
        ArtifactKind::TRAMPOLINE_PHASE_2,
        ArtifactKind::GIMLET_ROT_IMAGE_A,
        ArtifactKind::GIMLET_ROT_IMAGE_B,
        ArtifactKind::PSC_ROT_IMAGE_A,
        ArtifactKind::PSC_ROT_IMAGE_B,
        ArtifactKind::SWITCH_ROT_IMAGE_A,
        ArtifactKind::SWITCH_ROT_IMAGE_B,
    ] {
        assert!(expected_installable_kinds.insert(add));
    }

    // Ensure that this is a sensible result.
    let mut kinds = BTreeSet::new();
    let mut installable_kinds = BTreeSet::new();
    for artifact in response.artifacts {
        kinds.insert(artifact.artifact_id.kind.parse().unwrap());
        for installable in artifact.installable {
            installable_kinds.insert(installable.kind.parse().unwrap());
        }
    }
    assert_eq!(expected_kinds, kinds, "all expected kinds present");
    assert_eq!(
        expected_installable_kinds, installable_kinds,
        "all expected installable kinds present"
    );

    let target_sp = SpIdentifier { type_: SpType::Sled, slot: 0 };

    // Ensure wicketd knows our target_sp (which is simulated) is online and
    // available to update.
    let resp = wicketd_testctx
        .wicketd_client
        .get_inventory(&GetInventoryParams { force_refresh: vec![target_sp] })
        .await
        .expect("failed to get inventory");
    match resp.into_inner() {
        GetInventoryResponse::Response { inventory, .. } => {
            let mut found = false;
            for sp in &inventory.sps {
                if sp.id == target_sp {
                    assert!(sp.state.is_some(), "no state for target SP");
                    found = true;
                    break;
                }
            }
            assert!(
                found,
                "did not find SP {target_sp:?} in inventory {inventory:?}"
            );
        }
        GetInventoryResponse::Unavailable => {
            panic!("wicketd inventory is unavailable")
        }
    }

    // Now, try starting the update on SP 0.
    let options = StartUpdateOptions::default();
    let params = StartUpdateParams { targets: vec![target_sp], options };
    wicketd_testctx
        .wicketd_client
        .post_start_update(&params)
        .await
        .expect("update started successfully");

    let terminal_event = 'outer: loop {
        let event_report = wicketd_testctx
            .wicketd_client
            .get_update_sp(&target_sp.type_, target_sp.slot)
            .await
            .expect("get_update_sp successful")
            .into_inner();

        slog::debug!(log, "received event report"; "event_report" => ?event_report);

        for event in event_report.step_events {
            if let StepEventKind::ExecutionFailed { .. } = event.kind {
                break 'outer event;
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    match terminal_event.kind {
        StepEventKind::ExecutionFailed { failed_step, .. } => {
            // TODO: obviously we shouldn't stop here, get past more of the
            // update process in this test.
            assert_eq!(failed_step.info.component, UpdateComponent::Host);
        }
        other => {
            panic!("unexpected terminal event kind: {other:?}");
        }
    }

    // Try starting the update again -- this should fail because we require that
    // update state is cleared before starting a new one.
    {
        let error = wicketd_testctx
            .wicketd_client
            .post_start_update(&params)
            .await
            .expect_err(
                "post_start_update should fail \
                 since update data is already present",
            );
        let error_str = error.to_string();
        assert!(
            // Errors lose type information across the OpenAPI boundary, so
            // sadly we have to match on the error string.
            error_str.contains("existing update data found"),
            "unexpected error: {error_str}"
        );
    }

    // Try clearing the update via the wicket CLI.
    {
        let args = vec![
            "rack-update",
            "clear",
            "--sled",
            "0,1",
            "--message-format",
            "json",
        ];
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let output = OutputKind::Captured {
            log: wicketd_testctx.log().clone(),
            stdout: &mut stdout,
            stderr: &mut stderr,
        };

        wicket::exec_with_args(wicketd_testctx.wicketd_addr, args, output)
            .await
            .expect("wicket rack-update clear failed");

        // stdout should contain a JSON object.
        let response: Result<ClearUpdateStateResponse, NestedError> =
            serde_json::from_slice(&stdout).expect("stdout is valid JSON");
        assert_eq!(
            response.expect("expected Ok response"),
            ClearUpdateStateResponse {
                cleared: btreeset![SpIdentifier {
                    type_: SpType::Sled,
                    slot: 0
                }],
                no_update_data: btreeset![SpIdentifier {
                    type_: SpType::Sled,
                    slot: 1
                }],
            }
        );
    }

    // Check to see that the update state for SP 0 was cleared.
    let event_report = wicketd_testctx
        .wicketd_client
        .get_update_sp(&target_sp.type_, target_sp.slot)
        .await
        .expect("get_update_sp successful")
        .into_inner();
    assert!(
        event_report.step_events.is_empty(),
        "update state should be cleared (instead got {:?}",
        event_report
    );

    wicketd_testctx.teardown().await;
}

// See documentation for extract_nested_artifact_pair in update_plan.rs for why
// multi_thread is required.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_installinator_fetch() {
    let gateway = gateway_setup::test_setup("test_updates", SpPort::One).await;
    let wicketd_testctx = WicketdTestContext::setup(gateway).await;
    let log = wicketd_testctx.log();

    let temp_dir = Utf8TempDir::new().expect("temp dir created");
    let archive_path = temp_dir.path().join("archive.zip");

    let args = tufaceous::Args::try_parse_from([
        "tufaceous",
        "assemble",
        "../tufaceous/manifests/fake.toml",
        archive_path.as_str(),
    ])
    .expect("args parsed correctly");

    args.exec(log).await.expect("assemble command completed successfully");

    // Read the archive and upload it to the server.
    let zip_bytes =
        fs_err::read(&archive_path).expect("archive read correctly");
    wicketd_testctx
        .wicketd_client
        .put_repository(zip_bytes)
        .await
        .expect("bytes read and archived");

    let update_plan = wicketd_testctx
        .server
        .artifact_store
        .current_plan()
        .expect("we just uploaded a repository, so there should be a plan");
    let host_phase_2_hash = update_plan.host_phase_2_hash.to_string();
    let control_plane_hash = update_plan.control_plane_hash.to_string();

    // Are the artifacts available when looked up by hash?
    let host_phase_2_id = ArtifactHashId {
        kind: ArtifactKind::HOST_PHASE_2,
        hash: update_plan.host_phase_2_hash,
    };
    assert!(
        wicketd_testctx
            .server
            .artifact_store
            .contains_by_hash(&host_phase_2_id),
        "host phase 2 ID found by hash"
    );

    let control_plane_id = ArtifactHashId {
        kind: KnownArtifactKind::ControlPlane.into(),
        hash: update_plan.control_plane_hash,
    };
    assert!(
        wicketd_testctx
            .server
            .artifact_store
            .contains_by_hash(&control_plane_id),
        "control plane ID found by hash"
    );

    // Tell the installinator to download artifacts from that location.
    let peers_list = format!(
        "list:[{}]:{}",
        wicketd_testctx.artifact_addr.ip(),
        wicketd_testctx.artifact_addr.port()
    );

    // Create a new update ID and register it. This is required to ensure the
    // installinator reaches completion.
    let update_id = Uuid::new_v4();
    let start_receiver =
        wicketd_testctx.server.ipr_update_tracker.register(update_id);

    // Process the receiver rather than dropping it, since dropping it causes
    // 410 Gone errors.
    let recv_handle = tokio::task::spawn(async move {
        let mut receiver =
            start_receiver.await.expect("start_receiver succeeded");
        while receiver.changed().await.is_ok() {
            // TODO: do something with the reports?
        }
    });

    let update_id_str = update_id.to_string();
    let dest_path = temp_dir.path().join("installinator-out");
    let args = installinator::InstallinatorApp::try_parse_from([
        "installinator",
        "install",
        "--mechanism",
        peers_list.as_str(),
        "--update-id",
        update_id_str.as_str(),
        "--host-phase-2",
        host_phase_2_hash.as_str(),
        "--control-plane",
        control_plane_hash.as_str(),
        dest_path.as_str(),
        "--data-link0",
        "cxgbe0",
        "--data-link1",
        "cxgbe1",
    ])
    .expect("installinator args parsed successfully");

    args.exec(&log.new(slog::o!("crate" => "installinator")))
        .await
        .expect("installinator succeeded");

    // Check that the update status is marked as closed.
    assert_eq!(
        wicketd_testctx.server.ipr_update_tracker.update_state(update_id),
        Some(RunningUpdateState::Closed),
        "update should be marked as closed at the end of the run"
    );

    // Check that the host and control plane artifacts were downloaded
    // correctly.
    //
    // The control plane zone names here are defined in `fake.toml` which we
    // load above.
    for file_name in
        [HOST_PHASE_2_FILE_NAME, "zones/zone1.tar.gz", "zones/zone2.tar.gz"]
    {
        let path = dest_path.join(file_name);
        assert!(path.is_file(), "{path} was written out");
    }

    recv_handle.await.expect("recv_handle succeeded");

    wicketd_testctx.teardown().await;
}

// See documentation for extract_nested_artifact_pair in update_plan.rs for why
// multi_thread is required.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_races() {
    let gateway = gateway_setup::test_setup(
        "test_artifact_upload_while_updating",
        SpPort::One,
    )
    .await;
    let wicketd_testctx = WicketdTestContext::setup(gateway).await;
    let log = wicketd_testctx.log();

    let temp_dir = Utf8TempDir::new().expect("temp dir created");
    let archive_path = temp_dir.path().join("archive.zip");

    let args = tufaceous::Args::try_parse_from([
        "tufaceous",
        "assemble",
        "../tufaceous/manifests/fake.toml",
        archive_path.as_str(),
    ])
    .expect("args parsed correctly");

    args.exec(log).await.expect("assemble command completed successfully");

    // Read the archive and upload it to the server.
    let zip_bytes =
        fs_err::read(&archive_path).expect("archive read correctly");
    wicketd_testctx
        .wicketd_client
        .put_repository(zip_bytes.clone())
        .await
        .expect("bytes read and archived");

    // Now start an update.
    let sp = gateway_client::types::SpIdentifier {
        slot: 0,
        type_: gateway_client::types::SpType::Sled,
    };
    let sps: BTreeSet<_> = vec![sp].into_iter().collect();

    let (sender, receiver) = oneshot::channel();
    wicketd_testctx
        .server
        .update_tracker
        .start_fake_update(sps.clone(), receiver)
        .await
        .expect("start_fake_update successful");

    // An update is now running. Try uploading the repository again -- this time
    // it should fail.
    wicketd_testctx
        .wicketd_client
        .put_repository(zip_bytes.clone())
        .await
        .expect_err("failed because update is currently running");

    // Also try starting another fake update, which should fail -- we don't let updates be started
    // if there's current update state.
    {
        let (_, receiver) = oneshot::channel();
        let err = wicketd_testctx
            .server
            .update_tracker
            .start_fake_update(sps, receiver)
            .await
            .expect_err("start_fake_update failed while update is running");
        assert_eq!(err.len(), 1, "one error returned: {err:?}");
        assert_eq!(
            err.first().unwrap(),
            &StartUpdateError::ExistingUpdates(vec![sp])
        );
    }

    // Unblock the update, letting it run to completion.
    let (final_sender, final_receiver) = oneshot::channel();
    sender.send(final_sender).expect("receiver kept open by update engine");
    final_receiver.await.expect("update engine completed successfully");

    let event_buffer = wicketd_testctx
        .wicketd_client
        .get_update_sp(&SpType::Sled, 0)
        .await
        .expect("received event buffer successfully");
    let last_event =
        event_buffer.step_events.last().expect("at least one event");
    assert!(
        matches!(last_event.kind, StepEventKind::ExecutionCompleted { .. }),
        "last event is execution completed: {last_event:#?}"
    );

    // Try uploading the repository again -- since no updates are running, this
    // should succeed.
    wicketd_testctx
        .wicketd_client
        .put_repository(zip_bytes)
        .await
        .expect("no updates currently running");

    // Now that a new repository is uploaded, the event buffer should be wiped
    // clean.
    let event_buffer = wicketd_testctx
        .wicketd_client
        .get_update_sp(&SpType::Sled, 0)
        .await
        .expect("received event buffer successfully");
    assert!(
        event_buffer.step_events.is_empty(),
        "event buffer is empty: {event_buffer:#?}"
    );

    wicketd_testctx.teardown().await;
}
