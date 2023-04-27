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
use installinator::{CONTROL_PLANE_FILE_NAME, HOST_PHASE_2_FILE_NAME};
use omicron_common::{
    api::internal::nexus::KnownArtifactKind,
    update::{ArtifactHashId, ArtifactKind},
};
use uuid::Uuid;
use wicketd::RunningUpdateState;
use wicketd_client::{types::UpdateComponent, StepEventKind};

#[tokio::test]
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

    args.exec(log).expect("assemble command completed successfully");

    // Read the archive and upload it to the server.
    let zip_bytes =
        fs_err::read(&archive_path).expect("archive read correctly");
    wicketd_testctx
        .wicketd_client
        .put_repository(zip_bytes)
        .await
        .expect("bytes read and archived");

    // List out the artifacts in the repository.
    let artifacts = wicketd_testctx
        .wicketd_client
        .get_artifacts()
        .await
        .expect("get_artifacts succeeded")
        .into_inner();

    // Ensure that this is a sensible result.
    let kinds = artifacts
        .artifacts
        .iter()
        .map(|artifact| {
            artifact.kind.parse::<KnownArtifactKind>().unwrap_or_else(|error| {
                panic!("unrecognized artifact kind {}: {error}", artifact.kind)
            })
        })
        .collect();
    let expected_kinds: BTreeSet<_> = KnownArtifactKind::iter().collect();
    assert_eq!(expected_kinds, kinds, "all expected kinds present");

    // Now, try starting the update on SP 0.
    wicketd_testctx
        .wicketd_client
        .post_start_update(wicketd_client::types::SpType::Sled, 0)
        .await
        .expect("update started successfully");

    let terminal_event = 'outer: loop {
        let event_report = wicketd_testctx
            .wicketd_client
            .get_update_sp(wicketd_client::types::SpType::Sled, 0)
            .await
            .expect("get_update_sp successful")
            .into_inner();

        slog::debug!(log, "received event report"; "event_report" => ?event_report);

        for event in event_report.step_events {
            if let StepEventKind::ExecutionFailed { .. } = event.data {
                break 'outer event;
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    match terminal_event.data {
        StepEventKind::ExecutionFailed { failed_step, .. } => {
            // TODO: obviously we shouldn't stop here, get past more of the
            // update process in this test.
            assert_eq!(failed_step.info.component, UpdateComponent::Sp);
        }
        other => {
            panic!("unexpected terminal event kind: {other:?}");
        }
    }

    wicketd_testctx.teardown().await;
}

#[tokio::test]
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

    args.exec(log).expect("assemble command completed successfully");

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
            .get_by_hash(&host_phase_2_id)
            .is_some(),
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
            .get_by_hash(&control_plane_id)
            .is_some(),
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
    wicketd_testctx.server.ipr_update_tracker.register(update_id).await;

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
    ])
    .expect("installinator args parsed successfully");

    args.exec(&log.new(slog::o!("crate" => "installinator")))
        .await
        .expect("installinator succeeded");

    // Check that the update status is marked as closed.
    assert_eq!(
        wicketd_testctx.server.ipr_update_tracker.update_state(update_id).await,
        Some(RunningUpdateState::Closed),
        "update should be marked as closed at the end of the run"
    );

    // Check that the host and control plane artifacts were downloaded
    // correctly.
    for file_name in [HOST_PHASE_2_FILE_NAME, CONTROL_PLANE_FILE_NAME] {
        let path = dest_path.join(file_name);
        assert!(path.is_file(), "{path} was written out");
    }

    wicketd_testctx.teardown().await;
}
