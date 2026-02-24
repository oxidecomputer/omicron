// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for wicketd updates.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use super::setup::WicketdTestContext;
use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use clap::Parser;
use gateway_messages::SpPort;
use gateway_test_utils::setup as gateway_setup;
use installinator::HOST_PHASE_2_FILE_NAME;
use maplit::btreeset;
use omicron_common::{
    disk::DiskIdentity,
    update::{
        MupdateOverrideInfo, OmicronInstallManifest,
        OmicronInstallManifestSource,
    },
};
use omicron_uuid_kinds::{InternalZpoolUuid, MupdateUuid};
use semver::Version;
use sled_agent_config_reconciler::{
    InternalDiskDetails, InternalDisksReceiver, InternalDisksWithBootDisk,
};
use sled_agent_resolvable_files::ZoneImageSourceResolver;
use sled_agent_types::resolvable_files::MupdateOverrideNonBootResult;
use sled_storage::config::MountConfig;
use tokio::sync::oneshot;
use tufaceous::{Repository, edit::RepositoryEditor};
use tufaceous_artifact::{
    InstallinatorArtifact, InstallinatorArtifactKind, KnownArtifactTags,
};
use update_engine::NestedError;
use wicket::OutputKind;
use wicket_common::{
    inventory::{SpIdentifier, SpType},
    rack_update::{ClearUpdateStateResponse, StartUpdateOptions},
    update_events::{StepEventKind, UpdateComponent},
};
use wicketd::{RunningUpdateState, StartUpdateError};
use wicketd_client::types::{
    GetInventoryParams, GetInventoryResponse, StartUpdateParams,
};

/// The list of zone file names defined in fake-non-semver.toml.
static FAKE_NON_SEMVER_ZONE_FILE_NAMES: &[&str] = &[
    "clickhouse.tar.gz",
    "clickhouse_keeper.tar.gz",
    "clickhouse_server.tar.gz",
    "cockroachdb.tar.gz",
    "crucible.tar.gz",
    "crucible_pantry.tar.gz",
    "external_dns.tar.gz",
    "internal_dns.tar.gz",
    "ntp.tar.gz",
    "nexus.tar.gz",
    "oximeter.tar.gz",
];

#[tokio::test]
async fn test_updates() {
    let gateway = gateway_setup::test_setup("test_updates", SpPort::One).await;
    let wicketd_testctx = WicketdTestContext::setup(gateway).await;
    let log = wicketd_testctx.log();

    let zip_bytes = RepositoryEditor::fake(Version::new(1, 0, 0))
        .unwrap()
        .finish()
        .await
        .unwrap()
        .generate_root()
        .sign()
        .await
        .unwrap()
        .write_zip(Vec::new(), chrono::Utc::now())
        .await
        .unwrap();
    wicketd_testctx
        .wicketd_client
        .put_repository(zip_bytes)
        .await
        .expect("bytes read and archived");

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
            for sp in &inventory
                .mgs
                .as_ref()
                .expect("Should have MGS inventory")
                .inventory
                .sps
            {
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

#[tokio::test]
async fn test_installinator_fetch() {
    let gateway = gateway_setup::test_setup(
        "test_installinator_fetch_no_installinator_document",
        SpPort::One,
    )
    .await;
    let wicketd_testctx = WicketdTestContext::setup(gateway).await;

    let zip_bytes = RepositoryEditor::fake(Version::new(1, 0, 0))
        .unwrap()
        .finish()
        .await
        .unwrap()
        .generate_root()
        .sign()
        .await
        .unwrap()
        .write_zip(Vec::new(), chrono::Utc::now())
        .await
        .unwrap();
    wicketd_testctx
        .wicketd_client
        .put_repository(zip_bytes)
        .await
        .expect("bytes read and archived");

    let repo = wicketd_testctx
        .server
        .artifact_store
        .current_repository()
        .expect("we just uploaded a repository, so there should be a plan");

    installinator_fetch_impl(&wicketd_testctx, &repo).await;

    wicketd_testctx.teardown().await;
}

async fn installinator_fetch_impl(
    wicketd_testctx: &WicketdTestContext,
    repo: &Repository,
) {
    let log = wicketd_testctx.log();
    let temp_dir = Utf8TempDir::new().expect("temp dir created");

    let mut expected_ids = BTreeSet::new();
    for artifact in repo.artifacts() {
        let Some(kind) =
            artifact.known_tags().and_then(|tags| tags.to_installinator())
        else {
            continue;
        };
        if kind == InstallinatorArtifactKind::MeasurementCorpus {
            // is this correct?
            continue;
        }
        let file_name =
            Utf8Path::new(&artifact.target_name).file_name().unwrap().into();
        let artifact =
            InstallinatorArtifact { kind, sha256: artifact.hash, file_name };
        expected_ids.insert(artifact.downgrade());
    }

    let installinator_doc_hash = repo
        .artifacts()
        .get(KnownArtifactTags::InstallinatorDocument)
        .unwrap()
        .hash;
    assert!(
        wicketd_testctx
            .server
            .artifact_store
            .contains_installinator_artifact(installinator_doc_hash),
        "installinator document ID found by hash"
    );

    let installinator_doc_hash = installinator_doc_hash.to_string();

    // Tell the installinator to download artifacts from that location.
    let peers_list = format!(
        "list:[{}]:{}",
        wicketd_testctx.artifact_addr.ip(),
        wicketd_testctx.artifact_addr.port()
    );

    // Create a new update ID and register it. This is required to ensure the
    // installinator reaches completion.
    let mupdate_id = MupdateUuid::new_v4();
    let start_receiver =
        wicketd_testctx.server.ipr_update_tracker.register(mupdate_id);

    // Process the receiver rather than dropping it, since dropping it causes
    // 410 Gone errors.
    let recv_handle = tokio::task::spawn(async move {
        let mut receiver =
            start_receiver.await.expect("start_receiver succeeded");
        while receiver.changed().await.is_ok() {
            // TODO: do something with the reports?
        }
    });

    // Simulate a couple of zpools.
    let boot_zpool = InternalZpoolUuid::new_v4();
    let non_boot_zpool = InternalZpoolUuid::new_v4();
    let a_path = temp_dir.path().join("pool/int").join(boot_zpool.to_string());
    let b_path =
        temp_dir.path().join("pool/int").join(non_boot_zpool.to_string());

    let update_id_str = mupdate_id.to_string();

    let mut args = vec![
        "installinator",
        "install",
        "--mechanism",
        peers_list.as_str(),
        "--update-id",
        update_id_str.as_str(),
        a_path.as_str(),
        b_path.as_str(),
        "--data-link0",
        "cxgbe0",
        "--data-link1",
        "cxgbe1",
    ];

    // Pass in the installinator document hash
    args.extend(["--installinator-doc", installinator_doc_hash.as_str()]);

    let args = installinator::InstallinatorApp::try_parse_from(args)
        .expect("installinator args parsed successfully");

    args.exec(&log.new(slog::o!("crate" => "installinator")))
        .await
        .expect("installinator succeeded");

    // Check that the update status is marked as closed.
    assert_eq!(
        wicketd_testctx.server.ipr_update_tracker.update_state(mupdate_id),
        Some(RunningUpdateState::Closed),
        "update should be marked as closed at the end of the run"
    );

    // Check that the host and control plane artifacts were downloaded
    // correctly.
    //
    // The control plane zone names here are defined in `fake-non-semver.toml`
    // which we load above.
    for file_name in [HOST_PHASE_2_FILE_NAME.to_owned()].into_iter().chain(
        FAKE_NON_SEMVER_ZONE_FILE_NAMES.iter().map(|z| format!("install/{z}")),
    ) {
        let a_path = a_path.join(&file_name);
        assert!(a_path.is_file(), "{a_path} was written out");

        let b_path = b_path.join(&file_name);
        assert!(b_path.is_file(), "{b_path} was written out");
    }

    // Ensure that the MUPdate override files were written correctly.
    //
    // In the mode where we specify a destination directory to write to,
    // the install dataset translates to "<dest-path>/install".
    let b_override_path =
        a_path.join("install").join(MupdateOverrideInfo::FILE_NAME);
    assert!(b_override_path.is_file(), "{b_override_path} was written out");

    // Ensure that the MUPdate override file can be parsed.
    let a_override_bytes = std::fs::read(b_override_path)
        .expect("mupdate override file successfully read");
    let a_override_info =
        serde_json::from_slice::<MupdateOverrideInfo>(&a_override_bytes)
            .expect("mupdate override file successfully deserialized");

    assert_eq!(a_override_info.hash_ids, expected_ids);

    // Ensure that the B path also had the same file written out.
    let b_override_path =
        b_path.join("install").join(MupdateOverrideInfo::FILE_NAME);
    assert!(b_override_path.is_file(), "{b_override_path} was written out");

    // Ensure that the MUPdate override file can be parsed.
    let b_override_bytes = std::fs::read(b_override_path)
        .expect("mupdate override file successfully read");
    let b_override_info =
        serde_json::from_slice::<MupdateOverrideInfo>(&b_override_bytes)
            .expect("mupdate override file successfully deserialized");

    assert_eq!(
        a_override_info, b_override_info,
        "mupdate override info matches across A and B drives",
    );

    // Ensure that the zone manifest can be parsed.
    let a_manifest_path =
        a_path.join("install").join(OmicronInstallManifest::ZONES_FILE_NAME);
    let a_manifest_bytes = std::fs::read(a_manifest_path)
        .expect("zone manifest file successfully read");
    let a_zone_manifest =
        serde_json::from_slice::<OmicronInstallManifest>(&a_manifest_bytes)
            .expect("zone manifest file successfully deserialized");

    // Check that the source was correctly specified and that the mupdate ID
    // matches.
    assert_eq!(
        a_zone_manifest.source,
        OmicronInstallManifestSource::Installinator { mupdate_id },
        "mupdate ID matches",
    );

    // Check that the images are present in the zone set.
    for file_name in FAKE_NON_SEMVER_ZONE_FILE_NAMES {
        assert!(
            a_zone_manifest.files.contains_key(file_name),
            "{file_name} is present in the zone set"
        );
    }

    // Ensure that the B path also had the same file written out.
    let b_manifest_path =
        b_path.join("install").join(OmicronInstallManifest::ZONES_FILE_NAME);
    assert!(b_manifest_path.is_file(), "{b_manifest_path} was written out");
    // Ensure that the zone manifest can be parsed.
    let b_override_bytes = std::fs::read(b_manifest_path)
        .expect("zone manifest file successfully read");
    let b_zone_manifest =
        serde_json::from_slice::<OmicronInstallManifest>(&b_override_bytes)
            .expect("zone manifest file successfully deserialized");

    assert_eq!(
        a_zone_manifest, b_zone_manifest,
        "zone manifests match across A and B drives"
    );

    // Ensure that the measurement manifest can be parsed.
    let a_manifest_path = a_path
        .join("install")
        .join("measurements")
        .join(OmicronInstallManifest::MEASUREMENT_FILE_NAME);
    let a_manifest_bytes = std::fs::read(a_manifest_path)
        .expect("measurement manifest file successfully read");
    let a_measurement_manifest =
        serde_json::from_slice::<OmicronInstallManifest>(&a_manifest_bytes)
            .expect("measurement manifest file successfully deserialized");

    // Ensure that the B path also had the same file written out.
    let b_manifest_path = b_path
        .join("install")
        .join("measurements")
        .join(OmicronInstallManifest::MEASUREMENT_FILE_NAME);
    assert!(b_manifest_path.is_file(), "{b_manifest_path} was written out");
    // Ensure that the measurement manifest can be parsed.
    let b_override_bytes = std::fs::read(b_manifest_path)
        .expect("zone manifest file successfully read");
    let b_measurement_manifest =
        serde_json::from_slice::<OmicronInstallManifest>(&b_override_bytes)
            .expect("measurement manifest file successfully deserialized");

    assert_eq!(
        a_measurement_manifest, b_measurement_manifest,
        "measurement manifests match across A and B drives"
    );

    // Run sled-agent-zone-images against these paths, and ensure that the
    // mupdate override is correctly picked up. Pick zpool1 arbitrarily as the
    // boot zpool.
    let internal_disks =
        make_internal_disks(temp_dir.path(), boot_zpool, &[non_boot_zpool]);
    let image_resolver = ZoneImageSourceResolver::new(&log, internal_disks);

    // Ensure that the resolver picks up the zone manifest and mupdate override.
    let status = image_resolver.status();
    eprintln!("status: {:#?}", status);

    // Zone manifest:
    let zone_manifest_status = status.zone_manifest;
    let result = zone_manifest_status
        .boot_disk_result
        .expect("zone manifest successful");
    assert!(result.is_valid(), "zone manifest: boot disk result is valid");
    assert_eq!(
        result.manifest, a_zone_manifest,
        "zone manifest: manifest matches a_manifest"
    );

    let non_boot_result = zone_manifest_status
        .non_boot_disk_metadata
        .get(&non_boot_zpool)
        .expect("non-boot disk result should be present");
    assert!(
        non_boot_result.result.is_valid(),
        "zone manifest: non-boot disk result is valid"
    );

    // Mupdate override:
    let override_status = status.mupdate_override;

    let info = override_status
        .boot_disk_override
        .expect("mupdate override successful")
        .expect("mupdate override present");
    assert_eq!(
        info, a_override_info,
        "mupdate override: info matches a_override_info"
    );

    let non_boot_status = override_status
        .non_boot_disk_overrides
        .get(&non_boot_zpool)
        .expect("non-boot disk status should be present");
    assert_eq!(
        non_boot_status.result,
        MupdateOverrideNonBootResult::MatchesPresent,
        "mupdate override: non-boot disk status matches present",
    );

    recv_handle.await.expect("recv_handle succeeded");
}

#[tokio::test]
async fn test_update_races() {
    let gateway = gateway_setup::test_setup(
        "test_artifact_upload_while_updating",
        SpPort::One,
    )
    .await;
    let wicketd_testctx = WicketdTestContext::setup(gateway).await;

    let zip_bytes = RepositoryEditor::fake(Version::new(1, 0, 0))
        .unwrap()
        .finish()
        .await
        .unwrap()
        .generate_root()
        .sign()
        .await
        .unwrap()
        .write_zip(Vec::new(), chrono::Utc::now())
        .await
        .unwrap();
    wicketd_testctx
        .wicketd_client
        .put_repository(zip_bytes.clone())
        .await
        .expect("bytes read and archived");

    // Now start an update.
    let sp = SpIdentifier { slot: 0, type_: SpType::Sled };
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

fn make_internal_disks(
    root: &Utf8Path,
    boot_zpool: InternalZpoolUuid,
    other_zpools: &[InternalZpoolUuid],
) -> InternalDisksWithBootDisk {
    let fake_from_zpool = |zpool: InternalZpoolUuid, is_boot_disk: bool| {
        let identity = DiskIdentity {
            vendor: "wicketd-integration-test".to_string(),
            model: "fake-disk".to_string(),
            serial: zpool.to_string(),
        };
        InternalDiskDetails::fake_details(
            identity,
            zpool,
            is_boot_disk,
            None,
            None,
        )
    };
    let mount_config = MountConfig {
        root: root.to_path_buf(),
        synthetic_disk_root: root.to_path_buf(),
    };
    InternalDisksReceiver::fake_static(
        Arc::new(mount_config),
        std::iter::once(fake_from_zpool(boot_zpool, true)).chain(
            other_zpools
                .iter()
                .copied()
                .map(|pool| fake_from_zpool(pool, false)),
        ),
    )
    .current_with_boot_disk()
}
