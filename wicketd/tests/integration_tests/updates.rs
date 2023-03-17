// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for wicketd updates.

use std::collections::BTreeSet;

use super::setup::WicketdTestContext;
use camino::Utf8Path;
use clap::Parser;
use gateway_messages::SpPort;
use gateway_test_utils::setup as gateway_setup;
use omicron_common::api::internal::nexus::KnownArtifactKind;
use tempfile::TempDir;
use wicketd_client::types::{UpdateEventKind, UpdateTerminalEventKind};

#[tokio::test]
async fn test_updates() {
    let gateway = gateway_setup::test_setup("test_updates", SpPort::One).await;
    let wicketd_testctx = WicketdTestContext::setup(gateway).await;
    let log = wicketd_testctx.log();

    let temp_dir = TempDir::new().expect("temp dir created");
    let path: &Utf8Path =
        temp_dir.path().try_into().expect("temp dir is valid UTF-8");
    let archive_path = path.join("archive.zip");

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
        let status = wicketd_testctx
            .wicketd_client
            .get_update_sp(wicketd_client::types::SpType::Sled, 0)
            .await
            .expect("get_update_sp successful")
            .into_inner();

        for event in status.events {
            if let UpdateEventKind::Terminal(event) = event.kind {
                break 'outer event;
            }
        }
    };

    match terminal_event {
        UpdateTerminalEventKind::ArtifactUpdateFailed { artifact, .. } => {
            // TODO: obviously we shouldn't stop here, get past more of the
            // update process in this test.
            assert_eq!(artifact.kind, "gimlet_sp");
        }
        other => {
            panic!("unexpected terminal event kind: {other:?}");
        }
    }

    // TODO: This doesn't work yet since sp-sim can't understand update
    // commands.

    wicketd_testctx.teardown().await;
}
