//! Nexus integration tests
//!
//! See the driver in the parent directory for how and why this is structured
//! the way it is.

use gateway_client::types::SpType;
use gateway_messages::{SpPort, UpdateInProgressStatus, UpdateStatus};
use gateway_test_utils::setup as mgs_setup;
use hubtools::RawHubrisArchive;
use hubtools::{CabooseBuilder, HubrisArchiveBuilder};
use omicron_nexus::app::test_interfaces::{SpUpdater, UpdateProgress};
use sp_sim::SimulatedSp;
use sp_sim::SIM_GIMLET_BOARD;
use sp_sim::SIM_SIDECAR_BOARD;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use uuid::Uuid;

fn make_fake_sp_image(board: &str) -> Vec<u8> {
    let caboose = CabooseBuilder::default()
        .git_commit("fake-git-commit")
        .board(board)
        .version("0.0.0")
        .name("fake-name")
        .build();

    let mut builder = HubrisArchiveBuilder::with_fake_image();
    builder.write_caboose(caboose.as_slice()).unwrap();
    builder.build_to_vec().unwrap()
}

#[tokio::test]
async fn test_sp_updater_updates_sled() {
    // Start MGS + Sim SP.
    let (mgs_config, sp_sim_config) = mgs_setup::load_test_config();
    let mgs_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
    let mgstestctx = mgs_setup::test_setup_with_config(
        "test_sp_updater_updates_sled",
        SpPort::One,
        mgs_config,
        &sp_sim_config,
        Some(mgs_addr),
    )
    .await;

    let mgs_listen_addr = mgstestctx
        .server
        .dropshot_server_for_address(mgs_addr)
        .expect("missing dropshot server for localhost address")
        .local_addr();
    let mgs_client = Arc::new(gateway_client::Client::new(
        &format!("http://{mgs_listen_addr}"),
        mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient")),
    ));

    let sp_type = SpType::Sled;
    let sp_slot = 0;
    let update_id = Uuid::new_v4();
    let hubris_archive = make_fake_sp_image(SIM_GIMLET_BOARD);

    let sp_updater = SpUpdater::new(
        sp_type,
        sp_slot,
        update_id,
        hubris_archive.clone(),
        &mgstestctx.logctx.log,
    );

    sp_updater.update([mgs_client]).await.expect("update failed");

    let last_update_image = mgstestctx.simrack.gimlets[sp_slot as usize]
        .last_update_data()
        .await
        .expect("simulated SP did not receive an update");

    let hubris_archive = RawHubrisArchive::from_vec(hubris_archive).unwrap();

    assert_eq!(
        hubris_archive.image.data.as_slice(),
        &*last_update_image,
        "simulated SP update contents (len {}) \
         do not match test generated fake image (len {})",
        last_update_image.len(),
        hubris_archive.image.data.len()
    );
}

#[tokio::test]
async fn test_sp_updater_updates_switch() {
    // Start MGS + Sim SP.
    let (mgs_config, sp_sim_config) = mgs_setup::load_test_config();
    let mgs_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
    let mgstestctx = mgs_setup::test_setup_with_config(
        "test_sp_updater_updates_switch",
        SpPort::One,
        mgs_config,
        &sp_sim_config,
        Some(mgs_addr),
    )
    .await;

    let mgs_listen_addr = mgstestctx
        .server
        .dropshot_server_for_address(mgs_addr)
        .expect("missing dropshot server for localhost address")
        .local_addr();
    let mgs_client = Arc::new(gateway_client::Client::new(
        &format!("http://{mgs_listen_addr}"),
        mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient")),
    ));

    let sp_type = SpType::Switch;
    let sp_slot = 0;
    let update_id = Uuid::new_v4();
    let hubris_archive = make_fake_sp_image(SIM_SIDECAR_BOARD);

    let sp_updater = SpUpdater::new(
        sp_type,
        sp_slot,
        update_id,
        hubris_archive.clone(),
        &mgstestctx.logctx.log,
    );

    sp_updater.update([mgs_client]).await.expect("update failed");

    let last_update_image = mgstestctx.simrack.sidecars[sp_slot as usize]
        .last_update_data()
        .await
        .expect("simulated SP did not receive an update");

    let hubris_archive = RawHubrisArchive::from_vec(hubris_archive).unwrap();

    assert_eq!(
        hubris_archive.image.data.as_slice(),
        &*last_update_image,
        "simulated SP update contents (len {}) \
         do not match test generated fake image (len {})",
        last_update_image.len(),
        hubris_archive.image.data.len()
    );
}

#[tokio::test]
async fn test_sp_updater_remembers_successful_mgs_instance() {
    // Start MGS + Sim SP.
    let (mgs_config, sp_sim_config) = mgs_setup::load_test_config();
    let mgs_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
    let mgstestctx = mgs_setup::test_setup_with_config(
        "test_sp_updater_updates_switch",
        SpPort::One,
        mgs_config,
        &sp_sim_config,
        Some(mgs_addr),
    )
    .await;

    let mgs_listen_addr = mgstestctx
        .server
        .dropshot_server_for_address(mgs_addr)
        .expect("missing dropshot server for localhost address")
        .local_addr();

    // Also start a local TCP server that we will claim is an MGS instance, but
    // it will close connections immediately after accepting them. This will
    // allow us to count how many connections it receives, while simultaneously
    // causing errors in the SpUpdater when it attempts to use this "MGS".
    let (failing_mgs_task, failing_mgs_addr, failing_mgs_conn_counter) = {
        let socket = TcpListener::bind("[::1]:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let conn_count = Arc::new(AtomicUsize::new(0));

        let task = {
            let conn_count = Arc::clone(&conn_count);
            tokio::spawn(async move {
                loop {
                    let (mut stream, _peer) = socket.accept().await.unwrap();
                    conn_count.fetch_add(1, Ordering::SeqCst);
                    stream.shutdown().await.unwrap();
                }
            })
        };

        (task, addr, conn_count)
    };

    // Order the MGS clients such that the bogus MGS that immediately closes
    // connections comes first. `SpUpdater` should remember that the second MGS
    // instance succeeds, and only send subsequent requests to it: we should
    // only see a single attempted connection to the bogus MGS, even though
    // delivering an update requires a bare minimum of three requests (start the
    // update, query the status, reset the SP) and often more (if repeated
    // queries are required to wait for completion).
    let mgs_clients = [
        Arc::new(gateway_client::Client::new(
            &format!("http://{failing_mgs_addr}"),
            mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient1")),
        )),
        Arc::new(gateway_client::Client::new(
            &format!("http://{mgs_listen_addr}"),
            mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient2")),
        )),
    ];

    let sp_type = SpType::Sled;
    let sp_slot = 0;
    let update_id = Uuid::new_v4();
    let hubris_archive = make_fake_sp_image(SIM_GIMLET_BOARD);

    let sp_updater = SpUpdater::new(
        sp_type,
        sp_slot,
        update_id,
        hubris_archive.clone(),
        &mgstestctx.logctx.log,
    );

    sp_updater.update(mgs_clients).await.expect("update failed");

    let last_update_image = mgstestctx.simrack.gimlets[sp_slot as usize]
        .last_update_data()
        .await
        .expect("simulated SP did not receive an update");

    let hubris_archive = RawHubrisArchive::from_vec(hubris_archive).unwrap();

    assert_eq!(
        hubris_archive.image.data.as_slice(),
        &*last_update_image,
        "simulated SP update contents (len {}) \
         do not match test generated fake image (len {})",
        last_update_image.len(),
        hubris_archive.image.data.len()
    );

    assert_eq!(
        failing_mgs_conn_counter.load(Ordering::SeqCst),
        1,
        "bogus MGS instance didn't receive the expected number of connections"
    );
    failing_mgs_task.abort();
}

#[tokio::test]
async fn test_sp_updater_delivers_progress() {
    // Start MGS + Sim SP.
    let (mgs_config, sp_sim_config) = mgs_setup::load_test_config();
    let mgs_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
    let mgstestctx = mgs_setup::test_setup_with_config(
        "test_sp_updater_delivers_progress",
        SpPort::One,
        mgs_config,
        &sp_sim_config,
        Some(mgs_addr),
    )
    .await;

    let mgs_listen_addr = mgstestctx
        .server
        .dropshot_server_for_address(mgs_addr)
        .expect("missing dropshot server for localhost address")
        .local_addr();
    let mgs_client = Arc::new(gateway_client::Client::new(
        &format!("http://{mgs_listen_addr}"),
        mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient")),
    ));

    let sp_type = SpType::Sled;
    let sp_slot = 0;
    let update_id = Uuid::new_v4();
    let hubris_archive = make_fake_sp_image(SIM_GIMLET_BOARD);

    let sp_updater = SpUpdater::new(
        sp_type,
        sp_slot,
        update_id,
        hubris_archive.clone(),
        &mgstestctx.logctx.log,
    );

    let hubris_archive = RawHubrisArchive::from_vec(hubris_archive).unwrap();
    let sp_image_len = hubris_archive.image.data.len() as u32;

    let mut progress = sp_updater.progress_watcher();
    assert_eq!(*progress.borrow_and_update(), None);

    // Throttle the requests our target SP will receive so we can inspect
    // progress messages without racing.
    let target_sp = &mgstestctx.simrack.gimlets[sp_slot as usize];
    let sp_throttle = target_sp.install_udp_throttler().await;
    let mut sp_responses = target_sp.responses_sent_count().unwrap();

    // Spawn the update on a background task so we can watch `progress` as it is
    // applied.
    let do_update_task = tokio::spawn(sp_updater.update([mgs_client]));

    // Allow the SP to respond to 2 messages: the caboose check and the "prepare
    // update" messages that trigger the start of an update, then ensure we see
    // the "started" progress.
    sp_throttle.send(2).unwrap();
    progress.changed().await.unwrap();
    assert_eq!(*progress.borrow_and_update(), Some(UpdateProgress::Started));

    // Ensure our simulated SP is in the state we expect: it's prepared for an
    // update but has not yet received any data.
    assert_eq!(
        target_sp.current_update_status().await,
        UpdateStatus::InProgress(UpdateInProgressStatus {
            id: update_id.into(),
            bytes_received: 0,
            total_size: sp_image_len,
        })
    );

    // Record the number of responses the SP has sent; we'll use this to bracket
    // our checks of its state below.
    sp_responses.borrow_and_update();

    // At this point, there are two clients racing each other to talk to our
    // simulated SP:
    //
    // 1. MGS is trying to deliver the update
    // 2. `sp_updater` is trying to poll for update status
    //
    // and we want to ensure that we see any relevant progress reports from
    // `sp_updater`. We'll let one message through at a time (waiting until our
    // SP has responded by waiting for a change to `sp_responses`) then check
    // its update state: if it changed, the packet we let through was data from
    // MGS; otherwise, it was a status request from `sp_updater`.
    //
    // This loop will continue until either:
    //
    // 1. We see an `UpdateStatus::InProgress` message indicating 100% delivery,
    //    at which point we break out of the loop
    // 2. We time out waiting for 1 (by timing out for either the SP to process
    //    a request or `sp_updater` to realize there's been progress), at which
    //    point we panic and fail this test
    let mut prev_bytes_received = 0;
    let mut expect_progress_change = false;
    loop {
        sp_throttle.send(1).unwrap();

        // Safety rail that we haven't screwed up our untangle-the-race
        // logic: if we don't see the SP process any new messages after several
        // seconds, our test is broken, so fail.
        tokio::time::timeout(Duration::from_secs(10), sp_responses.changed())
            .await
            .expect("timeout waiting for SP response count to change")
            .expect("sp response count sender dropped");

        match target_sp.current_update_status().await {
            UpdateStatus::InProgress(sp_progress) => {
                if sp_progress.bytes_received > prev_bytes_received {
                    prev_bytes_received = sp_progress.bytes_received;
                    expect_progress_change = true;
                    continue;
                }
            }
            UpdateStatus::Complete(_) => {
                if prev_bytes_received < sp_image_len {
                    prev_bytes_received = sp_image_len;
                    continue;
                }
            }
            status @ (UpdateStatus::None
            | UpdateStatus::Preparing(_)
            | UpdateStatus::SpUpdateAuxFlashChckScan { .. }
            | UpdateStatus::Aborted(_)
            | UpdateStatus::Failed { .. }
            | UpdateStatus::RotError { .. }) => {
                panic!("unexpected status {status:?}");
            }
        }

        // If we get here, the most recent packet did _not_ change the SP's
        // internal update state, so it was a status request from `sp_updater`.
        // If we expect the updater to see new progress, wait for that change
        // here.
        if expect_progress_change || prev_bytes_received == sp_image_len {
            // Safety rail that we haven't screwed up our untangle-the-race
            // logic: if we don't see a new progress after several seconds, our
            // test is broken, so fail.
            tokio::time::timeout(Duration::from_secs(10), progress.changed())
                .await
                .expect("progress timeout")
                .expect("progress watch sender dropped");
            let status = progress.borrow_and_update().clone().unwrap();
            expect_progress_change = false;

            // We're done if we've observed the final progress message.
            if let UpdateProgress::InProgress { progress: Some(value) } = status
            {
                if value == 1.0 {
                    break;
                }
            } else {
                panic!("unexpected progerss status {status:?}");
            }
        }
    }

    // The update has been fully delivered to the SP, but we don't see an
    // `UpdateStatus::Complete` message until the SP is reset. Release the SP
    // throttle since we're no longer racing to observe intermediate progress,
    // and wait for the completion message.
    sp_throttle.send(usize::MAX).unwrap();
    progress.changed().await.unwrap();
    assert_eq!(*progress.borrow_and_update(), Some(UpdateProgress::Complete));

    do_update_task.await.expect("update task panicked").expect("update failed");

    let last_update_image = target_sp
        .last_update_data()
        .await
        .expect("simulated SP did not receive an update");

    assert_eq!(
        hubris_archive.image.data.as_slice(),
        &*last_update_image,
        "simulated SP update contents (len {}) \
         do not match test generated fake image (len {})",
        last_update_image.len(),
        hubris_archive.image.data.len()
    );
}
