// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests `SpUpdater`'s delivery of updates to SPs via MGS

use gateway_client::types::SpType;
use gateway_messages::{SpPort, UpdateInProgressStatus, UpdateStatus};
use gateway_test_utils::setup as mgs_setup;
use hubtools::RawHubrisArchive;
use hubtools::{CabooseBuilder, HubrisArchiveBuilder};
use omicron_nexus::app::test_interfaces::{
    MgsClients, SpUpdater, UpdateProgress,
};
use sp_sim::SimulatedSp;
use sp_sim::SIM_GIMLET_BOARD;
use sp_sim::SIM_SIDECAR_BOARD;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
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
    let mgstestctx =
        mgs_setup::test_setup("test_sp_updater_updates_sled", SpPort::One)
            .await;

    // Configure an MGS client.
    let mgs_clients = MgsClients::from_clients([gateway_client::Client::new(
        &mgstestctx.client.url("/").to_string(),
        mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient")),
    )]);

    // Configure and instantiate an `SpUpdater`.
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

    // Run the update.
    sp_updater.update(mgs_clients).await.expect("update failed");

    // Ensure the SP received the complete update.
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

    mgstestctx.teardown().await;
}

#[tokio::test]
async fn test_sp_updater_updates_switch() {
    // Start MGS + Sim SP.
    let mgstestctx =
        mgs_setup::test_setup("test_sp_updater_updates_switch", SpPort::One)
            .await;

    // Configure an MGS client.
    let mgs_clients = MgsClients::from_clients([gateway_client::Client::new(
        &mgstestctx.client.url("/").to_string(),
        mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient")),
    )]);

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

    sp_updater.update(mgs_clients).await.expect("update failed");

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

    mgstestctx.teardown().await;
}

#[tokio::test]
async fn test_sp_updater_remembers_successful_mgs_instance() {
    // Start MGS + Sim SP.
    let mgstestctx = mgs_setup::test_setup(
        "test_sp_updater_remembers_successful_mgs_instance",
        SpPort::One,
    )
    .await;

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
    let mgs_clients = MgsClients::from_clients([
        gateway_client::Client::new(
            &format!("http://{failing_mgs_addr}"),
            mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient1")),
        ),
        gateway_client::Client::new(
            &mgstestctx.client.url("/").to_string(),
            mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient")),
        ),
    ]);

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

    // Check that our bogus MGS only received a single connection attempt.
    // (After SpUpdater failed to talk to this instance, it should have fallen
    // back to the valid one for all further requests.)
    assert_eq!(
        failing_mgs_conn_counter.load(Ordering::SeqCst),
        1,
        "bogus MGS instance didn't receive the expected number of connections"
    );
    failing_mgs_task.abort();

    mgstestctx.teardown().await;
}

#[tokio::test]
async fn test_sp_updater_switches_mgs_instances_on_failure() {
    enum MgsProxy {
        One(TcpStream),
        Two(TcpStream),
    }

    // Start MGS + Sim SP.
    let mgstestctx = mgs_setup::test_setup(
        "test_sp_updater_switches_mgs_instances_on_failure",
        SpPort::One,
    )
    .await;
    let mgs_bind_addr = mgstestctx.client.bind_address;

    let spawn_mgs_proxy_task = |mut stream: TcpStream| {
        tokio::spawn(async move {
            let mut mgs_stream = TcpStream::connect(mgs_bind_addr)
                .await
                .expect("failed to connect to MGS");
            tokio::io::copy_bidirectional(&mut stream, &mut mgs_stream)
                .await
                .expect("failed to proxy connection to MGS");
        })
    };

    // Start two MGS proxy tasks; when each receives an incoming TCP connection,
    // it forwards that `TcpStream` along the `mgs_proxy_connections` channel
    // along with a tag of which proxy it is. We'll use this below to flip flop
    // between MGS "instances" (really these two proxies).
    let (mgs_proxy_connections_tx, mut mgs_proxy_connections_rx) =
        mpsc::unbounded_channel();
    let (mgs_proxy_one_task, mgs_proxy_one_addr) = {
        let socket = TcpListener::bind("[::1]:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let mgs_proxy_connections_tx = mgs_proxy_connections_tx.clone();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _peer) = socket.accept().await.unwrap();
                mgs_proxy_connections_tx.send(MgsProxy::One(stream)).unwrap();
            }
        });
        (task, addr)
    };
    let (mgs_proxy_two_task, mgs_proxy_two_addr) = {
        let socket = TcpListener::bind("[::1]:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _peer) = socket.accept().await.unwrap();
                mgs_proxy_connections_tx.send(MgsProxy::Two(stream)).unwrap();
            }
        });
        (task, addr)
    };

    // Disable connection pooling so each request gets a new TCP connection.
    let client =
        reqwest::Client::builder().pool_max_idle_per_host(0).build().unwrap();

    // Configure two MGS clients pointed at our two proxy tasks.
    let mgs_clients = MgsClients::from_clients([
        gateway_client::Client::new_with_client(
            &format!("http://{mgs_proxy_one_addr}"),
            client.clone(),
            mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient1")),
        ),
        gateway_client::Client::new_with_client(
            &format!("http://{mgs_proxy_two_addr}"),
            client,
            mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient2")),
        ),
    ]);

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

    // Spawn the actual update task.
    let mut update_task = tokio::spawn(sp_updater.update(mgs_clients));

    // Loop over incoming requests. We expect this sequence:
    //
    // 1. Connection arrives on the first proxy
    // 2. We spawn a task to service that request, and set `should_swap`
    // 3. Connection arrives on the first proxy
    // 4. We drop that connection, flip `expected_proxy`, and clear
    //    `should_swap`
    // 5. Connection arrives on the second proxy
    // 6. We spawn a task to service that request, and set `should_swap`
    // 7. Connection arrives on the second proxy
    // 8. We drop that connection, flip `expected_proxy`, and clear
    //    `should_swap`
    //
    // ... repeat until the update is complete.
    let mut expected_proxy = 0;
    let mut proxy_one_count = 0;
    let mut proxy_two_count = 0;
    let mut total_requests_handled = 0;
    let mut should_swap = false;
    loop {
        tokio::select! {
            Some(proxy_stream) = mgs_proxy_connections_rx.recv() => {
                let stream = match proxy_stream {
                    MgsProxy::One(stream) => {
                        assert_eq!(expected_proxy, 0);
                        proxy_one_count += 1;
                        stream
                    }
                    MgsProxy::Two(stream) => {
                        assert_eq!(expected_proxy, 1);
                        proxy_two_count += 1;
                        stream
                    }
                };

                // Should we trigger `SpUpdater` to swap to the other MGS
                // (proxy)? If so, do that by dropping this connection (which
                // will cause a client failure) and note that we expect the next
                // incoming request to come on the other proxy.
                if should_swap {
                    mem::drop(stream);
                    expected_proxy ^= 1;
                    should_swap = false;
                } else {
                    // Otherwise, handle this connection.
                    total_requests_handled += 1;
                    spawn_mgs_proxy_task(stream);
                    should_swap = true;
                }
            }

            result = &mut update_task => {
                match result {
                    Ok(Ok(())) => {
                        mgs_proxy_one_task.abort();
                        mgs_proxy_two_task.abort();
                        break;
                    }
                    Ok(Err(err)) => panic!("update failed: {err}"),
                    Err(err) => panic!("update task panicked: {err}"),
                }
            }
        }
    }

    // An SP update requires a minimum of 3 requests to MGS: post the update,
    // check the status, and post an SP reset. There may be more requests if the
    // update is not yet complete when the status is checked, but we can just
    // check that each of our proxies received at least 2 incoming requests;
    // based on our outline above, if we got the minimum of 3 requests, it would
    // look like this:
    //
    // 1. POST update -> first proxy (success)
    // 2. GET status -> first proxy (fail)
    // 3. GET status retry -> second proxy (success)
    // 4. POST reset -> second proxy (fail)
    // 5. POST reset -> first proxy (success)
    //
    // This pattern would repeat if multiple status requests were required, so
    // we always expect the first proxy to see exactly one more connection
    // attempt than the second (because it went first before they started
    // swapping), and the two together should see a total of one less than
    // double the number of successful requests required.
    assert!(total_requests_handled >= 3);
    assert_eq!(proxy_one_count, proxy_two_count + 1);
    assert_eq!(
        (proxy_one_count + proxy_two_count + 1) / 2,
        total_requests_handled
    );

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

    mgstestctx.teardown().await;
}

#[tokio::test]
async fn test_sp_updater_delivers_progress() {
    // Start MGS + Sim SP.
    let mgstestctx =
        mgs_setup::test_setup("test_sp_updater_delivers_progress", SpPort::One)
            .await;

    // Configure an MGS client.
    let mgs_clients = MgsClients::from_clients([gateway_client::Client::new(
        &mgstestctx.client.url("/").to_string(),
        mgstestctx.logctx.log.new(slog::o!("component" => "MgsClient")),
    )]);

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

    // Subscribe to update progress, and check that there is no status yet; we
    // haven't started the update.
    let mut progress = sp_updater.progress_watcher();
    assert_eq!(*progress.borrow_and_update(), None);

    // Install a semaphore on the requests our target SP will receive so we can
    // inspect progress messages without racing.
    let target_sp = &mgstestctx.simrack.gimlets[sp_slot as usize];
    let sp_accept_sema = target_sp.install_udp_accept_semaphore().await;
    let mut sp_responses = target_sp.responses_sent_count().unwrap();

    // Spawn the update on a background task so we can watch `progress` as it is
    // applied.
    let do_update_task = tokio::spawn(sp_updater.update(mgs_clients));

    // Allow the SP to respond to 2 messages: the caboose check and the "prepare
    // update" messages that trigger the start of an update, then ensure we see
    // the "started" progress.
    sp_accept_sema.send(2).unwrap();
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

    // Record the number of responses the SP has sent; we'll use
    // `sp_responses.changed()` in the loop below, and want to mark whatever
    // value this watch channel currently has as seen.
    sp_responses.borrow_and_update();

    // At this point, there are two clients racing each other to talk to our
    // simulated SP:
    //
    // 1. MGS is trying to deliver the update
    // 2. `sp_updater` is trying to poll (via MGS) for update status
    //
    // and we want to ensure that we see any relevant progress reports from
    // `sp_updater`. We'll let one MGS -> SP message through at a time (waiting
    // until our SP has responded by waiting for a change to `sp_responses`)
    // then check its update state: if it changed, the packet we let through was
    // data from MGS; otherwise, it was a status request from `sp_updater`.
    //
    // This loop will continue until either:
    //
    // 1. We see an `UpdateStatus::InProgress` message indicating 100% delivery,
    //    at which point we break out of the loop
    // 2. We time out waiting for the previous step (by timing out for either
    //    the SP to process a request or `sp_updater` to realize there's been
    //    progress), at which point we panic and fail this test.
    let mut prev_bytes_received = 0;
    let mut expect_progress_change = false;
    loop {
        // Allow the SP to accept and respond to a single UDP packet.
        sp_accept_sema.send(1).unwrap();

        // Wait until the SP has sent a response, with a safety rail that we
        // haven't screwed up our untangle-the-race logic: if we don't see the
        // SP process any new messages after several seconds, our test is
        // broken, so fail.
        tokio::time::timeout(Duration::from_secs(10), sp_responses.changed())
            .await
            .expect("timeout waiting for SP response count to change")
            .expect("sp response count sender dropped");

        // Inspec the SP's in-memory update state; we expect only `InProgress`
        // or `Complete`, and in either case we note whether we expect to see
        // status changes from `sp_updater`.
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
    // semaphore since we're no longer racing to observe intermediate progress,
    // and wait for the completion message.
    sp_accept_sema.send(usize::MAX).unwrap();
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

    mgstestctx.teardown().await;
}
