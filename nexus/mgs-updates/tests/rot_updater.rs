// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests `RotUpdater`'s delivery of updates to RoTs via MGS

use gateway_client::SpComponent;
use gateway_client::types::{GetRotBootInfoParams, SpType};
use gateway_messages::{RotBootInfo, SpPort};
use gateway_test_utils::setup as mgs_setup;
use gateway_types::rot::RotSlot;
use hubtools::RawHubrisArchive;
use hubtools::{CabooseBuilder, HubrisArchiveBuilder};
use nexus_mgs_updates::{MgsClients, RotUpdater, UpdateProgress};
use slog::debug;
use sp_sim::SIM_ROT_BOARD;
use sp_sim::SimulatedSp;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use uuid::Uuid;

fn make_fake_rot_archive() -> Vec<u8> {
    let caboose = make_fake_rot_archive_caboose();
    make_fake_rot_archive_with_caboose(&caboose)
}

fn make_fake_rot_archive_caboose() -> hubtools::Caboose {
    CabooseBuilder::default()
        .git_commit("fake-git-commit")
        .board(SIM_ROT_BOARD)
        .version("0.0.0")
        .name("fake-name")
        .build()
}

fn make_fake_rot_archive_with_caboose(caboose: &hubtools::Caboose) -> Vec<u8> {
    let mut builder = HubrisArchiveBuilder::with_fake_image();
    builder.write_caboose(caboose.as_slice()).unwrap();
    builder.build_to_vec().unwrap()
}

#[tokio::test]
async fn test_rot_updater_updates_sled() {
    // Start MGS + Sim SP.
    let mgstestctx = mgs_setup::test_setup(
        "test_rot_updater_updates_sled",
        SpPort::One,
        mgs_setup::DEFAULT_SP_SIM_CONFIG.into(),
    )
    .await;

    // Configure an MGS client.
    let mgs_client = mgstestctx.client();
    let mut mgs_clients = MgsClients::from_clients([mgs_client.clone()]);

    // Configure and instantiate an `RotUpdater`.
    let sp_type = SpType::Sled;
    let sp_slot = 0;
    let update_id = Uuid::new_v4();
    let deployed_caboose = make_fake_rot_archive_caboose();
    let hubris_archive = make_fake_rot_archive_with_caboose(&deployed_caboose);
    let target_rot_slot = RotSlot::B;

    let rot_updater = RotUpdater::new(
        sp_type,
        sp_slot,
        target_rot_slot,
        update_id,
        hubris_archive.clone(),
        &mgstestctx.logctx.log,
    );

    // Grab the initial cabooses, RoT state, and RoT boot info so that we can
    // later check that they've changed.
    let caboose_a_before = mgs_client
        .sp_component_caboose_get(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            0,
        )
        .await
        .unwrap()
        .into_inner();
    let caboose_b_before = mgs_client
        .sp_component_caboose_get(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            1,
        )
        .await
        .unwrap()
        .into_inner();
    let rot_state_before =
        mgs_client.sp_get(sp_type, sp_slot).await.unwrap().into_inner().rot;
    let rot_boot_info_before = mgs_client
        .sp_rot_boot_info(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            &GetRotBootInfoParams {
                version: RotBootInfo::HIGHEST_KNOWN_VERSION,
            },
        )
        .await
        .unwrap()
        .into_inner();

    // This test updates slot B, so we're assuming that the simulated SP is
    // running out of slot A.
    assert_eq!(rot_state_before, rot_boot_info_before);
    let gateway_client::types::RotState::V3 {
        slot_a_fwid: slot_a_fwid_before,
        slot_b_fwid: slot_b_fwid_before,
        active: RotSlot::A,
        pending_persistent_boot_preference: None,
        persistent_boot_preference: RotSlot::A,
        transient_boot_preference: None,
        ..
    } = rot_state_before
    else {
        panic!("unexpected initial RoT state: {:?}", rot_state_before);
    };

    // Run the update.
    rot_updater.update(&mut mgs_clients).await.expect("update failed");

    // Ensure the RoT received the complete update.
    let last_update_image = mgstestctx.simrack.gimlets[sp_slot as usize]
        .last_rot_update_data()
        .await
        .expect("simulated RoT did not receive an update");

    let hubris_archive = RawHubrisArchive::from_vec(hubris_archive).unwrap();

    assert_eq!(
        hubris_archive.image.data.as_slice(),
        &*last_update_image,
        "simulated RoT update contents (len {}) \
         do not match test generated fake image (len {})",
        last_update_image.len(),
        hubris_archive.image.data.len()
    );

    // Grab the final cabooses and other state so we can verify that they
    // changed as expected.
    let caboose_a_after = mgs_client
        .sp_component_caboose_get(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            0,
        )
        .await
        .unwrap()
        .into_inner();
    let caboose_b_after = mgs_client
        .sp_component_caboose_get(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            1,
        )
        .await
        .unwrap()
        .into_inner();
    let rot_state_after =
        mgs_client.sp_get(sp_type, sp_slot).await.unwrap().into_inner().rot;
    let rot_boot_info_after = mgs_client
        .sp_rot_boot_info(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            &GetRotBootInfoParams {
                version: RotBootInfo::HIGHEST_KNOWN_VERSION,
            },
        )
        .await
        .unwrap()
        .into_inner();
    assert_eq!(caboose_a_before, caboose_a_after);
    assert_ne!(caboose_b_before, caboose_b_after);
    assert_eq!(
        deployed_caboose.board().unwrap(),
        caboose_b_after.board.as_bytes()
    );
    assert_eq!(
        deployed_caboose.git_commit().unwrap(),
        caboose_b_after.git_commit.as_bytes(),
    );
    assert_eq!(
        deployed_caboose.version().unwrap(),
        caboose_b_after.version.as_bytes(),
    );

    assert_eq!(rot_state_after, rot_boot_info_after);
    let gateway_client::types::RotState::V3 {
        slot_a_fwid: slot_a_fwid_after,
        slot_b_fwid: slot_b_fwid_after,
        active: RotSlot::B,
        pending_persistent_boot_preference: None,
        persistent_boot_preference: RotSlot::B,
        transient_boot_preference: None,
        ..
    } = rot_state_after
    else {
        panic!("unexpected final RoT state: {:?}", rot_state_after);
    };
    assert_eq!(slot_a_fwid_before, slot_a_fwid_after);
    assert_ne!(slot_b_fwid_before, slot_b_fwid_after);

    mgstestctx.teardown().await;
}

#[tokio::test]
async fn test_rot_updater_updates_switch() {
    // Start MGS + Sim SP.
    let mgstestctx = mgs_setup::test_setup(
        "test_rot_updater_updates_switch",
        SpPort::One,
        mgs_setup::DEFAULT_SP_SIM_CONFIG.into(),
    )
    .await;

    // Configure an MGS client.
    let mgs_client = mgstestctx.client();
    let mut mgs_clients = MgsClients::from_clients([mgs_client.clone()]);

    // Configure and instantiate an `RotUpdater`.
    let sp_type = SpType::Switch;
    let sp_slot = 0;
    let update_id = Uuid::new_v4();
    let deployed_caboose = make_fake_rot_archive_caboose();
    let hubris_archive = make_fake_rot_archive_with_caboose(&deployed_caboose);
    let target_rot_slot = RotSlot::B;

    let rot_updater = RotUpdater::new(
        sp_type,
        sp_slot,
        target_rot_slot,
        update_id,
        hubris_archive.clone(),
        &mgstestctx.logctx.log,
    );

    // Grab the initial cabooses, RoT state, and RoT boot info so that we can
    // later check that they've changed.
    let caboose_a_before = mgs_client
        .sp_component_caboose_get(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            0,
        )
        .await
        .unwrap()
        .into_inner();
    let caboose_b_before = mgs_client
        .sp_component_caboose_get(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            1,
        )
        .await
        .unwrap()
        .into_inner();
    let rot_state_before =
        mgs_client.sp_get(sp_type, sp_slot).await.unwrap().into_inner().rot;
    let rot_boot_info_before = mgs_client
        .sp_rot_boot_info(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            &GetRotBootInfoParams {
                version: RotBootInfo::HIGHEST_KNOWN_VERSION,
            },
        )
        .await
        .unwrap()
        .into_inner();

    // This test updates slot B, so we're assuming that the simulated SP is
    // running out of slot A.
    assert_eq!(rot_state_before, rot_boot_info_before);
    let gateway_client::types::RotState::V3 {
        slot_a_fwid: slot_a_fwid_before,
        slot_b_fwid: slot_b_fwid_before,
        active: RotSlot::A,
        pending_persistent_boot_preference: None,
        persistent_boot_preference: RotSlot::A,
        transient_boot_preference: None,
        ..
    } = rot_state_before
    else {
        panic!("unexpected initial RoT state: {:?}", rot_state_before);
    };

    // Run the update.
    rot_updater.update(&mut mgs_clients).await.expect("update failed");

    // Ensure the RoT received the complete update.
    let last_update_image = mgstestctx.simrack.sidecars[sp_slot as usize]
        .last_rot_update_data()
        .await
        .expect("simulated RoT did not receive an update");

    let hubris_archive = RawHubrisArchive::from_vec(hubris_archive).unwrap();

    assert_eq!(
        hubris_archive.image.data.as_slice(),
        &*last_update_image,
        "simulated RoT update contents (len {}) \
         do not match test generated fake image (len {})",
        last_update_image.len(),
        hubris_archive.image.data.len()
    );

    // Grab the final cabooses and other state so we can verify that they
    // changed as expected.
    let caboose_a_after = mgs_client
        .sp_component_caboose_get(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            0,
        )
        .await
        .unwrap()
        .into_inner();
    let caboose_b_after = mgs_client
        .sp_component_caboose_get(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            1,
        )
        .await
        .unwrap()
        .into_inner();
    let rot_state_after =
        mgs_client.sp_get(sp_type, sp_slot).await.unwrap().into_inner().rot;
    let rot_boot_info_after = mgs_client
        .sp_rot_boot_info(
            sp_type,
            sp_slot,
            SpComponent::ROT.const_as_str(),
            &GetRotBootInfoParams {
                version: RotBootInfo::HIGHEST_KNOWN_VERSION,
            },
        )
        .await
        .unwrap()
        .into_inner();
    assert_eq!(caboose_a_before, caboose_a_after);
    assert_ne!(caboose_b_before, caboose_b_after);
    assert_eq!(
        deployed_caboose.board().unwrap(),
        caboose_b_after.board.as_bytes()
    );
    assert_eq!(
        deployed_caboose.git_commit().unwrap(),
        caboose_b_after.git_commit.as_bytes(),
    );
    assert_eq!(
        deployed_caboose.version().unwrap(),
        caboose_b_after.version.as_bytes(),
    );

    assert_eq!(rot_state_after, rot_boot_info_after);
    let gateway_client::types::RotState::V3 {
        slot_a_fwid: slot_a_fwid_after,
        slot_b_fwid: slot_b_fwid_after,
        active: RotSlot::B,
        pending_persistent_boot_preference: None,
        persistent_boot_preference: RotSlot::B,
        transient_boot_preference: None,
        ..
    } = rot_state_after
    else {
        panic!("unexpected final RoT state: {:?}", rot_state_after);
    };
    assert_eq!(slot_a_fwid_before, slot_a_fwid_after);
    assert_ne!(slot_b_fwid_before, slot_b_fwid_after);

    mgstestctx.teardown().await;
}

#[tokio::test]
async fn test_rot_updater_remembers_successful_mgs_instance() {
    // Start MGS + Sim SP.
    let mgstestctx = mgs_setup::test_setup(
        "test_rot_updater_remembers_successful_mgs_instance",
        SpPort::One,
        mgs_setup::DEFAULT_SP_SIM_CONFIG.into(),
    )
    .await;

    // Also start a local TCP server that we will claim is an MGS instance, but
    // it will close connections immediately after accepting them. This will
    // allow us to count how many connections it receives, while simultaneously
    // causing errors in the RotUpdater when it attempts to use this "MGS".
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
    // connections comes first. `RotUpdater` should remember that the second MGS
    // instance succeeds, and only send subsequent requests to it: we should
    // only see a single attempted connection to the bogus MGS, even though
    // delivering an update requires a bare minimum of three requests (start the
    // update, query the status, reset the RoT) and often more (if repeated
    // queries are required to wait for completion).
    let mut mgs_clients = MgsClients::from_clients([
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
    let hubris_archive = make_fake_rot_archive();
    let target_rot_slot = RotSlot::B;

    let rot_updater = RotUpdater::new(
        sp_type,
        sp_slot,
        target_rot_slot,
        update_id,
        hubris_archive.clone(),
        &mgstestctx.logctx.log,
    );

    rot_updater.update(&mut mgs_clients).await.expect("update failed");

    let last_update_image = mgstestctx.simrack.gimlets[sp_slot as usize]
        .last_rot_update_data()
        .await
        .expect("simulated RoT did not receive an update");

    let hubris_archive = RawHubrisArchive::from_vec(hubris_archive).unwrap();

    assert_eq!(
        hubris_archive.image.data.as_slice(),
        &*last_update_image,
        "simulated RoT update contents (len {}) \
         do not match test generated fake image (len {})",
        last_update_image.len(),
        hubris_archive.image.data.len()
    );

    // Check that our bogus MGS only received a single connection attempt.
    // (After RotUpdater failed to talk to this instance, it should have fallen
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
async fn test_rot_updater_switches_mgs_instances_on_failure() {
    enum MgsProxy {
        One(TcpStream),
        Two(TcpStream),
    }

    // Start MGS + Sim SP.
    let mgstestctx = mgs_setup::test_setup(
        "test_rot_updater_switches_mgs_instances_on_failure",
        SpPort::One,
        mgs_setup::DEFAULT_SP_SIM_CONFIG.into(),
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
    let mut mgs_clients = MgsClients::from_clients([
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
    let hubris_archive = make_fake_rot_archive();
    let target_rot_slot = RotSlot::B;

    let rot_updater = RotUpdater::new(
        sp_type,
        sp_slot,
        target_rot_slot,
        update_id,
        hubris_archive.clone(),
        &mgstestctx.logctx.log,
    );

    // Spawn the actual update task.
    let mut update_task =
        tokio::spawn(async move { rot_updater.update(&mut mgs_clients).await });

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

                // Should we trigger `RotUpdater` to swap to the other MGS
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

    // An RoT update requires a minimum of 4 requests to MGS: post the update,
    // check the status, post to mark the new target slot active, and post an
    // RoT reset. There may be more requests if the update is not yet complete
    // when the status is checked, but we can just check that each of our
    // proxies received at least 2 incoming requests; based on our outline
    // above, if we got the minimum of 4 requests, it would look like this:
    //
    // 1. POST update -> first proxy (success)
    // 2. GET status -> first proxy (fail)
    // 3. GET status retry -> second proxy (success)
    // 4. POST new target slot -> second proxy (fail)
    // 5. POST new target slot -> first proxy (success)
    // 6. POST reset -> first proxy (fail)
    // 7. POST reset -> second proxy (success)
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
        .last_rot_update_data()
        .await
        .expect("simulated RoT did not receive an update");

    let hubris_archive = RawHubrisArchive::from_vec(hubris_archive).unwrap();

    assert_eq!(
        hubris_archive.image.data.as_slice(),
        &*last_update_image,
        "simulated RoT update contents (len {}) \
         do not match test generated fake image (len {})",
        last_update_image.len(),
        hubris_archive.image.data.len()
    );

    mgstestctx.teardown().await;
}

#[tokio::test]
async fn test_rot_updater_delivers_progress() {
    // Start MGS + Sim SP.
    let mgstestctx = mgs_setup::test_setup(
        "test_rot_updater_delivers_progress",
        SpPort::One,
        mgs_setup::DEFAULT_SP_SIM_CONFIG.into(),
    )
    .await;

    // Configure an MGS client.
    let mut mgs_clients = MgsClients::from_clients([mgstestctx.client()]);

    let sp_type = SpType::Sled;
    let sp_slot = 0;
    let update_id = Uuid::new_v4();
    let hubris_archive = make_fake_rot_archive();
    let target_rot_slot = RotSlot::B;
    let target_sp = &mgstestctx.simrack.gimlets[sp_slot as usize];

    let rot_updater = RotUpdater::new(
        sp_type,
        sp_slot,
        target_rot_slot,
        update_id,
        hubris_archive.clone(),
        &mgstestctx.logctx.log,
    );

    let hubris_archive = RawHubrisArchive::from_vec(hubris_archive).unwrap();

    // Subscribe to update progress, and check that there is no status yet; we
    // haven't started the update.
    let mut progress = rot_updater.progress_watcher();
    assert_eq!(*progress.borrow_and_update(), None);

    // Spawn the update on a background task so we can watch `progress` as it is
    // applied.
    let do_update_task =
        tokio::spawn(async move { rot_updater.update(&mut mgs_clients).await });

    // Loop until we see `UpdateProgress::Complete`, ensuring that any
    // intermediate progress messages we see are in order.
    let mut saw_started = false;
    let mut prev_progress = 0.0;
    let log = mgstestctx.logctx.log.clone();
    tokio::time::timeout(
        Duration::from_secs(20),
        tokio::spawn(async move {
            loop {
                progress.changed().await.unwrap();
                let status = progress
                    .borrow_and_update()
                    .clone()
                    .expect("progress changed but still None");
                debug!(log, "saw new progress status"; "status" => ?status);
                match status {
                    UpdateProgress::Started => {
                        assert!(!saw_started, "saw Started multiple times");
                        saw_started = true;
                    }
                    UpdateProgress::InProgress { progress: Some(value) } => {
                        // even if we didn't see the explicit `Started` message,
                        // getting `InProgress` means we're past that point.
                        saw_started = true;
                        assert!(
                            value >= prev_progress,
                            "new progress {value} \
                             less than previous progress {prev_progress}"
                        );
                        prev_progress = value;
                    }
                    UpdateProgress::Complete => break,
                    _ => panic!("unexpected progress status {status:?}"),
                }
            }
        }),
    )
    .await
    .expect("timeout waiting for update completion")
    .expect("task panic");

    do_update_task.await.expect("update task panicked").expect("update failed");

    let last_update_image = target_sp
        .last_rot_update_data()
        .await
        .expect("simulated RoT did not receive an update");

    assert_eq!(
        hubris_archive.image.data.as_slice(),
        &*last_update_image,
        "simulated RoT update contents (len {}) \
         do not match test generated fake image (len {})",
        last_update_image.len(),
        hubris_archive.image.data.len()
    );

    mgstestctx.teardown().await;
}
