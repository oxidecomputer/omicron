//! Nexus integration tests
//!
//! See the driver in the parent directory for how and why this is structured
//! the way it is.

use gateway_client::types::SpType;
use gateway_messages::SpPort;
use gateway_test_utils::setup as mgs_setup;
use hubtools::RawHubrisArchive;
use hubtools::{CabooseBuilder, HubrisArchiveBuilder};
use nexus_test_utils::test_setup;
use omicron_nexus::TestInterfaces;
use sp_sim::SimulatedSp;
use sp_sim::SIM_GIMLET_BOARD;
use sp_sim::SIM_SIDECAR_BOARD;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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
    let mgs = mgs_setup::test_setup_with_config(
        "test_sp_updater_updates_sled",
        SpPort::One,
        mgs_config,
        &sp_sim_config,
        Some(mgs_addr),
    )
    .await;

    let cptestctx =
        test_setup::<omicron_nexus::Server>("test_sp_updater_updates_sled")
            .await;

    let mgs_listen_addr = mgs
        .server
        .dropshot_server_for_address(mgs_addr)
        .expect("missing dropshot server for localhost address")
        .local_addr();
    let mgs_client = Arc::new(gateway_client::Client::new(
        &format!("http://{mgs_listen_addr}"),
        cptestctx.logctx.log.new(slog::o!("component" => "MgsClient")),
    ));

    let sp_type = SpType::Sled;
    let sp_slot = 0;
    let update_id = Uuid::new_v4();
    let hubris_archive = make_fake_sp_image(SIM_GIMLET_BOARD);

    let sp_updater = cptestctx.server.apictx().nexus.sp_updater(
        sp_type,
        sp_slot,
        update_id,
        hubris_archive.clone(),
    );

    sp_updater.update([mgs_client]).await.expect("update failed");

    let last_update_image = mgs.simrack.gimlets[sp_slot as usize]
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
    let mgs = mgs_setup::test_setup_with_config(
        "test_sp_updater_updates_switch",
        SpPort::One,
        mgs_config,
        &sp_sim_config,
        Some(mgs_addr),
    )
    .await;

    let cptestctx =
        test_setup::<omicron_nexus::Server>("test_sp_updater_updates_switch")
            .await;

    let mgs_listen_addr = mgs
        .server
        .dropshot_server_for_address(mgs_addr)
        .expect("missing dropshot server for localhost address")
        .local_addr();
    let mgs_client = Arc::new(gateway_client::Client::new(
        &format!("http://{mgs_listen_addr}"),
        cptestctx.logctx.log.new(slog::o!("component" => "MgsClient")),
    ));

    let sp_type = SpType::Switch;
    let sp_slot = 0;
    let update_id = Uuid::new_v4();
    let hubris_archive = make_fake_sp_image(SIM_SIDECAR_BOARD);

    let sp_updater = cptestctx.server.apictx().nexus.sp_updater(
        sp_type,
        sp_slot,
        update_id,
        hubris_archive.clone(),
    );

    sp_updater.update([mgs_client]).await.expect("update failed");

    let last_update_image = mgs.simrack.sidecars[sp_slot as usize]
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
    let mgs = mgs_setup::test_setup_with_config(
        "test_sp_updater_updates_switch",
        SpPort::One,
        mgs_config,
        &sp_sim_config,
        Some(mgs_addr),
    )
    .await;

    let cptestctx = test_setup::<omicron_nexus::Server>(
        "test_sp_updater_remembers_successful_mgs_instance",
    )
    .await;

    let mgs_listen_addr = mgs
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
            cptestctx.logctx.log.new(slog::o!("component" => "MgsClient1")),
        )),
        Arc::new(gateway_client::Client::new(
            &format!("http://{mgs_listen_addr}"),
            cptestctx.logctx.log.new(slog::o!("component" => "MgsClient2")),
        )),
    ];

    let sp_type = SpType::Sled;
    let sp_slot = 0;
    let update_id = Uuid::new_v4();
    let hubris_archive = make_fake_sp_image(SIM_GIMLET_BOARD);

    let sp_updater = cptestctx.server.apictx().nexus.sp_updater(
        sp_type,
        sp_slot,
        update_id,
        hubris_archive.clone(),
    );

    sp_updater.update(mgs_clients).await.expect("update failed");

    let last_update_image = mgs.simrack.gimlets[sp_slot as usize]
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
