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
use sp_sim::SIM_GIMLET_BOARD;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::Arc;
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
async fn test_sp_updater_updates_gimlet() {
    // Start MGS + Sim SP.
    let (mgs_config, sp_sim_config) = mgs_setup::load_test_config();
    let mgs_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
    let mgs = mgs_setup::test_setup_with_config(
        "test_sp_updater_drives_update",
        SpPort::One,
        mgs_config,
        &sp_sim_config,
        Some(mgs_addr),
    )
    .await;

    let cptestctx =
        test_setup::<omicron_nexus::Server>("test_sp_updater_drives_update")
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
