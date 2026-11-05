// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_sled_agent::sim;
use omicron_test_utils::dev::test_setup_log;
use omicron_uuid_kinds::{RackUuid, SledUuid};
use sled_agent_types::inventory::SledRegistrationInfo;
use std::sync::Arc;

/// Tests that the simulated sled agent's `GET /sled-registration-info` returns
/// the sled ID and rack ID it was configured with.
#[tokio::test]
async fn sim_sled_registration_info_returns_configured_ids() {
    let logctx =
        test_setup_log("sim_sled_registration_info_returns_configured_ids");
    let log = &logctx.log;

    let sled_id = SledUuid::new_v4();
    let rack_id = RackUuid::new_v4();
    let config = sim::Config::for_testing(
        sled_id,
        rack_id,
        sim::SimMode::Explicit,
        None,
        sim::ZpoolConfig::None,
        sim::SledCpuFamily::AmdMilan,
    );
    let simulated_upstairs = Arc::new(sim::SimulatedUpstairs::new(log.clone()));
    let server = sim::Server::start(
        &config,
        log,
        sim::NexusRegistration::Background,
        &simulated_upstairs,
        0,
    )
    .await
    .expect("started simulated sled agent");

    let client = sled_agent_client::Client::new(
        &format!("http://{}", server.http_server.local_addr()),
        log.clone(),
    );
    let info = client
        .sled_registration_info()
        .await
        .expect("fetched sled registration info")
        .into_inner();
    assert_eq!(info, SledRegistrationInfo { sled_id, rack_id });

    server
        .http_server
        .close()
        .await
        .expect("closed simulated sled agent HTTP server");
    logctx.cleanup_successful();
}
