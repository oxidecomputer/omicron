// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_client::types::IgnitionCommand;
use gateway_messages::SpPort;
use gateway_test_utils::setup;
use gateway_types::component::PowerState;
use gateway_types::component::SpIdentifier;
use gateway_types::component::SpType;
use gateway_types::ignition::SpIgnition;
use http::StatusCode;

// TODO-RAINCLAUDE: the default sp-sim config has gimlets in slots 0 and 1; the test adds one more and powers it off, checking that ignition targets line up with gimlets added by add_simulated_gimlet_sps.
const POWERED_SLED: u16 = 2;
const NEIGHBOR_SLED: u16 = 1;

async fn ignition_power(
    client: &gateway_client::Client,
    typ: SpType,
    slot: u16,
) -> bool {
    let ignition = client.ignition_list().await.unwrap().into_inner();
    let sp_id = SpIdentifier { typ, slot };
    let info = ignition
        .iter()
        .find(|info| info.id == sp_id)
        .unwrap_or_else(|| panic!("ignition reports {sp_id:?}"));
    match info.details {
        SpIgnition::Present { power, .. } => power,
        SpIgnition::Absent => panic!("ignition reports {sp_id:?} absent"),
    }
}

async fn assert_sled_answers(client: &gateway_client::Client, slot: u16) {
    let state = client.sp_get(&SpType::Sled, slot).await.unwrap().into_inner();
    assert_eq!(state.serial_number, setup::sim_gimlet_serial(slot));
    assert_eq!(state.power_state, PowerState::A0);
}

async fn assert_sp_unreachable(
    client: &gateway_client::Client,
    typ: SpType,
    slot: u16,
) {
    let err = match client.sp_get(&typ, slot).await {
        Ok(state) => panic!("powered-off SP answered: {state:?}"),
        Err(gateway_client::Error::ErrorResponse(err)) => err,
        Err(err) => panic!("expected an error response, got {err:?}"),
    };
    assert_eq!(err.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(err.error_code.as_deref(), Some("SpCommunicationFailed"));
}

// TODO-RAINCLAUDE: both MGS instances share one rack, so a power-off through switch 0 must be seen by switch 1's ignition too, and either switch can power the sled back on.
#[tokio::test]
async fn ignition_power_off_silences_sp() {
    let (mut mgs_config0, mut sp_sim_config) =
        setup::load_test_config(setup::DEFAULT_SP_SIM_CONFIG.into());
    let added = setup::add_simulated_gimlet_sps(&mut sp_sim_config, 1);
    let mut mgs_config1 = mgs_config0.clone();
    setup::add_simulated_gimlet_ports(&mut mgs_config0, added);
    setup::add_simulated_gimlet_ports(&mut mgs_config1, added);

    let testctx0 = setup::test_setup_with_config(
        "ignition_power_off_silences_sp_0",
        SpPort::One,
        mgs_config0,
        &sp_sim_config,
        None,
    )
    .await;
    let testctx1 = setup::test_setup_with_simrack(
        "ignition_power_off_silences_sp_1",
        SpPort::Two,
        mgs_config1,
        testctx0.simrack.clone(),
        None,
    )
    .await;
    let clients = [&testctx0.client, &testctx1.client];

    for client in clients {
        assert!(ignition_power(client, SpType::Sled, POWERED_SLED).await);
        assert_sled_answers(client, POWERED_SLED).await;
    }

    testctx0
        .client
        .ignition_command(
            &SpType::Sled,
            POWERED_SLED,
            IgnitionCommand::PowerOff,
        )
        .await
        .unwrap();

    for client in clients {
        assert!(!ignition_power(client, SpType::Sled, POWERED_SLED).await);
        assert_sp_unreachable(client, SpType::Sled, POWERED_SLED).await;
        assert!(ignition_power(client, SpType::Sled, NEIGHBOR_SLED).await);
        assert_sled_answers(client, NEIGHBOR_SLED).await;
    }

    testctx1
        .client
        .ignition_command(&SpType::Sled, POWERED_SLED, IgnitionCommand::PowerOn)
        .await
        .unwrap();

    for client in clients {
        assert!(ignition_power(client, SpType::Sled, POWERED_SLED).await);
        assert_sled_answers(client, POWERED_SLED).await;
    }

    // TODO-RAINCLAUDE: switch 0 is powered off and on through MGS1, since MGS0 talks through switch 0 and is only used again once it is back; per-switch target numbering is in sp-sim's rack_target.
    let client1 = &testctx1.client;
    client1
        .ignition_command(&SpType::Switch, 0, IgnitionCommand::PowerOff)
        .await
        .unwrap();
    assert!(!ignition_power(client1, SpType::Switch, 0).await);
    assert!(ignition_power(client1, SpType::Switch, 1).await);
    assert_sp_unreachable(client1, SpType::Switch, 0).await;
    let state = client1.sp_get(&SpType::Switch, 1).await.unwrap().into_inner();
    assert_eq!(state.serial_number, "SimSidecar1");

    client1
        .ignition_command(&SpType::Switch, 0, IgnitionCommand::PowerOn)
        .await
        .unwrap();
    for client in clients {
        for switch in [0, 1] {
            assert!(ignition_power(client, SpType::Switch, switch).await);
            let state = client
                .sp_get(&SpType::Switch, switch)
                .await
                .unwrap()
                .into_inner();
            assert_eq!(state.serial_number, format!("SimSidecar{switch}"));
        }
    }

    testctx1.teardown().await;
    testctx0.teardown().await;
}
