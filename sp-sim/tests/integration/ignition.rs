// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SP_SIM_CONFIG_PATH;
use crate::sp_client::DualPortSpClient;
use gateway_messages::IgnitionCommand;
use gateway_messages::ignition::SystemPowerState;
use gateway_messages::ignition::SystemType;
use omicron_test_utils::dev::LogContext;
use omicron_test_utils::dev::test_setup_log;
use sp_sim::SimRack;

const LOCAL_SWITCH_TARGET: u8 = 0;
const PEER_SWITCH_TARGET: u8 = 1;

const POWERED_GIMLET: usize = 1;
const NEIGHBOR_GIMLET: usize = 0;

/// Returns the ignition target for the given gimlet.
fn gimlet_target(simrack: &SimRack, gimlet: usize) -> u8 {
    assert!(gimlet < simrack.gimlets.len(), "test config has gimlet {gimlet}");
    u8::try_from(simrack.sidecars.len() + gimlet)
        .expect("gimlet ignition target fits in a u8")
}

async fn start_rack(
    test_name: &str,
) -> (LogContext, SimRack, [DualPortSpClient; 2]) {
    let logctx = test_setup_log(test_name);
    let config = sp_sim::Config::from_file(SP_SIM_CONFIG_PATH)
        .expect("loaded sp-sim test config");
    let simrack = SimRack::start(&config, &logctx.log)
        .await
        .expect("started simulated rack");
    assert_eq!(simrack.sidecars.len(), 2, "test config has two sidecars");
    let switches = [
        DualPortSpClient::connect(&simrack.sidecars[0]).await,
        DualPortSpClient::connect(&simrack.sidecars[1]).await,
    ];
    (logctx, simrack, switches)
}

#[tokio::test]
async fn ignition_power_off_silences_sp() {
    let (logctx, simrack, mut switches) =
        start_rack("ignition_power_off_silences_sp").await;
    let powered_target = gimlet_target(&simrack, POWERED_GIMLET);
    let neighbor_target = gimlet_target(&simrack, NEIGHBOR_GIMLET);
    let mut powered =
        DualPortSpClient::connect(&simrack.gimlets[POWERED_GIMLET]).await;
    let mut neighbor =
        DualPortSpClient::connect(&simrack.gimlets[NEIGHBOR_GIMLET]).await;

    for switch in &mut switches {
        switch
            .assert_power(
                powered_target,
                SystemType::Gimlet,
                SystemPowerState::On,
            )
            .await;
    }
    powered.assert_discoverable().await;

    switches[0]
        .ignition_command(powered_target, IgnitionCommand::PowerOff)
        .await;

    for switch in &mut switches {
        switch
            .assert_power(
                powered_target,
                SystemType::Gimlet,
                SystemPowerState::Off,
            )
            .await;
        switch
            .assert_power(
                neighbor_target,
                SystemType::Gimlet,
                SystemPowerState::On,
            )
            .await;
    }
    powered.assert_not_discoverable().await;
    neighbor.assert_discoverable().await;

    switches[1]
        .ignition_command(powered_target, IgnitionCommand::PowerOn)
        .await;

    for switch in &mut switches {
        switch
            .assert_power(
                powered_target,
                SystemType::Gimlet,
                SystemPowerState::On,
            )
            .await;
    }
    powered.assert_discoverable().await;

    simrack.teardown().await.expect("simulated SP tasks ran until teardown");
    logctx.cleanup_successful();
}

#[tokio::test]
async fn ignition_power_off_silences_peer_switch() {
    let (logctx, simrack, mut switches) =
        start_rack("ignition_power_off_silences_peer_switch").await;

    for switch in &mut switches {
        for target in [LOCAL_SWITCH_TARGET, PEER_SWITCH_TARGET] {
            switch
                .assert_power(target, SystemType::Sidecar, SystemPowerState::On)
                .await;
        }
    }

    // Ensure that ignition commands work in both directions: switch 0 to switch
    // 1 and switch 1 to switch 0.
    for (local, peer) in [(1, 0), (0, 1)] {
        switches[local]
            .ignition_command(PEER_SWITCH_TARGET, IgnitionCommand::PowerOff)
            .await;

        switches[local]
            .assert_power(
                PEER_SWITCH_TARGET,
                SystemType::Sidecar,
                SystemPowerState::Off,
            )
            .await;
        switches[local]
            .assert_power(
                LOCAL_SWITCH_TARGET,
                SystemType::Sidecar,
                SystemPowerState::On,
            )
            .await;
        switches[peer].assert_not_discoverable().await;
        switches[local].assert_discoverable().await;

        let gimlet = usize::from(gimlet_target(&simrack, NEIGHBOR_GIMLET));
        for (sidecar, expected_switches) in [
            (peer, [SystemPowerState::Off, SystemPowerState::On]),
            (local, [SystemPowerState::On, SystemPowerState::Off]),
        ] {
            let state =
                simrack.sidecars[sidecar].current_ignition_state().await;
            let target_state = |target: usize| {
                state[target].target.unwrap_or_else(|| {
                    panic!("sidecar {sidecar} sees target {target} as present")
                })
            };
            assert_eq!(
                [target_state(0).power_state, target_state(1).power_state],
                expected_switches,
                "bulk switch target power from sidecar {sidecar}",
            );
            assert_eq!(
                (
                    target_state(gimlet).system_type,
                    target_state(gimlet).power_state
                ),
                (SystemType::Gimlet, SystemPowerState::On),
                "bulk gimlet target from sidecar {sidecar}",
            );
        }

        switches[local]
            .ignition_command(PEER_SWITCH_TARGET, IgnitionCommand::PowerOn)
            .await;

        switches[peer].assert_discoverable().await;
        for switch in &mut switches {
            for target in [LOCAL_SWITCH_TARGET, PEER_SWITCH_TARGET] {
                switch
                    .assert_power(
                        target,
                        SystemType::Sidecar,
                        SystemPowerState::On,
                    )
                    .await;
            }
        }
    }

    simrack.teardown().await.expect("simulated SP tasks ran until teardown");
    logctx.cleanup_successful();
}
