// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use gateway_client::types::{SpComponentInfo, SpComponentPresence};
use gateway_messages::DeviceCapabilities;
use gateway_messages::SpComponent;
use gateway_messages::SpPort;
use gateway_test_utils::current_simulator_state;
use gateway_test_utils::setup;
use gateway_types::component::SpType;

#[tokio::test]
async fn component_list() {
    let testctx = setup::test_setup("component_list", SpPort::One).await;
    let client = &testctx.client;
    let simrack = &testctx.simrack;

    // Double check that we have at least 1 sidecar and 2 gimlets and that all
    // SPs are enabled.
    let sim_state = current_simulator_state(simrack).await;
    assert!(
        sim_state
            .iter()
            .filter(|sp| sp.ignition.id.typ == SpType::Sled)
            .count()
            >= 2
    );
    assert!(sim_state.iter().any(|sp| sp.ignition.id.typ == SpType::Switch));
    assert!(sim_state.iter().all(|sp| sp.state.is_ok()));

    // Get the component list for sled 0.
    let resp =
        client.sp_component_list(&SpType::Sled, 0).await.unwrap().into_inner();

    assert_eq!(
        resp.components,
        &[
            SpComponentInfo {
                component: SpComponent::SP3_HOST_CPU.const_as_str().to_string(),
                device: SpComponent::SP3_HOST_CPU.const_as_str().to_string(),
                serial_number: None,
                description: "FAKE host cpu".to_string(),
                capabilities: 0,
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-0".to_string(),
                device: "fake-tmp-sensor".to_string(),
                serial_number: None,
                description: "FAKE temperature sensor".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Failed,
            },
            SpComponentInfo {
                component: "dev-1".to_string(),
                device: "tmp117".to_string(),
                serial_number: None,
                description: "FAKE temperature sensor".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-2".to_string(),
                device: "tmp117".to_string(),
                serial_number: None,
                description: "FAKE Southeast temperature sensor".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-6".to_string(),
                device: "at24csw080".to_string(),
                serial_number: None,
                description: "FAKE U.2 Sharkfin A VPD".to_string(),
                capabilities: 0,
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-7".to_string(),
                device: "max5970".to_string(),
                serial_number: None,
                description: "FAKE U.2 Sharkfin A hot swap controller"
                    .to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-8".to_string(),
                device: "nvme_bmc".to_string(),
                serial_number: None,
                description: "FAKE U.2 A NVMe Basic Management Command"
                    .to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-39".to_string(),
                device: "tmp451".to_string(),
                serial_number: None,
                description: "FAKE T6 temperature sensor".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-46".to_string(),
                device: "sbtsi".to_string(),
                serial_number: None,
                description: "CPU temperature sensor".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-53".to_string(),
                device: "max31790".to_string(),
                serial_number: None,
                description: "FAKE Fan controller".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
        ]
    );

    // Get the component list for sled 1.
    let resp =
        client.sp_component_list(&SpType::Sled, 1).await.unwrap().into_inner();

    assert_eq!(
        resp.components,
        &[
            SpComponentInfo {
                component: SpComponent::SP3_HOST_CPU.const_as_str().to_string(),
                device: SpComponent::SP3_HOST_CPU.const_as_str().to_string(),
                serial_number: None,
                description: "FAKE host cpu".to_string(),
                capabilities: 0,
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-0".to_string(),
                device: "tmp117".to_string(),
                serial_number: None,
                description: "FAKE temperature sensor".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-1".to_string(),
                device: "tmp117".to_string(),
                serial_number: None,
                description: "FAKE temperature sensor".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-2".to_string(),
                device: "tmp117".to_string(),
                serial_number: None,
                description: "FAKE Southeast temperature sensor".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-6".to_string(),
                device: "at24csw080".to_string(),
                serial_number: None,
                description: "FAKE U.2 Sharkfin A VPD".to_string(),
                capabilities: 0,
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-7".to_string(),
                device: "max5970".to_string(),
                serial_number: None,
                description: "FAKE U.2 Sharkfin A hot swap controller"
                    .to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-8".to_string(),
                device: "nvme_bmc".to_string(),
                serial_number: None,
                description: "FAKE U.2 A NVMe Basic Management Command"
                    .to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-39".to_string(),
                device: "tmp451".to_string(),
                serial_number: None,
                description: "FAKE T6 temperature sensor".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-46".to_string(),
                device: "sbtsi".to_string(),
                serial_number: None,
                description: "CPU temperature sensor".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-53".to_string(),
                device: "max31790".to_string(),
                serial_number: None,
                description: "FAKE Fan controller".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
        ]
    );

    // Get the component list for switch 0.
    let resp = client
        .sp_component_list(&SpType::Switch, 0)
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.components,
        &[
            SpComponentInfo {
                component: "dev-0".to_string(),
                device: "fake-tmp-sensor".to_string(),
                serial_number: None,
                description: "FAKE temperature sensor 1".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Present,
            },
            SpComponentInfo {
                component: "dev-1".to_string(),
                device: "fake-tmp-sensor".to_string(),
                serial_number: None,
                description: "FAKE temperature sensor 2".to_string(),
                capabilities: DeviceCapabilities::HAS_MEASUREMENT_CHANNELS
                    .bits(),
                presence: SpComponentPresence::Failed,
            },
        ]
    );

    testctx.teardown().await;
}
