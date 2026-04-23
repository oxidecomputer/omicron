// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use super::*;
use assert_matches::assert_matches;
use httpmock::Mock;
use httpmock::MockServer;
use omicron_test_utils::dev;
use sled_agent_types::early_networking::RackNetworkConfig;

struct Harness {
    handle: ScrimletReconcilers,
    networking_config_tx: watch::Sender<SystemNetworkingConfig>,
    mock_mgs: MockServer,
}

impl Harness {
    fn new(log: &Logger) -> Self {
        let (networking_config_tx, _) =
            watch::channel(SystemNetworkingConfig {
                rack_network_config: RackNetworkConfig {
                    rack_subnet: "fd00:1122:3344:0100::/56".parse().unwrap(),
                    infra_ip_first: "192.0.2.10".parse().unwrap(),
                    infra_ip_last: "192.0.2.100".parse().unwrap(),
                    ports: Vec::new(),
                    bgp: Vec::new(),
                    bfd: Vec::new(),
                },
                service_zone_nat_entries: None,
            });

        let mock_mgs = MockServer::start();
        let mut handle = ScrimletReconcilers::new(log);

        // Override how `handle` will attempt to contact MGS to point it at our
        // mock server instead.
        handle.override_make_mgs_client = {
            let url = format!("http://{}", mock_mgs.address());
            let client = gateway_client::Client::new_with_client(
                &url,
                // Tricky bit: we use tokio's paused time in tests below both
                // for consistency and test speed, but by default progenitor
                // clients have a 15-second connection timeout. That passes
                // _instantly_ if time is paused, which doesn't let us establish
                // real TCP connections to the httpmock server, causing tests to
                // spam connections as fast as possible. Pass a default reqwest
                // client, which has no such timeout, allowing connections to
                // wait (and succeed or fail as intended).
                reqwest::Client::new(),
                log.new(slog::o!("component" => "test-mgs-client")),
            );
            Some(Box::new(move || client.clone()))
        };

        Self { handle, networking_config_tx, mock_mgs }
    }

    fn sled_agent_networking_info(&self) -> SledAgentNetworkingInfo {
        SledAgentNetworkingInfo {
            system_networking_config_rx: self.networking_config_tx.subscribe(),
            switch_zone_underlay_ip:
                ThisSledSwitchZoneUnderlayIpAddr::TEST_FAKE,
        }
    }

    async fn wait_for_task_status<F>(&self, matches: F)
    where
        F: Fn(&ScrimletReconcilersStatus) -> bool,
    {
        let mut status = self.handle.status();
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            if matches(&status) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            status = self.handle.status();
        }
        panic!("timeout waiting for task status (got {status:?})");
    }

    fn mock_mgs_set_switch_slot(&self, slot: u16) -> Mock<'_> {
        let body =
            serde_json::json!({ "type": "switch", "slot": slot }).to_string();
        self.mock_mgs.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(200)
                .header("content-type", "application/json")
                .body(body);
        })
    }

    fn mock_mgs_set_503(&self) -> Mock<'_> {
        self.mock_mgs.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(503).header("content-type", "application/json").body(
                serde_json::json!({
                    "request_id": "test",
                    "message": "service unavailable",
                })
                .to_string(),
            );
        })
    }

    async fn wait_for_mock_to_be_called(&self, mock: &Mock<'_>) {
        // Tricky bit: we use tokio's paused time in tests below both for
        // consistency and test speed, but we need to wait for _real_ time for
        // httpmock to receive requests. Use `std::time::Instant` here so we
        // wait for 10 real seconds for a request to be received.
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            if mock.calls_async().await == 0 {
                // advance paused time...
                tokio::time::sleep(Duration::from_secs(1)).await;
                // and also _really_ sleep briefly to avoid spinning 100% CPU
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }

            // We got at least 1 call; assert and return.
            return mock.assert_async().await;
        }

        panic!("timed out wait for mock to be called");
    }
}

#[tokio::test]
#[should_panic(
    expected = "set_sled_agent_networking_info_once() called more than once"
)]
async fn calling_set_sled_agent_networking_info_once_multiple_times_panics() {
    let logctx = dev::test_setup_log(
        "calling_set_sled_agent_networking_info_once_multiple_times_panics",
    );
    let harness = Harness::new(&logctx.log);

    harness.handle.set_sled_agent_networking_info_once(
        harness.sled_agent_networking_info(),
    );
    harness.handle.set_sled_agent_networking_info_once(
        harness.sled_agent_networking_info(),
    );

    logctx.cleanup_successful();
}

// Happy-path test for non-scrimlets.
#[tokio::test]
async fn non_scrimlet_two_phase_initialization() {
    let logctx = dev::test_setup_log("non_scrimlet_two_phase_initialization");
    let harness = Harness::new(&logctx.log);

    // initial status
    assert_matches!(
        harness.handle.status(),
        ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo
    );

    harness.handle.set_sled_agent_networking_info_once(
        harness.sled_agent_networking_info(),
    );

    // terminal status for non-scrimlets
    assert_matches!(
        harness.handle.status(),
        ScrimletReconcilersStatus::DeterminingSwitchSlot(
            DetermineSwitchSlotStatus::NotScrimlet
        )
    );

    logctx.cleanup_successful();
}

// Happy-path test for scrimlets where we find out we're a scrimlet after
// getting our networking info.
#[tokio::test(start_paused = true)]
async fn scrimlet_three_phase_initialization_info_then_scrimlet() {
    let logctx = dev::test_setup_log(
        "scrimlet_three_phase_initialization_info_then_scrimlet",
    );
    let harness = Harness::new(&logctx.log);

    // Configure our mock MGS to claim to be switch slot 0.
    let success_mock = harness.mock_mgs_set_switch_slot(0);

    // initial status
    assert_matches!(
        harness.handle.status(),
        ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo
    );

    harness.handle.set_sled_agent_networking_info_once(
        harness.sled_agent_networking_info(),
    );

    // status before we become a scrimlet
    assert_matches!(
        harness.handle.status(),
        ScrimletReconcilersStatus::DeterminingSwitchSlot(
            DetermineSwitchSlotStatus::NotScrimlet
        )
    );

    harness.handle.set_scrimlet_status(ScrimletStatus::Scrimlet);

    // We should contact MGS, then transition to the running state.
    harness.wait_for_mock_to_be_called(&success_mock).await;
    harness
        .wait_for_task_status(|status| {
            matches!(status, ScrimletReconcilersStatus::Running { .. })
        })
        .await;

    logctx.cleanup_successful();
}

// Happy-path test for scrimlets where we find out we're a scrimlet before
// getting our networking info.
#[tokio::test(start_paused = true)]
async fn scrimlet_three_phase_initialization_scrimlet_then_info() {
    let logctx = dev::test_setup_log(
        "scrimlet_three_phase_initialization_scrimlet_then_info",
    );
    let harness = Harness::new(&logctx.log);

    // Configure our mock MGS to claim to be switch slot 0.
    let success_mock = harness.mock_mgs_set_switch_slot(0);

    // Become a scrimlet right away.
    harness.handle.set_scrimlet_status(ScrimletStatus::Scrimlet);

    // initial status
    assert_matches!(
        harness.handle.status(),
        ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo
    );

    harness.handle.set_sled_agent_networking_info_once(
        harness.sled_agent_networking_info(),
    );

    // We're already a scrimlet, so we contact MGS then transition to Running.
    harness.wait_for_mock_to_be_called(&success_mock).await;
    harness
        .wait_for_task_status(|status| {
            matches!(status, ScrimletReconcilersStatus::Running { .. })
        })
        .await;

    logctx.cleanup_successful();
}

// Scrimlet test case where MGS fails the first time we ask then succeeds later.
#[tokio::test(start_paused = true)]
async fn scrimlet_mgs_fails_first_attempt() {
    let logctx = dev::test_setup_log("scrimlet_mgs_fails_first_attempt");
    let harness = Harness::new(&logctx.log);

    // Configure our mock MGS to return an HTTP 503.
    let mut error_mock = harness.mock_mgs_set_503();

    // initial status
    assert_matches!(
        harness.handle.status(),
        ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo
    );

    harness.handle.set_sled_agent_networking_info_once(
        harness.sled_agent_networking_info(),
    );

    // status before we become a scrimlet
    assert_matches!(
        harness.handle.status(),
        ScrimletReconcilersStatus::DeterminingSwitchSlot(
            DetermineSwitchSlotStatus::NotScrimlet
        )
    );

    harness.handle.set_scrimlet_status(ScrimletStatus::Scrimlet);

    // Wait for our mock to receive a request and return a 503; we should then
    // see the `WaitingToRetry` state with an error.
    harness.wait_for_mock_to_be_called(&error_mock).await;
    // We should first see the "waiting to retry" status...
    harness
        .wait_for_task_status(|status| {
            matches!(
                status,
                ScrimletReconcilersStatus::DeterminingSwitchSlot(
                    DetermineSwitchSlotStatus::WaitingToRetry {
                        prev_attempt_err,
                    }
                ) if prev_attempt_err.contains("status: 503")
            )
        })
        .await;

    // Changing MGS to return success should transition us to `Running`.
    error_mock.delete();
    let success_mock = harness.mock_mgs_set_switch_slot(0);

    harness.wait_for_mock_to_be_called(&success_mock).await;
    harness
        .wait_for_task_status(|status| {
            matches!(status, ScrimletReconcilersStatus::Running { .. })
        })
        .await;

    logctx.cleanup_successful();
}

// Scrimlet test case where MGS fails, then we become "not a scrimlet", then we
// become a scrimlet again and MGS succeeds.
#[tokio::test(start_paused = true)]
async fn scrimlet_mgs_fails_then_we_become_not_a_scrimlet() {
    let logctx =
        dev::test_setup_log("scrimlet_mgs_fails_then_we_become_not_a_scrimlet");
    let harness = Harness::new(&logctx.log);

    // Configure our mock MGS to return an HTTP 503.
    let mut error_mock = harness.mock_mgs_set_503();

    // initial status
    assert_matches!(
        harness.handle.status(),
        ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo
    );

    harness.handle.set_sled_agent_networking_info_once(
        harness.sled_agent_networking_info(),
    );

    // status before we become a scrimlet
    assert_matches!(
        harness.handle.status(),
        ScrimletReconcilersStatus::DeterminingSwitchSlot(
            DetermineSwitchSlotStatus::NotScrimlet
        )
    );

    harness.handle.set_scrimlet_status(ScrimletStatus::Scrimlet);

    // Wait until we've failed to determine our switch slot.
    harness.wait_for_mock_to_be_called(&error_mock).await;
    harness
        .wait_for_task_status(|status| {
            matches!(
                status,
                ScrimletReconcilersStatus::DeterminingSwitchSlot(
                    DetermineSwitchSlotStatus::WaitingToRetry {
                        prev_attempt_err,
                    }
                ) if prev_attempt_err.contains("status: 503")
            )
        })
        .await;

    // Now sled-agent tells us we're not a scrimlet; we should transition back
    // to `NotScrimlet`.
    harness.handle.set_scrimlet_status(ScrimletStatus::NotScrimlet);
    harness
        .wait_for_task_status(|status| {
            matches!(
                status,
                ScrimletReconcilersStatus::DeterminingSwitchSlot(
                    DetermineSwitchSlotStatus::NotScrimlet
                )
            )
        })
        .await;

    // Reconfigure MGS to succeed.
    error_mock.delete();
    let success_mock = harness.mock_mgs_set_switch_slot(0);

    // Become a scrimlet again - we should transition through `ContactingMgs`
    // (with no previous error) then to `Running`.
    harness.handle.set_scrimlet_status(ScrimletStatus::Scrimlet);
    harness.wait_for_mock_to_be_called(&success_mock).await;
    harness
        .wait_for_task_status(|status| {
            matches!(status, ScrimletReconcilersStatus::Running { .. })
        })
        .await;

    logctx.cleanup_successful();
}
