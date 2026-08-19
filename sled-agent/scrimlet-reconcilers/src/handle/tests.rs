// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use assert_matches::assert_matches;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ScrimletReconcilersStatus;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::test_util::LogContext;
use gateway_messages::SpPort;
use gateway_test_utils::setup::GatewayTestContext;
use httpmock::Mock;
use httpmock::MockServer;
use omicron_test_utils::dev;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkPorts;
use std::time::Duration;

// For "happy path" tests, we spin up a real MGS instances (pointed at a
// simulated SP).
//
// For "sad path" tests, we use a mock MGS so we can inject failures.
trait MgsFlavor {
    fn address(&self) -> SocketAddr;
    async fn teardown(self);
}

impl MgsFlavor for GatewayTestContext {
    fn address(&self) -> SocketAddr {
        SocketAddr::V6(GatewayTestContext::address(self))
    }

    async fn teardown(self) {
        GatewayTestContext::teardown(self).await
    }
}

impl MgsFlavor for MockServer {
    fn address(&self) -> SocketAddr {
        *MockServer::address(self)
    }

    async fn teardown(self) {}
}

struct Harness<T> {
    handle: ScrimletReconcilers,
    networking_config_tx: watch::Sender<SystemNetworkingConfig>,
    mgs: T,
    logctx: LogContext,
}

impl<T: MgsFlavor> Harness<T> {
    fn new_common(logctx: LogContext, mgs: T) -> Self {
        // The tests in this module don't care about the details of the network
        // config uplink ports, but we're required by construction to have a
        // nonempty set.
        fn any_uplink_ports() -> UplinkPorts {
            UplinkPorts::new(vec![PortConfig {
                routes: Vec::new(),
                addresses: Vec::new(),
                switch: SwitchSlot::Switch0,
                port: "does-not-matter".to_owned(),
                uplink_port_speed: LinkSpeed::Speed0G,
                uplink_port_fec: None,
                bgp_peers: Vec::new(),
                autoneg: false,
                lldp: None,
                tx_eq: None,
            }])
            .unwrap()
        }

        let (networking_config_tx, _) =
            watch::channel(SystemNetworkingConfig {
                rack_network_config: RackNetworkConfig {
                    rack_subnet: "fd00:1122:3344:0100::/56".parse().unwrap(),
                    infra_ip_first: "192.0.2.10".parse().unwrap(),
                    infra_ip_last: "192.0.2.100".parse().unwrap(),
                    ports: any_uplink_ports(),
                    bgp: Vec::new(),
                    bfd: Vec::new(),
                },
                blueprint_external_networking_config: None,
            });

        let handle = ScrimletReconcilers::new(&logctx.log);

        Self { handle, networking_config_tx, mgs, logctx }
    }

    async fn teardown(self) {
        self.logctx.cleanup_successful();
        self.mgs.teardown().await;
    }

    fn sled_agent_networking_info(&self) -> SledAgentNetworkingInfo {
        let dummy_addr = "0.0.0.0:0".parse().unwrap();
        SledAgentNetworkingInfo {
            system_networking_config_rx: self.networking_config_tx.subscribe(),
            mode: ScrimletReconcilersMode::Test {
                mgs_addr: self.mgs.address(),
                dpd_addr: dummy_addr,
                mgd_addr: dummy_addr,
            },
        }
    }

    async fn wait_for_task_status<F>(&self, matches: F)
    where
        F: Fn(&ScrimletReconcilersStatus) -> bool,
    {
        let mut status = self.handle.status();
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(30) {
            if matches(&status) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            status = self.handle.status();
        }
        panic!("timeout waiting for task status (got {status:?})");
    }
}

impl Harness<GatewayTestContext> {
    async fn new_real_mgs(logctx: LogContext) -> Self {
        let ctx = gateway_test_utils::setup::test_setup(
            logctx.test_name(),
            SpPort::One,
        )
        .await;
        Self::new_common(logctx, ctx)
    }
}

impl Harness<MockServer> {
    fn new_mock_mgs(logctx: LogContext) -> Self {
        Self::new_common(logctx, MockServer::start())
    }

    fn mock_mgs_set_switch_slot(&self, slot: u16) -> Mock<'_> {
        let body =
            serde_json::json!({ "type": "switch", "slot": slot }).to_string();
        self.mgs.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(200)
                .header("content-type", "application/json")
                .body(body);
        })
    }

    fn mock_mgs_set_503(&self) -> Mock<'_> {
        self.mgs.mock(|when, then| {
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
    // Set up a stderr logger - we're going to panic and won't have the
    // opportunity to clean up a file-based one.
    let log_config =
        ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Trace };
    let logctx = LogContext::new(
        "calling_set_sled_agent_networking_info_once_multiple_times_panics",
        &log_config,
    );
    let harness = Harness::new_mock_mgs(logctx);

    harness.handle.set_sled_agent_networking_info_once(
        harness.sled_agent_networking_info(),
    );
    harness.handle.set_sled_agent_networking_info_once(
        harness.sled_agent_networking_info(),
    );
}

// Happy-path test for non-scrimlets.
#[tokio::test]
async fn non_scrimlet_two_phase_initialization() {
    let logctx = dev::test_setup_log("non_scrimlet_two_phase_initialization");
    let harness = Harness::new_mock_mgs(logctx);

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

    harness.teardown().await;
}

// Happy-path test for scrimlets where we find out we're a scrimlet after
// getting our networking info.
#[tokio::test]
async fn scrimlet_three_phase_initialization_info_then_scrimlet() {
    let logctx = dev::test_setup_log(
        "scrimlet_three_phase_initialization_info_then_scrimlet",
    );
    let harness = Harness::new_real_mgs(logctx).await;

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

    harness
        .wait_for_task_status(|status| {
            matches!(status, ScrimletReconcilersStatus::Running { .. })
        })
        .await;

    harness.teardown().await;
}

// Happy-path test for scrimlets where we find out we're a scrimlet before
// getting our networking info.
#[tokio::test]
async fn scrimlet_three_phase_initialization_scrimlet_then_info() {
    let logctx = dev::test_setup_log(
        "scrimlet_three_phase_initialization_scrimlet_then_info",
    );
    let harness = Harness::new_real_mgs(logctx).await;

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
    harness
        .wait_for_task_status(|status| {
            matches!(status, ScrimletReconcilersStatus::Running { .. })
        })
        .await;

    harness.teardown().await;
}

// Scrimlet test case where MGS fails the first time we ask then succeeds later.
#[tokio::test(start_paused = true)]
async fn scrimlet_mgs_fails_first_attempt() {
    let logctx = dev::test_setup_log("scrimlet_mgs_fails_first_attempt");
    let harness = Harness::new_mock_mgs(logctx);

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

    harness.teardown().await;
}

// Scrimlet test case where MGS fails, then we become "not a scrimlet", then we
// become a scrimlet again and MGS succeeds.
#[tokio::test(start_paused = true)]
async fn scrimlet_mgs_fails_then_we_become_not_a_scrimlet() {
    let logctx =
        dev::test_setup_log("scrimlet_mgs_fails_then_we_become_not_a_scrimlet");
    let harness = Harness::new_mock_mgs(logctx);

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

    harness.teardown().await;
}
