// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ScrimletStatus;
use crate::ThisSledSwitchZoneUnderlayIpAddr;
use gateway_client::Client;
use gateway_types::component::SpType;
use omicron_common::address::MGS_PORT;
use sled_agent_types::early_networking::SwitchSlot;
use slog::Logger;
use slog::error;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::SetOnce;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;
use tokio::task::JoinHandle;

/// Newtype wrapper around [`SwitchSlot`]. This type is always the physical slot
/// of our own, local switch.
///
/// This information can only be determined by asking MGS inside our own switch
/// zone. An instance of this type can only be created if we are indeed a
/// scrimlet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ThisSledSwitchSlot(SwitchSlot);

impl PartialEq<SwitchSlot> for ThisSledSwitchSlot {
    fn eq(&self, other: &SwitchSlot) -> bool {
        self.0 == *other
    }
}

impl PartialEq<ThisSledSwitchSlot> for SwitchSlot {
    fn eq(&self, other: &ThisSledSwitchSlot) -> bool {
        *self == other.0
    }
}

impl ThisSledSwitchSlot {
    #[cfg(test)]
    pub(crate) const TEST_FAKE: Self = Self(SwitchSlot::Switch0);

    pub(crate) fn spawn_task_to_determine(
        scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        switch_slot_tx: Arc<SetOnce<Self>>,
        parent_log: &Logger,
    ) -> JoinHandle<()> {
        let baseurl = format!("http://[{switch_zone_underlay_ip}]:{MGS_PORT}");
        let client = Client::new(
            &baseurl,
            parent_log
                .new(slog::o!("component" => "ThisSledSwitchSlotMgsClient")),
        );
        let log = parent_log
            .new(slog::o!("component" => "ThisSledSwitchSlotDetermination"));

        tokio::spawn(async move {
            match determine_switch_slot(client, scrimlet_status_rx, &log).await
            {
                Ok(slot) => {
                    info!(
                        log, "determined this sled's switch slot";
                        "slot" => ?slot,
                    );

                    // `ThisSledSwitchSlot` cannot be constructed outside this
                    // module, so it's not possible for `switch_slot_tx` to
                    // already be set (unless someone calls this method twice
                    // with the same `SetOnce<_>`, which is a programmer error).
                    switch_slot_tx.set(slot).expect(
                        "only ThisSledSwitchSlot can populate this SetOnce",
                    );
                }
                Err(_recv_error) => {
                    error!(
                        log,
                        "failed to determine this sled's switch slot: input \
                         watch channel closed (unexpected except in tests)",
                    );
                }
            }
        })
    }
}

const MGS_RETRY_TIMEOUT: Duration = Duration::from_secs(5);

async fn determine_switch_slot(
    client: Client,
    mut scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
    log: &Logger,
) -> Result<ThisSledSwitchSlot, RecvError> {
    loop {
        // Wait until we become a scrimlet; there's no point in trying to
        // contact our switch zone if it doesn't exist.
        loop {
            let scrimlet_status = *scrimlet_status_rx.borrow_and_update();
            match scrimlet_status {
                ScrimletStatus::Scrimlet => break,
                ScrimletStatus::NotScrimlet => {
                    scrimlet_status_rx.changed().await?;
                    continue;
                }
            }
        }

        // We are a scrimlet - see if we know our own slot yet.
        match client.sp_local_switch_id().await.map(|resp| resp.into_inner()) {
            Ok(identity) => match (identity.type_, identity.slot) {
                (SpType::Switch, 0) => {
                    return Ok(ThisSledSwitchSlot(SwitchSlot::Switch0));
                }
                (SpType::Switch, 1) => {
                    return Ok(ThisSledSwitchSlot(SwitchSlot::Switch1));
                }
                (sp_type, sp_slot) => {
                    // We should never get any other response; if we do,
                    // something has gone very wrong with MGS. It's not likely
                    // retrying will fix this, but there isn't anything else we
                    // can do.
                    error!(
                        log,
                        "failed to determine this sled's switch slot: got \
                         unexpected identity; will retry";
                        "sp_type" => ?sp_type,
                        "sp_slot" => sp_slot,
                    );
                }
            },
            Err(err) => {
                warn!(
                    log,
                    "failed to determine this sled's switch slot; will retry";
                    InlineErrorChain::new(&err),
                );
            }
        }

        // Sleep briefly before retrying.
        tokio::time::sleep(MGS_RETRY_TIMEOUT).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use httpmock::Mock;
    use httpmock::MockServer;
    use sled_agent_types::early_networking::SwitchSlot;

    fn mock_mgs_client(server: &MockServer, log: &Logger) -> Client {
        let url = format!("http://{}", server.address());
        Client::new_with_client(
            &url,
            // Tricky bit: we use tokio's paused time in tests below both for
            // consistency and test speed, but by default progenitor clients
            // have a 15-second connection timeout. That passes _instantly_ if
            // time is paused, which doesn't let us establish real TCP
            // connections to the httpmock server, causing tests to spam
            // connections as fast as possible. Pass a default reqwest client,
            // which has no such timeout, allowing connections to wait (and
            // succeed or fail as intended).
            reqwest::Client::new(),
            log.new(slog::o!("component" => "test-mgs-client")),
        )
    }

    fn sp_identifier_json(sp_type: &str, slot: u16) -> String {
        serde_json::json!({ "type": sp_type, "slot": slot }).to_string()
    }

    // Several tests below spawn `determine_switch_slot()` onto a separate tokio
    // task, which means our MockServer will receive requests asynchronously.
    // This helper will wait up to a timeout for requests to be received.
    async fn wait_for_mock_then_assert(mock: &Mock<'_>) {
        // Tricky bit: we use tokio's paused time in tests below both for
        // consistency and test speed, but we need to wait for _real_ time for
        // httpmock to receive requests. Use `std::time::Instant` here so we
        // wait for 5 real seconds for a request to be received.
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            if mock.calls_async().await == 0 {
                // advance paused time...
                tokio::time::sleep(Duration::from_millis(10)).await;
                // and also _really_ sleep briefly to avoid spinning 100% CPU
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }

            // We got at least 1 call; assert and return.
            return mock.assert_async().await;
        }

        panic!("timed out wait for mock to be called");
    }

    #[tokio::test(start_paused = true)]
    async fn already_scrimlet_slot_0() {
        let logctx =
            omicron_test_utils::dev::test_setup_log("already_scrimlet_slot_0");
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(200)
                .header("content-type", "application/json")
                .body(sp_identifier_json("switch", 0));
        });

        let (_tx, rx) = watch::channel(ScrimletStatus::Scrimlet);
        let client = mock_mgs_client(&server, &logctx.log);

        let result = determine_switch_slot(client, rx, &logctx.log).await;
        assert_eq!(result.unwrap(), ThisSledSwitchSlot(SwitchSlot::Switch0));

        logctx.cleanup_successful();
    }

    #[tokio::test(start_paused = true)]
    async fn already_scrimlet_slot_1() {
        let logctx =
            omicron_test_utils::dev::test_setup_log("already_scrimlet_slot_1");
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(200)
                .header("content-type", "application/json")
                .body(sp_identifier_json("switch", 1));
        });

        let (_tx, rx) = watch::channel(ScrimletStatus::Scrimlet);
        let client = mock_mgs_client(&server, &logctx.log);

        let result = determine_switch_slot(client, rx, &logctx.log).await;
        assert_eq!(result.unwrap(), ThisSledSwitchSlot(SwitchSlot::Switch1));

        logctx.cleanup_successful();
    }

    #[tokio::test(start_paused = true)]
    async fn not_scrimlet_then_becomes_scrimlet() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "not_scrimlet_then_becomes_scrimlet",
        );
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(200)
                .header("content-type", "application/json")
                .body(sp_identifier_json("switch", 0));
        });

        let (tx, rx) = watch::channel(ScrimletStatus::NotScrimlet);
        let client = mock_mgs_client(&server, &logctx.log);
        let log = logctx.log.clone();

        let task = tokio::spawn(async move {
            determine_switch_slot(client, rx, &log).await
        });

        // Sleep briefly to give the task a chance to start (and observe that
        // it's not yet a scrimlet).
        tokio::time::sleep(Duration::from_secs(10)).await;

        // MGS should not have been contacted yet.
        assert_eq!(0, mock.calls());

        // Transition to Scrimlet.
        info!(logctx.log, "SETTING SCRIMLET");
        tx.send(ScrimletStatus::Scrimlet).unwrap();

        let result = task.await.unwrap();
        assert_eq!(result.unwrap(), ThisSledSwitchSlot(SwitchSlot::Switch0));

        logctx.cleanup_successful();
    }

    #[tokio::test(start_paused = true)]
    async fn channel_closed_while_not_scrimlet() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "channel_closed_while_not_scrimlet",
        );
        let server = MockServer::start();

        let (tx, rx) = watch::channel(ScrimletStatus::NotScrimlet);
        let client = mock_mgs_client(&server, &logctx.log);
        let log = logctx.log.clone();

        let task = tokio::spawn(async move {
            determine_switch_slot(client, rx, &log).await
        });

        // Let the task reach `changed().await`.
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Drop the sender to close the channel.
        drop(tx);

        let result = task.await.unwrap();
        assert_matches!(result, Err(RecvError { .. }));

        logctx.cleanup_successful();
    }

    #[tokio::test(start_paused = true)]
    async fn mgs_error_retries_then_succeeds() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "mgs_error_retries_then_succeeds",
        );
        let server = MockServer::start();

        // First call: return a 503 error.
        let mut error_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(503).header("content-type", "application/json").body(
                serde_json::json!({
                    "request_id": "test",
                    "message": "service unavailable",
                })
                .to_string(),
            );
        });

        let (_tx, rx) = watch::channel(ScrimletStatus::Scrimlet);
        let client = mock_mgs_client(&server, &logctx.log);
        let log = logctx.log.clone();

        let task = tokio::spawn(async move {
            determine_switch_slot(client, rx, &log).await
        });

        // Wait until the task contacts MGS and gets our 503.
        wait_for_mock_then_assert(&error_mock).await;

        // Remove the error mock and add a success mock.
        error_mock.delete();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(200)
                .header("content-type", "application/json")
                .body(sp_identifier_json("switch", 0));
        });

        let result = task.await.unwrap();
        assert_eq!(result.unwrap(), ThisSledSwitchSlot(SwitchSlot::Switch0));

        logctx.cleanup_successful();
    }

    #[tokio::test(start_paused = true)]
    async fn mgs_unexpected_identity_retries_then_succeeds() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "mgs_unexpected_identity_retries_then_succeeds",
        );
        let server = MockServer::start();

        // First call: return an unexpected identity (SpType::Sled).
        let mut bad_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(200)
                .header("content-type", "application/json")
                .body(sp_identifier_json("sled", 0));
        });

        let (_tx, rx) = watch::channel(ScrimletStatus::Scrimlet);
        let client = mock_mgs_client(&server, &logctx.log);
        let log = logctx.log.clone();

        let task = tokio::spawn(async move {
            determine_switch_slot(client, rx, &log).await
        });

        // Let the task hit the unexpected identity and start sleeping.
        wait_for_mock_then_assert(&bad_mock).await;

        // Remove the bad mock and add a correct one.
        bad_mock.delete();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(200)
                .header("content-type", "application/json")
                .body(sp_identifier_json("switch", 1));
        });

        let result = task.await.unwrap();
        assert_eq!(result.unwrap(), ThisSledSwitchSlot(SwitchSlot::Switch1));

        logctx.cleanup_successful();
    }

    // Test the most involved flow: we're a scrimlet but we get a failure
    // contacting MGS, then we become NotScrimlet. Later we go back to being a
    // Scrimlet and contacting MGS finally succeeds.
    #[tokio::test(start_paused = true)]
    async fn demoted_during_retry_waits_again() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "demoted_during_retry_waits_again",
        );
        let server = MockServer::start();

        // First call: return an error so we enter the retry sleep.
        let mut error_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(503).header("content-type", "application/json").body(
                serde_json::json!({
                    "request_id": "test",
                    "message": "service unavailable",
                })
                .to_string(),
            );
        });

        let (tx, rx) = watch::channel(ScrimletStatus::Scrimlet);
        let client = mock_mgs_client(&server, &logctx.log);
        let log = logctx.log.clone();

        let task = tokio::spawn(async move {
            determine_switch_slot(client, rx, &log).await
        });

        // Let the task hit the error and start the retry sleep.
        wait_for_mock_then_assert(&error_mock).await;

        // Demote to NotScrimlet while the task is sleeping.
        tx.send(ScrimletStatus::NotScrimlet).unwrap();

        // Remove the error mock and register a success mock for when we
        // eventually become a scrimlet again.
        error_mock.delete();
        let success_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/local/switch-id");
            then.status(200)
                .header("content-type", "application/json")
                .body(sp_identifier_json("switch", 1));
        });

        // Wait until the retry sleep expires; the task should not try to
        // contact the server again because it should see that we're now
        // `NotScrimlet`.
        tokio::time::sleep(MGS_RETRY_TIMEOUT + Duration::from_secs(10)).await;

        // We should not have gotten any calls.
        success_mock.assert_calls_async(0).await;

        // Set up successful mock then promote back to scrimlet.
        tx.send(ScrimletStatus::Scrimlet).unwrap();

        let result = task.await.unwrap();
        assert_eq!(result.unwrap(), ThisSledSwitchSlot(SwitchSlot::Switch1));

        logctx.cleanup_successful();
    }
}
