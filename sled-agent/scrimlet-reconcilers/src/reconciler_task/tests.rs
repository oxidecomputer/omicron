// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use assert_matches::assert_matches;
use sled_agent_types::early_networking::RackNetworkConfig;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::Instant;

struct MockReconciler {
    do_reconciliation_calls: Arc<Mutex<Vec<SystemNetworkingConfig>>>,
    do_reconciliation_results: mpsc::UnboundedReceiver<String>,
}

impl Reconciler for MockReconciler {
    type Status = String;

    const LOGGER_COMPONENT_NAME: &'static str = "MockReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(
        _switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        _switch_slot: ThisSledSwitchSlot,
        _parent_log: &Logger,
    ) -> Self {
        unimplemented!("not called by tests")
    }

    async fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        _log: &Logger,
    ) -> Self::Status {
        self.do_reconciliation_calls
            .lock()
            .unwrap()
            .push(system_networking_config.clone());
        self.do_reconciliation_results
            .recv()
            .await
            .expect("test never closes sending side of channel")
    }
}

fn test_system_networking_config_1() -> SystemNetworkingConfig {
    SystemNetworkingConfig {
        rack_network_config: RackNetworkConfig {
            rack_subnet: "fd00:1122:3344:0100::/56".parse().unwrap(),
            infra_ip_first: "192.0.2.10".parse().unwrap(),
            infra_ip_last: "192.0.2.100".parse().unwrap(),
            ports: Vec::new(),
            bgp: Vec::new(),
            bfd: Vec::new(),
        },
        service_zone_nat_entries: None,
    }
}

fn test_system_networking_config_2() -> SystemNetworkingConfig {
    SystemNetworkingConfig {
        rack_network_config: RackNetworkConfig {
            rack_subnet: "fd00:aabb:ccdd:0200::/56".parse().unwrap(),
            infra_ip_first: "192.0.2.20".parse().unwrap(),
            infra_ip_last: "192.0.2.200".parse().unwrap(),
            ports: Vec::new(),
            bgp: Vec::new(),
            bfd: Vec::new(),
        },
        service_zone_nat_entries: None,
    }
}

struct Harness {
    task: ReconcilerTaskHandle<MockReconciler>,
    scrimlet_status_tx: watch::Sender<ScrimletStatus>,
    system_networking_config_tx: watch::Sender<SystemNetworkingConfig>,
    do_reconciliation_results_tx: mpsc::UnboundedSender<String>,
    do_reconciliation_calls: Arc<Mutex<Vec<SystemNetworkingConfig>>>,
}

impl Harness {
    const WAIT_FOR_STATUS_CHECK_INTERVAL: Duration = Duration::from_millis(100);

    fn new(log: &Logger) -> Self {
        let (scrimlet_status_tx, scrimlet_status_rx) =
            watch::channel(ScrimletStatus::Scrimlet);
        let (system_networking_config_tx, system_networking_config_rx) =
            watch::channel(test_system_networking_config_1());

        let (do_reconciliation_results_tx, do_reconciliation_results) =
            mpsc::unbounded_channel();
        let do_reconciliation_calls = Arc::new(Mutex::new(Vec::new()));

        let task = {
            let do_reconciliation_calls = Arc::clone(&do_reconciliation_calls);
            ReconcilerTaskHandle::spawn_impl(
                scrimlet_status_rx,
                system_networking_config_rx,
                ThisSledSwitchZoneUnderlayIpAddr::TEST_FAKE,
                ThisSledSwitchSlot::TEST_FAKE,
                log,
                |_ip, _slot, _log| MockReconciler {
                    do_reconciliation_calls,
                    do_reconciliation_results,
                },
            )
        };

        Self {
            task,
            scrimlet_status_tx,
            system_networking_config_tx,
            do_reconciliation_results_tx,
            do_reconciliation_calls,
        }
    }

    fn set_scrimlet_status(&self, status: ScrimletStatus) {
        self.scrimlet_status_tx.send_modify(|s| {
            *s = status;
        });
    }

    async fn wait_for_do_reconciliation_call_count(&self, count: usize) {
        let mut last_seen = self.do_reconciliation_calls.lock().unwrap().len();
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if last_seen == count {
                return;
            }
            tokio::time::sleep(Self::WAIT_FOR_STATUS_CHECK_INTERVAL).await;
            last_seen = self.do_reconciliation_calls.lock().unwrap().len();
        }
        panic!(
            "timeout waiting for do_reconciliation call count {count} \
             (got {last_seen})"
        );
    }

    async fn wait_for_task_status<F>(
        &self,
        description: &str,
        matches: F,
    ) -> ReconcilerStatus<String>
    where
        F: Fn(&ReconcilerCurrentStatus) -> bool,
    {
        let mut status = self.task.status();
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if matches(&status.current_status) {
                return status;
            }
            tokio::time::sleep(Self::WAIT_FOR_STATUS_CHECK_INTERVAL).await;
            status = self.task.status();
        }
        panic!(
            "timeout waiting for task status {description} (got {status:?})"
        );
    }

    async fn wait_for_task_status_no_longer_a_scrimlet(
        &self,
    ) -> ReconcilerStatus<String> {
        self.wait_for_task_status("Inert(NoLongerAScrimlet)", |status| {
            matches!(
                status,
                ReconcilerCurrentStatus::Inert(
                    ReconcilerInertReason::NoLongerAScrimlet
                )
            )
        })
        .await
    }

    async fn wait_for_task_status_idle(&self) -> ReconcilerStatus<String> {
        self.wait_for_task_status("Idle", |status| {
            matches!(status, ReconcilerCurrentStatus::Idle)
        })
        .await
    }

    async fn shutdown_cleanly(self) {
        let Self {
            task, scrimlet_status_tx, do_reconciliation_results_tx, ..
        } = self;

        // Dropping this watch channel should cause the task to exit.
        mem::drop(scrimlet_status_tx);

        task._task.await.expect("task didn't panic");
        let final_status = task.status_rx.borrow();
        assert_matches!(
            final_status.current_status,
            ReconcilerCurrentStatus::Inert(
                ReconcilerInertReason::TaskExitedUnexpectedly
            )
        );

        // Explicitly drop this _after_ the task exits so we're guaranteed
        // not to hit the `.expect()` in
        // `MockReconciler::do_reconciliation()` above.
        mem::drop(do_reconciliation_results_tx);
    }
}

// Test the first activation.
#[tokio::test(start_paused = true)]
async fn first_activation() {
    let logctx = omicron_test_utils::dev::test_setup_log("first_activation");
    let harness = Harness::new(&logctx.log);

    // Confirm we start in Idle.
    assert_matches!(
        harness.task.status().current_status,
        ReconcilerCurrentStatus::Idle
    );

    // Task should call do_reconciliation() for the first time.
    harness.wait_for_do_reconciliation_call_count(1).await;
    harness.do_reconciliation_results_tx.send("first".to_string()).unwrap();
    let status = harness.wait_for_task_status_idle().await;

    let completion =
        status.last_completion.expect("last_completion should be preserved");
    assert_eq!(completion.activation_count, 0);
    assert_eq!(completion.status, "first");
    assert_matches!(
        completion.activation_reason,
        ReconcilerActivationReason::Startup
    );

    harness.shutdown_cleanly().await;
    logctx.cleanup_successful();
}

// Test: after completing a reconciliation and reaching the select!,
// setting NotScrimlet causes the task to loop back to
// wait_if_this_sled_is_no_longer_a_scrimlet and go inert — without
// performing another reconciliation.
#[tokio::test(start_paused = true)]
async fn scrimlet_becomes_not_scrimlet_during_select() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "scrimlet_becomes_not_scrimlet_during_select",
    );
    let harness = Harness::new(&logctx.log);

    // Complete the first reconciliation so the task reaches the select!.
    harness.wait_for_do_reconciliation_call_count(1).await;
    harness.do_reconciliation_results_tx.send("first".to_string()).unwrap();
    harness.wait_for_task_status_idle().await;

    // Become NotScrimlet → the select! fires with ScrimletStatusChanged,
    // the loop goes back to wait_if_this_sled_is_no_longer_a_scrimlet, and the
    // task becomes Inert(NoLongerAScrimlet).
    harness.set_scrimlet_status(ScrimletStatus::NotScrimlet);
    let status = harness.wait_for_task_status_no_longer_a_scrimlet().await;

    // No additional do_reconciliation call should have happened: the
    // ScrimletStatusChanged activation saw NotScrimlet and went inert
    // instead of reconciling.
    assert_eq!(harness.do_reconciliation_calls.lock().unwrap().len(), 1);

    // last_completion from the prior run should still be present.
    let completion =
        status.last_completion.expect("last_completion should be preserved");
    assert_eq!(completion.activation_count, 0);
    assert_eq!(completion.status, "first");

    harness.shutdown_cleanly().await;
    logctx.cleanup_successful();
}

// Test: after the first reconciliation completes, changing the
// SystemNetworkingConfig triggers a second reconciliation with
// activation_reason = SystemNetworkingConfigChanged and the new config.
#[tokio::test(start_paused = true)]
async fn system_networking_config_change_triggers_re_reconciliation() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "system_networking_config_change_triggers_re_reconciliation",
    );
    let harness = Harness::new(&logctx.log);

    // Wait for the first do_reconciliation call (Startup).
    harness.wait_for_do_reconciliation_call_count(1).await;

    // Complete the first reconciliation.
    harness.do_reconciliation_results_tx.send("first".to_string()).unwrap();
    harness.wait_for_task_status_idle().await;

    // Send a new SystemNetworkingConfig.
    let first_config = test_system_networking_config_1();
    let second_config = test_system_networking_config_2();
    assert_ne!(first_config, second_config);
    harness.system_networking_config_tx.send(second_config.clone()).unwrap();

    // Wait for the second do_reconciliation call.
    harness.wait_for_do_reconciliation_call_count(2).await;

    // The second call should have received the new config.
    let received_configs =
        harness.do_reconciliation_calls.lock().unwrap().clone();
    assert_eq!(received_configs.len(), 2);
    assert_eq!(received_configs[0], first_config);
    assert_eq!(received_configs[1], second_config);

    // Status should be Running with SystemNetworkingConfigChanged.
    let status = harness.task.status();
    match &status.current_status {
        ReconcilerCurrentStatus::Running(running) => {
            assert_matches!(
                running.activation_reason(),
                ReconcilerActivationReason::SystemNetworkingConfigChanged
            );
        }
        other => panic!("expected Running status, got {other:?}"),
    }

    // Complete the second reconciliation.
    harness.do_reconciliation_results_tx.send("second".to_string()).unwrap();
    let status = harness.wait_for_task_status_idle().await;

    let completion =
        status.last_completion.expect("should have last_completion");
    assert_matches!(
        completion.activation_reason,
        ReconcilerActivationReason::SystemNetworkingConfigChanged
    );
    assert_eq!(completion.activation_count, 1);
    assert_eq!(completion.status, "second");

    harness.shutdown_cleanly().await;
    logctx.cleanup_successful();
}

// Test: after the first reconciliation completes, the periodic timer
// fires after RE_RECONCILE_INTERVAL and triggers a second
// reconciliation with activation_reason = PeriodicTimer.
#[tokio::test(start_paused = true)]
async fn periodic_timer_triggers_re_reconciliation() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "periodic_timer_triggers_re_reconciliation",
    );
    let harness = Harness::new(&logctx.log);

    // Complete the first reconciliation (Startup).
    harness.wait_for_do_reconciliation_call_count(1).await;
    harness.do_reconciliation_results_tx.send("first".to_string()).unwrap();
    harness.wait_for_task_status_idle().await;

    // Advance time just short of the interval — no second call yet.
    tokio::time::advance(
        MockReconciler::RE_RECONCILE_INTERVAL - Duration::from_millis(1),
    )
    .await;
    assert_eq!(harness.do_reconciliation_calls.lock().unwrap().len(), 1);

    // Advance past the interval — periodic timer fires.
    tokio::time::advance(Duration::from_millis(1)).await;
    harness.wait_for_do_reconciliation_call_count(2).await;

    // Status should be Running with PeriodicTimer.
    let status = harness.task.status();
    match &status.current_status {
        ReconcilerCurrentStatus::Running(running) => {
            assert_matches!(
                running.activation_reason(),
                ReconcilerActivationReason::PeriodicTimer
            );
        }
        other => panic!("expected Running status, got {other:?}"),
    }

    // Complete and verify.
    harness.do_reconciliation_results_tx.send("periodic".to_string()).unwrap();
    let status = harness.wait_for_task_status_idle().await;
    let completion =
        status.last_completion.expect("should have last_completion");
    assert_matches!(
        completion.activation_reason,
        ReconcilerActivationReason::PeriodicTimer
    );
    assert_eq!(completion.activation_count, 1);
    assert_eq!(completion.status, "periodic");

    harness.shutdown_cleanly().await;
    logctx.cleanup_successful();
}

// Test: if the SystemNetworkingConfig changes while do_reconciliation is
// in-flight, the task should notice when it reaches the select! and
// immediately perform another reconciliation with
// activation_reason = SystemNetworkingConfigChanged using the latest config.
#[tokio::test(start_paused = true)]
async fn config_change_during_inflight_reconciliation() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "config_change_during_inflight_reconciliation",
    );
    let harness = Harness::new(&logctx.log);

    // Wait for the first do_reconciliation call (Startup) to be entered.
    harness.wait_for_do_reconciliation_call_count(1).await;

    // While the first reconciliation is still in-flight, change the
    // config. The task won't see this until it finishes and hits the
    // select!.
    harness
        .system_networking_config_tx
        .send(test_system_networking_config_2())
        .unwrap();

    // Complete the first reconciliation.
    harness.do_reconciliation_results_tx.send("first".to_string()).unwrap();

    // The task should immediately start a second reconciliation because
    // system_networking_config_rx.changed() fires in the select!. Because we
    // have time paused, the elapsed time should be exactly one check interval
    // of our test harness.
    let before = tokio::time::Instant::now();
    harness.wait_for_do_reconciliation_call_count(2).await;
    assert_eq!(before.elapsed(), Harness::WAIT_FOR_STATUS_CHECK_INTERVAL);

    // The second call should have received the new config (via
    // borrow_and_update()).
    let received_configs =
        harness.do_reconciliation_calls.lock().unwrap().clone();
    assert_eq!(received_configs[0], test_system_networking_config_1());
    assert_eq!(received_configs[1], test_system_networking_config_2());

    // Status should be Running with SystemNetworkingConfigChanged.
    let status = harness.task.status();
    match &status.current_status {
        ReconcilerCurrentStatus::Running(running) => {
            assert_matches!(
                running.activation_reason(),
                ReconcilerActivationReason::SystemNetworkingConfigChanged
            );
        }
        other => panic!("expected Running status, got {other:?}"),
    }

    // Complete the second reconciliation and verify.
    harness.do_reconciliation_results_tx.send("second".to_string()).unwrap();
    let status = harness.wait_for_task_status_idle().await;
    let completion =
        status.last_completion.expect("should have last_completion");
    assert_matches!(
        completion.activation_reason,
        ReconcilerActivationReason::SystemNetworkingConfigChanged
    );
    assert_eq!(completion.activation_count, 1);
    assert_eq!(completion.status, "second");

    harness.shutdown_cleanly().await;
    logctx.cleanup_successful();
}

// Test: full scrimlet status round-trip. Start as scrimlet, complete
// reconciliation #0 (Startup). Set NotScrimlet → task goes inert. Set
// Scrimlet again → reconciliation #1 fires with activation_reason =
// ScrimletStatusChanged and activation_count = 1.
#[tokio::test(start_paused = true)]
async fn scrimlet_status_round_trip() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("scrimlet_status_round_trip");
    let harness = Harness::new(&logctx.log);

    // First reconciliation (Startup).
    harness.wait_for_do_reconciliation_call_count(1).await;
    harness.do_reconciliation_results_tx.send("first".to_string()).unwrap();
    let status = harness.wait_for_task_status_idle().await;
    let completion =
        status.last_completion.expect("should have last_completion");
    assert_matches!(
        completion.activation_reason,
        ReconcilerActivationReason::Startup
    );
    assert_eq!(completion.activation_count, 0);

    // Become NotScrimlet → task should go inert.
    harness.set_scrimlet_status(ScrimletStatus::NotScrimlet);
    harness.wait_for_task_status_no_longer_a_scrimlet().await;

    // No additional do_reconciliation call should have happened.
    assert_eq!(harness.do_reconciliation_calls.lock().unwrap().len(), 1);

    // Become Scrimlet again → reconciliation #1 fires.
    harness.set_scrimlet_status(ScrimletStatus::Scrimlet);
    harness.wait_for_do_reconciliation_call_count(2).await;

    // Status should be Running with ScrimletStatusChanged.
    let status = harness.task.status();
    match &status.current_status {
        ReconcilerCurrentStatus::Running(running) => {
            assert_matches!(
                running.activation_reason(),
                ReconcilerActivationReason::ScrimletStatusChanged
            );
        }
        other => panic!("expected Running status, got {other:?}"),
    }

    // Complete the second reconciliation and check last_completion.
    harness.do_reconciliation_results_tx.send("second".to_string()).unwrap();
    let status = harness.wait_for_task_status_idle().await;
    let completion =
        status.last_completion.expect("should have last_completion");
    assert_matches!(
        completion.activation_reason,
        ReconcilerActivationReason::ScrimletStatusChanged
    );
    assert_eq!(completion.activation_count, 1);
    assert_eq!(completion.status, "second");

    harness.shutdown_cleanly().await;
    logctx.cleanup_successful();
}

// Test: dropping the system_networking_config sender while the task is in
// the select! (after completing a reconciliation) causes the task to
// exit with TaskExitedUnexpectedly.
#[tokio::test(start_paused = true)]
async fn channel_closure_system_networking_config_during_select() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "channel_closure_system_networking_config_during_select",
    );
    let harness = Harness::new(&logctx.log);

    // Complete one reconciliation so the task reaches the select!.
    harness.wait_for_do_reconciliation_call_count(1).await;
    harness.do_reconciliation_results_tx.send("done".to_string()).unwrap();
    harness.wait_for_task_status_idle().await;

    // Drop the system_networking_config sender. This closes the watch channel,
    // which causes the `system_networking_config_rx.changed()` arm in the
    // select! to return Err(RecvError), causing the task to exit.
    let Harness {
        task,
        scrimlet_status_tx,
        system_networking_config_tx,
        do_reconciliation_results_tx,
        do_reconciliation_calls,
    } = harness;

    mem::drop(system_networking_config_tx);

    // Wait for the task to exit and verify the final status.
    task._task.await.expect("task didn't panic");
    let final_status = task.status_rx.borrow();
    assert_matches!(
        final_status.current_status,
        ReconcilerCurrentStatus::Inert(
            ReconcilerInertReason::TaskExitedUnexpectedly
        )
    );

    // do_reconciliation should have been called exactly once (the initial
    // Startup reconciliation); no second call after the channel closed.
    assert_eq!(do_reconciliation_calls.lock().unwrap().len(), 1);

    mem::drop(scrimlet_status_tx);
    mem::drop(do_reconciliation_results_tx);

    logctx.cleanup_successful();
}

// Test: dropping the scrimlet_status sender while the task is in the
// select! (after completing a reconciliation) causes the task to exit
// with TaskExitedUnexpectedly.
#[tokio::test(start_paused = true)]
async fn channel_closure_scrimlet_status_during_select() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "channel_closure_scrimlet_status_during_select",
    );
    let harness = Harness::new(&logctx.log);

    // Complete one reconciliation so the task reaches the select!.
    harness.wait_for_do_reconciliation_call_count(1).await;
    harness.do_reconciliation_results_tx.send("done".to_string()).unwrap();
    harness.wait_for_task_status_idle().await;

    // Destructure the harness so we can drop the scrimlet_status sender
    // while still holding the other pieces we need.
    let Harness {
        task,
        scrimlet_status_tx,
        do_reconciliation_results_tx,
        do_reconciliation_calls,
        ..
    } = harness;

    // Drop the scrimlet_status sender. This closes the watch channel,
    // which causes the `scrimlet_status_rx.changed()` arm in the
    // select! to return Err(RecvError), causing the task to exit.
    mem::drop(scrimlet_status_tx);

    // Wait for the task to exit and verify the final status.
    task._task.await.expect("task didn't panic");
    let final_status = task.status_rx.borrow();
    assert_matches!(
        final_status.current_status,
        ReconcilerCurrentStatus::Inert(
            ReconcilerInertReason::TaskExitedUnexpectedly
        )
    );

    // do_reconciliation should have been called exactly once (the initial
    // Startup reconciliation); no second call after the channel closed.
    assert_eq!(do_reconciliation_calls.lock().unwrap().len(), 1);

    // Explicitly drop after the task exits so we can't hit the .expect()
    // in MockReconciler::do_reconciliation().
    mem::drop(do_reconciliation_results_tx);

    logctx.cleanup_successful();
}

// Test: dropping the scrimlet_status sender while the task is blocked in
// wait_if_this_sled_is_no_longer_a_scrimlet() waiting for the switch slot
// causes the task to exit with TaskExitedUnexpectedly.
#[tokio::test(start_paused = true)]
async fn channel_closure_scrimlet_status_during_not_scrimlet() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "channel_closure_scrimlet_status_during_not_scrimlet",
    );
    let harness = Harness::new(&logctx.log);

    // Set ourselves as "not a scrimlet".
    harness.set_scrimlet_status(ScrimletStatus::NotScrimlet);

    // Wait for the task to reach Inert(NoLongerAScrimlet).
    harness.wait_for_task_status_no_longer_a_scrimlet().await;

    // Destructure the harness so we can drop the scrimlet_status sender
    // while still holding the other pieces we need.
    let Harness {
        task,
        scrimlet_status_tx,
        system_networking_config_tx,
        do_reconciliation_results_tx,
        do_reconciliation_calls,
    } = harness;

    // Drop the scrimlet_status sender. This closes the watch channel,
    // which causes scrimlet_status_rx.changed().await to return
    // Err(RecvError) inside wait_if_this_sled_is_no_longer_a_scrimlet(),
    // causing the task to exit.
    mem::drop(scrimlet_status_tx);

    // Wait for the task to exit and verify the final status.
    task._task.await.expect("task didn't panic");
    let final_status = task.status_rx.borrow();
    assert_matches!(
        final_status.current_status,
        ReconcilerCurrentStatus::Inert(
            ReconcilerInertReason::TaskExitedUnexpectedly
        )
    );

    // do_reconciliation() should never have been called.
    assert_eq!(do_reconciliation_calls.lock().unwrap().len(), 0);

    // Explicitly drop after the task exits.
    mem::drop(do_reconciliation_results_tx);
    mem::drop(system_networking_config_tx);

    logctx.cleanup_successful();
}

// Test: dropping the system_networking_config sender while the task is blocked
// in wait_if_this_sled_is_no_longer_a_scrimlet() causes the task to exit with
// TaskExitedUnexpectedly.
#[tokio::test(start_paused = true)]
async fn channel_closure_system_networking_config_during_not_scrimlet() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "channel_closure_system_networking_config_during_not_scrimlet",
    );
    let harness = Harness::new(&logctx.log);

    // Set ourselves as "not a scrimlet".
    harness.set_scrimlet_status(ScrimletStatus::NotScrimlet);

    // Wait for the task to reach Inert(NoLongerAScrimlet).
    harness.wait_for_task_status_no_longer_a_scrimlet().await;

    // Destructure the harness so we can drop the system_networking_config
    // sender while still holding the other pieces we need.
    let Harness {
        task,
        scrimlet_status_tx,
        system_networking_config_tx,
        do_reconciliation_results_tx,
        do_reconciliation_calls,
    } = harness;

    // Drop the system_networking_config sender. The task is currently blocked
    // waiting to become a scrimlet again, but also monitors this channel for
    // closure; it should notice and exit.
    mem::drop(system_networking_config_tx);

    // Wait for the task to exit and verify the final status.
    task._task.await.expect("task didn't panic");
    let final_status = task.status_rx.borrow();
    assert_matches!(
        final_status.current_status,
        ReconcilerCurrentStatus::Inert(
            ReconcilerInertReason::TaskExitedUnexpectedly
        )
    );

    // do_reconciliation() should never have been called.
    assert_eq!(do_reconciliation_calls.lock().unwrap().len(), 0);

    // Explicitly drop after the task exits.
    mem::drop(do_reconciliation_results_tx);
    mem::drop(scrimlet_status_tx);

    logctx.cleanup_successful();
}
