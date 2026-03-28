// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use assert_matches::assert_matches;
use std::mem;
use std::net::Ipv6Addr;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::Instant;

struct MockReconciler {
    do_reconciliation_calls: Arc<Mutex<Vec<RackNetworkConfig>>>,
    do_reconciliation_results: mpsc::UnboundedReceiver<String>,
}

impl Reconciler for MockReconciler {
    type Status = String;

    const LOGGER_COMPONENT_NAME: &'static str = "MockReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(
        _switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        _parent_log: &Logger,
    ) -> Self {
        unimplemented!("not called by tests")
    }

    async fn do_reconciliation(
        &mut self,
        rack_network_config: &RackNetworkConfig,
        _log: &Logger,
    ) -> Self::Status {
        self.do_reconciliation_calls
            .lock()
            .unwrap()
            .push(rack_network_config.clone());
        self.do_reconciliation_results
            .recv()
            .await
            .expect("test never closes sending side of channel")
    }
}

fn test_rack_network_config_1() -> RackNetworkConfig {
    RackNetworkConfig {
        rack_subnet: "fd00:1122:3344:0100::/56".parse().unwrap(),
        infra_ip_first: "192.0.2.10".parse().unwrap(),
        infra_ip_last: "192.0.2.100".parse().unwrap(),
        ports: Vec::new(),
        bgp: Vec::new(),
        bfd: Vec::new(),
    }
}

fn test_rack_network_config_2() -> RackNetworkConfig {
    RackNetworkConfig {
        rack_subnet: "fd00:aabb:ccdd:0200::/56".parse().unwrap(),
        infra_ip_first: "192.0.2.20".parse().unwrap(),
        infra_ip_last: "192.0.2.200".parse().unwrap(),
        ports: Vec::new(),
        bgp: Vec::new(),
        bfd: Vec::new(),
    }
}

struct Harness {
    task: ReconcilerTaskHandle<MockReconciler>,
    scrimlet_status_tx: watch::Sender<ScrimletStatus>,
    do_reconciliation_results_tx: mpsc::UnboundedSender<String>,
    do_reconciliation_calls: Arc<Mutex<Vec<RackNetworkConfig>>>,
    prereqs: Arc<SetOnce<ScrimletReconcilersPrereqs>>,
}

impl Harness {
    fn new(log: &Logger) -> Self {
        let (scrimlet_status_tx, scrimlet_status_rx) =
            watch::channel(ScrimletStatus::NotScrimlet);
        let prereqs = Arc::new(SetOnce::new());

        let (do_reconciliation_results_tx, do_reconciliation_results) =
            mpsc::unbounded_channel();
        let do_reconciliation_calls = Arc::new(Mutex::new(Vec::new()));

        let task = {
            let do_reconciliation_calls = Arc::clone(&do_reconciliation_calls);
            ReconcilerTaskHandle::spawn_with_inner_constructor(
                scrimlet_status_rx,
                Arc::clone(&prereqs),
                log,
                |_ip, _log| MockReconciler {
                    do_reconciliation_calls,
                    do_reconciliation_results,
                },
            )
        };

        Self {
            task,
            scrimlet_status_tx,
            do_reconciliation_results_tx,
            do_reconciliation_calls,
            prereqs,
        }
    }

    fn provide_prereqs(
        &self,
        ip: ThisSledSwitchZoneUnderlayIpAddr,
    ) -> watch::Sender<RackNetworkConfig> {
        let (tx, rx) = watch::channel(test_rack_network_config_1());
        self.prereqs
            .set(ScrimletReconcilersPrereqs {
                rack_network_config_rx: rx,
                switch_zone_underlay_ip: ip,
            })
            .expect("set_switch_zone_ip() called only once per harness");
        tx
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
            tokio::time::sleep(Duration::from_millis(100)).await;
            last_seen = self.do_reconciliation_calls.lock().unwrap().len();
        }
        panic!(
            "timeout waiting for do_reconciliation call count {count} \
             (got {last_seen})"
        );
    }

    async fn wait_for_task_status_not_a_scrimlet(
        &self,
    ) -> ReconcilerStatus<String> {
        let mut status = self.task.status();
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if matches!(
                status.current_status,
                ReconcilerCurrentStatus::Inert(
                    ReconcilerInertReason::NotAScrimlet
                )
            ) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            status = self.task.status();
        }
        panic!(
            "timeout waiting for task status Inert(NotAScrimlet) \
             (got {status:?})"
        );
    }

    async fn wait_for_task_status_idle(&self) -> ReconcilerStatus<String> {
        let mut status = self.task.status();
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if matches!(status.current_status, ReconcilerCurrentStatus::Idle) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            status = self.task.status();
        }
        panic!("timeout waiting for task status Idle (got {status:?})");
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

// Test: immediately after construction (before prereqs are provided),
// the task's status is Inert(WaitingForPrereqs) with no last_completion.
#[tokio::test(start_paused = true)]
async fn initial_status_is_waiting_for_prereqs() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "initial_status_is_waiting_for_prereqs",
    );
    let harness = Harness::new(&logctx.log);

    let status = harness.task.status();
    assert_matches!(
        status.current_status,
        ReconcilerCurrentStatus::Inert(
            ReconcilerInertReason::WaitingForPrereqs
        )
    );
    assert!(status.last_completion.is_none());

    // do_reconciliation should never have been called.
    assert_eq!(harness.do_reconciliation_calls.lock().unwrap().len(), 0);

    harness.shutdown_cleanly().await;
    logctx.cleanup_successful();
}

// Test: prereqs arrive while scrimlet status is NotScrimlet (the
// default). The task should transition from WaitingForPrereqs to
// Inert(NotAScrimlet) without ever calling do_reconciliation.
#[tokio::test(start_paused = true)]
async fn prereqs_arrive_but_not_scrimlet() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "prereqs_arrive_but_not_scrimlet",
    );
    let harness = Harness::new(&logctx.log);

    // Confirm we start in WaitingForPrereqs.
    assert_matches!(
        harness.task.status().current_status,
        ReconcilerCurrentStatus::Inert(
            ReconcilerInertReason::WaitingForPrereqs
        )
    );

    // Provide prereqs but leave scrimlet status as NotScrimlet (the
    // default from Harness::new).
    let _rack_network_config_tx = harness.provide_prereqs(
        ThisSledSwitchZoneUnderlayIpAddr::for_test(Ipv6Addr::LOCALHOST),
    );

    // Task should transition to Inert(NotAScrimlet).
    harness.wait_for_task_status_not_a_scrimlet().await;

    // do_reconciliation should never have been called.
    assert_eq!(harness.do_reconciliation_calls.lock().unwrap().len(), 0);

    harness.shutdown_cleanly().await;
    logctx.cleanup_successful();
}

// Test: after completing a reconciliation and reaching the select!,
// setting NotScrimlet causes the task to loop back to
// wait_if_this_sled_is_not_a_scrimlet and go inert — without
// performing another reconciliation.
#[tokio::test(start_paused = true)]
async fn scrimlet_becomes_not_scrimlet_during_select() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "scrimlet_becomes_not_scrimlet_during_select",
    );
    let harness = Harness::new(&logctx.log);

    // Provide all prereqs as a scrimlet.
    let _rack_network_config_tx = harness.provide_prereqs(
        ThisSledSwitchZoneUnderlayIpAddr::for_test(Ipv6Addr::LOCALHOST),
    );
    harness.set_scrimlet_status(ScrimletStatus::Scrimlet);

    // Complete the first reconciliation so the task reaches the select!.
    harness.wait_for_do_reconciliation_call_count(1).await;
    harness.do_reconciliation_results_tx.send("first".to_string()).unwrap();
    harness.wait_for_task_status_idle().await;

    // Become NotScrimlet → the select! fires with ScrimletStatusChanged,
    // the loop goes back to wait_if_this_sled_is_not_a_scrimlet, and the
    // task becomes Inert(NotAScrimlet).
    harness.set_scrimlet_status(ScrimletStatus::NotScrimlet);
    let status = harness.wait_for_task_status_not_a_scrimlet().await;

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

// Test: when the sled is already a scrimlet, the first reconciliation
// runs with activation_reason = Startup and the result from
// do_reconciliation appears in last_completion.
#[tokio::test(start_paused = true)]
async fn first_reconciliation_on_startup() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "first_reconciliation_on_startup",
    );
    let harness = Harness::new(&logctx.log);

    // Provide all prereqs to start the inner reconciler.
    let _rack_network_config_tx = harness.provide_prereqs(
        ThisSledSwitchZoneUnderlayIpAddr::for_test(Ipv6Addr::LOCALHOST),
    );
    harness.set_scrimlet_status(ScrimletStatus::Scrimlet);

    // Wait for do_reconciliation to be entered: the mock blocks on the
    // results channel, so once a call is recorded we know it's in-flight.
    harness.wait_for_do_reconciliation_call_count(1).await;

    // While do_reconciliation is blocked, status should be Running with
    // activation_reason = Startup.
    let status = harness.task.status();
    match &status.current_status {
        ReconcilerCurrentStatus::Running(running) => {
            assert_matches!(
                running.activation_reason(),
                ReconcilerActivationReason::Startup
            );
        }
        other => panic!("expected Running status, got {other:?}"),
    }

    // Verify the config that was passed to do_reconciliation.
    let received_configs =
        harness.do_reconciliation_calls.lock().unwrap().clone();
    assert_eq!(received_configs.len(), 1);
    assert_eq!(received_configs[0], test_rack_network_config_1());

    // Release do_reconciliation with a specific status string.
    harness.do_reconciliation_results_tx.send("all good".to_string()).unwrap();
    let status = harness.wait_for_task_status_idle().await;

    let completion =
        status.last_completion.expect("should have last_completion");
    assert!(matches!(
        completion.activation_reason,
        ReconcilerActivationReason::Startup
    ));
    assert_eq!(completion.activation_count, 0);
    assert_eq!(completion.status, "all good");

    harness.shutdown_cleanly().await;
    logctx.cleanup_successful();
}

// Test: after the first reconciliation completes, changing the
// RackNetworkConfig triggers a second reconciliation with
// activation_reason = RackNetworkConfigChanged and the new config.
#[tokio::test(start_paused = true)]
async fn rack_network_config_change_triggers_re_reconciliation() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "rack_network_config_change_triggers_re_reconciliation",
    );
    let harness = Harness::new(&logctx.log);

    // Provide all prereqs.
    let rack_network_config_tx = harness.provide_prereqs(
        ThisSledSwitchZoneUnderlayIpAddr::for_test(Ipv6Addr::LOCALHOST),
    );
    harness.set_scrimlet_status(ScrimletStatus::Scrimlet);

    // Wait for the first do_reconciliation call (Startup).
    harness.wait_for_do_reconciliation_call_count(1).await;

    // Complete the first reconciliation.
    harness.do_reconciliation_results_tx.send("first".to_string()).unwrap();
    harness.wait_for_task_status_idle().await;

    // Send a new RackNetworkConfig.
    let first_config = test_rack_network_config_1();
    let second_config = test_rack_network_config_2();
    assert_ne!(first_config, second_config);
    rack_network_config_tx.send(second_config.clone()).unwrap();

    // Wait for the second do_reconciliation call.
    harness.wait_for_do_reconciliation_call_count(2).await;

    // The second call should have received the new config.
    let received_configs =
        harness.do_reconciliation_calls.lock().unwrap().clone();
    assert_eq!(received_configs.len(), 2);
    assert_eq!(received_configs[0], first_config);
    assert_eq!(received_configs[1], second_config);

    // Status should be Running with RackNetworkConfigChanged.
    let status = harness.task.status();
    match &status.current_status {
        ReconcilerCurrentStatus::Running(running) => {
            assert_matches!(
                running.activation_reason(),
                ReconcilerActivationReason::RackNetworkConfigChanged
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
        ReconcilerActivationReason::RackNetworkConfigChanged
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

    // Provide all prereqs.
    let _rack_network_config_tx = harness.provide_prereqs(
        ThisSledSwitchZoneUnderlayIpAddr::for_test(Ipv6Addr::LOCALHOST),
    );
    harness.set_scrimlet_status(ScrimletStatus::Scrimlet);

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

// Test: full scrimlet status round-trip. Start as scrimlet, complete
// reconciliation #0 (Startup). Set NotScrimlet → task goes inert. Set
// Scrimlet again → reconciliation #1 fires with activation_reason =
// ScrimletStatusChanged and activation_count = 1.
#[tokio::test(start_paused = true)]
async fn scrimlet_status_round_trip() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("scrimlet_status_round_trip");
    let harness = Harness::new(&logctx.log);

    // Provide all prereqs as a scrimlet.
    let _rack_network_config_tx = harness.provide_prereqs(
        ThisSledSwitchZoneUnderlayIpAddr::for_test(Ipv6Addr::LOCALHOST),
    );
    harness.set_scrimlet_status(ScrimletStatus::Scrimlet);

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
    harness.wait_for_task_status_not_a_scrimlet().await;

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

// Test: dropping the rack_network_config sender while the task is in
// the select! (after completing a reconciliation) causes the task to
// exit with TaskExitedUnexpectedly.
#[tokio::test(start_paused = true)]
async fn channel_closure_rack_network_config_during_select() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "channel_closure_rack_network_config_during_select",
    );
    let harness = Harness::new(&logctx.log);

    // Provide all prereqs.
    let rack_network_config_tx = harness.provide_prereqs(
        ThisSledSwitchZoneUnderlayIpAddr::for_test(Ipv6Addr::LOCALHOST),
    );
    harness.set_scrimlet_status(ScrimletStatus::Scrimlet);

    // Complete one reconciliation so the task reaches the select!.
    harness.wait_for_do_reconciliation_call_count(1).await;
    harness.do_reconciliation_results_tx.send("done".to_string()).unwrap();
    harness.wait_for_task_status_idle().await;

    // Drop the rack_network_config sender. This closes the watch channel,
    // which causes the `rack_network_config_rx.changed()` arm in the
    // select! to return Err(RecvError), causing the task to exit.
    mem::drop(rack_network_config_tx);

    // Wait for the task to exit and verify the final status.
    harness.task._task.await.expect("task didn't panic");
    let final_status = harness.task.status_rx.borrow();
    assert_matches!(
        final_status.current_status,
        ReconcilerCurrentStatus::Inert(
            ReconcilerInertReason::TaskExitedUnexpectedly
        )
    );

    // do_reconciliation should have been called exactly once (the initial
    // Startup reconciliation); no second call after the channel closed.
    assert_eq!(harness.do_reconciliation_calls.lock().unwrap().len(), 1);

    logctx.cleanup_successful();
}

// Test: dropping the scrimlet_status sender while the task is blocked
// in wait_if_this_sled_is_not_a_scrimlet() causes the task to exit
// with TaskExitedUnexpectedly.
#[tokio::test(start_paused = true)]
async fn channel_closure_scrimlet_status_during_not_scrimlet_wait() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "channel_closure_scrimlet_status_during_not_scrimlet_wait",
    );
    let harness = Harness::new(&logctx.log);

    // Provide config and IP prereqs, but leave scrimlet status as
    // NotScrimlet (the default).
    let _rack_network_config_tx = harness.provide_prereqs(
        ThisSledSwitchZoneUnderlayIpAddr::for_test(Ipv6Addr::LOCALHOST),
    );

    // Wait for the task to reach Inert(NotAScrimlet), confirming it's
    // blocked in wait_if_this_sled_is_not_a_scrimlet().
    harness.wait_for_task_status_not_a_scrimlet().await;

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
    // which causes scrimlet_status_rx.changed().await to return
    // Err(RecvError) inside wait_if_this_sled_is_not_a_scrimlet(),
    // propagating up through run() and causing the task to exit.
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

    // do_reconciliation should never have been called: we were never a
    // scrimlet.
    assert_eq!(do_reconciliation_calls.lock().unwrap().len(), 0);

    // Explicitly drop after the task exits so we can't hit the .expect()
    // in MockReconciler::do_reconciliation().
    mem::drop(do_reconciliation_results_tx);

    logctx.cleanup_successful();
}

// Test: dropping the rack_network_config sender while the task is
// blocked in wait_if_this_sled_is_not_a_scrimlet() causes the task to
// exit with TaskExitedUnexpectedly.
#[tokio::test(start_paused = true)]
async fn channel_closure_rack_network_config_during_not_scrimlet_wait() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "channel_closure_rack_network_config_during_not_scrimlet_wait",
    );
    let harness = Harness::new(&logctx.log);

    // Provide config and IP prereqs, but leave scrimlet status as
    // NotScrimlet (the default).
    let rack_network_config_tx = harness.provide_prereqs(
        ThisSledSwitchZoneUnderlayIpAddr::for_test(Ipv6Addr::LOCALHOST),
    );

    // Wait for the task to reach Inert(NotAScrimlet), confirming it's
    // blocked in wait_if_this_sled_is_not_a_scrimlet().
    harness.wait_for_task_status_not_a_scrimlet().await;

    // Destructure the harness so we can drop the rack_network_config
    // sender while still holding the other pieces we need.
    let Harness {
        task,
        do_reconciliation_results_tx,
        do_reconciliation_calls,
        ..
    } = harness;

    // Drop the rack_network_config sender. The task is currently blocked
    // waiting for scrimlet status, so it should also notice that this
    // channel has closed and exit.
    mem::drop(rack_network_config_tx);

    // Wait for the task to exit and verify the final status.
    task._task.await.expect("task didn't panic");
    let final_status = task.status_rx.borrow();
    assert_matches!(
        final_status.current_status,
        ReconcilerCurrentStatus::Inert(
            ReconcilerInertReason::TaskExitedUnexpectedly
        )
    );

    // do_reconciliation should never have been called: we were never a
    // scrimlet.
    assert_eq!(do_reconciliation_calls.lock().unwrap().len(), 0);

    // Explicitly drop after the task exits so we can't hit the .expect()
    // in MockReconciler::do_reconciliation().
    mem::drop(do_reconciliation_results_tx);

    logctx.cleanup_successful();
}
