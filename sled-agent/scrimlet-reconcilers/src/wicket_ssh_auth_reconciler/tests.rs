// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use assert_matches::assert_matches;
use camino_tempfile::Utf8TempDir;
use omicron_test_utils::dev;
use omicron_test_utils::dev::poll::wait_for_watch_channel_condition;
use sled_agent_config_reconciler::FakeWicketSshAuthConfig;
use sled_agent_config_reconciler::FakeWicketSshAuthSender;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;

const TEST_RETRY_INTERVAL: Duration = Duration::from_millis(20);
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

// Our reconciler does no parsing/validation of SSH keys itself (it acts purely
// on `String`s), so we can use arbitrary "keys" for these tests.
const KEY_A: &str = "fake-ssh-key A";
const KEY_B: &str = "fake-ssh-key B";
const KEY_C: &str = "fake-ssh-key C";

fn unauthenticated(n: u32) -> WicketSshAuth {
    WicketSshAuth {
        generation: n.into(),
        kind: WicketSshAuthKind::Unauthenticated,
    }
}

fn public_keys(
    n: u32,
    keys: &[&str],
    allow_support_keys: bool,
) -> WicketSshAuth {
    WicketSshAuth {
        generation: n.into(),
        kind: WicketSshAuthKind::PublicKeys {
            authorized_keys: keys.iter().map(|k| k.to_string()).collect(),
            allow_support_keys,
        },
    }
}

fn active_status(n: u32) -> WicketSshAuthStatus {
    WicketSshAuthStatus::SwitchZone {
        effective_auth_gen: n.into(),
        last_error: None,
    }
}

// Given a list of keys, what do we expect to be in the `authorized_keys` file
// written out by the reconciler?
fn expected_keys_file(keys: &[&str]) -> String {
    let sorted = keys.iter().copied().collect::<BTreeSet<_>>();
    sorted.into_iter().map(|k| format!("{k}\n")).collect()
}

// These two `expected_*()` fns are unfortunately very close to "just
// reimplement `write_wicket_sshd_config_snippet()`", but this seems important
// enough to get right that it's worth the maintenance pain of needing to update
// both (even if we only change the formatting of the comments or something).
fn expected_unauthenticated_snippet(n: u32) -> String {
    format!(
        "# wicket ssh auth: generation {n} (unauthenticated)\n\
         # Allow `wicket` to log in without a password\n\
         PermitEmptyPasswords yes\n\
         PasswordAuthentication yes\n\
         AuthenticationMethods none\n"
    )
}
fn expected_public_keys_snippet(n: u32, allow_support_keys: bool) -> String {
    let support = if allow_support_keys {
        " /opt/oxide/support/authorized_keys"
    } else {
        ""
    };
    format!(
        "# wicket ssh auth: generation {n} (public key auth)\n\
         # Require an authorized key to access `wicket`\n\
         PubkeyAuthentication yes\n\
         AuthenticationMethods publickey\n\
         AuthorizedKeysFile /opt/oxide/wicket/authorized_keys{support}\n"
    )
}

// Helper for a fake switch zone; the only thing the reconciler actually needs
// is a directory in which to write files.
#[derive(Debug, Clone)]
struct FakeSwitchZone {
    tmpdir: Arc<Utf8TempDir>,
}

impl FakeSwitchZone {
    fn new() -> Self {
        let tmpdir = Arc::new(Utf8TempDir::new().expect("created temp dir"));

        // In prod, this directory is created by the zone install process; the
        // reconciler expects it to already exist.
        std::fs::create_dir_all(tmpdir.path().join("opt/oxide/wicket"))
            .expect("created subdirectory under temp dir");

        Self { tmpdir }
    }

    fn root(&self) -> &Utf8Path {
        self.tmpdir.path()
    }

    /// Remove the directory the reconciler expects to exist for writing its
    /// file. This will cause I/O errors during reconciliation.
    fn remove_expected_output_dir(&self) {
        match std::fs::remove_dir_all(
            self.tmpdir.path().join("opt/oxide/wicket"),
        ) {
            Ok(()) => (),
            Err(err) => {
                if err.kind() != io::ErrorKind::NotFound {
                    panic!("failed to remove directory: {err}");
                }
            }
        }
    }

    fn read_file(&self, path_in_zone_root: &str) -> Option<String> {
        let path = self.tmpdir.path().join(path_in_zone_root);
        match std::fs::read_to_string(&path) {
            Ok(contents) => Some(contents),
            Err(err) => {
                if err.kind() == io::ErrorKind::NotFound {
                    None
                } else {
                    panic!("unexpected error reading {path}: {err}");
                }
            }
        }
    }

    fn read_keys(&self) -> Option<String> {
        self.read_file(WICKET_AUTHORIZED_KEYS_PATH)
    }

    fn read_config(&self) -> Option<String> {
        self.read_file(WICKET_SSHD_CONFIG_PATH)
    }
}

/// Record of the contents of the keys and config files present in the
/// [`FakeRefresher`]'s fake zone when it was told to refresh sshd.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RefreshCall {
    authorized_keys: Option<String>,
    sshd_config: Option<String>,
}

#[derive(Debug)]
struct FakeRefresherInner {
    fake_zone: FakeSwitchZone,
    failing: bool,
    calls: Vec<RefreshCall>,
}

/// Implementation of [`SshdRefresher`] that inspects a fake zone and records
/// its contents when told to refresh sshd, instead of interacting with SMF as
/// [`RealSshdRefresher`] does.
#[derive(Debug)]
struct FakeRefresher {
    inner: Mutex<FakeRefresherInner>,
}

impl FakeRefresher {
    fn new(fake_zone: FakeSwitchZone) -> Self {
        Self {
            inner: Mutex::new(FakeRefresherInner {
                fake_zone,
                failing: false,
                calls: Vec::new(),
            }),
        }
    }

    fn set_fake_zone(&self, fake_zone: FakeSwitchZone) {
        self.inner.lock().unwrap().fake_zone = fake_zone;
    }

    fn set_failing(&self, failing: bool) {
        self.inner.lock().unwrap().failing = failing;
    }

    fn calls(&self) -> Vec<RefreshCall> {
        self.inner.lock().unwrap().calls.clone()
    }
}

impl SshdRefresher for FakeRefresher {
    fn refresh(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let authorized_keys = inner.fake_zone.read_keys();
        let sshd_config = inner.fake_zone.read_config();
        inner.calls.push(RefreshCall { authorized_keys, sshd_config });
        if inner.failing {
            bail!("fake refresh failure");
        }
        Ok(())
    }
}

/// Harness for most tests: sets up a fake zone, a fake refresher watching that
/// fake zone, a fake channel for specifying the wicket SSH auth, and a real
/// reconciler pointed at all of those fakes.
struct Harness {
    reconciler: WicketSshAuthReconciler,
    auth_tx: FakeWicketSshAuthSender,
    refresher: Arc<FakeRefresher>,
    fake_zone: FakeSwitchZone,
}

impl Harness {
    fn new(log: &Logger, initial: FakeWicketSshAuthConfig) -> Self {
        let (auth_rx, auth_tx) = WicketSshAuthReceiver::fake_dynamic(initial);
        let fake_zone = FakeSwitchZone::new();
        let refresher = Arc::new(FakeRefresher::new(fake_zone.clone()));
        let reconciler = WicketSshAuthReconciler::new_impl(
            auth_rx,
            log,
            refresher.clone(),
            TEST_RETRY_INTERVAL,
        );
        Self { reconciler, auth_tx, refresher, fake_zone }
    }

    fn set_auth(&self, auth: WicketSshAuth) {
        self.auth_tx.set(FakeWicketSshAuthConfig::Ledgered(auth));
    }

    fn read_keys(&self) -> Option<String> {
        self.fake_zone.read_keys()
    }

    fn read_config(&self) -> Option<String> {
        self.fake_zone.read_config()
    }

    /// Set up a new fake switch zone, returning the old one.
    ///
    /// This allows us to emulate the switch zone being torn down and
    /// reconstructed. In prod, both switch zones end up in the same path; for
    /// tests, we keep two separate tempdirs for easier assertion on the
    /// contents of each.
    fn make_new_fake_switch_zone(&mut self) -> FakeSwitchZone {
        let mut previous = FakeSwitchZone::new();
        std::mem::swap(&mut self.fake_zone, &mut previous);
        self.refresher.set_fake_zone(self.fake_zone.clone());
        previous
    }

    /// Instruct the reconciler to configure the harness's fake switch zone.
    async fn configure(
        &self,
    ) -> Result<SwitchZoneBootedGuard<'_>, WicketSshAuthReconcilerError> {
        self.reconciler
            .configure_zone_root_before_boot(self.fake_zone.root().into())
            .await
    }

    /// Helper that calls `self.configure()`, unwraps the result, and calls
    /// `mark_zone_booted()` on the returned [`SwitchZoneBootedGuard`].
    async fn configure_and_boot(&self) {
        self.configure()
            .await
            .expect("configured fake zone")
            .mark_zone_booted();
    }

    /// Wait until the reconciler's reported state passes `cond`, panicking
    /// after [`WAIT_TIMEOUT`] if the condition hasn't become true.
    async fn wait_for_status<F>(&self, description: &str, cond: F)
    where
        F: FnMut(&WicketSshAuthStatus) -> bool,
    {
        let mut status_rx = self.reconciler.status_rx.clone();
        if let Err(err) =
            wait_for_watch_channel_condition(&mut status_rx, cond, WAIT_TIMEOUT)
                .await
        {
            panic!(
                "failed waiting for status {description}: {err} (got {:?})",
                self.reconciler.status(),
            );
        }
    }

    async fn wait_for_active_generation(&self, n: u32) {
        let expected = active_status(n);
        self.wait_for_status(&format!("active at generation {n}"), |status| {
            *status == expected
        })
        .await;
    }

    async fn wait_for_last_error(&self) -> String {
        self.wait_for_status("with last_error set", |status| {
            matches!(
                status,
                WicketSshAuthStatus::SwitchZone { last_error: Some(_), .. }
            )
        })
        .await;
        match self.reconciler.status() {
            WicketSshAuthStatus::SwitchZone {
                last_error: Some(err), ..
            } => err,
            status => panic!("unexpected status {status:?}"),
        }
    }

    async fn wait_for_no_switch_zone(&self) {
        self.wait_for_status("NoSwitchZone", |status| {
            *status == WicketSshAuthStatus::NoSwitchZone
        })
        .await;
    }

    /// This is an unfortunate attempt to provide a way for tests to "wait and
    /// see that nothing happens"; e.g., if the reconciler is activated when
    /// there is no switch zone, it should do nothing. We attempt to cover that
    /// by sleeping for a multiple of the reconciler's retry interval. If the
    /// reconciler ever starts to act when it shouldn't, hopefully this sleep is
    /// long enough to cause test failures (even if they're flaky!).
    async fn sleep_several_retry_intervals(&self) {
        tokio::time::sleep(TEST_RETRY_INTERVAL * 10).await;
    }
}

#[test]
fn authorized_keys_empty_when_unauthenticated() {
    let logctx =
        dev::test_setup_log("authorized_keys_empty_when_unauthenticated");
    let zone = FakeSwitchZone::new();

    write_wicket_authorized_keys(zone.root(), &unauthenticated(1), &logctx.log)
        .unwrap();
    assert_eq!(zone.read_keys().as_deref(), Some(""));

    logctx.cleanup_successful();
}

#[test]
fn authorized_keys_one_key_per_line() {
    let logctx = dev::test_setup_log("authorized_keys_one_key_per_line");
    let zone = FakeSwitchZone::new();

    write_wicket_authorized_keys(
        zone.root(),
        &public_keys(2, &[KEY_B, KEY_A], false),
        &logctx.log,
    )
    .unwrap();
    assert_eq!(zone.read_keys().unwrap(), format!("{KEY_A}\n{KEY_B}\n"),);

    logctx.cleanup_successful();
}

#[test]
fn authorized_keys_overwrite_drops_stale_keys() {
    let logctx =
        dev::test_setup_log("authorized_keys_overwrite_drops_stale_keys");
    let zone = FakeSwitchZone::new();

    write_wicket_authorized_keys(
        zone.root(),
        &public_keys(2, &[KEY_A, KEY_B, KEY_C], false),
        &logctx.log,
    )
    .unwrap();
    write_wicket_authorized_keys(
        zone.root(),
        &public_keys(3, &[KEY_B], false),
        &logctx.log,
    )
    .unwrap();
    assert_eq!(zone.read_keys().unwrap(), format!("{KEY_B}\n"));

    write_wicket_authorized_keys(zone.root(), &unauthenticated(4), &logctx.log)
        .unwrap();
    assert_eq!(zone.read_keys().unwrap(), "");

    logctx.cleanup_successful();
}

#[test]
fn sshd_snippet_unauthenticated() {
    let logctx = dev::test_setup_log("sshd_snippet_unauthenticated");
    let zone = FakeSwitchZone::new();

    write_wicket_sshd_config_snippet(
        zone.root(),
        &unauthenticated(7),
        &logctx.log,
    )
    .unwrap();
    assert_eq!(
        zone.read_config().unwrap(),
        "# wicket ssh auth: generation 7 (unauthenticated)\n\
         # Allow `wicket` to log in without a password\n\
         PermitEmptyPasswords yes\n\
         PasswordAuthentication yes\n\
         AuthenticationMethods none\n",
    );

    logctx.cleanup_successful();
}

#[test]
fn sshd_snippet_public_keys_without_support_keys() {
    let logctx =
        dev::test_setup_log("sshd_snippet_public_keys_without_support_keys");
    let zone = FakeSwitchZone::new();

    write_wicket_sshd_config_snippet(
        zone.root(),
        &public_keys(8, &[KEY_A], false),
        &logctx.log,
    )
    .unwrap();
    assert_eq!(
        zone.read_config().unwrap(),
        "# wicket ssh auth: generation 8 (public key auth)\n\
         # Require an authorized key to access `wicket`\n\
         PubkeyAuthentication yes\n\
         AuthenticationMethods publickey\n\
         AuthorizedKeysFile /opt/oxide/wicket/authorized_keys\n",
    );

    logctx.cleanup_successful();
}

#[test]
fn sshd_snippet_public_keys_with_support_keys() {
    let logctx =
        dev::test_setup_log("sshd_snippet_public_keys_with_support_keys");
    let zone = FakeSwitchZone::new();

    write_wicket_sshd_config_snippet(
        zone.root(),
        &public_keys(9, &[], true),
        &logctx.log,
    )
    .unwrap();
    assert_eq!(
        zone.read_config().unwrap(),
        "# wicket ssh auth: generation 9 (public key auth)\n\
         # Require an authorized key to access `wicket`\n\
         PubkeyAuthentication yes\n\
         AuthenticationMethods publickey\n\
         AuthorizedKeysFile /opt/oxide/wicket/authorized_keys /opt/oxide/support/authorized_keys\n",
    );

    logctx.cleanup_successful();
}

#[test]
fn written_files_are_mode_0644_with_no_leftover_temp_files() {
    let logctx = dev::test_setup_log(
        "written_files_are_mode_0644_with_no_leftover_temp_files",
    );
    let zone = FakeSwitchZone::new();
    let auth = public_keys(2, &[KEY_A], true);

    for _ in 0..2 {
        write_wicket_authorized_keys(zone.root(), &auth, &logctx.log).unwrap();
        write_wicket_sshd_config_snippet(zone.root(), &auth, &logctx.log)
            .unwrap();
    }

    let wicket_dir = zone.root().join("opt/oxide/wicket");
    let mut entries = BTreeSet::new();
    for entry in wicket_dir.read_dir_utf8().unwrap() {
        let entry = entry.unwrap();
        let mode = entry.metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o644, "unexpected mode on {}", entry.path());
        entries.insert(entry.file_name().to_string());
    }
    assert_eq!(
        entries,
        BTreeSet::from([
            "authorized_keys".to_string(),
            "wicket_sshd_config".to_string(),
        ]),
    );

    logctx.cleanup_successful();
}

#[test]
fn write_fails_if_wicket_dir_missing() {
    let logctx = dev::test_setup_log("write_fails_if_wicket_dir_missing");
    let root = Utf8TempDir::new().unwrap();

    let err = write_wicket_authorized_keys(
        root.path(),
        &unauthenticated(1),
        &logctx.log,
    )
    .unwrap_err();
    assert_matches!(
        err,
        WicketSshAuthReconcilerError::WriteFailed { path, .. }
            if path == root.path().join(WICKET_AUTHORIZED_KEYS_PATH)
    );

    let err = write_wicket_sshd_config_snippet(
        root.path(),
        &unauthenticated(1),
        &logctx.log,
    )
    .unwrap_err();
    assert_matches!(
        err,
        WicketSshAuthReconcilerError::WriteFailed { path, .. }
            if path == root.path().join(WICKET_SSHD_CONFIG_PATH)
    );

    logctx.cleanup_successful();
}

// Attempt to catch any drift between the top-level `sshd_config` that gets
// installed with the switch zone and the paths that this reconciler expects.
#[test]
fn sshd_config_paths_match_constants() {
    let sshd_config =
        include_str!("../../../../wicket/zone-etc/ssh/sshd_config");
    let lines = sshd_config.lines().map(str::trim).collect::<Vec<_>>();

    let include = format!("Include /{WICKET_SSHD_CONFIG_PATH}");
    assert!(
        lines.contains(&include.as_str()),
        "sshd_config missing `{include}`",
    );

    let support_keys =
        format!("AuthorizedKeysFile /{SUPPORT_AUTHORIZED_KEYS_PATH}");
    assert!(
        lines.contains(&support_keys.as_str()),
        "sshd_config missing `{support_keys}`",
    );

    let wicket_match = lines
        .iter()
        .position(|line| *line == "Match User wicket")
        .expect("sshd_config has `Match User wicket`");
    let support_match = lines
        .iter()
        .position(|line| *line == "Match User support")
        .expect("sshd_config has `Match User support`");
    let include_pos = lines.iter().position(|line| *line == include).unwrap();
    assert!(
        wicket_match < include_pos && include_pos < support_match,
        "`{include}` is not inside the `Match User wicket` block",
    );
}

#[tokio::test]
async fn configure_before_boot_pre_rss_is_unauthenticated() {
    let logctx =
        dev::test_setup_log("configure_before_boot_pre_rss_is_unauthenticated");
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::WaitingForInitialConfig,
    );
    assert_eq!(harness.reconciler.status(), WicketSshAuthStatus::NoSwitchZone);

    harness.configure_and_boot().await;

    assert_eq!(harness.read_keys().unwrap(), "");
    assert_eq!(
        harness.read_config().unwrap(),
        expected_unauthenticated_snippet(1),
    );
    assert_eq!(harness.reconciler.status(), active_status(1));
    assert_eq!(harness.refresher.calls(), vec![]);

    logctx.cleanup_successful();
}

#[tokio::test]
async fn configure_before_boot_applies_ledgered_public_keys() {
    let logctx = dev::test_setup_log(
        "configure_before_boot_applies_ledgered_public_keys",
    );
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(public_keys(
            5,
            &[KEY_A, KEY_B],
            true,
        )),
    );

    harness.configure_and_boot().await;

    assert_eq!(
        harness.read_keys().unwrap(),
        expected_keys_file(&[KEY_A, KEY_B]),
    );
    assert_eq!(
        harness.read_config().unwrap(),
        expected_public_keys_snippet(5, true),
    );
    assert_eq!(harness.reconciler.status(), active_status(5));
    assert_eq!(harness.refresher.calls(), vec![]);

    logctx.cleanup_successful();
}

#[tokio::test]
async fn configure_before_boot_fails_without_internal_disks() {
    let logctx = dev::test_setup_log(
        "configure_before_boot_fails_without_internal_disks",
    );
    let harness =
        Harness::new(&logctx.log, FakeWicketSshAuthConfig::NoInternalDisks);

    let err = harness.configure().await.unwrap_err();
    assert_matches!(
        err,
        WicketSshAuthReconcilerError::WicketSshAuthReceiver(
            WicketSshAuthReceiverError::NoInternalDisks
        )
    );
    assert_eq!(harness.read_keys(), None);
    assert_eq!(harness.read_config(), None);
    assert_eq!(harness.reconciler.status(), WicketSshAuthStatus::NoSwitchZone);

    harness.set_auth(public_keys(3, &[KEY_A], false));
    harness.configure_and_boot().await;

    assert_eq!(harness.read_keys().unwrap(), expected_keys_file(&[KEY_A]));
    assert_eq!(
        harness.read_config().unwrap(),
        expected_public_keys_snippet(3, false),
    );
    assert_eq!(harness.reconciler.status(), active_status(3));

    logctx.cleanup_successful();
}

#[tokio::test]
async fn configure_before_boot_reports_write_failure() {
    let logctx =
        dev::test_setup_log("configure_before_boot_reports_write_failure");
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(unauthenticated(1)),
    );

    harness.fake_zone.remove_expected_output_dir();
    let err = harness.configure().await.unwrap_err();
    assert_matches!(err, WicketSshAuthReconcilerError::WriteFailed { .. });
    assert_eq!(harness.reconciler.status(), WicketSshAuthStatus::NoSwitchZone);

    logctx.cleanup_successful();
}

// Test that if we receive multiple distinct configs while we have no switch
// zone, then boot a switch zone, only the most recent config is applied.
#[tokio::test]
async fn config_change_while_inactive_is_applied_at_next_boot() {
    let logctx = dev::test_setup_log(
        "config_change_while_inactive_is_applied_at_next_boot",
    );
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::WaitingForInitialConfig,
    );

    harness.set_auth(public_keys(2, &[KEY_A], false));
    harness.set_auth(public_keys(3, &[KEY_B], false));
    harness.sleep_several_retry_intervals().await;
    assert_eq!(harness.read_keys(), None);
    assert_eq!(harness.read_config(), None);
    assert_eq!(harness.reconciler.status(), WicketSshAuthStatus::NoSwitchZone);

    harness.configure_and_boot().await;

    // We should have configured the most recent config...
    assert_eq!(harness.read_keys().unwrap(), expected_keys_file(&[KEY_B]));
    assert_eq!(
        harness.read_config().unwrap(),
        expected_public_keys_snippet(3, false),
    );
    assert_eq!(harness.reconciler.status(), active_status(3));

    // ... and should not attempt to reconfigure+refresh the running sshd.
    harness.sleep_several_retry_intervals().await;
    assert_eq!(harness.refresher.calls(), vec![]);

    logctx.cleanup_successful();
}

#[tokio::test]
async fn guard_dropped_without_boot_marks_zone_uninstalled() {
    let logctx = dev::test_setup_log(
        "guard_dropped_without_boot_marks_zone_uninstalled",
    );
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(public_keys(2, &[KEY_A], false)),
    );

    let guard = harness.configure().await.unwrap();
    assert_eq!(harness.reconciler.status(), active_status(2));
    std::mem::drop(guard);
    harness.wait_for_no_switch_zone().await;

    // Pass in a new config...
    harness.set_auth(public_keys(3, &[KEY_B], false));
    harness.sleep_several_retry_intervals().await;

    // ... but the reconciler should not have replaced anything in the fake zone
    // root, because we dropped the guard (which should have made the reconciler
    // inert).
    assert_eq!(harness.read_keys().unwrap(), expected_keys_file(&[KEY_A]));
    assert_eq!(
        harness.read_config().unwrap(),
        expected_public_keys_snippet(2, false),
    );
    assert_eq!(harness.reconciler.status(), WicketSshAuthStatus::NoSwitchZone);
    assert_eq!(harness.refresher.calls(), vec![]);

    logctx.cleanup_successful();
}

// When we receive a new config specifying "public keys", we should write
// the authorized_keys file before refreshing sshd with the new config; doing it
// the other way around risks a window where ssh is configured to read an empty
// keyfile (denying all access).
#[tokio::test]
async fn reconfigure_pubkey_refreshes_after_writing_keys() {
    let logctx =
        dev::test_setup_log("reconfigure_pubkey_refreshes_after_writing_keys");
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(unauthenticated(1)),
    );
    harness.configure_and_boot().await;

    harness.set_auth(public_keys(2, &[KEY_A, KEY_B], false));
    harness.wait_for_active_generation(2).await;

    // The refresh should've seen the expected keys already written.
    let expected = RefreshCall {
        authorized_keys: Some(expected_keys_file(&[KEY_A, KEY_B])),
        sshd_config: Some(expected_public_keys_snippet(2, false)),
    };
    assert_eq!(harness.refresher.calls(), vec![expected.clone()]);
    assert_eq!(harness.read_keys(), expected.authorized_keys);
    assert_eq!(harness.read_config(), expected.sshd_config);

    logctx.cleanup_successful();
}

// When we receive a new config specifying "unauthenticated", we should refresh
// sshd with the new config first before clearing the keys; doing it the other
// way around risks a window where ssh is configured to read an empty keyfile
// (denying all access).
#[tokio::test]
async fn reconfigure_unauth_refreshes_before_clearing_keys() {
    let logctx = dev::test_setup_log(
        "reconfigure_unauth_refreshes_before_clearing_keys",
    );
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(public_keys(
            2,
            &[KEY_A, KEY_B],
            true,
        )),
    );
    harness.configure_and_boot().await;

    harness.set_auth(unauthenticated(3));
    harness.wait_for_active_generation(3).await;

    // Refresh should've seen the keys populated with a config specifying
    // "unauthenticated"...
    assert_eq!(
        harness.refresher.calls(),
        vec![RefreshCall {
            authorized_keys: Some(expected_keys_file(&[KEY_A, KEY_B])),
            sshd_config: Some(expected_unauthenticated_snippet(3)),
        }],
    );

    // ... and the current state after reconciliation is that the keyfile is now
    // empty.
    assert_eq!(harness.read_keys().unwrap(), "");
    assert_eq!(
        harness.read_config().unwrap(),
        expected_unauthenticated_snippet(3),
    );

    logctx.cleanup_successful();
}

#[tokio::test]
async fn change_keys_and_support_flag() {
    let logctx = dev::test_setup_log("change_keys_and_support_flag");
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(public_keys(
            2,
            &[KEY_A, KEY_B],
            false,
        )),
    );
    harness.configure_and_boot().await;

    harness.set_auth(public_keys(3, &[KEY_B, KEY_C], true));
    harness.wait_for_active_generation(3).await;

    let expected = RefreshCall {
        authorized_keys: Some(expected_keys_file(&[KEY_B, KEY_C])),
        sshd_config: Some(expected_public_keys_snippet(3, true)),
    };
    assert_eq!(harness.refresher.calls(), vec![expected.clone()]);
    assert_eq!(harness.read_keys(), expected.authorized_keys);
    assert_eq!(harness.read_config(), expected.sshd_config);

    harness.set_auth(public_keys(4, &[KEY_B, KEY_C], false));
    harness.wait_for_active_generation(4).await;

    assert_eq!(harness.refresher.calls().len(), 2);
    assert_eq!(
        harness.read_keys().unwrap(),
        expected_keys_file(&[KEY_B, KEY_C]),
    );
    assert_eq!(
        harness.read_config().unwrap(),
        expected_public_keys_snippet(4, false),
    );

    logctx.cleanup_successful();
}

// If the SMF refresh fails, the reconciler should retry periodically (without
// waiting for new config changes to arrive).
#[tokio::test]
async fn refresh_failure_sets_last_error_and_retries() {
    let logctx =
        dev::test_setup_log("refresh_failure_sets_last_error_and_retries");
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(unauthenticated(1)),
    );
    harness.configure_and_boot().await;

    harness.refresher.set_failing(true);
    harness.set_auth(public_keys(2, &[KEY_A], false));
    let last_error = harness.wait_for_last_error().await;

    assert!(
        last_error.contains("fake refresh failure"),
        "unexpected last_error: {last_error}",
    );
    assert_matches!(
        harness.reconciler.status(),
        WicketSshAuthStatus::SwitchZone { effective_auth_gen, .. }
            if effective_auth_gen == 1.into()
    );
    assert_eq!(harness.read_keys().unwrap(), expected_keys_file(&[KEY_A]));
    assert_eq!(
        harness.read_config().unwrap(),
        expected_public_keys_snippet(2, false),
    );

    let failed_calls = harness.refresher.calls().len();
    harness.refresher.set_failing(false);
    harness.wait_for_active_generation(2).await;

    assert!(harness.refresher.calls().len() > failed_calls);
    assert_eq!(harness.read_keys().unwrap(), expected_keys_file(&[KEY_A]));
    assert_eq!(
        harness.read_config().unwrap(),
        expected_public_keys_snippet(2, false),
    );

    let successful_calls = harness.refresher.calls().len();
    harness.sleep_several_retry_intervals().await;
    assert_eq!(harness.refresher.calls().len(), successful_calls);

    logctx.cleanup_successful();
}

// If refreshing fails, the reconciler will go into a retry loop (see
// `refresh_failure_sets_last_error_and_retries()` above). If we receive a new
// config while in that loop, we should apply that one immediately instead.
#[tokio::test]
async fn newer_config_supersedes_failing_one() {
    let logctx = dev::test_setup_log("newer_config_supersedes_failing_one");
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(unauthenticated(1)),
    );
    harness.configure_and_boot().await;

    harness.refresher.set_failing(true);
    harness.set_auth(public_keys(2, &[KEY_A], false));
    harness.wait_for_last_error().await;

    harness.set_auth(public_keys(3, &[KEY_B], true));
    harness.refresher.set_failing(false);
    harness.wait_for_active_generation(3).await;

    assert_eq!(harness.read_keys().unwrap(), expected_keys_file(&[KEY_B]));
    assert_eq!(
        harness.read_config().unwrap(),
        expected_public_keys_snippet(3, true),
    );

    logctx.cleanup_successful();
}

// If we receive a config change after uninstalling the switch zone, we
// shouldn't act on it.
#[tokio::test]
async fn uninstall_stops_reconfiguration() {
    let logctx = dev::test_setup_log("uninstall_stops_reconfiguration");
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(unauthenticated(1)),
    );
    harness.configure_and_boot().await;

    harness.reconciler.mark_zone_uninstalled();
    harness.wait_for_no_switch_zone().await;

    harness.set_auth(public_keys(2, &[KEY_A], false));
    harness.sleep_several_retry_intervals().await;

    assert_eq!(harness.read_keys().unwrap(), "");
    assert_eq!(
        harness.read_config().unwrap(),
        expected_unauthenticated_snippet(1),
    );
    assert_eq!(harness.reconciler.status(), WicketSshAuthStatus::NoSwitchZone);
    assert_eq!(harness.refresher.calls(), vec![]);

    logctx.cleanup_successful();
}

// If refreshing fails, the reconciler will go into a retry loop (see
// `refresh_failure_sets_last_error_and_retries()` above). If we're notified
// that the zone has been uninstalled while in that loop, we should go inert.
#[tokio::test]
async fn uninstall_cancels_pending_retry() {
    let logctx = dev::test_setup_log("uninstall_cancels_pending_retry");
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(unauthenticated(1)),
    );
    harness.configure_and_boot().await;

    harness.refresher.set_failing(true);
    harness.set_auth(public_keys(2, &[KEY_A], false));
    harness.wait_for_last_error().await;

    harness.reconciler.mark_zone_uninstalled();
    harness.wait_for_no_switch_zone().await;

    let calls = harness.refresher.calls().len();
    harness.sleep_several_retry_intervals().await;
    assert_eq!(harness.refresher.calls().len(), calls);
    assert_eq!(harness.reconciler.status(), WicketSshAuthStatus::NoSwitchZone);

    logctx.cleanup_successful();
}

#[tokio::test]
async fn mark_zone_uninstalled_is_idempotent() {
    let logctx = dev::test_setup_log("mark_zone_uninstalled_is_idempotent");
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(unauthenticated(1)),
    );

    harness.reconciler.mark_zone_uninstalled();
    harness.reconciler.mark_zone_uninstalled();
    harness.sleep_several_retry_intervals().await;
    assert_eq!(harness.reconciler.status(), WicketSshAuthStatus::NoSwitchZone);

    harness.configure_and_boot().await;
    assert_eq!(harness.reconciler.status(), active_status(1));

    harness.reconciler.mark_zone_uninstalled();
    harness.reconciler.mark_zone_uninstalled();
    harness.wait_for_no_switch_zone().await;

    harness.configure_and_boot().await;
    assert_eq!(harness.reconciler.status(), active_status(1));

    logctx.cleanup_successful();
}

#[tokio::test]
async fn reinstall_uses_new_zone_root() {
    let logctx = dev::test_setup_log("reinstall_uses_new_zone_root");
    let mut harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(unauthenticated(1)),
    );
    harness.configure_and_boot().await;

    harness.reconciler.mark_zone_uninstalled();
    harness.wait_for_no_switch_zone().await;

    // Have the harness set up a new fake zone and boot it; we save off the
    // first zone to inspect it later.
    let first_fake_zone = harness.make_new_fake_switch_zone();
    harness.configure_and_boot().await;
    assert_eq!(harness.reconciler.status(), active_status(1));

    harness.set_auth(public_keys(2, &[KEY_A], false));
    harness.wait_for_active_generation(2).await;

    let expected = RefreshCall {
        authorized_keys: Some(expected_keys_file(&[KEY_A])),
        sshd_config: Some(expected_public_keys_snippet(2, false)),
    };
    assert_eq!(harness.refresher.calls(), vec![expected.clone()]);
    assert_eq!(harness.read_keys(), expected.authorized_keys);
    assert_eq!(harness.read_config(), expected.sshd_config);

    assert_eq!(first_fake_zone.read_keys().unwrap(), "");
    assert_eq!(
        first_fake_zone.read_config().unwrap(),
        expected_unauthenticated_snippet(1),
    );

    logctx.cleanup_successful();
}

// This should never happen in production, but confirm the appropriate error
// bubbles out if the reconciliation actor task exits.
#[tokio::test]
async fn configure_returns_error_if_actor_exited() {
    let logctx = dev::test_setup_log("configure_returns_error_if_actor_exited");
    let harness = Harness::new(
        &logctx.log,
        FakeWicketSshAuthConfig::Ledgered(unauthenticated(1)),
    );
    harness.configure_and_boot().await;

    let Harness { reconciler, auth_tx, refresher: _, fake_zone } = harness;
    let WicketSshAuthReconciler {
        zone_lifecycle_tx,
        status_rx,
        log: _,
        _actor_task: actor_task,
    } = reconciler;

    std::mem::drop(auth_tx);
    tokio::time::timeout(WAIT_TIMEOUT, actor_task)
        .await
        .expect("actor task exited before timeout")
        .expect("actor task did not panic");

    let result = tokio::time::timeout(
        WAIT_TIMEOUT,
        zone_lifecycle_tx
            .configure_switch_zone_before_boot(fake_zone.root().into()),
    )
    .await
    .expect("configure returned before timeout");
    assert_matches!(result, Err(WicketSshAuthReconcilerError::ActorTaskDied));
    assert_eq!(*status_rx.borrow(), active_status(1));

    logctx.cleanup_successful();
}

// Poll `fut` once and panic if it's ready.
async fn assert_pending<F: Future>(fut: &mut Pin<&mut F>) {
    tokio::select! {
        biased;
        _ = fut => panic!("future completed unexpectedly"),
        () = std::future::ready(()) => (),
    }
}

// This should never happen in production, but confirm the appropriate error
// bubbles out: if sled-agent tries to configure the switch zone twice without
// waiting for the first to complete, the first attempt will fail.
#[tokio::test]
async fn superseded_configure_gets_response_channel_dropped() {
    let (tx, _rx) = SwitchZoneLifecycle::channel();
    let fake_zone = FakeSwitchZone::new();
    let zone_root: Arc<Utf8Path> = fake_zone.root().into();

    let first = tx.configure_switch_zone_before_boot(Arc::clone(&zone_root));
    tokio::pin!(first);
    assert_pending(&mut first).await;

    let second = tx.configure_switch_zone_before_boot(zone_root);
    tokio::pin!(second);
    assert_pending(&mut second).await;

    assert_matches!(
        first.await,
        Err(WicketSshAuthReconcilerError::ResponseChannelDropped)
    );

    // The second attempt should still be pending; if the zone gets uninstalled
    // while it is (which also shouldn't happen in prod!), it'll see the same
    // error.
    assert_pending(&mut second).await;

    tx.mark_zone_uninstalled();
    assert_matches!(
        second.await,
        Err(WicketSshAuthReconcilerError::ResponseChannelDropped)
    );
}

// Basic smoke-tests that the "refresh sshd via SMF" path is functional.
#[cfg(target_os = "illumos")]
mod smf {
    use super::*;
    use scuffle::isolated::IsolatedConfigd;

    #[test]
    fn smf_refresh_succeeds_against_isolated_configd() {
        let isolated = IsolatedConfigd::builder(SSHD_SERVICE_NAME)
            .unwrap()
            .build()
            .unwrap();
        let scf = Scf::connect_isolated(&isolated).unwrap();

        RealSshdRefresher::refresh_via_scf_handle(&scf).unwrap();
    }

    #[test]
    fn smf_refresh_fails_if_service_missing() {
        let isolated = IsolatedConfigd::builder("network/not-ssh")
            .unwrap()
            .build()
            .unwrap();
        let scf = Scf::connect_isolated(&isolated).unwrap();

        let err = RealSshdRefresher::refresh_via_scf_handle(&scf).unwrap_err();
        assert!(
            err.to_string().contains(SSHD_SERVICE_NAME),
            "unexpected error: {err:#}",
        );
    }
}
