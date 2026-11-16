// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler for configuration of `sshd` within a scrimlet's switch zone,
//! specific to how the `wicket` captive shell can be accessed.
//!
//! Unlike most reconcilers in this crate, `sshd`'s configuration is managed
//! via files on disk, not a dropshot server. This reconciler also does not
//! implement the [`crate::reconciler_task::Reconciler`] trait; it is not based
//! on the `SystemNetworkingConfig`, and must be usable _before_ the rack is
//! initialized.

use anyhow::bail;
use atomicwrites::AtomicFile;
use atomicwrites::OverwriteBehavior;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use illumos_utils::running_zone::InstalledZone;
use omicron_generation_kinds::WicketSshAuthGeneration;
use scuffle::Scf;
use sled_agent_config_reconciler::WicketSshAuthReceiver;
use sled_agent_config_reconciler::WicketSshAuthReceiverError;
use sled_agent_types::inventory::WicketSshAuth;
use sled_agent_types::inventory::WicketSshAuthKind;
use sled_agent_types::inventory::WicketSshAuthStatus;
use slog::Logger;
use slog::error;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::convert::Infallible;
use std::fs::Permissions;
use std::io;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_util::task::AbortOnDropHandle;

use crate::SWITCH_ZONE_NAME;

// Paths to the files this reconciler writes into the switch zone root,
// _without_ leading `/`s so they can be joined against the zone root in the gz.
const WICKET_AUTHORIZED_KEYS_PATH: &str = "opt/oxide/wicket/authorized_keys";
const WICKET_SSHD_CONFIG_PATH: &str = "opt/oxide/wicket/wicket_sshd_config";
const SUPPORT_AUTHORIZED_KEYS_PATH: &str = "opt/oxide/support/authorized_keys";

const SSHD_SERVICE_NAME: &str = "network/ssh";
const SSHD_INSTANCE_NAME: &str = "default";

#[derive(Debug, thiserror::Error)]
pub enum WicketSshAuthReconcilerError {
    #[error("current config not available")]
    WicketSshAuthReceiver(#[from] WicketSshAuthReceiverError),
    #[error("failed writing to {path}")]
    WriteFailed {
        path: Utf8PathBuf,
        #[source]
        err: io::Error,
    },
    #[error("response channel dropped before actor could respond")]
    ResponseChannelDropped,
    #[error("inner actor task died (this should be impossible outside tests)")]
    ActorTaskDied,
}

/// Reconciler to manage the configuration of `sshd` in the switch zone,
/// specifically for the `wicket` user.
///
/// This type interacts with the switch zone lifecycle managed by sled-agent
/// proper. The lifecycle management is very complex and has several known bugs
/// (e.g., see
/// <https://github.com/oxidecomputer/omicron/issues/11512#issuecomment-6222289558>).
/// This type attempts to integrate with that existing system without making
/// anything worse. The integration points are:
///
/// 1. After installing the switch zone but before booting it, sled-agent must
///    call [`WicketSshAuthReconciler::configure_switch_zone_before_boot()`],
///    and it must fail to start the switch zone if that method fails. A
///    successful return from that method ensures that `sshd` is configured
///    correctly when the switch zone starts (e.g., after a sled or rack cold
///    boot).
/// 2. After booting the zone, sled-agent must call
///    [`SwitchZoneBootedGuard::mark_zone_booted()`] on the guard returned by
///    `configure_switch_zone_before_boot()`. If sled-agent fails to boot the
///    zone, dropping the guard without calling `mark_zone_booted()` will result
///    in a call to [`WicketSshAuthReconciler::mark_zone_uninstalled()`], which
///    will stop the reconciler from attempting to run against a zone that
///    doesn't exist.
/// 3. While the switch zone is running, the inner actor task spawned by
///    [`WicketSshAuthReconciler::new()`] will watch for changes to the desired
///    wicket SSH auth config. When changes arrive, it will rewrite files inside
///    the switch zone and then send an SMF refresh to `sshd`. This happens
///    independently from the rest of sled-agent's switch zone management.
/// 4. When uninstalling the switch zone, sled-agent must call
///    [`WicketSshAuthReconciler::mark_zone_uninstalled()`]. This method is
///    infallible, and it notifies the inner actor task that there is no longer
///    a switch zone and that it should therefore go inert.
#[derive(Debug)]
pub struct WicketSshAuthReconciler {
    zone_lifecycle_tx: SwitchZoneLifecycleSender,
    status_rx: watch::Receiver<WicketSshAuthStatus>,
    log: Logger,
    // The reconciler actor task should live for the lifetime of sled-agent,
    // except in tests. We keep the task handle here and abort it if we're
    // dropped.
    _actor_task: AbortOnDropHandle<()>,
}

impl WicketSshAuthReconciler {
    pub fn new(
        wicket_auth_rx: WicketSshAuthReceiver,
        base_log: &Logger,
    ) -> Self {
        let log =
            base_log.new(slog::o!("component" => "WicketSshAuthReconciler"));
        let (actor, zone_lifecycle_tx, status_rx) =
            WicketSshAuthActor::new(wicket_auth_rx, log.clone());

        let actor_task = tokio::spawn({
            let log = log.clone();
            async move {
                match actor.run().await {
                    Ok(never) => match never {},
                    Err(err) => {
                        error!(
                            log,
                            "wicket ssh auth reconciler actor task exited! \
                             this should only happen in (unusual) tests!";
                            InlineErrorChain::new(&err),
                        );
                    }
                }
            }
        });

        Self {
            zone_lifecycle_tx,
            status_rx,
            log,
            _actor_task: AbortOnDropHandle::new(actor_task),
        }
    }

    /// Current status of wicket SSH auth reconciliation.
    pub fn status(&self) -> WicketSshAuthStatus {
        self.status_rx.borrow().clone()
    }

    /// Configure an installed-but-not-yet-booted switch zone.
    ///
    /// The caller _must_ call [`SwitchZoneBootedGuard::mark_zone_booted()`]
    /// once any subsequent setup is completed and the zone is booted. If the
    /// returned guard is dropped without doing so (e.g., because some part of
    /// the subsequent set failed), the guard will call
    /// [`WicketSshAuthReconciler::mark_zone_uninstalled()`] to avoid attempting
    /// to reconcile a not-booted zone.
    ///
    /// See the discussion on [`WicketSshAuthReconciler`].
    pub async fn configure_switch_zone_before_boot(
        &self,
        switch_zone: &InstalledZone,
    ) -> Result<SwitchZoneBootedGuard<'_>, WicketSshAuthReconcilerError> {
        self.zone_lifecycle_tx
            .configure_switch_zone_before_boot(switch_zone)
            .await?;
        Ok(SwitchZoneBootedGuard { booted: false, inner: self })
    }

    /// Notify the reconciler task that the switch zone has been uninstalled.
    ///
    /// See the discussion on [`WicketSshAuthReconciler`].
    pub fn mark_zone_uninstalled(&self) {
        self.zone_lifecycle_tx.mark_zone_uninstalled();
    }
}

#[derive(Debug)]
pub struct SwitchZoneBootedGuard<'a> {
    booted: bool,
    inner: &'a WicketSshAuthReconciler,
}

impl Drop for SwitchZoneBootedGuard<'_> {
    fn drop(&mut self) {
        if !self.booted {
            warn!(
                self.inner.log,
                "SwitchZoneBootedGuard dropped without being notified that \
                 switch zone booted; marking zone uninstalled"
            );
            self.inner.mark_zone_uninstalled();
        }
    }
}

impl SwitchZoneBootedGuard<'_> {
    /// Disarm this guard; must be called once the switch zone has booted.
    ///
    /// See [`WicketSshAuthReconciler::configure_switch_zone_before_boot()`].
    pub fn mark_zone_booted(mut self) {
        self.booted = true;
    }
}

fn write_wicket_authorized_keys(
    zone_root: &Utf8Path,
    wicket_ssh_auth: &WicketSshAuth,
    log: &Logger,
) -> Result<(), WicketSshAuthReconcilerError> {
    let path = zone_root.join(WICKET_AUTHORIZED_KEYS_PATH);
    let f = AtomicFile::new(&path, OverwriteBehavior::AllowOverwrite);

    info!(
        log, "writing wicket authorized_keys file";
        "path" => path.as_str(),
        "wicket_ssh_auth_gen" => wicket_ssh_auth.generation,
    );
    f.write(|f| {
        f.set_permissions(Permissions::from_mode(0o644))?;
        match &wicket_ssh_auth.kind {
            WicketSshAuthKind::Unauthenticated => {
                // do nothing - this will create an empty file
            }
            WicketSshAuthKind::PublicKeys {
                authorized_keys,
                allow_support_keys: _,
            } => {
                for key in authorized_keys {
                    writeln!(f, "{key}")?;
                }
            }
        }
        Ok(())
    })
    .map_err(|err| WicketSshAuthReconcilerError::WriteFailed {
        path,
        err: err.into(),
    })
}

fn write_wicket_sshd_config_snippet(
    zone_root: &Utf8Path,
    wicket_ssh_auth: &WicketSshAuth,
    log: &Logger,
) -> Result<(), WicketSshAuthReconcilerError> {
    let path = zone_root.join(WICKET_SSHD_CONFIG_PATH);
    let f = AtomicFile::new(&path, OverwriteBehavior::AllowOverwrite);

    info!(
        log, "writing wicket sshd_config snippet";
        "path" => path.as_str(),
        "wicket_ssh_auth_gen" => wicket_ssh_auth.generation,
    );
    f.write(|f| {
        f.set_permissions(Permissions::from_mode(0o644))?;
        match &wicket_ssh_auth.kind {
            WicketSshAuthKind::Unauthenticated => {
                writeln!(
                    f,
                    "# wicket ssh auth: generation {} (unauthenticated)
# Allow `wicket` to log in without a password
PermitEmptyPasswords yes
PasswordAuthentication yes
AuthenticationMethods none",
                    wicket_ssh_auth.generation,
                )
            }
            WicketSshAuthKind::PublicKeys {
                allow_support_keys,
                authorized_keys: _,
            } => {
                let mut auth_keys_file =
                    format!("/{WICKET_AUTHORIZED_KEYS_PATH}");
                if *allow_support_keys {
                    auth_keys_file
                        .push_str(&format!(" /{SUPPORT_AUTHORIZED_KEYS_PATH}"));
                }
                writeln!(
                    f,
                    "# wicket ssh auth: generation {} (public key auth)
# Require an authorized key to access `wicket`
PubkeyAuthentication yes
AuthenticationMethods publickey
AuthorizedKeysFile {auth_keys_file}",
                    wicket_ssh_auth.generation,
                )
            }
        }
    })
    .map_err(|err| WicketSshAuthReconcilerError::WriteFailed {
        path,
        err: err.into(),
    })
}

fn send_smf_refresh_to_sshd_in_switch_zone() -> anyhow::Result<()> {
    let scf = Scf::connect_zone(SWITCH_ZONE_NAME)?;
    let scope = scf.scope_local()?;
    let Some(service) = scope.service(SSHD_SERVICE_NAME)? else {
        bail!("no `{SSHD_SERVICE_NAME}` service present in switch zone");
    };
    let Some(mut instance) = service.instance(SSHD_INSTANCE_NAME)? else {
        bail!("no `{SSHD_INSTANCE_NAME}` instance within {}", service.fmri());
    };
    instance.smf_refresh()?;
    Ok(())
}

#[derive(Debug)]
struct WicketSshAuthActor {
    wicket_auth_rx: WicketSshAuthReceiver,
    zone_lifecycle_rx: SwitchZoneLifecycleReceiver,
    zone_state: ZoneState,
    log: Logger,
}

impl WicketSshAuthActor {
    fn new(
        wicket_auth_rx: WicketSshAuthReceiver,
        log: Logger,
    ) -> (Self, SwitchZoneLifecycleSender, watch::Receiver<WicketSshAuthStatus>)
    {
        let (zone_lifecycle_tx, zone_lifecycle_rx) =
            SwitchZoneLifecycle::channel();
        let (zone_state, zone_state_rx) = ZoneState::new();
        (
            Self { wicket_auth_rx, zone_lifecycle_rx, zone_state, log },
            zone_lifecycle_tx,
            zone_state_rx,
        )
    }

    async fn run(mut self) -> Result<Infallible, watch::error::RecvError> {
        const RECONFIGURE_RETRY_INTERVAL: Duration = Duration::from_secs(5);
        let mut need_reconfigure_retry = false;

        loop {
            // Check whether we have a switch zone. (If we don't, wait until we
            // get a "lifecycle change" event from sled-agent; i.e., wait for
            // sled-agent to tell us it's started the switch zone.)
            let Some(active_zone) = self.zone_state.active_switch_zone() else {
                let event = self.zone_lifecycle_rx.changed_value().await?;
                self.handle_lifecycle_changed(event).await;
                // Lifecycle events either succeed or fail; they cannot be
                // meaningfully retried. (Uninstalling the zone is infallible.
                // If configuring the zone for installation fails, there's
                // nothing to retry: our caller will have to try to install the
                // zone again.)
                need_reconfigure_retry = false;
                continue;
            };

            // This enum separates "use `select!` to decide what to do next"
            // from "actually do that", so we can make this `select!` more
            // obviously futurelock free: none of the arms contain an inner
            // `.await`.
            enum Op {
                LifecycleChanged(SwitchZoneLifecycle),
                Reconfigure(WicketSshAuth),
            }
            let op = tokio::select! {
                // Cancel-safe per the docs on changed().
                result = self.zone_lifecycle_rx.changed_value() => {
                    let event = result?;
                    info!(
                        self.log,
                        "reconciler activating due to \
                         switch zone lifecycle event",
                    );
                    Op::LifecycleChanged(event)
                }

                // Cancel-safe per the docs on changed_value().
                result = self.wicket_auth_rx.changed_value() => {
                    match result {
                        Ok(wicket_ssh_auth) => {
                            info!(
                                self.log,
                                "reconciler activating due to \
                                 change in wicket_ssh_auth";
                                "wicket_ssh_auth_gen" =>
                                    wicket_ssh_auth.generation,
                            );
                            Op::Reconfigure(wicket_ssh_auth)
                        }
                        Err(WicketSshAuthReceiverError::NoInternalDisks) => {
                            // The M.2s have gone away! We must have had them at
                            // some point (or we couldn't have successfully
                            // configured the zone initially); there's nothing
                            // we can do now in response to them going away. We
                            // have to leave the zone in whatever state it is
                            // until they come back.
                            warn!(
                                self.log,
                                "reconciler activated due to \
                                 change in wicket_ssh_auth, but \
                                 internal disks are no longer available!",
                            );
                            need_reconfigure_retry = false;
                            continue;
                        }
                        Err(WicketSshAuthReceiverError::RecvError(err)) => {
                            return Err(err);
                        }
                    }
                }

                // Inherently cancel-safe (it's just a sleep).
                () = tokio::time::sleep(RECONFIGURE_RETRY_INTERVAL),
                    if need_reconfigure_retry =>
                {
                    match self.wicket_auth_rx.current_and_update() {
                        Ok(wicket_ssh_auth) => {
                            info!(
                                self.log,
                                "reconciler activating to \
                                 retry after previous failure";
                                "wicket_ssh_auth_gen" =>
                                    wicket_ssh_auth.generation,
                            );
                            Op::Reconfigure(wicket_ssh_auth)
                        }
                        Err(WicketSshAuthReceiverError::NoInternalDisks) => {
                            // This is even more unfortunate: we _had_ an auth
                            // we were trying to apply, but failed, and on our
                            // retry now our M.2s are gone. As above there isn't
                            // anything we can do until this changes: we can
                            // stop retrying, and will be woken by the
                            // `.changed_value()` arm if/when the M.2s come
                            // back.
                            warn!(
                                self.log,
                                "reconciler activated to \
                                 retry after previous failure, but \
                                 internal disks are no longer available!",
                            );
                            need_reconfigure_retry = false;
                            continue;
                        }
                        Err(WicketSshAuthReceiverError::RecvError(err)) => {
                            return Err(err);
                        }
                    }
                }
            };

            // We've woken up from the `select!` and have something to do; do
            // that thing.
            match op {
                Op::LifecycleChanged(event) => {
                    self.handle_lifecycle_changed(event).await;

                    // Either we successfully configured the zone or cleared our
                    // zone state; either way, if we were previously retrying
                    // from a failed reconfigure, we can stop.
                    need_reconfigure_retry = false;
                }

                Op::Reconfigure(wicket_ssh_auth) => {
                    if Self::reconfigure_running_zone(
                        active_zone,
                        wicket_ssh_auth,
                        &self.log,
                    )
                    .await
                    .is_ok()
                    {
                        need_reconfigure_retry = false;
                    } else {
                        // If we have a new auth setting that we failed to
                        // reconfigure, we'll keep trying until we succeed (or
                        // more likely, until sled-agent tells us it's
                        // uninstalled the zone; us racing with zone shutdown is
                        // the most likely reason for this to fail).
                        need_reconfigure_retry = true;
                    }
                }
            }
        }
    }

    async fn handle_lifecycle_changed(&mut self, event: SwitchZoneLifecycle) {
        match event {
            SwitchZoneLifecycle::ConfigureForBoot { zone_root, response } => {
                let result = self.configure_for_boot(zone_root).await;
                // We don't care if our caller has gone away (although it
                // presumably shouldn't, since it's sled-agent!).
                response.send(result).ok();
            }
            SwitchZoneLifecycle::Uninstalled => {
                if self.zone_state.is_active() {
                    info!(
                        self.log,
                        "clearing state (switch zone being uninstalled)",
                    );
                }
                self.zone_state.set_inactive();
            }
        }
    }

    async fn configure_for_boot(
        &mut self,
        zone_root: Arc<Utf8Path>,
    ) -> Result<(), WicketSshAuthReconcilerError> {
        let (new_zone_state, result) =
            match self.wicket_auth_rx.current_and_update() {
                Ok(wicket_ssh_auth) => {
                    let log = self.log.clone();
                    let wicket_auth_gen = wicket_ssh_auth.generation;
                    let result = tokio::task::spawn_blocking(move || {
                        // When setting up the zone, the order here doesn't
                        // matter: both writes must succeed to proceed.
                        write_wicket_authorized_keys(
                            &zone_root,
                            &wicket_ssh_auth,
                            &log,
                        )?;
                        write_wicket_sshd_config_snippet(
                            &zone_root,
                            &wicket_ssh_auth,
                            &log,
                        )?;
                        Ok(zone_root)
                    })
                    .await
                    .expect("task did not panic");

                    match result {
                        Ok(zone_root) => {
                            info!(
                                self.log,
                                "configured initial switch zone";
                                "wicket_ssh_auth_gen" => wicket_auth_gen,
                            );
                            (Some((zone_root, wicket_auth_gen)), Ok(()))
                        }
                        Err(err) => {
                            warn!(
                                self.log,
                                "failed to configure initial switch zone";
                                "wicket_ssh_auth_gen" => wicket_auth_gen,
                                InlineErrorChain::new(&err),
                            );
                            (None, Err(err))
                        }
                    }
                }

                Err(err) => {
                    warn!(
                        self.log,
                        "failed to configure initial switch zone";
                        InlineErrorChain::new(&err),
                    );
                    (None, Err(WicketSshAuthReconcilerError::from(err)))
                }
            };

        match new_zone_state {
            Some((zone_root, effective_auth_gen)) => {
                self.zone_state.set_active(zone_root, effective_auth_gen);
            }
            None => {
                self.zone_state.set_inactive();
            }
        }

        result
    }

    async fn reconfigure_running_zone(
        active_zone: ActiveSwitchZone<'_>,
        wicket_ssh_auth: WicketSshAuth,
        log: &Logger,
    ) -> Result<(), ()> {
        let zone_root = Arc::clone(active_zone.zone_root());
        let wicket_auth_gen = wicket_ssh_auth.generation;

        let result: anyhow::Result<()> = {
            let log = log.clone();
            tokio::task::spawn_blocking(move || {
                // The order of operations here is a little tricky, and we
                // attempt to gracefully handle partial success as best we can.
                // We have three possible changes we're trying to effect:
                //
                // 1. Unauthenticated -> Authenticated
                // 2. Authenticated -> Authenticated with different keys
                // 3. Authenticated -> Unauthenticated
                //
                // and three (individually atomic, but not collectively)
                // operations to perform:
                //
                // * Write a new authorized_keys file
                // * Write a new sshd_config snippet
                // * Send an SMF refresh to sshd (must always come after writing
                //   the new sshd_config snippet, but can come before or after
                //   writing a new authorized_keys file)
                //
                // In state transitions 1 and 2, we should write
                // `authorized_keys` first. If we fail a subsequent step, then
                // our results are either:
                //
                // 1. We still allow unauthenticated access
                // 2. We've partially applied the new setting: at least the
                //    explicit `authorized_keys` file reflects the new desired
                //    state (even if we haven't successfully changed whether
                //    support keys are included)
                //
                // In state transition 3, the `authorized_keys` file we're going
                // to write is _empty_, so we should do that _after_ changing
                // the effective config of the running service. Writing an empty
                // `authorized_keys` first will lock *everyone* out of wicket
                // if we fail to write the new config or fail to send the SMF
                // refresh.
                match &wicket_ssh_auth.kind {
                    WicketSshAuthKind::Unauthenticated => {
                        // write and apply new config first, then clear
                        // authorized_keys
                        write_wicket_sshd_config_snippet(
                            &zone_root,
                            &wicket_ssh_auth,
                            &log,
                        )?;
                        send_smf_refresh_to_sshd_in_switch_zone()?;
                        write_wicket_authorized_keys(
                            &zone_root,
                            &wicket_ssh_auth,
                            &log,
                        )?;
                    }
                    WicketSshAuthKind::PublicKeys { .. } => {
                        // write authorized_keys first, then write and apply new
                        // config
                        write_wicket_authorized_keys(
                            &zone_root,
                            &wicket_ssh_auth,
                            &log,
                        )?;
                        write_wicket_sshd_config_snippet(
                            &zone_root,
                            &wicket_ssh_auth,
                            &log,
                        )?;
                        send_smf_refresh_to_sshd_in_switch_zone()?;
                    }
                }
                Ok(())
            })
            .await
            .expect("task did not panic")
        };

        match result {
            Ok(()) => {
                // TODO-correctness Technically at this point, we only know
                // we've successfully written the new config and sent an SMF
                // refresh to sshd. It's possible it hasn't picked up the new
                // config yet - should we try to account for that (presumably by
                // polling sshd's state somehow before returning?). For now we
                // don't, and assume that a successful refresh is sufficient.
                info!(
                    log,
                    "successfully refreshed sshd config in running switch zone";
                    "wicket_ssh_auth_gen" => wicket_auth_gen,
                );
                active_zone.set_effective_auth_gen(wicket_auth_gen);
                Ok(())
            }
            Err(err) => {
                let err = InlineErrorChain::new(&*err);
                warn!(
                    log,
                    "failed to update ssh config in running zone";
                    "wicket_ssh_auth_gen" => wicket_auth_gen,
                    &err,
                );
                active_zone.set_last_error(err.to_string());
                Err(())
            }
        }
    }
}

/// This is a separate module to enforce privacy of the [`ZoneState`] fields
/// separately from the [`WicketSshAuthActor`] that uses `ZoneState`.
mod zone_state {
    use super::*;

    // Invariants maintained by this type:
    //
    // * `zone_root` is `None` IFF `status_tx` is
    //   `WicketSshAuthStatus::NoSwitchZone`.
    // * `zone_root` is `Some(_)` IFF `status_tx` is
    //   `WicketSshAuthStatus::SwitchZone { .. }`.
    #[derive(Debug)]
    pub(super) struct ZoneState {
        // Path to `/` of the zone, from the perspective of the global zone
        // (i.e., where we're running).
        //
        // If `None`, we have no active switch zone.
        //
        zone_root: Option<Arc<Utf8Path>>,

        // Status channel for sled-agent inventory consumption.
        status_tx: watch::Sender<WicketSshAuthStatus>,
    }

    impl ZoneState {
        pub(super) fn new() -> (Self, watch::Receiver<WicketSshAuthStatus>) {
            let (status_tx, status_rx) =
                watch::channel(WicketSshAuthStatus::NoSwitchZone);
            (Self { zone_root: None, status_tx }, status_rx)
        }

        pub(super) fn set_active(
            &mut self,
            zone_root: Arc<Utf8Path>,
            effective_auth_gen: WicketSshAuthGeneration,
        ) {
            // Maintain invariant:
            //   `zone_root = Some(_)` <=> `WicketSshAuthStatus::SwitchZone`
            self.zone_root = Some(zone_root);
            self.status_tx.send_modify(|status| {
                *status = WicketSshAuthStatus::SwitchZone {
                    effective_auth_gen,
                    last_error: None,
                };
            });
        }

        pub(super) fn set_inactive(&mut self) {
            // Maintain invariant:
            //   `zone_root = None` <=> `WicketSshAuthStatus::NoSwitchZone`
            self.zone_root = None;
            self.status_tx.send_modify(|status| {
                *status = WicketSshAuthStatus::NoSwitchZone;
            });
        }

        pub(super) fn is_active(&self) -> bool {
            self.zone_root.is_some()
        }

        pub(super) fn active_switch_zone(
            &mut self,
        ) -> Option<ActiveSwitchZone<'_>> {
            let zone_root = self.zone_root.as_ref()?;
            Some(ActiveSwitchZone { zone_root, status_tx: &self.status_tx })
        }
    }

    // Projection of `ZoneState`'s fields when we have an active switch zone.
    //
    // Per the invariants on `ZoneState`, we're guaranteed that because we have
    // a `zone_root`, `status_tx` is in the `WicketSshAuthStatus::SwitchZone {
    // .. }` state.
    #[derive(Debug)]
    pub(super) struct ActiveSwitchZone<'a> {
        zone_root: &'a Arc<Utf8Path>,
        status_tx: &'a watch::Sender<WicketSshAuthStatus>,
    }

    impl ActiveSwitchZone<'_> {
        pub(super) fn zone_root(&self) -> &Arc<Utf8Path> {
            self.zone_root
        }

        pub(super) fn set_effective_auth_gen(
            &self,
            generation: WicketSshAuthGeneration,
        ) {
            self.status_tx.send_modify(|status| match &mut *status {
                WicketSshAuthStatus::NoSwitchZone => {
                    unreachable!("internal invariant violated")
                }
                WicketSshAuthStatus::SwitchZone {
                    effective_auth_gen,
                    last_error,
                } => {
                    *effective_auth_gen = generation;
                    *last_error = None;
                }
            });
        }

        pub(super) fn set_last_error(&self, err: String) {
            self.status_tx.send_modify(|status| match &mut *status {
                WicketSshAuthStatus::NoSwitchZone => {
                    unreachable!("internal invariant violated")
                }
                WicketSshAuthStatus::SwitchZone { last_error, .. } => {
                    *last_error = Some(err);
                }
            });
        }
    }
}
use self::zone_state::ActiveSwitchZone;
use self::zone_state::ZoneState;

/// As with `zone_state` above, use a private module to hide some gnarly details
/// from the [`WicketSshAuthActor`] that uses it.
mod switch_zone_lifecycle {
    use super::*;

    /// Variants describe the two lifecycle events we receive from sled-agent.
    ///
    /// See the discussion on [`WicketSshAuthReconciler`].
    #[derive(Debug)]
    pub(super) enum SwitchZoneLifecycle {
        ConfigureForBoot {
            zone_root: Arc<Utf8Path>,
            response: oneshot::Sender<Result<(), WicketSshAuthReconcilerError>>,
        },
        Uninstalled,
    }

    impl SwitchZoneLifecycle {
        /// Construct a new watch channel pair for communication between
        /// [`WicketSshAuthReconciler`] and [`WicketSshAuthActor`].
        ///
        /// This uses a watch channel so that zone uninstalls can be infallible.
        /// (Our actor should not be able to tell sled-agent it can't uninstall
        /// the switch zone!) This does make the
        /// [`SwitchZoneLifecycle::ConfigureForBoot`] variant a little tricky;
        /// see the discussion inside
        /// [`SwitchZoneLifecycleReceiver::changed_value()`].
        pub(super) fn channel()
        -> (SwitchZoneLifecycleSender, SwitchZoneLifecycleReceiver) {
            let (tx, rx) =
                watch::channel(SwitchZoneLifecycleInner::Uninstalled);
            (SwitchZoneLifecycleSender(tx), SwitchZoneLifecycleReceiver(rx))
        }
    }

    #[derive(Debug)]
    enum SwitchZoneLifecycleInner {
        ConfigureForBoot {
            zone_root: Arc<Utf8Path>,
            /// This is a very spicy type. We're using a watch channel as a
            /// quasi-mpsc channel, but without any queuing: sled-agent never
            /// tries to configure multiple switch zones concurrently. When
            /// sled-agent wants to configure the switch zone, we'll insert a
            /// oneshot response (behind a mutex, because the receiver only has
            /// read-only access), send this request to [`WicketSshAuthActor`],
            /// then wait for a response on this channel. See the discussion
            /// inside [`SwitchZoneLifecycleReceiver`] for more details.
            response: Mutex<
                Option<
                    oneshot::Sender<Result<(), WicketSshAuthReconcilerError>>,
                >,
            >,
        },
        Uninstalled,
    }

    #[derive(Debug)]
    pub(super) struct SwitchZoneLifecycleSender(
        watch::Sender<SwitchZoneLifecycleInner>,
    );

    impl SwitchZoneLifecycleSender {
        pub(super) async fn configure_switch_zone_before_boot(
            &self,
            switch_zone: &InstalledZone,
        ) -> Result<(), WicketSshAuthReconcilerError> {
            let (tx, rx) = oneshot::channel();
            self.0.send_modify(|inner| {
                *inner = SwitchZoneLifecycleInner::ConfigureForBoot {
                    zone_root: switch_zone.root().into(),
                    response: Mutex::new(Some(tx)),
                };
            });

            tokio::select! {
                result = rx => match result {
                    Ok(result) => result,
                    Err(_) => {
                        // This shouldn't happen: either the actor task took the
                        // response channel out of the `ConfigureForBoot` but
                        // dropped it without responding (a bug in the actor),
                        // or we had two concurrent
                        // `configure_switch_zone_before_boot()` calls, and the
                        // second caused the first response channel to be
                        // dropped before the actor could take it (a bug in the
                        // switch zone lifecycle). In either case, return an
                        // error here instead of unwrapping.
                        Err(WicketSshAuthReconcilerError::ResponseChannelDropped)
                    }
                },

                () = self.0.closed() => {
                    // This shouldn't happen either: the actor task has exited
                    // and is no longer holding the receiving half of this
                    // channel, which means it will never send us a response.
                    Err(WicketSshAuthReconcilerError::ActorTaskDied)
                }
            }
        }

        pub(super) fn mark_zone_uninstalled(&self) {
            self.0.send_modify(|inner| {
                *inner = SwitchZoneLifecycleInner::Uninstalled;
            });
        }
    }

    #[derive(Debug)]
    pub(super) struct SwitchZoneLifecycleReceiver(
        watch::Receiver<SwitchZoneLifecycleInner>,
    );

    impl SwitchZoneLifecycleReceiver {
        /// Wait for a new value to be received by this watch channel.
        ///
        /// Cancel-safe: the only `.await` point is a call to
        /// [`watch::Receiver::changed()`], which is itself cancel-safe.
        pub(super) async fn changed_value(
            &mut self,
        ) -> Result<SwitchZoneLifecycle, watch::error::RecvError> {
            loop {
                self.0.changed().await?;

                match &*self.0.borrow_and_update() {
                    SwitchZoneLifecycleInner::ConfigureForBoot {
                        zone_root,
                        response,
                    } => {
                        if let Some(response) = response.lock().unwrap().take()
                        {
                            return Ok(SwitchZoneLifecycle::ConfigureForBoot {
                                zone_root: Arc::clone(&zone_root),
                                response,
                            });
                        } else {
                            // This should be unreachable unless we spuriously
                            // wake up from `self.0.changed()` (and watch
                            // channels are not supposed to emit spurious
                            // wakeups!).
                            //
                            // The only way for us to see
                            // `SwitchZoneLifecycleInner::ConfigureForBoot` is
                            // if someone called
                            // `configure_switch_zone_before_boot()` on our
                            // sending half, and it always fills in `Some(_)`
                            // response. Therefore, the first time we see
                            // `ConfigureForBoot`, we'll always have
                            // `Some(response)`. We won't wake up again unless
                            // someone either calls
                            // `configure_switch_zone_before_boot()` again
                            // (which will fill in `Some(response)`, again) or
                            // if someone calls `mark_zone_uninstalled()` (which
                            // will put us in the `Uninstalled` arm below).
                            //
                            // If we happen to see this, just continue: wait for
                            // the next `.changed()` event: we can't do anything
                            // with this one.
                            continue;
                        }
                    }
                    SwitchZoneLifecycleInner::Uninstalled => {
                        return Ok(SwitchZoneLifecycle::Uninstalled);
                    }
                }
            }
        }
    }
}
use self::switch_zone_lifecycle::SwitchZoneLifecycle;
use self::switch_zone_lifecycle::SwitchZoneLifecycleReceiver;
use self::switch_zone_lifecycle::SwitchZoneLifecycleSender;
