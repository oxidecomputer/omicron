// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tokio task responsible for managing the [`OmicronSledConfig`] ledger.

use camino::Utf8PathBuf;
use dropshot::HttpError;
use legacy_configs::convert_legacy_ledgers;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use omicron_common::api::external::Generation;
use omicron_common::ledger;
use omicron_common::ledger::Ledger;
use sled_agent_api::ArtifactConfig;
use slog::Logger;
use slog::error;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::ops::Deref as _;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tufaceous_artifact::ArtifactHash;

use crate::InternalDisksReceiver;
use crate::SledAgentArtifactStore;

mod legacy_configs;

const CONFIG_LEDGER_FILENAME: &str = "omicron-sled-config.json";

#[derive(Debug, thiserror::Error)]
pub enum LedgerTaskError {
    #[error("ledger task has not been started yet")]
    NotYetStarted,
    #[error("ledger task busy; cannot service new requests")]
    Busy,
    #[error("internal error: ledger task exited!")]
    Exited,
}

impl From<LedgerTaskError> for HttpError {
    fn from(err: LedgerTaskError) -> Self {
        let message = InlineErrorChain::new(&err).to_string();
        match err {
            LedgerTaskError::NotYetStarted | LedgerTaskError::Busy => {
                HttpError::for_unavail(None, message)
            }
            LedgerTaskError::Exited => HttpError::for_internal_error(message),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LedgerNewConfigError {
    #[error("cannot write sled config ledger: no internal disks available")]
    NoInternalDisks,
    #[error(
        "sled config generation out of date (got {requested}, have {current})"
    )]
    GenerationOutdated { current: Generation, requested: Generation },
    #[error("sled config changed with the same generation ({generation})")]
    ConfigurationChanged { generation: Generation },
    #[error("failed to commit sled config to ledger")]
    LedgerCommitFailed(#[source] ledger::Error),
    #[error("sled config failed artifact store existence checks: {0}")]
    ArtifactStoreValidationFailed(String),
}

impl From<LedgerNewConfigError> for HttpError {
    fn from(err: LedgerNewConfigError) -> Self {
        let message = InlineErrorChain::new(&err).to_string();
        match err {
            LedgerNewConfigError::NoInternalDisks => {
                HttpError::for_unavail(None, message)
            }
            LedgerNewConfigError::GenerationOutdated { .. }
            | LedgerNewConfigError::ConfigurationChanged { .. }
            | LedgerNewConfigError::ArtifactStoreValidationFailed(_) => {
                HttpError::for_bad_request(None, message)
            }
            LedgerNewConfigError::LedgerCommitFailed(_) => {
                HttpError::for_internal_error(message)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LedgerArtifactConfigError {
    #[error("cannot validate artifact config: no internal disks available")]
    NoInternalDisks,
    #[error(
        "Artifacts in use by ledgered sled config are not present \
         in new artifact config: {0:?}"
    )]
    InUseArtifactedMissing(BTreeSet<ArtifactHash>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CurrentSledConfig {
    /// We're still waiting on the M.2 drives to be found: We don't yet know
    /// whether we have a ledgered config, nor would we be able to write one.
    WaitingForInternalDisks,
    /// We have at least one M.2 drive, but there is no ledgered config: we're
    /// waiting for our initial config (from RSS, if the rack hasn't been set up
    /// yet, or from Nexus if we're a newly-added sled).
    WaitingForInitialConfig,
    /// We have a ledgered config.
    Ledgered(OmicronSledConfig),
}

#[derive(Debug)]
pub(crate) struct LedgerTaskHandle {
    request_tx: mpsc::Sender<LedgerTaskRequest>,
    current_config_rx: watch::Receiver<CurrentSledConfig>,
}

impl LedgerTaskHandle {
    pub fn spawn_ledger_task<T: SledAgentArtifactStore>(
        internal_disks_rx: InternalDisksReceiver,
        artifact_store: T,
        log: Logger,
    ) -> (Self, watch::Receiver<CurrentSledConfig>) {
        // We only accept two kinds of requests on this channel, both of which
        // come from HTTP requests to sled agent:
        //
        // 1. New `OmicronSledConfig`s to ledger
        // 2. Validate that a new incoming `ArtifactConfig` doesn't remove
        //    artifacts we need for our current `OmicronSledConfig`
        //
        // All running Nexuses will send us both kinds of requests periodically,
        // so we should have a channel depth of at least 6 (3 Nexuses * 2 kinds
        // of requests could all arrive concurrently). If everything is healthy,
        // we should finish processing any such request pretty quickly, so we
        // probably don't want a depth much higher than that (so we can detect
        // problems via sending back HTTP unavails from a full channel if we're
        // backed up). We'll double it to give a little bit of headroom but not
        // too much. This is a WAG at a reasonable number.
        let (request_tx, request_rx) = mpsc::channel(12);

        // We always start in the "waiting for internal disks" state. The
        // internal disk management task is started more or less concurrently
        // with us, so we won't stay in this state for long unless something is
        // very wrong.
        let (current_config_tx, current_config_rx) =
            watch::channel(CurrentSledConfig::WaitingForInternalDisks);

        tokio::spawn(
            LedgerTask {
                artifact_store,
                request_rx,
                internal_disks_rx,
                current_config_tx,
                log,
            }
            .run(),
        );

        (
            Self { request_tx, current_config_rx: current_config_rx.clone() },
            current_config_rx,
        )
    }

    pub(crate) fn current_config(&self) -> CurrentSledConfig {
        self.current_config_rx.borrow().clone()
    }

    pub async fn set_new_config(
        &self,
        new_config: OmicronSledConfig,
    ) -> Result<Result<(), LedgerNewConfigError>, LedgerTaskError> {
        self.try_send_request(|tx| LedgerTaskRequest::WriteNewConfig {
            new_config,
            tx,
        })
        .await
    }

    /// Confirm that a new [`ArtifactConfig`] is valid given the contents of the
    /// currently-ledgered [`OmicronSledConfig`].
    ///
    /// In particular, this confirms that a new artifact config does not
    /// _remove_ any artifacts needed by zones described by the current sled
    /// config.
    ///
    /// The artifact store in sled agent and this task need to coordinate with
    /// each other whenever changes are made to either kind of config. This
    /// method provides a path for the artifact store to validate its incoming
    /// artifact configs against this task, and this task uses the
    /// implementation of [`SledAgentArtifactStore`] to validate its incoming
    /// sled configs against the artifact store. Validation is always performed
    /// by this task, which enforces serialization of the checks in the event of
    /// requests arriving concurrently to change both configs.
    pub async fn validate_artifact_config(
        &self,
        new_config: ArtifactConfig,
    ) -> Result<Result<(), LedgerArtifactConfigError>, LedgerTaskError> {
        self.try_send_request(|tx| LedgerTaskRequest::ValidateArtifactConfig {
            new_config,
            tx,
        })
        .await
    }

    async fn try_send_request<T, F>(
        &self,
        make_request: F,
    ) -> Result<T, LedgerTaskError>
    where
        F: FnOnce(oneshot::Sender<T>) -> LedgerTaskRequest,
    {
        let (tx, rx) = oneshot::channel();
        let request = make_request(tx);
        self.request_tx.try_send(request).map_err(|err| match err {
            // We should only see this error if the ledger task has gotten badly
            // behind updating ledgers on M.2s.
            TrySendError::Full(_) => LedgerTaskError::Busy,
            // We should never see this error in production, as the ledger task
            // never exits, but may see it in tests.
            TrySendError::Closed(_) => LedgerTaskError::Exited,
        })?;
        rx.await.map_err(|_| {
            // As above, we should never see this error in production.
            LedgerTaskError::Exited
        })
    }
}

#[derive(Debug)]
enum LedgerTaskRequest {
    WriteNewConfig {
        new_config: OmicronSledConfig,
        tx: oneshot::Sender<Result<(), LedgerNewConfigError>>,
    },
    ValidateArtifactConfig {
        new_config: ArtifactConfig,
        tx: oneshot::Sender<Result<(), LedgerArtifactConfigError>>,
    },
}

struct LedgerTask<T> {
    artifact_store: T,
    request_rx: mpsc::Receiver<LedgerTaskRequest>,
    internal_disks_rx: InternalDisksReceiver,
    current_config_tx: watch::Sender<CurrentSledConfig>,
    log: Logger,
}

impl<T: SledAgentArtifactStore> LedgerTask<T> {
    async fn run(self) {
        // This pattern match looks strange, but `run_impl()` cannot return
        // `Ok(_)`; it must run forever (or until failure).
        let Err((log, err)) = self.run_impl().await;
        error!(
            log,
            "LedgerTask::run() unexpectedly exited; this should only be \
             possible in tests due to runtime shutdown ordering";
            InlineErrorChain::new(&err),
        );
    }

    async fn run_impl(
        mut self,
    ) -> Result<Infallible, (Logger, LedgerTaskExit)> {
        // We created `self.current_config_tx` in `spawn_ledger_task()` and own
        // the only sender, so it should start out in the `WaitingForM2Disks`
        // state. We need to wait for at least one M.2 before we can do
        // anything, and `wait_for_m2_disks` should not return until we've seen
        // one.
        if let Err(err) = self.wait_for_m2_disks().await {
            return Err((self.log, err));
        }
        assert_ne!(
            *self.current_config_tx.borrow(),
            CurrentSledConfig::WaitingForInternalDisks
        );

        loop {
            let Some(request) = self.request_rx.recv().await else {
                return Err((
                    self.log,
                    LedgerTaskExit::LedgerTaskHandleDropped,
                ));
            };

            match request {
                LedgerTaskRequest::WriteNewConfig { new_config, tx } => {
                    // We don't care if the receiver is gone.
                    _ = tx.send(self.set_new_config(new_config).await);
                }
                LedgerTaskRequest::ValidateArtifactConfig {
                    new_config,
                    tx,
                } => {
                    // We don't care if the receiver is gone.
                    _ = tx.send(self.validate_artifact_config(new_config));
                }
            }
        }
    }

    async fn set_new_config(
        &mut self,
        new_config: OmicronSledConfig,
    ) -> Result<(), LedgerNewConfigError> {
        let internal_disks = self.internal_disks_rx.current_and_update();
        let mut config_datasets =
            internal_disks.all_config_datasets().peekable();

        // This would be very unusual: We waited for at least one M.2 to be
        // present in `run_impl`, so if this is empty that means we've lost all
        // M.2s. Refuse to accept new config.
        if config_datasets.peek().is_none() {
            error!(self.log, "no internal disks available any longer?!");
            return Err(LedgerNewConfigError::NoInternalDisks);
        }

        // Check that we can accept the new ledger.
        self.validate_sled_config(&new_config).await?;

        // `validate_sled_config` confirmed that the new config is valid in
        // terms of generation numbers. The most common valid case is
        // "generation the same, contents unchanged", because Nexus periodically
        // sends us our current config regardless of whether we already have it.
        // In that most common case, we now fall through to committing the
        // ledger to disk. This seems unnecessary: if the generation matches, we
        // already committed the ledger previously. But we don't know that it's
        // been written to both M.2s:
        //
        // * It's possible only one M.2 was available when we previously
        //   committed
        // * It's possible we had two M.2s, but one of the writes failed
        //   (`Ledger::commit()` returns `Ok(())` as long as at least one of the
        //   writes succeeds)
        //
        // We could probably address both of those and avoid spurious writes,
        // but it seems a little tricky and not particularly important. Sled
        // configs are currently on the order of 25 KiB. If we get a request
        // from each Nexus every minute, that's ~75 KiB of usually-spurious
        // writes to both M.2 drives every minute. Rounding up to 100 KiB/minute
        // (to give some room for the config to grow), that's ~150 MiB / day,
        // and the our drive endurance is 1 TiB / day (1 DWPD). We're using up
        // something like 0.01% of the drive endurance via these writes.

        // Write the validated configs to our ledger.
        let config_paths = config_datasets
            .map(|p| p.join(CONFIG_LEDGER_FILENAME))
            .collect::<Vec<_>>();
        let mut ledger = Ledger::new_with(&self.log, config_paths, new_config);
        match ledger.commit().await {
            Ok(()) => {
                info!(
                    self.log, "updated sled config ledger";
                    "generation" => %ledger.data().generation,
                );

                // Now that we've committed the ledger, update our watch channel
                let new_config =
                    CurrentSledConfig::Ledgered(ledger.into_inner());
                self.current_config_tx.send_if_modified(|c| {
                    if *c == new_config {
                        false
                    } else {
                        *c = new_config;
                        true
                    }
                });

                Ok(())
            }
            Err(err) => {
                warn!(
                    self.log, "Failed to write sled config to ledger";
                    "generation" => %ledger.data().generation,
                    InlineErrorChain::new(&err),
                );
                Err(LedgerNewConfigError::LedgerCommitFailed(err))
            }
        }
    }

    // Perform checks on a new incoming config that it's valid given our current
    // config (and other external state, like our artifact store contents).
    async fn validate_sled_config(
        &self,
        new_config: &OmicronSledConfig,
    ) -> Result<(), LedgerNewConfigError> {
        // The first check is whether the generation is too old. Clone our
        // current config to avoid holding the watch channel lock while we do
        // our check.
        let current_config = self.current_config_tx.borrow().clone();
        match current_config {
            CurrentSledConfig::WaitingForInternalDisks => {
                unreachable!("already waited for internal disks")
            }
            // If this is the first config we've gotten, we have no previous
            // generation to check.
            CurrentSledConfig::WaitingForInitialConfig => (),
            CurrentSledConfig::Ledgered(omicron_sled_config) => {
                if new_config.generation < omicron_sled_config.generation {
                    info!(
                        self.log,
                        "rejecting config request due to \
                         out-of-date generation";
                        "current-gen" => %omicron_sled_config.generation,
                        "request-gen" => %new_config.generation,
                    );
                    return Err(LedgerNewConfigError::GenerationOutdated {
                        current: omicron_sled_config.generation,
                        requested: new_config.generation,
                    });
                } else if new_config.generation
                    == omicron_sled_config.generation
                {
                    if *new_config != omicron_sled_config {
                        warn!(
                            self.log,
                            "requested config changed (with same generation)";
                            "generation" => %new_config.generation,
                        );
                        return Err(
                            LedgerNewConfigError::ConfigurationChanged {
                                generation: new_config.generation,
                            },
                        );
                    }
                    info!(
                        self.log, "configuration unchanged";
                        "generation" => %new_config.generation,
                    );
                }
            }
        }

        // Continue validating the incoming config. For now, the only other
        // thing we confirm is that any referenced artifacts are present in the
        // artifact store.
        let mut artifact_validation_errors = Vec::new();
        for artifact_hash in config_artifact_hashes(new_config) {
            match self
                .artifact_store
                .validate_artifact_exists_in_storage(artifact_hash)
                .await
            {
                Ok(()) => (),
                Err(err) => {
                    artifact_validation_errors.push(format!("{err:#}"));
                }
            }
        }
        if !artifact_validation_errors.is_empty() {
            return Err(LedgerNewConfigError::ArtifactStoreValidationFailed(
                artifact_validation_errors.join(", "),
            ));
        }

        // We could do more validation; e.g., self-consistency checks a la what
        // `blippy` does for blueprints (zones only reference datasets that
        // exist, at most one dataset kind per zpool, etc.), but we'll rely on
        // Nexus only sending us internally consistent configs for now.
        Ok(())
    }

    fn validate_artifact_config(
        &mut self,
        new_config: ArtifactConfig,
    ) -> Result<(), LedgerArtifactConfigError> {
        // It's okay to hold this borrow for the check below; this is quick and
        // synchronous.
        let current_config = self.current_config_tx.borrow();
        let sled_config = match current_config.deref() {
            CurrentSledConfig::WaitingForInternalDisks => {
                // If we have no disks, we don't know yet whether we have a sled
                // config, so it's not safe to accept any artifact config
                // changes.
                return Err(LedgerArtifactConfigError::NoInternalDisks);
            }
            CurrentSledConfig::WaitingForInitialConfig => {
                // If we have no config, any artifact config is fine.
                return Ok(());
            }
            CurrentSledConfig::Ledgered(sled_config) => sled_config,
        };

        let mut missing = BTreeSet::new();
        for artifact_hash in config_artifact_hashes(sled_config) {
            if !new_config.artifacts.contains(&artifact_hash) {
                missing.insert(artifact_hash);
            }
        }

        if missing.is_empty() {
            Ok(())
        } else {
            Err(LedgerArtifactConfigError::InUseArtifactedMissing(missing))
        }
    }

    // Wait for at least one M.2 disk to be available.
    //
    // # Panics
    //
    // This method panics if called when the current config state is anything
    // other than `CurrentSledConfig::WaitingForInternalDisks`, and is
    // guaranteed to set the state to something _other_ than
    // `CurrentSledConfig::WaitingForInternalDisks` on a successful return. It
    // should only be called once, at the beginning of the execution of this
    // task.
    async fn wait_for_m2_disks(&mut self) -> Result<(), LedgerTaskExit> {
        assert_eq!(
            *self.current_config_tx.borrow(),
            CurrentSledConfig::WaitingForInternalDisks
        );
        loop {
            let internal_disks = self.internal_disks_rx.current_and_update();
            let config_datasets =
                internal_disks.all_config_datasets().peekable();

            // Check for a sled config on all our config datasets. (If we have
            // no config datasets, we'll keep waiting!)
            match load_sled_config(
                &config_datasets.collect::<Vec<_>>(),
                &self.log,
            )
            .await
            {
                // If we're still waiting, fall through to the `select!` below.
                CurrentSledConfig::WaitingForInternalDisks => (),
                // Otherwise, we're done waiting: set our loaded config (which
                // might be `WaitingForInitialConfig` if we have disks but they
                // have no ledger contents!).
                config => {
                    self.current_config_tx.send_modify(|c| *c = config);
                    return Ok(());
                }
            }

            // We don't have at least one M.2; wait for a change in
            // internal_disks, and reject any incoming requests in the meantime.
            tokio::select! {
                // Cancel-safe per docs on `changed()`
                result = self.internal_disks_rx.changed() => {
                    if result.is_err() {
                        return Err(LedgerTaskExit::InternalDisksSenderDropped);
                    }
                    continue;
                }

                // Cancel-safe per docs on `recv()`
                request = self.request_rx.recv() => {
                    let Some(request) = request else {
                        return Err(LedgerTaskExit::LedgerTaskHandleDropped);
                    };
                    // If we got a request, return an error. (We should never
                    // get these requests, because sled-agent shouldn't progress
                    // to a point where it could send them if we still don't
                    // have any internal disks.)
                    match request {
                        LedgerTaskRequest::WriteNewConfig { tx, .. } => {
                            // We don't care if the receiver is gone.
                            let _ = tx.send(Err(
                                LedgerNewConfigError::NoInternalDisks,
                            ));
                        }
                        LedgerTaskRequest::ValidateArtifactConfig {
                            tx,
                            ..
                        } => {
                            // We don't care if the receiver is gone.
                            let _ = tx.send(Err(
                                LedgerArtifactConfigError::NoInternalDisks,
                            ));
                        }
                    }
                }
            }
        }
    }
}

// Helper to return all the `ArtifactHash`es contained in a config.
fn config_artifact_hashes(
    config: &OmicronSledConfig,
) -> impl Iterator<Item = ArtifactHash> + '_ {
    config
        .zones
        .iter()
        .filter_map(|zone| zone.image_source.artifact_hash())
        .chain(config.host_phase_2.slot_a.artifact_hash())
        .chain(config.host_phase_2.slot_b.artifact_hash())
}

async fn load_sled_config(
    config_datasets: &[Utf8PathBuf],
    log: &Logger,
) -> CurrentSledConfig {
    if config_datasets.is_empty() {
        return CurrentSledConfig::WaitingForInternalDisks;
    }

    // First try to load the ledger from our expected path(s).
    let paths = config_datasets
        .iter()
        .map(|p| p.join(CONFIG_LEDGER_FILENAME))
        .collect();
    info!(
        log, "Attempting to load sled config from ledger";
        "paths" => ?paths,
    );
    if let Some(config) = Ledger::new(log, paths).await {
        info!(log, "Ledger of sled config exists");
        return CurrentSledConfig::Ledgered(config.into_inner());
    }

    // If we have no ledgered config, see if we can convert from the previous
    // triple of legacy ledgers.
    if let Some(config) = convert_legacy_ledgers(&config_datasets, log).await {
        info!(log, "Converted legacy triple of ledgers into new sled config");
        return CurrentSledConfig::Ledgered(config);
    }

    // We have no ledger and didn't find legacy ledgers to convert; we must be
    // waiting for RSS (if we're pre-rack-setup) or for Nexus to send us a
    // config (if we're a sled being added to an existing rack).
    info!(log, "No sled config ledger exists");
    CurrentSledConfig::WaitingForInitialConfig
}

// `LedgerTask` should not exit in production, but may exit during tests
// depending on the drop order of various channels. All such exits are covered
// by this error (in the hopefully-impossible event we see it outside of tests).
#[derive(Debug, thiserror::Error)]
enum LedgerTaskExit {
    #[error("internal error: InternalDisks watch channel Sender dropped")]
    InternalDisksSenderDropped,
    #[error("internal error: LedgerTaskHandle dropped")]
    LedgerTaskHandleDropped,
}

#[cfg(test)]
mod tests {
    use super::legacy_configs::tests::LEGACY_DATASETS_PATH;
    use super::legacy_configs::tests::LEGACY_DISKS_PATH;
    use super::legacy_configs::tests::LEGACY_ZONES_PATH;
    use super::*;
    use crate::ledger::legacy_configs::tests::test_data_merged_config;
    use anyhow::anyhow;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use id_map::IdMap;
    use illumos_utils::zpool::ZpoolName;
    use nexus_sled_agent_shared::inventory::HostPhase2DesiredContents;
    use nexus_sled_agent_shared::inventory::HostPhase2DesiredSlots;
    use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
    use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
    use nexus_sled_agent_shared::inventory::OmicronZoneType;
    use omicron_common::disk::DiskIdentity;
    use omicron_common::disk::OmicronPhysicalDiskConfig;
    use omicron_test_utils::dev;
    use omicron_test_utils::dev::poll::CondCheckError;
    use omicron_test_utils::dev::poll::wait_for_watch_channel_condition;
    use omicron_uuid_kinds::InternalZpoolUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_storage::config::MountConfig;
    use std::collections::BTreeSet;
    use std::time::Duration;

    #[derive(Debug, Default)]
    struct FakeArtifactStore {
        artifacts: Option<BTreeSet<ArtifactHash>>,
    }

    impl SledAgentArtifactStore for FakeArtifactStore {
        async fn validate_artifact_exists_in_storage(
            &self,
            artifact: ArtifactHash,
        ) -> anyhow::Result<()> {
            let Some(artifacts) = self.artifacts.as_ref() else {
                return Ok(());
            };
            if artifacts.contains(&artifact) {
                Ok(())
            } else {
                Err(anyhow!("no such artifact: {artifact}"))
            }
        }
    }

    fn make_fake_disk() -> (DiskIdentity, InternalZpoolUuid) {
        (
            DiskIdentity {
                vendor: "ledger-test".into(),
                model: "ledger-test".into(),
                serial: "ledger-test-disk".into(),
            },
            InternalZpoolUuid::new_v4(),
        )
    }

    // Helper for constructing a LedgerTask backed by a fake internal disk that
    // points to a temporary directory.
    struct TestHarness {
        _tempdir: Utf8TempDir,
        task_handle: LedgerTaskHandle,
        internal_disks_rx: InternalDisksReceiver,
        current_config_rx: watch::Receiver<CurrentSledConfig>,
    }

    impl TestHarness {
        async fn new(log: Logger) -> Self {
            Self::build(log, FakeArtifactStore::default(), None, false).await
        }

        async fn with_fake_artifacts(
            log: Logger,
            artifacts: impl Iterator<Item = ArtifactHash>,
        ) -> Self {
            Self::build(
                log,
                FakeArtifactStore { artifacts: Some(artifacts.collect()) },
                None,
                false,
            )
            .await
        }

        async fn with_initial_config(
            log: Logger,
            config: &OmicronSledConfig,
        ) -> Self {
            Self::build(log, FakeArtifactStore::default(), Some(config), false)
                .await
        }

        async fn with_legacy_ledgers(log: Logger) -> Self {
            Self::build(log, FakeArtifactStore::default(), None, true).await
        }

        // If `sled_config` is `Some(_)`, that config will be written to the
        // tempdir's config dataset before the ledger task is spawned.
        // Otherwise, if `copy_legacy_ledgers` is true, we'll copy our test data
        // legacy ledgers into the tempdir's config dataset.
        async fn build(
            log: Logger,
            fake_artifact_store: FakeArtifactStore,
            sled_config: Option<&OmicronSledConfig>,
            copy_legacy_ledgers: bool,
        ) -> Self {
            // Create the tempdir.
            let tempdir = Utf8TempDir::new().expect("created temp directory");

            // Set up a fake `InternalDisksReceiver` and create the "config
            // dataset" inside our tempdir.
            let mount_config = MountConfig {
                root: tempdir.path().to_owned(),
                synthetic_disk_root: "/irrelevant".into(),
            };
            let (_disks_tx, disks_rx) = watch::channel(vec![make_fake_disk()]);
            let internal_disks_rx = InternalDisksReceiver::fake_dynamic(
                mount_config.into(),
                disks_rx,
            );
            for path in internal_disks_rx.current().all_config_datasets() {
                tokio::fs::create_dir_all(&path)
                    .await
                    .expect("created config dataset paths in tempdir");
                if let Some(sled_config) = sled_config {
                    let path = path.join(CONFIG_LEDGER_FILENAME);
                    let file = std::fs::File::create_new(&path)
                        .expect("created config file");
                    serde_json::to_writer(file, &sled_config)
                        .expect("wrote config to file");
                } else if copy_legacy_ledgers {
                    for src in [
                        LEGACY_DISKS_PATH,
                        LEGACY_DATASETS_PATH,
                        LEGACY_ZONES_PATH,
                    ] {
                        let src = Utf8Path::new(src);
                        let dst = path.join(src.file_name().unwrap());

                        tokio::fs::copy(src, dst)
                            .await
                            .expect("staged file in tempdir");
                    }
                }
            }

            // Spawn the ledger task.
            let (task_handle, mut current_config_rx) =
                LedgerTaskHandle::spawn_ledger_task(
                    internal_disks_rx.clone(),
                    fake_artifact_store,
                    log,
                );

            // Wait for the task to check our fake disk and progress to either
            // `Ledgered` (if we copied in a config) or
            // `WaitingForInitialConfig` (if we didn't).
            wait_for_watch_channel_condition(
                &mut current_config_rx,
                async |config| match config {
                    CurrentSledConfig::WaitingForInternalDisks => {
                        Err(CondCheckError::<()>::NotYet)
                    }
                    CurrentSledConfig::WaitingForInitialConfig => {
                        assert!(sled_config.is_none() && !copy_legacy_ledgers);
                        Ok(())
                    }
                    CurrentSledConfig::Ledgered(_) => {
                        assert!(sled_config.is_some() || copy_legacy_ledgers);
                        Ok(())
                    }
                },
                Duration::from_secs(30),
            )
            .await
            .expect("ledger task loaded config from disk");

            Self {
                _tempdir: tempdir,
                task_handle,
                internal_disks_rx,
                current_config_rx,
            }
        }
    }

    fn make_dummy_zone_config_using_artifact_hash(
        hash: ArtifactHash,
    ) -> OmicronZoneConfig {
        OmicronZoneConfig {
            id: OmicronZoneUuid::new_v4(),
            filesystem_pool: Some(ZpoolName::new_external(ZpoolUuid::new_v4())),
            zone_type: OmicronZoneType::Oximeter {
                address: "[::1]:0".parse().unwrap(),
            },
            image_source: OmicronZoneImageSource::Artifact { hash },
        }
    }

    fn make_dummy_disk_config(serial: &str) -> OmicronPhysicalDiskConfig {
        OmicronPhysicalDiskConfig {
            identity: DiskIdentity {
                vendor: "test-vendor".into(),
                model: "test-model".into(),
                serial: serial.into(),
            },
            id: PhysicalDiskUuid::new_v4(),
            pool_id: ZpoolUuid::new_v4(),
        }
    }

    // Several tests below want a nonempty sled config but don't care about the
    // particular contents.
    fn make_nonempty_sled_config() -> OmicronSledConfig {
        OmicronSledConfig {
            generation: Generation::new().next(),
            disks: [make_dummy_disk_config("test-serial")]
                .into_iter()
                .collect(),
            datasets: IdMap::default(),
            zones: IdMap::default(),
            remove_mupdate_override: None,
            host_phase_2: HostPhase2DesiredSlots::current_contents(),
        }
    }

    #[tokio::test]
    async fn wait_for_at_least_one_internal_disk() {
        let logctx = dev::test_setup_log("wait_for_at_least_one_internal_disk");

        // Setup: create a fake InternalDisksReceiver with no disks.
        let tempdir = Utf8TempDir::new().expect("created temp directory");
        let mount_config = MountConfig {
            root: tempdir.path().to_owned(),
            synthetic_disk_root: "/irrelevant".into(),
        };
        let (disks_tx, disks_rx) = watch::channel(Vec::new());
        let internal_disks_rx =
            InternalDisksReceiver::fake_dynamic(mount_config.into(), disks_rx);

        // Spawn the ledger task. It should sit in the `WaitingForInternalDisks`
        // state.
        let (_task_handle, mut current_config_rx) =
            LedgerTaskHandle::spawn_ledger_task(
                internal_disks_rx,
                FakeArtifactStore::default(),
                logctx.log.clone(),
            );

        assert_eq!(
            *current_config_rx.borrow_and_update(),
            CurrentSledConfig::WaitingForInternalDisks
        );

        // Populate a fake disk.
        disks_tx.send(vec![make_fake_disk()]).expect("receiver still exists");

        // Confirm the ledger task notices and progresses to
        // `WaitingForInitialConfig`.
        wait_for_watch_channel_condition(
            &mut current_config_rx,
            async |config| match config {
                CurrentSledConfig::WaitingForInternalDisks => {
                    Err(CondCheckError::<()>::NotYet)
                }
                CurrentSledConfig::WaitingForInitialConfig => Ok(()),
                CurrentSledConfig::Ledgered(config) => {
                    panic!("unexpected config found: {config:?}");
                }
            },
            Duration::from_secs(30),
        )
        .await
        .expect("ledger task noticed internal disk");

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn first_config_is_ledgered() {
        let logctx = dev::test_setup_log("first_config_is_ledgered");

        // Set up the ledger task with no initial config.
        let mut test_harness = TestHarness::new(logctx.log.clone()).await;

        // Send a new config; the contents here don't matter much, but we'll do
        // something slightly more than `::default()` and give it a single fake
        // disk. Below we'll check that this config was successfully ledgered.
        let sled_config = make_nonempty_sled_config();
        test_harness
            .task_handle
            .set_new_config(sled_config.clone())
            .await
            .expect("no ledger task error")
            .expect("no config error");

        // Confirm that the watch channel was updated.
        assert_eq!(
            *test_harness.current_config_rx.borrow_and_update(),
            CurrentSledConfig::Ledgered(sled_config.clone()),
        );

        // Also confirm the config was persisted as expected.
        for path in
            test_harness.internal_disks_rx.current().all_config_datasets()
        {
            let path = path.join(CONFIG_LEDGER_FILENAME);
            let persisted = tokio::fs::read(&path)
                .await
                .expect("read persisted sled config");
            let parsed: OmicronSledConfig = serde_json::from_slice(&persisted)
                .expect("parsed persisted sled config");
            assert_eq!(parsed, sled_config);
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn read_existing_config() {
        let logctx = dev::test_setup_log("read_existing_config");

        // Set up the ledger task with an initial config.
        let sled_config = make_nonempty_sled_config();
        let test_harness =
            TestHarness::with_initial_config(logctx.log.clone(), &sled_config)
                .await;

        // It should have read that config.
        assert_eq!(
            *test_harness.current_config_rx.borrow(),
            CurrentSledConfig::Ledgered(sled_config)
        );

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn reject_old_config_generation() {
        let logctx = dev::test_setup_log("reject_old_config_generation");

        // Set up the ledger task with an initial config (with a generation >
        // `Generation::new()`, so we can test sending a lower generation).
        let mut sled_config = make_nonempty_sled_config();
        assert!(sled_config.generation > Generation::new());

        let test_harness =
            TestHarness::with_initial_config(logctx.log.clone(), &sled_config)
                .await;

        // Try to send a lower generation; the task should reject this.
        sled_config.generation = Generation::new();
        let err = test_harness
            .task_handle
            .set_new_config(sled_config)
            .await
            .expect("no ledger task error")
            .expect_err("new config should be rejected");

        match err {
            LedgerNewConfigError::GenerationOutdated { .. } => (),
            _ => panic!("unexpected error {}", InlineErrorChain::new(&err)),
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn reject_config_changed_without_generation_bump() {
        const TEST_NAME: &str =
            "reject_configs_referencing_nonexistent_artifacts";
        let logctx = dev::test_setup_log(TEST_NAME);

        // Set up the ledger task with an initial config.
        let mut sled_config = make_nonempty_sled_config();
        let test_harness =
            TestHarness::with_initial_config(logctx.log.clone(), &sled_config)
                .await;

        // Mutate the sled_config but don't bump the generation.
        sled_config.disks.insert(make_dummy_disk_config(TEST_NAME));
        let err = test_harness
            .task_handle
            .set_new_config(sled_config)
            .await
            .expect("no ledger task error")
            .expect_err("new config should be rejected");

        match err {
            LedgerNewConfigError::ConfigurationChanged { .. } => (),
            _ => panic!("unexpected error {}", InlineErrorChain::new(&err)),
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn reject_configs_referencing_nonexistent_artifacts() {
        let logctx = dev::test_setup_log(
            "reject_configs_referencing_nonexistent_artifacts",
        );

        // Claim we have an artifact with the all-zeroes hash.
        let existing_artifact_hash = ArtifactHash([0; 32]);
        let nonexisting_artifact_hash = ArtifactHash([1; 32]);

        let test_harness = TestHarness::with_fake_artifacts(
            logctx.log.clone(),
            [existing_artifact_hash].into_iter(),
        )
        .await;

        // Create a config that references a zone with a different artifact
        // hash.
        let mut config = OmicronSledConfig {
            generation: Generation::new().next(),
            disks: IdMap::new(),
            datasets: IdMap::new(),
            zones: [make_dummy_zone_config_using_artifact_hash(
                nonexisting_artifact_hash,
            )]
            .into_iter()
            .collect(),
            remove_mupdate_override: None,
            host_phase_2: HostPhase2DesiredSlots::current_contents(),
        };

        // The ledger task should reject this config due to a missing artifact.
        let err = test_harness
            .task_handle
            .set_new_config(config.clone())
            .await
            .expect("can communicate with task")
            .expect_err("config should fail");
        match err {
            LedgerNewConfigError::ArtifactStoreValidationFailed(_) => (),
            _ => panic!("unexpected error {}", InlineErrorChain::new(&err)),
        }

        // Change the config to reference the artifact that does exist; this one
        // should be accepted.
        config.zones = [make_dummy_zone_config_using_artifact_hash(
            existing_artifact_hash,
        )]
        .into_iter()
        .collect();

        test_harness
            .task_handle
            .set_new_config(config.clone())
            .await
            .expect("can communicate with task")
            .expect("config should be ledgered");

        // Try a config that references a host phase 2 artifact that isn't in
        // the store; this should be rejected.
        config.generation = config.generation.next();
        config.zones = IdMap::new();
        config.host_phase_2 = HostPhase2DesiredSlots {
            slot_a: HostPhase2DesiredContents::CurrentContents,
            slot_b: HostPhase2DesiredContents::Artifact {
                hash: nonexisting_artifact_hash,
            },
        };
        let err = test_harness
            .task_handle
            .set_new_config(config.clone())
            .await
            .expect("can communicate with task")
            .expect_err("config should fail");
        match err {
            LedgerNewConfigError::ArtifactStoreValidationFailed(_) => (),
            _ => panic!("unexpected error {}", InlineErrorChain::new(&err)),
        }

        // Change the config to reference the artifact that does exist; this one
        // should be accepted.
        config.host_phase_2 = HostPhase2DesiredSlots {
            slot_a: HostPhase2DesiredContents::CurrentContents,
            slot_b: HostPhase2DesiredContents::Artifact {
                hash: existing_artifact_hash,
            },
        };

        test_harness
            .task_handle
            .set_new_config(config)
            .await
            .expect("can communicate with task")
            .expect("config should be ledgered");

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn reject_artifact_configs_removing_referenced_artifacts() {
        let logctx = dev::test_setup_log(
            "reject_artifact_configs_removing_referenced_artifacts",
        );

        // Construct some artifacts hashes we'll claim to be using and one we
        // won't.
        let used_zone_artifact_hash = ArtifactHash([0; 32]);
        let used_host_artifact_hash = ArtifactHash([1; 32]);
        let unused_artifact_hash = ArtifactHash([2; 32]);

        // Claim we have a host phase 2
        // Set up the ledger task with an initial config.
        let mut sled_config = make_nonempty_sled_config();
        sled_config.zones.insert(make_dummy_zone_config_using_artifact_hash(
            used_zone_artifact_hash,
        ));
        sled_config.host_phase_2.slot_a = HostPhase2DesiredContents::Artifact {
            hash: used_host_artifact_hash,
        };

        let test_harness =
            TestHarness::with_initial_config(logctx.log.clone(), &sled_config)
                .await;

        let mut artifact_config = ArtifactConfig {
            generation: Generation::new(),
            artifacts: BTreeSet::new(),
        };

        // Try some ArtifactConfigs that don't contain both `used_*` artifact
        // hashes; all of these should be rejected.
        for incomplete_artifacts in [
            &[used_zone_artifact_hash] as &[ArtifactHash],
            &[used_host_artifact_hash],
            &[unused_artifact_hash],
            &[used_zone_artifact_hash, unused_artifact_hash],
            &[used_host_artifact_hash, unused_artifact_hash],
        ] {
            artifact_config.artifacts =
                incomplete_artifacts.iter().cloned().collect();

            let err = test_harness
                .task_handle
                .validate_artifact_config(artifact_config.clone())
                .await
                .expect("no ledger task error")
                .expect_err("config should be rejected");

            match err {
                LedgerArtifactConfigError::InUseArtifactedMissing(
                    artifacts,
                ) => {
                    if !incomplete_artifacts.contains(&used_zone_artifact_hash)
                    {
                        assert!(
                            artifacts.contains(&used_zone_artifact_hash),
                            "unexpected error contents: {artifacts:?}"
                        );
                    }
                    if !incomplete_artifacts.contains(&used_host_artifact_hash)
                    {
                        assert!(
                            artifacts.contains(&used_host_artifact_hash),
                            "unexpected error contents: {artifacts:?}"
                        );
                    }
                }
                _ => panic!("unexpected error {}", InlineErrorChain::new(&err)),
            }
        }

        // Put both used artifact hashes into the artifact config; now it should
        // pass validation.
        artifact_config.artifacts.insert(used_zone_artifact_hash);
        artifact_config.artifacts.insert(used_host_artifact_hash);
        test_harness
            .task_handle
            .validate_artifact_config(artifact_config)
            .await
            .expect("no ledger task error")
            .expect("config is valid");

        logctx.cleanup_successful();
    }

    // Basic test that we convert the legacy triple of config ledgers if they're
    // present. The `legacy_configs` submodule has more extensive tests of this
    // functionality.
    #[tokio::test]
    async fn convert_legacy_ledgers_if_present() {
        let logctx = dev::test_setup_log("convert_legacy_ledgers_if_present");

        let test_harness =
            TestHarness::with_legacy_ledgers(logctx.log.clone()).await;

        // It should have combined the legacy ledgers.
        assert_eq!(
            *test_harness.current_config_rx.borrow(),
            CurrentSledConfig::Ledgered(test_data_merged_config())
        );

        logctx.cleanup_successful();
    }
}
