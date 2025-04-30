// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tokio task responsible for managing the [`OmicronSledConfig`] ledger.

use dropshot::HttpError;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use omicron_common::api::external::Generation;
use omicron_common::ledger;
use sled_agent_api::ArtifactConfig;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tufaceous_artifact::ArtifactHash;

use crate::InternalDisksReceiver;
use crate::SledAgentArtifactStore;

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
    #[error("cannot write sled config ledger: no M.2 disks available")]
    NoM2Disks,
    #[error("cannot accept sled config: waiting for key manager")]
    WaitingForKeyManager,
    #[error(
        "sled config generation out of date (got {requested}, have {current})"
    )]
    GenerationOutdated { current: Generation, requested: Generation },
    #[error("sled config changed with the same generation ({generation})")]
    ConfigurationChanged { generation: Generation },
    #[error("failed to commit sled config to ledger")]
    LedgerCommitFailed(#[source] ledger::Error),
}

impl From<LedgerNewConfigError> for HttpError {
    fn from(err: LedgerNewConfigError) -> Self {
        let message = InlineErrorChain::new(&err).to_string();
        match err {
            LedgerNewConfigError::NoM2Disks
            | LedgerNewConfigError::WaitingForKeyManager => {
                HttpError::for_unavail(None, message)
            }
            LedgerNewConfigError::GenerationOutdated { .. }
            | LedgerNewConfigError::ConfigurationChanged { .. } => {
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
    #[error(
        "Artifacts in use by ledgered sled config are not present \
         in new artifact config: {0:?}"
    )]
    InUseArtifactedMissing(BTreeMap<ArtifactHash, &'static str>),
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
}

impl LedgerTaskHandle {
    pub fn spawn_ledger_task<T: SledAgentArtifactStore>(
        internal_disks_rx: InternalDisksReceiver,
        artifact_store: T,
        log: Logger,
    ) -> (Self, watch::Receiver<CurrentSledConfig>) {
        // TODO pick and explain a channel size
        let (request_tx, request_rx) = mpsc::channel(8);

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

        (Self { request_tx }, current_config_rx)
    }

    pub async fn set_new_config(
        &self,
        _new_config: OmicronSledConfig,
    ) -> Result<Result<(), LedgerNewConfigError>, LedgerTaskError> {
        unimplemented!()
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
        _new_config: ArtifactConfig,
    ) -> Result<Result<(), LedgerArtifactConfigError>, LedgerTaskError> {
        unimplemented!()
    }
}

#[derive(Debug)]
enum LedgerTaskRequest {
    WriteNewConfig {
        new_config: OmicronSledConfig,
        tx: oneshot::Sender<Result<(), LedgerNewConfigError>>,
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
        unimplemented!()
    }
}
