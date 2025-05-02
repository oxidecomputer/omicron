// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The primary task for sled config reconciliation.

use chrono::DateTime;
use chrono::Utc;
use illumos_utils::dladm::EtherstubVnic;
use illumos_utils::running_zone::RunCommandError;
use illumos_utils::zpool::PathInPool;
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use sled_agent_types::time_sync::TimeSync;
use slog::Logger;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;

use crate::TimeSyncConfig;
use crate::ledger::CurrentSledConfig;
use crate::sled_agent_facilities::SledAgentFacilities;

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn<T: SledAgentFacilities>(
    key_requester: StorageKeyRequester,
    time_sync_config: TimeSyncConfig,
    underlay_vnic: EtherstubVnic,
    current_config_rx: watch::Receiver<CurrentSledConfig>,
    reconciler_result_tx: watch::Sender<ReconcilerResult>,
    currently_managed_zpools_tx: watch::Sender<Arc<CurrentlyManagedZpools>>,
    sled_agent_facilities: T,
    log: Logger,
) {
    tokio::spawn(
        ReconcilerTask {
            key_requester,
            time_sync_config,
            underlay_vnic,
            current_config_rx,
            reconciler_result_tx,
            currently_managed_zpools_tx,
            sled_agent_facilities,
            log,
        }
        .run(),
    );
}

#[derive(Debug, Clone)]
pub(crate) struct ReconcilerResult {
    status: ReconcilerTaskStatus,
    latest_result: Option<Arc<LatestReconcilerTaskResultInner>>,
}

impl Default for ReconcilerResult {
    fn default() -> Self {
        Self {
            status: ReconcilerTaskStatus::NotYetRunning,
            latest_result: None,
        }
    }
}

impl ReconcilerResult {
    pub fn timesync_status(&self) -> TimeSyncStatus {
        self.latest_result
            .as_deref()
            .map(|inner| inner.timesync_status.clone())
            .unwrap_or(TimeSyncStatus::NotYetChecked)
    }

    pub fn all_mounted_debug_datasets(
        &self,
    ) -> impl Iterator<Item = PathInPool> + '_ {
        // unimplemented!() doesn't work with `-> impl Iterator`
        if 1 > 0 {
            panic!("unimplemented!");
        }
        std::iter::empty()
    }

    pub fn all_mounted_zone_root_datasets(
        &self,
    ) -> impl Iterator<Item = PathInPool> + '_ {
        // unimplemented!() doesn't work with `-> impl Iterator`
        if 1 > 0 {
            panic!("unimplemented!");
        }
        std::iter::empty()
    }
}

#[derive(Debug, Clone)]
pub enum ReconcilerTaskStatus {
    NotYetRunning,
    WaitingForInternalDisks,
    WaitingForRackSetup,
    PerformingReconciliation {
        config: OmicronSledConfig,
        started_at_time: DateTime<Utc>,
        started_at_instant: Instant,
    },
    Idle {
        completed_at_time: DateTime<Utc>,
        ran_for: Duration,
    },
}

#[derive(Debug, Clone)]
pub enum TimeSyncStatus {
    NotYetChecked,
    ConfiguredToSkip,
    FailedToGetSyncStatus(Arc<TimeSyncError>),
    TimeSync(TimeSync),
}

#[derive(Debug, thiserror::Error)]
pub enum TimeSyncError {
    #[error("no running NTP zone")]
    NoRunningNtpZone,
    #[error("multiple running NTP zones - this should never happen!")]
    MultipleRunningNtpZones,
    #[error("failed to execute chronyc within NTP zone")]
    ExecuteChronyc(#[source] RunCommandError),
    #[error(
        "failed to parse chronyc tracking output: {reason} (stdout: {stdout:?})"
    )]
    FailedToParse { reason: &'static str, stdout: String },
}

#[derive(Debug, Clone)]
pub struct CurrentlyManagedZpoolsReceiver {
    inner: CurrentlyManagedZpoolsReceiverInner,
}

#[derive(Debug, Clone)]
enum CurrentlyManagedZpoolsReceiverInner {
    Real(watch::Receiver<Arc<CurrentlyManagedZpools>>),
    #[cfg(any(test, feature = "testing"))]
    FakeStatic(BTreeSet<ZpoolName>),
}

impl CurrentlyManagedZpoolsReceiver {
    #[cfg(any(test, feature = "testing"))]
    pub fn fake_static(zpools: impl Iterator<Item = ZpoolName>) -> Self {
        Self {
            inner: CurrentlyManagedZpoolsReceiverInner::FakeStatic(
                zpools.collect(),
            ),
        }
    }

    pub(crate) fn new(
        rx: watch::Receiver<Arc<CurrentlyManagedZpools>>,
    ) -> Self {
        Self { inner: CurrentlyManagedZpoolsReceiverInner::Real(rx) }
    }

    pub fn current(&self) -> Arc<CurrentlyManagedZpools> {
        match &self.inner {
            CurrentlyManagedZpoolsReceiverInner::Real(rx) => {
                Arc::clone(&*rx.borrow())
            }
            #[cfg(any(test, feature = "testing"))]
            CurrentlyManagedZpoolsReceiverInner::FakeStatic(zpools) => {
                Arc::new(CurrentlyManagedZpools(zpools.clone()))
            }
        }
    }

    pub fn current_and_update(&mut self) -> Arc<CurrentlyManagedZpools> {
        match &mut self.inner {
            CurrentlyManagedZpoolsReceiverInner::Real(rx) => {
                Arc::clone(&*rx.borrow_and_update())
            }
            #[cfg(any(test, feature = "testing"))]
            CurrentlyManagedZpoolsReceiverInner::FakeStatic(zpools) => {
                Arc::new(CurrentlyManagedZpools(zpools.clone()))
            }
        }
    }

    /// Wait for changes in the underlying watch channel.
    ///
    /// Cancel-safe.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        match &mut self.inner {
            CurrentlyManagedZpoolsReceiverInner::Real(rx) => rx.changed().await,
            #[cfg(any(test, feature = "testing"))]
            CurrentlyManagedZpoolsReceiverInner::FakeStatic(_) => {
                // Static set of zpools never changes
                std::future::pending().await
            }
        }
    }
}

/// Set of currently managed zpools.
///
/// This handle should only be used to decide to _stop_ using a zpool (e.g., if
/// a previously-launched zone is on a zpool that is no longer managed). It does
/// not expose a means to list or choose from the currently-managed pools;
/// instead, consumers should choose mounted datasets.
///
/// This level of abstraction even for "when to stop using a zpool" is probably
/// wrong: if we choose a dataset on which to place a zone's root, we should
/// shut that zone down if the _dataset_ goes away, not the zpool. For now we
/// live with "assume the dataset bases we choose stick around as long as their
/// parent zpool does".
#[derive(Default, Debug, Clone)]
pub struct CurrentlyManagedZpools(BTreeSet<ZpoolName>);

impl CurrentlyManagedZpools {
    /// Returns true if `zpool` is currently managed.
    pub fn contains(&self, zpool: &ZpoolName) -> bool {
        self.0.contains(zpool)
    }
}

#[derive(Debug)]
struct LatestReconcilerTaskResultInner {
    sled_config: OmicronSledConfig,
    timesync_status: TimeSyncStatus,
}

struct ReconcilerTask<T> {
    key_requester: StorageKeyRequester,
    time_sync_config: TimeSyncConfig,
    underlay_vnic: EtherstubVnic,
    current_config_rx: watch::Receiver<CurrentSledConfig>,
    reconciler_result_tx: watch::Sender<ReconcilerResult>,
    currently_managed_zpools_tx: watch::Sender<Arc<CurrentlyManagedZpools>>,
    sled_agent_facilities: T,
    log: Logger,
}

impl<T: SledAgentFacilities> ReconcilerTask<T> {
    async fn run(self) {
        unimplemented!()
    }
}
