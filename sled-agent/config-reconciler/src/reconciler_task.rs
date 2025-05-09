// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The primary task for sled config reconciliation.

use chrono::DateTime;
use chrono::Utc;
use illumos_utils::dladm::EtherstubVnic;
use illumos_utils::zpool::PathInPool;
use key_manager::StorageKeyRequester;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use slog::Logger;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;

use crate::TimeSyncConfig;
use crate::ledger::CurrentSledConfig;
use crate::sled_agent_facilities::SledAgentFacilities;

mod external_disks;
mod zones;

pub use self::external_disks::CurrentlyManagedZpools;
pub use self::external_disks::CurrentlyManagedZpoolsReceiver;
pub use self::zones::TimeSyncError;
pub use self::zones::TimeSyncStatus;

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
    // TODO where do we want to do dump setup? Needs both internal and external
    // disks. Maybe this task, or maybe a task just for dump setup?
    // Invokes dumpadm(8) and savecore(8) when new disks are encountered
    // dump_setup: DumpSetup,
}

impl<T: SledAgentFacilities> ReconcilerTask<T> {
    async fn run(self) {
        unimplemented!()
    }
}
