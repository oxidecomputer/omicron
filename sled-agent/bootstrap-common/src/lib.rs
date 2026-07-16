// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types and functionality shared among bootstrap agent related crates
//!
//! This is unversioned, internal data. It's only expected to be used by RSS
//! and the Multirack Join Service. ! Please do not use it in public facing,
//! non-lockstep, interfaces.

use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use omicron_ledger::{Ledger, Ledgerable};
use serde::{Deserialize, Serialize};
use sled_agent_config_reconciler::InternalDisksReceiver;
use sled_agent_measurements::MeasurementsHandle;
use slog::{Logger, error, info};
use slog_error_chain::SlogInlineError;
use sprockets_tls::keys::SprocketsConfig;
use std::net::Ipv6Addr;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Serialize, Deserialize, Default)]
struct RssStartedMarker {}

impl Ledgerable for RssStartedMarker {
    fn is_newer_than(&self, _other: &Self) -> bool {
        true
    }
    fn generation_bump(&mut self) {}
}

const RSS_STARTED_FILENAME: &str = "rss-started.marker";

#[derive(Clone, Serialize, Deserialize, Default)]
struct RssCompleteMarker {}

impl Ledgerable for RssCompleteMarker {
    fn is_newer_than(&self, _other: &Self) -> bool {
        true
    }
    fn generation_bump(&mut self) {}
}
const RSS_COMPLETED_FILENAME: &str = "rss-plan-completed.marker";

/// Functionality necessary to run rack initialization or multirack cluster join
///
/// This is usually input to some constructor function
#[derive(Clone)]
pub struct RssContext {
    pub base_log: Logger,
    pub global_zone_bootstrap_ip: Ipv6Addr,
    pub internal_disks_rx: InternalDisksReceiver,
    pub bootstore_node_handle: bootstore::NodeHandle,
    pub sprockets_config: SprocketsConfig,
    pub trust_quorum_handle: trust_quorum::NodeTaskHandle,
    pub measurements: Arc<MeasurementsHandle>,
}

impl RssContext {
    fn started_marker_paths(&self) -> Vec<Utf8PathBuf> {
        let config_dataset_paths = self
            .internal_disks_rx
            .current()
            .all_config_datasets()
            .collect::<Vec<_>>();

        config_dataset_paths
            .iter()
            .map(|p| p.join(RSS_STARTED_FILENAME))
            .collect()
    }

    fn completed_marker_paths(&self) -> Vec<Utf8PathBuf> {
        let config_dataset_paths = self
            .internal_disks_rx
            .current()
            .all_config_datasets()
            .collect::<Vec<_>>();

        config_dataset_paths
            .iter()
            .map(|p| p.join(RSS_COMPLETED_FILENAME))
            .collect()
    }

    /// Check if a previous RSS or multirack join has completed successfully.
    ///
    /// If we see the completion marker in the `completed_ledger` then the
    /// system should be up-and-running. If we see the started marker in
    /// the `started_ledger`, then RSS or multirack join did not complete and
    /// the rack should be clean-slated before either is run again.
    pub async fn is_rss_complete(
        &self,
        log: &Logger,
    ) -> Result<(), RunRssError> {
        let started_ledger =
            Ledger::<RssStartedMarker>::new(log, self.started_marker_paths())
                .await;
        let completed_ledger = Ledger::<RssCompleteMarker>::new(
            log,
            self.completed_marker_paths(),
        )
        .await;

        // Check if a previous RSS plan has completed successfully.
        //
        // If we see the completion marker in the `completed_ledger` then the
        // system should be up-and-running. If we see the started marker in
        // the `started_ledger`, then RSS did not complete and the rack should
        // be clean-slated before RSS is run again.
        if completed_ledger.is_some() {
            info!(log, "RSS configuration has already been applied",);
            return Err(RunRssError::RackAlreadyInitialized);
        } else if started_ledger.is_some() {
            error!(log, "RSS failed to complete rack initialization");
            return Err(RunRssError::RackInitInterrupted);
        }

        Ok(())
    }

    pub async fn write_rss_started_ledger(
        &self,
        log: &Logger,
    ) -> Result<(), omicron_ledger::Error> {
        // Record that we have started RSS
        let mut ledger = Ledger::<RssStartedMarker>::new_with(
            log,
            self.started_marker_paths(),
            RssStartedMarker::default(),
        );
        ledger.commit().await
    }

    pub async fn write_rss_completed_ledger(
        &self,
        log: &Logger,
    ) -> Result<(), omicron_ledger::Error> {
        let mut ledger = Ledger::<RssCompleteMarker>::new_with(
            &log,
            self.completed_marker_paths(),
            RssCompleteMarker::default(),
        );
        ledger.commit().await
    }
}

#[derive(Error, Debug, SlogInlineError)]
pub enum RunRssError {
    #[error("Rack already initialized")]
    RackAlreadyInitialized,

    #[error("Rack initialization was interrupted. Clean-slate required")]
    RackInitInterrupted,
}
