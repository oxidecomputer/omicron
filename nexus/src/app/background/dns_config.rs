// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for keeping track of DNS configuration

use super::common::BackgroundTask;
use dns_service_client::types::DnsConfigParams;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use std::sync::Arc;
use tokio::sync::watch;

/// Background task that keeps track of the latest configuration for a DNS group
pub struct DnsConfigWatcher {
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
    last: Option<DnsConfigParams>,
    tx: watch::Sender<Option<DnsConfigParams>>,
    rx: watch::Receiver<Option<DnsConfigParams>>,
}

impl DnsConfigWatcher {
    pub fn new(
        datastore: Arc<DataStore>,
        dns_group: DnsGroup,
    ) -> DnsConfigWatcher {
        let (tx, rx) = watch::channel(None);
        DnsConfigWatcher { datastore, dns_group, last: None, tx, rx }
    }

    /// Exposes the latest DNS configuration for this DNS group
    ///
    /// You can use the returned [`watch::Receiver`] to look at the latest
    /// configuration or to be notified when it changes.
    pub fn watcher(&self) -> watch::Receiver<Option<DnsConfigParams>> {
        self.rx.clone()
    }
}

impl BackgroundTask for DnsConfigWatcher {
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, ()>
    where
        'a: 'c,
        'b: 'c,
    {
        async {
            // Set up a logger for this activation that includes metadata about
            // the current generation.
            let log = match &self.last {
                None => opctx.log.clone(),
                Some(old) => opctx.log.new(o!(
                    "current_generation" => old.generation,
                    "current_time_created" => old.time_created.to_string(),
                )),
            };

            // Read the latest configuration for this DNS group.
            let result =
                self.datastore.dns_config_read(opctx, self.dns_group).await;

            // Decide what to do with the result.
            match (&self.last, result) {
                (_, Err(error)) => {
                    // We failed to read the DNS configuration.  There's nothing
                    // to do but log an error.  We'll retry when we're activated
                    // again.
                    warn!(
                        &log,
                        "failed to read DNS config";
                        "error" => format!("{:#}", error)
                    );
                }

                (None, Ok(new_config)) => {
                    // We've found a DNS configuration for the first time.
                    // Save it and notify any watchers.
                    info!(
                        &log,
                        "found latest generation (first find)";
                        "generation" => new_config.generation
                    );
                    self.last = Some(new_config.clone());
                    self.tx.send_replace(Some(new_config));
                }

                (Some(old), Ok(new)) => {
                    if old.generation > new.generation {
                        // We previously had a generation that's newer than what
                        // we just read.  This should never happen because we
                        // never remove the latest generation.
                        error!(
                            &log,
                            "found latest DNS generation ({}) is older \
                            than the one we already know about ({})",
                            new.generation,
                            old.generation
                        );
                    } else if old.generation == new.generation {
                        if *old != new {
                            // We found the same generation _number_ as what we
                            // already had, but the contents were different.
                            // This should never happen because generations are
                            // immutable once created.
                            error!(
                                &log,
                                "found DNS config at generation {} that does \
                                not match the config that we already have for \
                                the same generation",
                                new.generation
                            );
                        } else {
                            // We found a DNS configuration and it exactly
                            // matches what we already had.  This is the common
                            // case when we're activated by a timeout.
                            debug!(
                                &log,
                                "found latest DNS generation (unchanged)";
                                "generation" => new.generation,
                            );
                        }
                    } else {
                        // We found a DNS configuration that's newer than what
                        // we currently have.  Save it and notify any watchers.
                        info!(
                            &log,
                            "found latest DNS generation (newer than we had)";
                            "generation" => new.generation,
                            "time_created" => new.time_created.to_string(),
                            "old_generation" => old.generation,
                            "old_time_created" => old.time_created.to_string(),
                        );
                        self.last = Some(new.clone());
                        self.tx.send_replace(Some(new));
                    }
                }
            };
        }
        .boxed()
    }
}
