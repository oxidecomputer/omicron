// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for loading the current fault management sitrep
//! from the DB

use crate::app::background::BackgroundTask;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::fm::Sitrep;
use nexus_types::fm::SitrepVersion;
use nexus_types::internal_api::background::SitrepLoadStatus as Status;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

pub struct SitrepLoader {
    datastore: Arc<DataStore>,
    tx: watch::Sender<CurrentSitrep>,
}

type CurrentSitrep = Option<Arc<(SitrepVersion, Sitrep)>>;

impl BackgroundTask for SitrepLoader {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = self.load_if_needed(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    let err = format!(
                        "could not serialize task status: {}",
                        InlineErrorChain::new(&err)
                    );
                    json!({ "error": err })
                }
            }
        })
    }
}

impl SitrepLoader {
    pub fn new(
        datastore: Arc<DataStore>,
        tx: watch::Sender<CurrentSitrep>,
    ) -> Self {
        Self { datastore, tx }
    }

    #[allow(dead_code)] // subsequent PRs will consume this
    pub fn watcher(&self) -> watch::Receiver<CurrentSitrep> {
        self.tx.subscribe()
    }

    async fn load_if_needed(&self, opctx: &OpContext) -> Status {
        // Set up a logger for this activation that includes metadata about
        // the current sitrep.
        let (old, log) = match &*self.tx.borrow() {
            None => (None, opctx.log.clone()),
            Some(old) => {
                let (ref old_version, _) = **old;
                let log = opctx.log.new(slog::o!(
                    // since this is a TypedUuid, use `Debug` to avoid
                    // including ()
                    "original_id" => format!("{:?}", old_version.id),
                    "original_made_current" => old_version.time_made_current.to_string(),
                    "original_version" => old_version.version,
                ));
                (Some(old_version.clone()), log)
            }
        };

        // Get the ID of the current sitrep.
        let time_loaded = Utc::now();
        let current_version: SitrepVersion = match self
            .datastore
            .fm_current_sitrep_version(opctx)
            .await
        {
            Ok(Some(version)) => version,
            Ok(None) => match old {
                Some(SitrepVersion { version, id, .. }) => {
                    // We should never go from "some sitrep" to "no sitrep";
                    // pruning should always keep a small number of old sitreps
                    // around until we have new ones to replace them.
                    //
                    // In this case we won't replace our channel contents with
                    // `None`; we'll keep around whatever old collection we had
                    // loaded.
                    warn!(log, "previously had a sitrep, but now none exist");
                    return Status::Error(format!(
                        "previously loaded sitrep {id:?} (v{version}), \
                         but now no sitreps exist",
                    ));
                }
                None => {
                    // Had no sitrep; still have no sitrep.
                    return Status::NoSitrep;
                }
            },
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    log,
                    "failed to read current sitrep version";
                    &err
                );
                return Status::Error(format!(
                    "failed to read current sitrep version: {err}"
                ));
            }
        };

        // Have we already loaded this sitrep?
        match old {
            Some(version) if version.id == current_version.id => {
                debug!(log, "current sitrep has not changed");
                return Status::Loaded { version, time_loaded };
            }
            Some(SitrepVersion { version, id, .. })
                if version == current_version.version
                    && id != current_version.id =>
            {
                // Well, this is weird! Entries in the `sitrep_version` table
                // should not change IDs once they are created, that seems like
                // a bug. Nonetheless, we will load the new UUID, but we should
                // say something about this, as it's a bit odd.
                warn!(
                    log,
                    "sitrep ID associated with the current version in the \
                     database has changed; this is not supposed to happen!";
                     "current_id" => ?current_version.id,
                );
            }
            _ => (),
        }

        let sitrep = match self
            .datastore
            .fm_sitrep_read(opctx, current_version.id)
            .await
        {
            Ok(sitrep) => sitrep,
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                error!(
                    log,
                    "failed to load current sitrep";
                    "current_id" => ?current_version.id,
                    "current_version" => ?current_version.version,
                    &err
                );
                return Status::Error(format!(
                    "failed to read current sitrep {:?} (v{}): {err}",
                    current_version.id, current_version.version
                ));
            }
        };

        let sitrep = Arc::new((current_version.clone(), sitrep));
        self.tx.send_modify(|s| {
            *s = Some(sitrep);
        });

        Status::Loaded { version: current_version, time_loaded }
    }
}

#[cfg(test)]
mod test {
    // TODO
}
