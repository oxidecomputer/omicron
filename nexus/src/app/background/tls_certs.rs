// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for keeping track of Nexus's external TLS certificates
// XXX-dap rename this whole thing?

use super::common::BackgroundTask;
use crate::app::certificate::silos_load_dns_tls;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

// XXX-dap rename / refactor callers?
pub type TlsCerts = crate::app::certificate::SiloDnsCerts;

/// Background task that keeps track of the latest list of TLS certificates for
/// Nexus's external endpoint
pub struct TlsCertsWatcher {
    datastore: Arc<DataStore>,
    last: Option<TlsCerts>,
    tx: watch::Sender<Option<TlsCerts>>,
    rx: watch::Receiver<Option<TlsCerts>>,
}

impl TlsCertsWatcher {
    pub fn new(datastore: Arc<DataStore>) -> TlsCertsWatcher {
        let (tx, rx) = watch::channel(None);
        TlsCertsWatcher { datastore, last: None, tx, rx }
    }

    /// Exposes the latest set of TLS certificates
    ///
    /// You can use the returned [`watch::Receiver`] to look at the latest
    /// configuration or to be notified when it changes.
    pub fn watcher(&self) -> watch::Receiver<Option<TlsCerts>> {
        self.rx.clone()
    }
}

impl BackgroundTask for TlsCertsWatcher {
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, serde_json::Value>
    where
        'a: 'c,
        'b: 'c,
    {
        async {
            let log = &opctx.log;

            let result = silos_load_dns_tls(&self.datastore, opctx).await;

            if let Err(error) = result {
                warn!(
                    &log,
                    "failed to read Silo/DNS/TLS configuration";
                    "error" => format!("{:#}", error)
                );
                return json!({
                    "error":
                        format!(
                            "failed to read Silo/DNS/TLS configuration: \
                            {:#}",
                            error
                        )
                });
            }

            let new_config = result.unwrap();
            let rv = serde_json::to_value(&new_config.serialize())
                .unwrap_or_else(|error| {
                    json!({
                        "error":
                            format!(
                                "failed to serialize final value: {:#}",
                                error
                            )
                    })
                });

            match &self.last {
                None => {
                    info!(
                        &log,
                        "found Silo/DNS/TLS config (initial)";
                        "config" => ?new_config.serialize(),
                    );
                    self.last = Some(new_config.clone());
                    self.tx.send_replace(Some(new_config));
                }

                Some(old) => {
                    if old.serialize() == new_config.serialize() {
                        debug!(
                            &log,
                            "found Silo/DNS/TLS config (no change)";
                            "config" => ?new_config.serialize(),
                        );
                    } else {
                        info!(
                            &log,
                            "found Silo/DNS/TLS config (changed)";
                            "config" => ?new_config.serialize(),
                        );
                        self.last = Some(new_config.clone());
                        self.tx.send_replace(Some(new_config));
                    }
                }
            };

            rv
        }
        .boxed()
    }
}

// XXX-dap TODO-coverage testing (see dns_servers.rs)
