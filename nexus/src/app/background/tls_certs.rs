// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for keeping track of Nexus's external TLS certificates

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::ServiceKind;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::DataPageParams;
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::watch;

// XXX-dap
const MAX_TLS_CERTS: usize = 10;

// XXX-dap TODO-doc TODO-coverage
// XXX-dap should probably go into its own module
// XXX-dap add support for wildcard certs (consider `prefix_tree_map` or `tst`
// crates?)
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct TlsCerts {
    keys_by_name: BTreeMap<String, CertChain>,
}

impl TlsCerts {
    pub fn cert_for_sni(
        &self,
        sni: &str,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        self.keys_by_name.get(sni).map(|c| c.0.clone())
    }
}

// XXX-dap TODO-doc newtype for CertifiedKey that lets us impl Debug,
// Serialize, PartialEq that just prints the fingerprints.
#[derive(Clone)]
struct CertChain(Arc<rustls::sign::CertifiedKey>);

impl fmt::Debug for CertChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!() // XXX-dap
    }
}

impl PartialEq for CertChain {
    fn eq(&self, other: &Self) -> bool {
        todo!() // XXX-dap
    }
}
impl Eq for CertChain {}

impl Serialize for CertChain {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        todo!() // XXX-dap
    }
}

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

            // XXX-dap proper pagination
            let pagparams = PaginatedBy::Id(DataPageParams {
                marker: None,
                limit: NonZeroU32::try_from(
                    u32::try_from(MAX_TLS_CERTS).unwrap(),
                )
                .unwrap(),
                direction: dropshot::PaginationOrder::Ascending,
            });

            let result = self
                .datastore
                .certificate_list_for(
                    opctx,
                    // XXX-dap should be "external"?
                    Some(ServiceKind::Nexus),
                    &pagparams,
                )
                .await;

            if let Err(error) = result {
                warn!(
                    &log,
                    "failed to read list of external TLS certificates";
                    "error" => format!("{:#}", error)
                );
                return json!({
                    "error":
                        format!(
                            "failed to read list of external TLS certificates: \
                            {:#}",
                            error
                        )
                });
            }

            let certs = result.unwrap();
            if certs.len() >= MAX_TLS_CERTS {
                warn!(
                    &log,
                    "found {} certificates, which is more than MAX_TLS_CERTS \
                    ({}).  There may be more that will not be used.",
                    certs.len(),
                    MAX_TLS_CERTS
                );
            }

            let new_config = TlsCerts {
                keys_by_name: certs
                    .into_iter()
                    .filter_map(|db_cert| match parse_db_cert(db_cert) {
                        Ok(c) => Some(c),
                        Err(error) => {
                            warn!(
                                &log,
                                "failed to parse certificate";
                                "error" => ?error,
                            );
                            None
                        }
                    })
                    .flatten()
                    .collect(),
            };
            let rv =
                serde_json::to_value(&new_config).unwrap_or_else(|error| {
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
                        "found TLS certificates (initial)";
                        "certs" => ?new_config,
                    );
                    self.last = Some(new_config.clone());
                    self.tx.send_replace(Some(new_config));
                }

                Some(old) => {
                    // The datastore should be sorting the results by id in
                    // order to paginate through them.  Thus, it should be valid
                    // to compare what we got directly to what we had before
                    // without worrying about the order being different.
                    if *old == new_config {
                        debug!(
                            &log,
                            "found TLS certificates (no change)";
                            "certs" => ?new_config,
                        );
                    } else {
                        info!(
                            &log,
                            "found DNS servers (changed)";
                            "certs" => ?new_config,
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

fn parse_db_cert(
    c: nexus_db_model::Certificate,
) -> Result<Vec<(String, CertChain)>, anyhow::Error> {
    todo!(); // XXX-dap
}

// XXX-dap TODO-coverage testing (see dns_servers.rs)
