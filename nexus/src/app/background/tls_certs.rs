// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for keeping track of Nexus's external TLS certificates

use super::common::BackgroundTask;
use anyhow::anyhow;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::ServiceKind;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::DataPageParams;
use openssl::pkey::PKey;
use rustls::sign::CertifiedKey;
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::watch;

// XXX-dap
const MAX_TLS_CERTS: usize = 10;

// XXX-dap TODO-doc TODO-coverage
// XXX-dap should probably go into its own module
// XXX-dap add support for wildcard certs (consider `prefix_tree_map` or `tst`
// crates?)
#[derive(Clone)]
pub struct TlsCerts {
    keys_by_name: BTreeMap<String, Arc<CertifiedKey>>,
}

impl TlsCerts {
    pub fn cert_for_sni(&self, sni: &str) -> Option<Arc<CertifiedKey>> {
        self.keys_by_name.get(sni).cloned()
    }

    fn serialize(&self) -> TlsCertsDebug {
        TlsCertsDebug {
            keys_by_name: self
                .keys_by_name
                .iter()
                .map(|(k, v)| (k.clone(), CertifiedKeyDebug::new(v)))
                .collect(),
        }
    }
}

// XXX-dap TODO-doc newtype for CertifiedKey that lets us impl Debug,
// Serialize, PartialEq that just prints the fingerprints.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct TlsCertsDebug {
    keys_by_name: BTreeMap<String, CertifiedKeyDebug>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct CertifiedKeyDebug(String);

impl CertifiedKeyDebug {
    fn new(key: &CertifiedKey) -> CertifiedKeyDebug {
        // XXX-dap unwraps
        let cert_der_encoded = &key.end_entity_cert().unwrap().0;
        let x509 = openssl::x509::X509::from_der(cert_der_encoded).unwrap();
        let digest =
            x509.digest(openssl::hash::MessageDigest::sha256()).unwrap();
        let digest_hex = hex::encode(&digest);
        CertifiedKeyDebug(digest_hex)
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
                    false,
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
                    .filter_map(|db_cert|  {
                        let id = db_cert.id();
                        match parse_db_cert(db_cert) {
                        Ok(c) => Some(c),
                        Err(error) => {
                            warn!(
                                &log,
                                "failed to parse certificate found in database";
                                "id" => id.to_string(),
                                "error" => ?error,
                            );
                            None
                        }
                        }
                    })
                    .flatten()
                    .collect(),
            };
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
                        "found TLS certificates (initial)";
                        "certs" => ?new_config.serialize(),
                    );
                    self.last = Some(new_config.clone());
                    self.tx.send_replace(Some(new_config));
                }

                Some(old) => {
                    // The datastore should be sorting the results by id in
                    // order to paginate through them.  Thus, it should be valid
                    // to compare what we got directly to what we had before
                    // without worrying about the order being different.
                    if old.serialize() == new_config.serialize() {
                        debug!(
                            &log,
                            "found TLS certificates (no change)";
                            "certs" => ?new_config.serialize(),
                        );
                    } else {
                        info!(
                            &log,
                            "found TLS certificates (changed)";
                            "certs" => ?new_config.serialize(),
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
) -> Result<Vec<(String, Arc<CertifiedKey>)>, anyhow::Error> {
    // XXX-dap all this code needs to be commonized / rationalized with the db
    // model code and the code in Dropshot that already exists to use this stuff
    // XXX-dap this is probably very inefficient

    // Parse and validate what we've got.
    let certs_pem = openssl::x509::X509::stack_from_pem(&c.cert)
        .context("parsing PEM stack")?;
    let end_cert = certs_pem
        .iter()
        .next()
        .ok_or_else(|| anyhow!("no certificates in PEM stack"))?;
    let private_key =
        PKey::private_key_from_pem(&c.key).context("parsing private key PEM")?;
    anyhow::ensure!(
        !end_cert
            .public_key()
            .context("certificate publickey")?
            .public_eq(&private_key),
        "certificate public key does not match stored private key"
    );

    // Compute a digest (fingerprint) that we can use for debugging.
    let digest = {
        let digest_bytes = end_cert
            .digest(openssl::hash::MessageDigest::sha256())
            .context("computing fingerprint")?;
        hex::encode(&digest_bytes)
    };

    // Assemble a rustls CertifiedKey with both the certificate and the key.
    let certified_key = {
        let rustls_private_key = rustls::PrivateKey(c.key);
        let rustls_signing_key =
            rustls::sign::any_supported_type(&rustls_private_key)
                .context("parsing DER private key")?;
        let rustls_certs = certs_pem
            .into_iter()
            .map(|x509| {
                x509.to_der()
                    .context("serializing cert to DER")
                    .map(rustls::Certificate)
            })
            .collect::<Result<_, _>>()?;
        Arc::new(CertifiedKey::new(rustls_certs, rustls_signing_key))
    };

    // XXX-dap working here: my thought at this point was to extract the names
    // from the parsed certificate.  That won't work with wildcards, of course.
    // But really, this isn't what we want, I think: eventually, certificates
    // will be scoped to *Silos*, and with each Silo we'll know its canonical
    // DNS name and any aliases that operators have configured for it.  We
    // should use those to associate these certificates.
    // This suggests rethinking the TlsCerts struct and the Debug version.  We
    // probably want one map with all the certificates, by fingerprint.  Then we
    // want a separate thing that maps known Silo DNS names to their
    // corresponding certificate.  (Or is that a separate background task?)
    //
    // In fact, can we do most of the work of this PR *without* doing per-Silo
    // certificates by starting out by mapping *all* Silo DNS names to all
    // certificates?

    // XXX-dap see the checks that rustls::ResolvesSererCertUsingSni does

    todo!(); // XXX-dap
}

// XXX-dap TODO-coverage testing (see dns_servers.rs)
