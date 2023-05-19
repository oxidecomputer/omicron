// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of external HTTP endpoints

use super::silo::silo_dns_name;
use crate::db::model::ServiceKind;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use nexus_db_model::Certificate;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::Name as ExternalName;
use omicron_common::bail_unless;
use openssl::pkey::PKey;
use openssl::x509::X509;
use rustls::sign::CertifiedKey;
use serde::Serialize;
use serde_with::SerializeDisplay;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

// XXX-dap TODO-doc TODO-coverage
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExternalEndpoints {
    by_dns_name: BTreeMap<String, Arc<ExternalEndpoint>>,
    warnings: Vec<ExternalEndpointError>,
}

#[cfg(test)]
impl ExternalEndpoints {
    pub fn has_domain(&self, dns_name: &str) -> bool {
        self.by_dns_name.contains_key(dns_name)
    }

    pub fn ndomains(&self) -> usize {
        self.by_dns_name.len()
    }

    pub fn nwarnings(&self) -> usize {
        self.warnings.len()
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct ExternalEndpoint {
    silo_id: Uuid,
    tls_certs: Vec<TlsCertificate>,
}

impl ExternalEndpoint {
    /// Chooses a TLS certificate (chain) to use when handling connections to
    /// this endpoint (Silo)
    fn best_certificate(&self) -> Result<&TlsCertificate, anyhow::Error> {
        // We expect the most common case to be that there's only one
        // certificate chain here.  The next most common case is that there are
        // two because the administrator is in the process of rotating
        // certificates, usually due to upcoming expiration.  In principle, it
        // would be useful to allow operators to control which certificate chain
        // gets used, and maybe even do something like a canary to mitigate the
        // risk of a botched certificate update.  Absent that, we're going to do
        // our best to pick the best chain automatically.
        //
        // This could be a lot more sophisticated than it is.  We could try to
        // avoid using certificates that are clearly not valid based on the
        // "not_after" and "not_before" bounds.  We could check each certificate
        // in the chain, not just the last one.  We could use a margin of error
        // when doing this to account for small variations in the wall clock
        // between us and the client.  We could try to avoid using a certificate
        // that doesn't appear to be compatible with the SNI value (DNS domain)
        // that this request came in on.
        //
        // IMPORTANT: If we ever decide to do those things, they should only be
        // used to decide which of several certificates is preferred.  We should
        // always pick a certificate if we possibly can, even if it seems to be
        // invalid.  A client can always choose not to trust it.  But in the
        // unfortunate case where there are no good certificates, a customer's
        // only option may be to instruct their client to trust an invalid
        // certificate _so that they can log in and fix the certificate
        // problem_.  If we provide no certificate at all here, a customer may
        // have no way to fix the problem.
        //
        // Anyway, we don't yet do anything of these things.  For now, pick the
        // certificate chain whose leaf certificate has the latest expiration
        // time.

        // This would be cleaner if Asn1Time impl'd Ord or even just a way to
        // convert it to a Unix timestamp or any other comparable timestamp.
        let mut latest_expiration: Option<&TlsCertificate> = None;
        for t in &self.tls_certs {
            // We'll choose this certificate (so far) if we find that it's
            // anything other than "earlier" than the best we've seen so far.
            // That includes the case where we haven't seen any so far, where
            // this one is greater than or equal to the best so far, as well as
            // the case where they're incomparable for whatever reason.  (This
            // ensures that we always pick at least one.)
            if latest_expiration.is_none()
                || !matches!(
                    t.parsed.not_after().partial_cmp(
                        latest_expiration.unwrap().parsed.not_after()
                    ),
                    Some(std::cmp::Ordering::Less)
                )
            {
                latest_expiration = Some(t);
            }
        }

        latest_expiration
            .ok_or_else(|| anyhow!("silo {} has no certificates", self.silo_id))
    }
}

#[derive(Clone, Debug, Error, SerializeDisplay)]
enum ExternalEndpointError {
    #[error(
        "ignoring silo {dup_silo_id} ({dup_silo_name}): has the same DNS \
        name ({dns_name}) as previously-found silo {first_silo_id} \
        ({first_silo_name})"
    )]
    DupDnsName {
        dup_silo_id: Uuid,
        dup_silo_name: String,
        first_silo_id: Uuid,
        first_silo_name: String,
        dns_name: String,
    },

    #[error("ignoring certificate for silo {silo_id}: {reason:#}")]
    BadCert {
        silo_id: Uuid,
        #[source]
        reason: Arc<anyhow::Error>,
    },

    #[error("silo {silo_id} with DNS name {dns_name} has no certificates")]
    NoSiloCerts { silo_id: Uuid, dns_name: String },
}

impl Eq for ExternalEndpointError {}
impl PartialEq for ExternalEndpointError {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

/// A parsed, validated TLS certificate ready to use with an external TLS server
#[derive(Serialize)]
#[serde(transparent)]
struct TlsCertificate {
    /// This is what we need to provide to the TLS stack when we decide to use
    /// this certificate for an incoming TLS connection
    #[serde(skip)]
    certified_key: Arc<CertifiedKey>,

    /// Parsed representation of the whole certificate chain
    ///
    /// This is used to extract metadata like the expiration time.
    #[serde(skip)]
    parsed: X509,

    /// certificate digest (historically sometimes called a "fingerprint")
    digest: String,
}

impl fmt::Debug for TlsCertificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsCertificate").field("digest", &self.digest).finish()
    }
}

impl Eq for TlsCertificate {}
impl PartialEq for TlsCertificate {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl TryFrom<Certificate> for TlsCertificate {
    type Error = anyhow::Error;

    fn try_from(db_cert: Certificate) -> Result<TlsCertificate, anyhow::Error> {
        // XXX-dap all this code needs to be commonized / rationalized with the
        // db model code and the code in Dropshot that already exists to use
        // this stuff
        // XXX-dap this is probably very inefficient

        // Parse and validate what we've got.
        let certs_pem = openssl::x509::X509::stack_from_pem(&db_cert.cert)
            .context("parsing PEM stack")?;
        let private_key = PKey::private_key_from_pem(&db_cert.key)
            .context("parsing private key PEM")?;

        // Assemble a rustls CertifiedKey with both the certificate and the key.
        let certified_key = {
            let private_key_der = private_key
                .private_key_to_der()
                .context("serializing private key to DER")?;
            let rustls_private_key = rustls::PrivateKey(private_key_der);
            let rustls_signing_key =
                rustls::sign::any_supported_type(&rustls_private_key)
                    .context("parsing DER private key")?;
            let rustls_certs = certs_pem
                .iter()
                .map(|x509| {
                    x509.to_der()
                        .context("serializing cert to DER")
                        .map(rustls::Certificate)
                })
                .collect::<Result<_, _>>()?;
            Arc::new(CertifiedKey::new(rustls_certs, rustls_signing_key))
        };

        let end_cert = certs_pem
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("no certificates in PEM stack"))?;
        anyhow::ensure!(
            end_cert
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

        Ok(TlsCertificate { certified_key, digest, parsed: end_cert })
    }
}

// XXX-dap TODO-doc make sure we include a note about this function not
// failing if we can possibly avoid it.
pub async fn read_all_endpoints(
    datastore: &DataStore,
    opctx: &OpContext,
) -> Result<ExternalEndpoints, Error> {
    let silos = datastore.silos_list_all(opctx).await?;
    const MAX_EXTERNAL_DNS_ZONES: u32 = 10;
    // XXX needs to actually paginate or list all of them
    let pagparams = DataPageParams {
        marker: None,
        limit: NonZeroU32::new(10).unwrap(), // XXX-dap paginate?
        direction: dropshot::PaginationOrder::Ascending,
    };
    let external_dns_zones =
        datastore.dns_zones_list(opctx, DnsGroup::External, &pagparams).await?;
    bail_unless!(
        !external_dns_zones.is_empty()
            && external_dns_zones.len()
                < usize::try_from(MAX_EXTERNAL_DNS_ZONES).unwrap(),
        "expected between 1 and {} DNS zones, but found {}",
        MAX_EXTERNAL_DNS_ZONES,
        external_dns_zones.len(),
    );

    // XXX needs to actually paginate or list all of them
    let pagparams = PaginatedBy::Id(DataPageParams {
        marker: None,
        direction: dropshot::PaginationOrder::Ascending,
        limit: NonZeroU32::new(200).unwrap(), // XXX-dap
    });
    let certs = datastore
        .certificate_list_for(
            opctx,
            Some(ServiceKind::Nexus), // XXX-dap "external"?
            &pagparams,
            false,
        )
        .await?;

    // XXX-dap move the rest of this into a separate function for
    // testability.

    // Keep track of any issues in constructing this data structure.
    let mut warnings = vec![];

    // Compute a mapping from external DNS name to Silo id.
    // Detect any duplicates and leave them out (but report them).
    let silo_names: BTreeMap<Uuid, ExternalName> = silos
        .iter()
        .map(|db_silo| (db_silo.id(), db_silo.name().clone()))
        .collect();
    let mut dns_names: BTreeMap<String, Uuid> = BTreeMap::new();
    for z in external_dns_zones {
        for s in &silos {
            let dns_name =
                format!("{}.{}", silo_dns_name(s.name()), z.zone_name);
            match dns_names.entry(dns_name.clone()) {
                Entry::Vacant(vac) => {
                    vac.insert(s.id());
                }
                Entry::Occupied(occ) => {
                    let first_silo_id = *occ.get();
                    let first_silo_name = silo_names
                        .get(&first_silo_id)
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "<unknown>".to_string());
                    warnings.push(ExternalEndpointError::DupDnsName {
                        dup_silo_id: s.id(),
                        dup_silo_name: s.name().to_string(),
                        first_silo_id,
                        first_silo_name,
                        dns_name,
                    })
                }
            };
        }
    }

    // Compute a mapping from silo id to a list of usable TLS certificates
    // for the Silo.  By "usable" here, we just mean that we are capable of
    // providing it to the client.  This basically means that we can parse
    // it.  A certificate might be invalid for some other reason (e.g., does
    // not match the right DNS name or it's expired).  In the end, we'll
    // try not to select invalid certificates.  It's the responsibility of
    // other code paths (or people) to avoid putting us in the position
    // where we have no valid certificates.  But if that's where we find
    // ourselves at this point, it's better to provide a cert that's, say,
    // expired, than none at all.
    let (silo_tls_certs, cert_warnings): (Vec<_>, Vec<_>) = certs
        .into_iter()
        .map(|db_cert| (db_cert.silo_id, TlsCertificate::try_from(db_cert)))
        .partition(|(_, e)| e.is_ok());
    warnings.extend(cert_warnings.into_iter().map(|(silo_id, e)| {
        let reason = match e {
            // We partitioned above by whether this is an error not, so we
            // shouldn't find a non-error here.  (We cannot use unwrap_err()
            // because the `Ok` type doesn't impl `Debug`.)
            Ok(_) => unreachable!("found certificate in list of errors"),
            Err(e) => Arc::new(e),
        };

        ExternalEndpointError::BadCert { silo_id, reason }
    }));
    let mut certs_by_silo_id = BTreeMap::new();
    for (silo_id, tls_cert) in silo_tls_certs.into_iter() {
        // This was partitioned above so we should only have the non-errors
        // here.
        let tls_cert = tls_cert.unwrap();
        let silo_entry = certs_by_silo_id.entry(silo_id).or_insert_with(|| {
            ExternalEndpoint { silo_id, tls_certs: Vec::new() }
        });
        silo_entry.tls_certs.push(tls_cert)
    }

    let certs_by_silo_id: BTreeMap<_, _> =
        certs_by_silo_id.into_iter().map(|(k, v)| (k, Arc::new(v))).collect();

    let by_dns_name: BTreeMap<_, _> = dns_names
        .into_iter()
        .map(|(dns_name, silo_id)| {
            let silo_info =
                certs_by_silo_id.get(&silo_id).cloned().unwrap_or_else(|| {
                    Arc::new(ExternalEndpoint { silo_id, tls_certs: vec![] })
                });
            (dns_name, silo_info)
        })
        .collect();

    for (dns_name, silo_dns_cert) in &by_dns_name {
        if silo_dns_cert.tls_certs.is_empty() {
            warnings.push(ExternalEndpointError::NoSiloCerts {
                silo_id: silo_dns_cert.silo_id,
                dns_name: dns_name.clone(),
            })
        }
    }

    Ok(ExternalEndpoints { by_dns_name, warnings })
}

// XXX-dap TODO-doc
pub struct NexusCertResolver {
    log: slog::Logger,
    config_rx: watch::Receiver<Option<ExternalEndpoints>>,
}

impl NexusCertResolver {
    pub fn new(
        log: slog::Logger,
        config_rx: watch::Receiver<Option<ExternalEndpoints>>,
    ) -> NexusCertResolver {
        NexusCertResolver { log, config_rx }
    }

    fn do_resolve(
        &self,
        server_name: Option<&str>,
    ) -> Result<Arc<ExternalEndpoint>, anyhow::Error> {
        let server_name = match server_name {
            Some(s) => s,
            None => bail!("TLS session had no server name"),
        };

        let config_ref = self.config_rx.borrow();
        let config = match &*config_ref {
            Some(c) => c,
            None => bail!("no TLS config found"),
        };

        config
            .by_dns_name
            .get(server_name)
            .ok_or_else(|| anyhow!("unrecognized server name: {}", server_name))
            .cloned()
    }
}

impl rustls::server::ResolvesServerCert for NexusCertResolver {
    fn resolve(
        &self,
        client_hello: rustls::server::ClientHello,
    ) -> Option<Arc<CertifiedKey>> {
        let server_name = client_hello.server_name();
        let log =
            self.log.new(o!("server_name" => server_name.map(String::from)));

        trace!(&log, "resolving TLS certificate");
        let resolved = self.do_resolve(server_name);
        let result = match resolved {
            Ok(ref endpoint) => match endpoint.best_certificate() {
                Ok(certificate) => Ok((endpoint.silo_id, certificate)),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        match result {
            Ok((silo_id, certificate)) => {
                debug!(log, "resolved TLS certificate";
                    "silo_id" => silo_id.to_string(),
                    "certificate" => ?certificate
                );
                Some(certificate.certified_key.clone())
            }
            Err(error) => {
                // TODO-security There is a (limited) DoS risk here, in that the
                // client controls the request made to this endpoint and we're
                // going to emit something to the log every time this happens.
                // But at this stage it's pretty valuable to be able to debug
                // this problem.
                warn!(
                    log,
                    "failed to resolve TLS certificate";
                    "error" => format!("{:#}", error),
                );
                None
            }
        }
    }
}
