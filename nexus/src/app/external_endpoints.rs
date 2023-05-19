// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of external HTTPS endpoints
//!
//! Whenever a client connects to one of our external endpoints and attempts to
//! establish a TLS session, we must provide a TLS certificate to authenticate
//! ourselves to the client.  But each Silo has a separate external DNS name and
//! may have its own TLS certificate for that DNS name.  These all resolve to
//! the same set of IPs, so we cannot tell from the IP address alone which
//! Silo's endpoint the client is trying to reach nor which certificate to
//! present.  TLS provides a mechanism called Server Name Indication (SNI) for
//! clients to specify the name of the server they're trying to reach _before_
//! the TLS session is established.  We use this to determine which Silo
//! endpoint the client is trying to reach and so which TLS certificate to
//! present.
//!
//! To achieve this, we first need to know what DNS names, Silos, and TLS
//! certificates are available at any given time.  This is summarized in
//! [`ExternalEndpoints`].  A background task is responsible for maintaining
//! this, providing the latest version to whoever needs it via a `watch`
//! channel.  How do we tell the TLS stack what certificate to use?  When
//! setting up the Dropshot server in the first place, we provide a
//! [`rustls::ServerConfig`] that describes various TLS settings, including an
//! "certificate resolver" object that impls
//! [`rustls::server::ResolvesServerCert`].  See [`NexusCertResolver`].

use super::silo::silo_dns_name;
use crate::db::model::ServiceKind;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use nexus_db_model::Certificate;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::Discoverability;
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

/// Describes the set of external endpoints, organized by DNS name
///
/// This data structure provides a quick way to determine which Silo and TLS
/// certificate(s) make sense for an incoming request, based on the TLS
/// session's SNI (DNS name).  See module-level docs for details.
///
/// This object provides no interfaces outside this module.  It's only used by
/// the `NexusCertResolver` that's also in this module.
///
/// This structure impls `Serialize` only so that background tasks can easily
/// present the latest configuration that they've found (e.g., via a debug API)
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExternalEndpoints {
    by_dns_name: BTreeMap<String, Arc<ExternalEndpoint>>,
    warnings: Vec<ExternalEndpointError>,
}

impl ExternalEndpoints {
    /// Assemble a list of Silos, TLS certificates, and external DNS zones into
    /// a structure that we can use for quickly figuring out which Silo and TLS
    /// certificates are associated with each incoming DNS name
    fn new(
        silos: Vec<nexus_db_model::Silo>,
        certs: Vec<Certificate>,
        external_dns_zones: Vec<nexus_db_model::DnsZone>,
    ) -> ExternalEndpoints {
        // We want to avoid failing this operation even if we encounter problems
        // because we want to serve as many DNS certificates as we can (so that
        // an operator has a chance of fixing any problems that do exist).
        // Instead of returning any errors, keep track of any issues as
        // warnings.
        let mut warnings = vec![];

        // Compute a mapping from external DNS name to Silo id.  Detect any
        // duplicates and leave them out (but report them).  There should not
        // be any duplicates since the DNS names are constructed from the
        // (unique) Silo names.  Even if we support aliases in the future, they
        // will presumably need to be unique, too.
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
        // not match the right DNS name or it's expired).  We may later choose
        // to prefer some certificates over others, but that'll be decided later
        // (see best_certificate()).  And in the end it'll be better to provide
        // an expired certificate than none at all.
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
            let silo_entry =
                certs_by_silo_id.entry(silo_id).or_insert_with(|| {
                    ExternalEndpoint { silo_id, tls_certs: Vec::new() }
                });
            silo_entry.tls_certs.push(tls_cert)
        }

        let certs_by_silo_id: BTreeMap<_, _> = certs_by_silo_id
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect();

        let by_dns_name: BTreeMap<_, _> = dns_names
            .into_iter()
            .map(|(dns_name, silo_id)| {
                let silo_info = certs_by_silo_id
                    .get(&silo_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        Arc::new(ExternalEndpoint {
                            silo_id,
                            tls_certs: vec![],
                        })
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

        ExternalEndpoints { by_dns_name, warnings }
    }

    #[cfg(test)]
    pub fn has_domain(&self, dns_name: &str) -> bool {
        self.by_dns_name.contains_key(dns_name)
    }

    #[cfg(test)]
    pub fn ndomains(&self) -> usize {
        self.by_dns_name.len()
    }

    #[cfg(test)]
    pub fn nwarnings(&self) -> usize {
        self.warnings.len()
    }
}

/// Describes a single external "endpoint", by which we mean an external DNS
/// name that's associated with a particular Silo
#[derive(Debug, Eq, PartialEq, Serialize)]
struct ExternalEndpoint {
    /// the Silo associated with this endpoint
    silo_id: Uuid,
    /// the set of TLS certificate chains that could be appropriate for this
    /// endpoint
    tls_certs: Vec<TlsCertificate>,
}

impl ExternalEndpoint {
    /// Chooses a TLS certificate (chain) to use when handling connections to
    /// this endpoint
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

/// Describes a problem encountered while assembling an [`ExternalEndpoints`]
/// object
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
    // NOTE: It's important that we do not serialize the private key!
    #[serde(skip)]
    certified_key: Arc<CertifiedKey>,

    /// Parsed representation of the whole certificate chain
    ///
    /// This is used to extract metadata like the expiration time.
    // NOTE: It's important that we do not serialize the private key!
    #[serde(skip)]
    parsed: X509,

    /// certificate digest (historically sometimes called a "fingerprint")
    // This is the only field that appears in the serialized output or debug
    // output.
    digest: String,
}

impl fmt::Debug for TlsCertificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // It's important that only the digest appear in the debug output.  We
        // definitely don't want to leak the private key this way.  Really,
        // we don't want even the public parts adding noise to debug output.
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

/// Read the lists of all Silos, external DNS zones, and external TLS
/// certificates from the database and assemble an `ExternalEndpoints` structure
/// that describes what DNS names exist, which Silos they correspond to, and
/// what TLS certificates can be used for them
// This structure is used to determine what TLS certificates are used for
// incoming connections to the external console/API endpoints.  As such, it's
// critical that we produce a usable result if at all possible, even if it's
// incomplete.  Otherwise, we won't be able to serve _any_ incoming connections
// to _any_ of our external endpoints!  If data from the database is invalid or
// inconsistent, that data is discarded and a warning is produced, but we'll
// still return a usable object.
pub async fn read_all_endpoints(
    datastore: &DataStore,
    opctx: &OpContext,
) -> Result<ExternalEndpoints, Error> {
    // We will not look for more than this number of external DNS zones, Silos,
    // or certificates.  We do not expect very many of any of these objects.
    const MAX: u32 = 200;
    let pagparams_id = DataPageParams {
        marker: None,
        limit: NonZeroU32::new(MAX).unwrap(),
        direction: dropshot::PaginationOrder::Ascending,
    };
    let pagbyid = PaginatedBy::Id(pagparams_id.clone());
    let pagparams_name = DataPageParams {
        marker: None,
        limit: NonZeroU32::new(MAX).unwrap(),
        direction: dropshot::PaginationOrder::Ascending,
    };

    let silos =
        datastore.silos_list(opctx, &pagbyid, Discoverability::All).await?;
    let external_dns_zones = datastore
        .dns_zones_list(opctx, DnsGroup::External, &pagparams_name)
        .await?;
    bail_unless!(
        !external_dns_zones.is_empty(),
        "expected at least one external DNS zone"
    );
    let certs = datastore
        .certificate_list_for(opctx, Some(ServiceKind::Nexus), &pagbyid, false)
        .await?;

    // If we found too many of any of these things, complain as loudly as we
    // can.  Our results will be wrong.  But we still don't want to fail if we
    // can avoid it because we want to be able to serve as many endpoints as we
    // can.
    // TODO-reliability we should prevent people from creating more than this
    // maximum number of Silos and certificates.
    let max = usize::try_from(MAX).unwrap();
    if silos.len() >= max {
        error!(
            &opctx.log,
            "reading endpoints: expected at most {} silos, but found at \
            least {}.  TLS may not work on some Silos' external endpoints.",
            MAX,
            silos.len(),
        );
    }
    if external_dns_zones.len() >= max {
        error!(
            &opctx.log,
            "reading endpoints: expected at most {} external DNS zones, but \
            found at least {}.  TLS may not work on some Silos' external \
            endpoints.",
            MAX,
            external_dns_zones.len(),
        );
    }
    if certs.len() >= max {
        error!(
            &opctx.log,
            "reading endpoints: expected at most {} external DNS zones, but \
            found at least {}.  TLS may not work on some Silos' external \
            endpoints.",
            MAX,
            certs.len(),
        );
    }

    Ok(ExternalEndpoints::new(silos, certs, external_dns_zones))
}

/// TLS SNI certificate resolver for use with rustls/Dropshot
///
/// This object exists to impl `rustls::server::ResolvesServerCert`.  This
/// object looks at an incoming TLS session's SNI field, matches it against the
/// latest `ExternalEndpoints` configuration (available via a watch channel),
/// and then determines which certificate (if any) to provide for the new
/// session.
///
/// See the module-level comment for more details.
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
