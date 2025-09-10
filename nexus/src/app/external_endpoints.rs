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

use crate::context::ApiContext;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use nexus_db_model::AuthenticationMode;
use nexus_db_model::Certificate;
use nexus_db_model::DnsGroup;
use nexus_db_model::DnsZone;
use nexus_db_model::Silo;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::Discoverability;
use nexus_db_queries::db::model::ServiceKind;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::identity::Resource;
use nexus_types::silo::DEFAULT_SILO_ID;
use nexus_types::silo::silo_dns_name;
use omicron_common::api::external::Error;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::bail_unless;
use openssl::pkey::PKey;
use openssl::x509::X509;
use rustls::sign::CertifiedKey;
use serde::Serialize;
use serde_with::SerializeDisplay;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
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
    default_endpoint: Option<Arc<ExternalEndpoint>>,
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
        let silos_by_id: BTreeMap<Uuid, Arc<nexus_db_model::Silo>> = silos
            .into_iter()
            .map(|db_silo| (db_silo.id(), Arc::new(db_silo)))
            .collect();
        let mut dns_names: BTreeMap<String, Uuid> = BTreeMap::new();
        for z in external_dns_zones {
            for (_, db_silo) in &silos_by_id {
                let dns_name = format!(
                    "{}.{}",
                    silo_dns_name(db_silo.name()),
                    z.zone_name
                );
                match dns_names.entry(dns_name.clone()) {
                    Entry::Vacant(vac) => {
                        vac.insert(db_silo.id());
                    }
                    Entry::Occupied(occ) => {
                        let first_silo_id = *occ.get();
                        let first_silo_name = silos_by_id
                            .get(&first_silo_id)
                            .unwrap()
                            .name()
                            .to_string();
                        warnings.push(ExternalEndpointError::DupDnsName {
                            dup_silo_id: db_silo.id(),
                            dup_silo_name: db_silo.name().to_string(),
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
        let parsed_certificates = certs.into_iter().map(|db_cert| {
            let silo_id = db_cert.silo_id;
            let tls_cert = TlsCertificate::try_from(db_cert).map_err(|e| {
                ExternalEndpointError::BadCert { silo_id, reason: Arc::new(e) }
            })?;
            let db_silo = silos_by_id
                .get(&silo_id)
                .ok_or_else(|| ExternalEndpointError::BadCert {
                    silo_id,
                    reason: Arc::new(anyhow!("silo not found")),
                })?
                .clone();
            Ok((silo_id, db_silo, tls_cert))
        });

        let mut certs_by_silo_id = BTreeMap::new();
        for parsed_cert in parsed_certificates {
            match parsed_cert {
                Err(error) => {
                    warnings.push(error);
                }
                Ok((silo_id, db_silo, tls_cert)) => {
                    let silo_entry = certs_by_silo_id
                        .entry(silo_id)
                        .or_insert_with(|| ExternalEndpoint {
                            silo_id,
                            db_silo,
                            tls_certs: Vec::new(),
                        });
                    silo_entry.tls_certs.push(tls_cert)
                }
            };
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
                        // For something to appear in `dns_names`, we must have
                        // found it in `silos`, and so it must be in
                        // `silos_by_id`.
                        let db_silo =
                            silos_by_id.get(&silo_id).unwrap().clone();
                        Arc::new(ExternalEndpoint {
                            silo_id,
                            db_silo,
                            tls_certs: vec![],
                        })
                    });

                if silo_info.tls_certs.is_empty() {
                    warnings.push(ExternalEndpointError::NoSiloCerts {
                        silo_id,
                        dns_name: dns_name.clone(),
                    })
                }

                (dns_name, silo_info)
            })
            .collect();

        if by_dns_name.is_empty() {
            warnings.push(ExternalEndpointError::NoEndpoints);
        }

        // Pick a default endpoint.  This will be used if a request arrives
        // without specifying an endpoint via the HTTP/1.1 Host header or the
        // HTTP2 URL.  This is only intended for development, where external DNS
        // may not be set up.
        //
        // We somewhat arbitrarily choose the first Silo we find that's not JIT.
        // This would usually be the recovery Silo.
        let default_endpoint = silos_by_id
            .values()
            .filter(|s| {
                // Ignore the built-in Silo, which people are not supposed to
                // log into.
                s.id() != DEFAULT_SILO_ID
            })
            .find(|s| s.authentication_mode == AuthenticationMode::Local)
            .and_then(|s| {
                by_dns_name
                    .iter()
                    .find(|(_, endpoint)| endpoint.silo_id == s.id())
                    .map(|(_, endpoint)| endpoint.clone())
            });

        ExternalEndpoints { by_dns_name, warnings, default_endpoint }
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
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct ExternalEndpoint {
    /// the id of the Silo associated with this endpoint
    // This is redundant with `db_silo`, but it's convenient to put it here and
    // it shows up in the serialized form this way.
    silo_id: Uuid,
    /// the silo associated with this endpoint
    #[serde(skip)]
    db_silo: Arc<nexus_db_model::Silo>,
    /// the set of TLS certificate chains that could be appropriate for this
    /// endpoint
    tls_certs: Vec<TlsCertificate>,
}

impl ExternalEndpoint {
    pub fn silo(&self) -> &nexus_db_model::Silo {
        &self.db_silo
    }

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

        latest_expiration.ok_or_else(|| {
            anyhow!("silo {} has no usable certificates", self.silo_id)
        })
    }
}

/// Describes a problem encountered while assembling an [`ExternalEndpoints`]
/// object
#[derive(Clone, Debug, Error, SerializeDisplay)]
enum ExternalEndpointError {
    #[error(
        "ignoring silo {dup_silo_id} ({dup_silo_name:?}): has the same DNS \
        name ({dns_name:?}) as previously-found silo {first_silo_id} \
        ({first_silo_name:?})"
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

    #[error(
        "silo {silo_id} with DNS name {dns_name:?} has no usable certificates"
    )]
    NoSiloCerts { silo_id: Uuid, dns_name: String },

    #[error("no external endpoints were found")]
    NoEndpoints,
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
            let mut cursor = std::io::Cursor::new(db_cert.key.clone());
            let rustls_private_key = rustls_pemfile::private_key(&mut cursor)
                .expect("parsing private key PEM")
                .expect("no private keys found");
            let rustls_signing_key =
                rustls::crypto::ring::sign::any_supported_type(
                    &rustls_private_key,
                )
                .context("parsing DER private key")?;
            let rustls_certs = certs_pem
                .iter()
                .map(|x509| {
                    x509.to_der()
                        .context("serializing cert to DER")
                        .map(rustls::pki_types::CertificateDer::from)
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
pub(crate) async fn read_all_endpoints(
    datastore: &DataStore,
    opctx: &OpContext,
) -> Result<ExternalEndpoints, Error> {
    // The batch size here is pretty arbitrary.  On the vast majority of
    // systems, there will only ever be a handful of any of these objects.  Some
    // systems are known to have a few dozen silos and a few hundred TLS
    // certificates.  This code path is not particularly latency-sensitive.  Our
    // purpose in limiting the batch size is just to avoid unbounded-size
    // database transactions.
    //
    // unwrap(): safe because 200 is non-zero.
    let batch_size = NonZeroU32::new(200).unwrap();

    // Fetch all silos.
    let mut silos = Vec::new();
    let mut paginator =
        Paginator::new(batch_size, dropshot::PaginationOrder::Ascending);
    while let Some(p) = paginator.next() {
        let batch = datastore
            .silos_list(
                opctx,
                &PaginatedBy::Id(p.current_pagparams()),
                Discoverability::All,
            )
            .await?;
        paginator = p.found_batch(&batch, &|s: &Silo| s.id());
        silos.extend(batch.into_iter());
    }

    // Fetch all external DNS zones.  We should really only ever have one, but
    // we may as well paginate this.
    let mut external_dns_zones = Vec::new();
    let mut paginator =
        Paginator::new(batch_size, dropshot::PaginationOrder::Ascending);
    while let Some(p) = paginator.next() {
        let batch = datastore
            .dns_zones_list(opctx, DnsGroup::External, &p.current_pagparams())
            .await?;
        paginator = p.found_batch(&batch, &|z: &DnsZone| z.zone_name.clone());
        external_dns_zones.extend(batch.into_iter());
    }
    bail_unless!(
        !external_dns_zones.is_empty(),
        "expected at least one external DNS zone"
    );

    // Fetch all TLS certificates.
    let mut certs = Vec::new();
    let mut paginator =
        Paginator::new(batch_size, dropshot::PaginationOrder::Ascending);
    while let Some(p) = paginator.next() {
        let batch = datastore
            .certificate_list_for(
                opctx,
                Some(ServiceKind::Nexus),
                &PaginatedBy::Id(p.current_pagparams()),
                false,
            )
            .await?;
        paginator = p.found_batch(&batch, &|s: &Certificate| s.id());
        certs.extend(batch);
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
#[derive(Debug)]
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

    fn do_resolve_endpoint(
        &self,
        server_name: Option<&str>,
    ) -> Result<Arc<ExternalEndpoint>, anyhow::Error> {
        let Some(server_name) = server_name else {
            bail!("TLS session had no server name")
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

    fn do_resolve(
        &self,
        server_name: Option<&str>,
    ) -> Option<Arc<CertifiedKey>> {
        let log =
            self.log.new(o!("server_name" => server_name.map(String::from)));

        trace!(&log, "resolving TLS certificate");
        let resolved = self.do_resolve_endpoint(server_name);
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

impl rustls::server::ResolvesServerCert for NexusCertResolver {
    fn resolve(
        &self,
        client_hello: rustls::server::ClientHello,
    ) -> Option<Arc<CertifiedKey>> {
        let server_name = client_hello.server_name();
        self.do_resolve(server_name)
    }
}

impl super::Nexus {
    /// Attempts to determine which external endpoint the given request is
    /// attempting to reach
    ///
    /// This is intended primarily for unauthenticated requests so that we can
    /// determine which Silo's identity providers we should refer a user to so
    /// that they can log in.  For authenticated users, we know which Silo
    /// they're in.  In the future, we may also want this for authenticated
    /// requests to restrict access to a Silo only via one of its endpoints.
    ///
    /// Normally, this works as follows: a client (whether a browser, CLI, or
    /// otherwise) would be given a Silo-specific DNS name to use to reach one
    /// of our external endpoints.  They'd use our own external DNS servers
    /// (mostly likely indirectly) to resolve this to one of Nexus's external
    /// IPs.  Clients then put that DNS name in either the "host" header (in
    /// HTTP 1.1) or the URL's authority section (in HTTP 2 and later).
    ///
    /// In development, we do not assume that DNS has been set up properly.
    /// That means we might receive requests that appear targeted at an IP
    /// address or maybe are missing these fields altogether.  To support that
    /// case, we'll choose an arbitrary Silo.
    pub fn endpoint_for_request(
        &self,
        rqctx: &dropshot::RequestContext<ApiContext>,
    ) -> Result<Arc<ExternalEndpoint>, Error> {
        let log = &rqctx.log;
        let rqinfo = &rqctx.request;
        let requested_authority = authority_for_request(rqinfo)
            .map_err(|e| Error::invalid_request(&format!("{:#}", e)))?;
        endpoint_for_authority(
            log,
            &requested_authority,
            &self.background_tasks_internal.external_endpoints,
        )
    }
}

/// Returns the host and port of the server that the client is trying to
/// reach
///
/// Recall that Nexus serves many external endpoints on the same set of IP
/// addresses, each corresponding to a particular Silo.  We use the standard
/// HTTP 1.1 "host" header or HTTP2 URI authority to determine which
/// Silo's endpoint the client is trying to reach.
pub fn authority_for_request(
    rqinfo: &dropshot::RequestInfo,
) -> Result<http::uri::Authority, String> {
    if rqinfo.version() > hyper::Version::HTTP_11 {
        // For HTTP2, the server name is specified in the URL's "authority".
        rqinfo
            .uri()
            .authority()
            .cloned()
            .ok_or_else(|| String::from("request URL missing authority"))
    } else {
        // For earlier versions of HTTP, the server name is specified by the
        // "Host" header.
        rqinfo
            .headers()
            .get(http::header::HOST)
            .ok_or_else(|| String::from("request missing \"host\" header"))?
            .to_str()
            .map_err(|e| format!("failed to decode \"host\" header: {:#}", e))
            .and_then(|hostport| {
                hostport.parse().map_err(|e| {
                    format!("unsupported \"host\" header: {:#}", e)
                })
            })
    }
}

// See `Nexus::endpoint_for_request()` above.  This is factored out to be able
// to test it without a whole server.
fn endpoint_for_authority(
    log: &slog::Logger,
    requested_authority: &http::uri::Authority,
    config_rx: &tokio::sync::watch::Receiver<Option<ExternalEndpoints>>,
) -> Result<Arc<ExternalEndpoint>, Error> {
    let requested_host = requested_authority.host();
    let log = log.new(o!("server_name" => requested_host.to_string()));
    trace!(&log, "determining endpoint");

    // If we have not successfully loaded the endpoint configuration yet,
    // there's nothing we can do here.  We could try to do better (e.g., use
    // the recovery Silo?).  But if we failed to load endpoints, it's likely
    // the database is down, and we're not going to get much further anyway.
    let endpoint_config = config_rx.borrow();
    let endpoints = endpoint_config.as_ref().ok_or_else(|| {
        error!(&log, "received request with no endpoints loaded");
        Error::unavail("endpoints not loaded")
    })?;

    // See if there's an endpoint for the requested name.  If so, use it.
    if let Some(endpoint) = endpoints.by_dns_name.get(requested_host) {
        trace!(
            &log,
            "received request for endpoint";
            "silo_name" => ?endpoint.db_silo.name(),
            "silo_id" => ?endpoint.silo_id,
        );

        return Ok(endpoint.clone());
    }

    // There was no endpoint for the requested name.  This should generally
    // not happen in deployed systems where we expect people to have set up
    // DNS to find the external endpoints.  But in development, we don't
    // always have DNS set up.  People may use an IP address to get here.
    // To accommodate this use case, we make a best-effort to pick a default
    // endpoint when we can't find one for the name we were given.
    //
    // If this ever does happen in a production system, this might be
    // confusing.  The best thing to do in a production system is probably
    // to return an error saying that the requested server name was unknown.
    // Instead, we'll wind up choosing some Silo here.  This has no impact
    // on authenticated requests because for those we use the authenticated
    // identity's Silo.  (That's as of this writing.  Again, we may want to
    // disallow this and produce an error instead.)  If the request is not
    // authenticated, we may wind up sending them to a login page for this
    // Silo that may not be the Silo they meant.
    endpoints
        .default_endpoint
        .as_ref()
        .ok_or_else(|| {
            error!(
                &log,
                "received request for unknown host and no default \
                    endpoint is available",
            );
            Error::invalid_request(&format!(
                "HTTP request for unknown server name {:?}",
                requested_host,
            ))
        })
        .map(|c| c.clone())
}

#[cfg(test)]
mod test {
    use super::ExternalEndpoints;
    use super::TlsCertificate;
    use super::endpoint_for_authority;
    use crate::app::external_endpoints::ExternalEndpointError;
    use crate::app::external_endpoints::NexusCertResolver;
    use crate::app::external_endpoints::authority_for_request;
    use chrono::Utc;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingIfExists;
    use dropshot::ConfigLoggingLevel;
    use dropshot::endpoint;
    use dropshot::test_util::LogContext;
    use http::uri::Authority;
    use nexus_db_model::Certificate;
    use nexus_db_model::DnsGroup;
    use nexus_db_model::DnsZone;
    use nexus_db_model::ServiceKind;
    use nexus_db_model::Silo;
    use nexus_types::external_api::params;
    use nexus_types::external_api::shared;
    use nexus_types::identity::Resource;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde::Serialize;
    use std::net::SocketAddr;
    use uuid::Uuid;

    fn create_silo(silo_id: Option<Uuid>, name: &str, saml: bool) -> Silo {
        let identity_mode = if saml {
            shared::SiloIdentityMode::SamlJit
        } else {
            shared::SiloIdentityMode::LocalOnly
        };
        let params = params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: name.parse().unwrap(),
                description: String::new(),
            },
            quotas: params::SiloQuotasCreate::empty(),
            discoverable: false,
            identity_mode,
            admin_group_name: None,
            tls_certificates: vec![],
            mapped_fleet_roles: Default::default(),
            network_admin_required: None,
        };

        if let Some(silo_id) = silo_id {
            Silo::new_with_id(silo_id, params)
        } else {
            Silo::new(params)
        }
        .unwrap()
    }

    fn create_certificate(
        domain: &str,
        expired: bool,
    ) -> params::CertificateCreate {
        let mut cert_params =
            rcgen::CertificateParams::new(vec![domain.to_string()]);
        if expired {
            cert_params.not_after = std::time::UNIX_EPOCH.into();
        }
        let cert = rcgen::Certificate::from_params(cert_params).unwrap();
        let cert_pem =
            cert.serialize_pem().expect("serializing certificate as PEM");
        let key_pem = cert.serialize_private_key_pem();
        let namestr = format!("cert-for-{}", domain.replace('.', "-"));
        params::CertificateCreate {
            identity: IdentityMetadataCreateParams {
                name: namestr.parse().unwrap(),
                description: String::new(),
            },
            cert: cert_pem,
            key: key_pem,
            service: shared::ServiceUsingCertificate::ExternalApi,
        }
    }

    fn create_dns_zone(domain: &str) -> DnsZone {
        DnsZone {
            id: Uuid::new_v4(),
            time_created: Utc::now(),
            dns_group: DnsGroup::External,
            zone_name: format!("{}.test", domain),
        }
    }

    fn cert_matches(tls_cert: &TlsCertificate, cert: &Certificate) -> bool {
        let parse_right = openssl::x509::X509::from_pem(&cert.cert).unwrap();
        tls_cert.parsed == parse_right
    }

    #[test]
    fn test_external_endpoints_empty() {
        // Truly trivial case: no endpoints at all.
        let ee1 = ExternalEndpoints::new(vec![], vec![], vec![]);
        assert_eq!(ee1.ndomains(), 0);
        assert_eq!(ee1.nwarnings(), 1);
        assert_eq!(
            ee1.warnings[0].to_string(),
            "no external endpoints were found"
        );
        assert!(ee1.default_endpoint.is_none());

        // There are also no endpoints if there's a Silo but no external DNS
        // zones.
        let silo_id: Uuid =
            "6bcbd3bb-f93b-e8b3-d41c-dce6d98281d3".parse().unwrap();
        let silo = create_silo(Some(silo_id), "dummy", false);
        let ee2 = ExternalEndpoints::new(vec![silo], vec![], vec![]);
        assert_eq!(ee2.ndomains(), 0);
        assert_eq!(ee2.nwarnings(), 1);
        assert_eq!(
            ee2.warnings[0].to_string(),
            "no external endpoints were found"
        );
        assert!(ee2.default_endpoint.is_none());
        // Test PartialEq impl.
        assert_eq!(ee1, ee2);

        // There are also no endpoints if there's an external DNS zone but no
        // Silo.
        let dns_zone1 = create_dns_zone("oxide1");
        let ee2 = ExternalEndpoints::new(vec![], vec![], vec![dns_zone1]);
        assert_eq!(ee2.ndomains(), 0);
        assert_eq!(ee2.nwarnings(), 1);
        assert_eq!(
            ee2.warnings[0].to_string(),
            "no external endpoints were found"
        );
        assert!(ee2.default_endpoint.is_none());
        // Test PartialEq impl.
        assert_eq!(ee1, ee2);

        // Finally, there are no endpoints if there's a certificate and nothing
        // else.  This isn't really valid.  But it's useful to verify that we
        // won't crash or otherwise fail if we get a certificate with an invalid
        // silo_id.
        let cert_create = create_certificate("dummy.sys.oxide1.test", false);
        let cert = Certificate::new(
            silo_id,
            Uuid::new_v4(),
            ServiceKind::Nexus,
            cert_create,
            &["dummy.sys.oxide1.test".to_string()],
        )
        .unwrap();
        let ee2 = ExternalEndpoints::new(vec![], vec![cert], vec![]);
        assert_eq!(ee2.ndomains(), 0);
        assert_eq!(ee2.nwarnings(), 2);
        assert!(ee2.warnings[0].to_string().contains("silo not found"),);
        assert_eq!(
            ee2.warnings[1].to_string(),
            "no external endpoints were found"
        );
        assert!(ee2.default_endpoint.is_none());
    }

    #[test]
    fn test_external_endpoints_basic() {
        // Empty case for comparison.
        let ee1 = ExternalEndpoints::new(vec![], vec![], vec![]);

        // Sample data
        let silo_id: Uuid =
            "6bcbd3bb-f93b-e8b3-d41c-dce6d98281d3".parse().unwrap();
        let silo = create_silo(Some(silo_id), "dummy", false);
        let dns_zone1 = create_dns_zone("oxide1");
        let cert_create = create_certificate("dummy.sys.oxide1.test", false);
        let cert = Certificate::new(
            silo_id,
            Uuid::new_v4(),
            ServiceKind::Nexus,
            cert_create,
            &["dummy.sys.oxide1.test".to_string()],
        )
        .unwrap();

        // Simple case: one silo, one DNS zone.  We should see an endpoint for
        // the Silo.  Since it has no certificates, we'll get a warning.
        let ee3 = ExternalEndpoints::new(
            vec![silo.clone()],
            vec![],
            vec![dns_zone1.clone()],
        );
        // Test PartialEq impl.
        assert_ne!(ee1, ee3);
        assert_eq!(ee3.ndomains(), 1);
        assert!(ee3.has_domain("dummy.sys.oxide1.test"));
        assert_eq!(ee3.nwarnings(), 1);
        assert_eq!(
            ee3.warnings[0].to_string(),
            "silo 6bcbd3bb-f93b-e8b3-d41c-dce6d98281d3 with DNS name \
            \"dummy.sys.oxide1.test\" has no usable certificates"
        );
        // This also exercises best_certificate() with zero certificates.
        assert_eq!(
            ee3.by_dns_name["dummy.sys.oxide1.test"]
                .best_certificate()
                .unwrap_err()
                .to_string(),
            "silo 6bcbd3bb-f93b-e8b3-d41c-dce6d98281d3 has no usable \
            certificates"
        );
        assert_eq!(ee3.default_endpoint.as_ref().unwrap().silo_id, silo_id);

        // Now try with a certificate.
        let ee4 = ExternalEndpoints::new(
            vec![silo.clone()],
            vec![cert.clone()],
            vec![dns_zone1.clone()],
        );
        assert_ne!(ee3, ee4);
        assert_eq!(ee4.ndomains(), 1);
        assert!(ee4.has_domain("dummy.sys.oxide1.test"));
        assert_eq!(ee4.nwarnings(), 0);
        let endpoint = &ee4.by_dns_name["dummy.sys.oxide1.test"];
        assert_eq!(endpoint.silo_id, silo_id);
        assert_eq!(endpoint.tls_certs.len(), 1);
        assert!(cert_matches(&endpoint.tls_certs[0], &cert));
        // This also exercises best_certificate() with one certificate.
        assert_eq!(
            *endpoint.best_certificate().unwrap(),
            endpoint.tls_certs[0]
        );
        assert_eq!(ee4.default_endpoint.as_ref().unwrap().silo_id, silo_id);

        // Add a second external DNS zone.  There should now be two endpoints,
        // both pointing to the same Silo.
        let dns_zone2 = DnsZone {
            id: Uuid::new_v4(),
            time_created: Utc::now(),
            dns_group: DnsGroup::External,
            zone_name: String::from("oxide2.test"),
        };
        let ee5 = ExternalEndpoints::new(
            vec![silo.clone()],
            vec![cert.clone()],
            vec![dns_zone1.clone(), dns_zone2],
        );
        assert_ne!(ee4, ee5);
        assert_eq!(ee5.ndomains(), 2);
        assert!(ee5.has_domain("dummy.sys.oxide1.test"));
        assert!(ee5.has_domain("dummy.sys.oxide2.test"));
        assert_eq!(ee5.nwarnings(), 0);
        assert_eq!(ee5.default_endpoint.as_ref().unwrap().silo_id, silo_id);
        let endpoint1 = &ee5.by_dns_name["dummy.sys.oxide1.test"];
        let endpoint2 = &ee5.by_dns_name["dummy.sys.oxide2.test"];
        assert_eq!(endpoint1, endpoint2);
        assert_eq!(endpoint1.silo_id, silo_id);
        assert_eq!(endpoint1.tls_certs.len(), 1);
        assert_eq!(endpoint2.silo_id, silo_id);
        assert_eq!(endpoint2.tls_certs.len(), 1);

        // Add a second Silo with the same name as the first one.  This should
        // not be possible in practice.  In the future, we expect other features
        // (e.g., DNS aliases) to make it possible for silos' DNS names to
        // overlap like this.
        let silo2_same_name_id =
            "e3f36f20-56c3-c545-8320-c19d98b82c1d".parse().unwrap();
        let silo2_same_name =
            create_silo(Some(silo2_same_name_id), "dummy", false);
        let ee6 = ExternalEndpoints::new(
            vec![silo, silo2_same_name],
            vec![cert],
            vec![dns_zone1],
        );
        assert_ne!(ee5, ee6);
        assert_eq!(ee6.ndomains(), 1);
        assert!(ee6.has_domain("dummy.sys.oxide1.test"));
        assert_eq!(ee6.default_endpoint.as_ref().unwrap().silo_id, silo_id);
        let endpoint = &ee6.by_dns_name["dummy.sys.oxide1.test"];
        assert_eq!(endpoint.silo_id, silo_id);
        assert_eq!(endpoint.tls_certs.len(), 1);
        assert_eq!(ee6.nwarnings(), 1);
        assert_eq!(
            ee6.warnings[0].to_string(),
            "ignoring silo e3f36f20-56c3-c545-8320-c19d98b82c1d (\"dummy\"): \
            has the same DNS name (\"dummy.sys.oxide1.test\") as \
            previously-found silo 6bcbd3bb-f93b-e8b3-d41c-dce6d98281d3 \
            (\"dummy\")"
        );
    }

    #[test]
    fn test_external_endpoints_complex() {
        // Set up a somewhat complex scenario:
        //
        // - four Silos
        //   - silo1: two certificates, one of which is expired
        //   - silo2: two certificates, one of which is expired
        //     (in the other order to make sure it's not working by accident)
        //   - silo3: one certificate that is invalid
        //   - silo4: one certificate that is expired
        // - two DNS zones
        //
        // We should wind up with eight endpoints and one warning.
        let silo1 = create_silo(None, "silo1", true);
        let silo2 = create_silo(None, "silo2", true);
        let silo3 = create_silo(None, "silo3", false);
        let silo4 = create_silo(None, "silo4", true);
        let silo1_cert1_params =
            create_certificate("silo1.sys.oxide1.test", false);
        let silo1_cert1 = Certificate::new(
            silo1.identity().id,
            Uuid::new_v4(),
            ServiceKind::Nexus,
            silo1_cert1_params,
            &["silo1.sys.oxide1.test".to_string()],
        )
        .unwrap();
        let silo1_cert2_params =
            create_certificate("silo1.sys.oxide1.test", true);
        let silo1_cert2 = Certificate::new_unvalidated(
            silo1.identity().id,
            Uuid::new_v4(),
            ServiceKind::Nexus,
            silo1_cert2_params,
        );
        let silo2_cert1_params =
            create_certificate("silo2.sys.oxide1.test", true);
        let silo2_cert1 = Certificate::new_unvalidated(
            silo2.identity().id,
            Uuid::new_v4(),
            ServiceKind::Nexus,
            silo2_cert1_params,
        );
        let silo2_cert2_params =
            create_certificate("silo2.sys.oxide1.test", false);
        let silo2_cert2 = Certificate::new(
            silo2.identity().id,
            Uuid::new_v4(),
            ServiceKind::Nexus,
            silo2_cert2_params,
            &["silo2.sys.oxide1.test".to_string()],
        )
        .unwrap();
        let silo3_cert_params =
            create_certificate("silo3.sys.oxide1.test", false);
        let mut silo3_cert = Certificate::new(
            silo3.identity().id,
            Uuid::new_v4(),
            ServiceKind::Nexus,
            silo3_cert_params,
            &["silo3.sys.oxide1.test".to_string()],
        )
        .unwrap();
        // Corrupt a byte of this last certificate.  (This has to be done after
        // constructing it or we would fail validation.)
        silo3_cert.cert[0] ^= 1;
        let silo4_cert_params =
            create_certificate("silo4.sys.oxide1.test", true);
        let silo4_cert = Certificate::new_unvalidated(
            silo4.identity().id,
            Uuid::new_v4(),
            ServiceKind::Nexus,
            silo4_cert_params,
        );
        let dns_zone1 = create_dns_zone("oxide1");
        let dns_zone2 = create_dns_zone("oxide2");

        let ee = ExternalEndpoints::new(
            vec![silo1.clone(), silo2.clone(), silo3.clone(), silo4.clone()],
            vec![
                silo1_cert1.clone(),
                silo1_cert2.clone(),
                silo2_cert1,
                silo2_cert2.clone(),
                silo3_cert.clone(),
                silo4_cert.clone(),
            ],
            vec![dns_zone1, dns_zone2],
        );
        println!("{:?}", ee);
        assert_eq!(ee.ndomains(), 8);
        assert_eq!(ee.nwarnings(), 3);
        assert_eq!(
            2,
            ee.warnings
                .iter()
                .filter(|warning| matches!(warning,
                    ExternalEndpointError::NoSiloCerts { silo_id, .. }
                        if *silo_id == silo3.id()
                ))
                .count()
        );
        assert_eq!(
            1,
            ee.warnings
                .iter()
                .filter(|warning| matches!(warning,
                    ExternalEndpointError::BadCert { silo_id, .. }
                        if *silo_id == silo3.id()
                ))
                .count()
        );

        assert_eq!(
            ee.by_dns_name["silo1.sys.oxide1.test"],
            ee.by_dns_name["silo1.sys.oxide2.test"]
        );
        assert_eq!(
            ee.by_dns_name["silo2.sys.oxide1.test"],
            ee.by_dns_name["silo2.sys.oxide2.test"]
        );
        assert_eq!(
            ee.by_dns_name["silo3.sys.oxide1.test"],
            ee.by_dns_name["silo3.sys.oxide2.test"]
        );
        assert_eq!(
            ee.by_dns_name["silo4.sys.oxide1.test"],
            ee.by_dns_name["silo4.sys.oxide2.test"]
        );
        assert_eq!(
            ee.default_endpoint.as_ref().unwrap().silo_id,
            silo3.identity().id
        );

        let e1 = &ee.by_dns_name["silo1.sys.oxide1.test"];
        assert_eq!(e1.silo_id, silo1.id());
        let c1 = e1.best_certificate().unwrap();
        // It must be cert1 because cert2 is expired.
        assert!(cert_matches(c1, &silo1_cert1));

        let e2 = &ee.by_dns_name["silo2.sys.oxide1.test"];
        assert_eq!(e2.silo_id, silo2.id());
        let c2 = e2.best_certificate().unwrap();
        // It must be cert2 because cert1 is expired.
        assert!(cert_matches(c2, &silo2_cert2));
        assert!(!cert_matches(c2, &silo1_cert1));
        assert!(!cert_matches(c2, &silo1_cert2));

        let e3 = &ee.by_dns_name["silo3.sys.oxide1.test"];
        assert_eq!(e3.silo_id, silo3.id());
        assert!(e3.best_certificate().is_err());

        // We should get an expired cert if it's the only option.
        let e4 = &ee.by_dns_name["silo4.sys.oxide1.test"];
        assert_eq!(e4.silo_id, silo4.id());
        let c4 = e4.best_certificate().unwrap();
        assert!(cert_matches(c4, &silo4_cert));

        //
        // Test endpoint lookup by authority.
        //
        let logctx = LogContext::new(
            "test_external_endpoints_complex",
            &ConfigLogging::File {
                level: ConfigLoggingLevel::Trace,
                path: "UNUSED".into(),
                if_exists: ConfigLoggingIfExists::Append,
            },
        );
        let log = &logctx.log;
        let (_, watch_rx) = tokio::sync::watch::channel(Some(ee.clone()));

        // Basic cases: look up a few Silos by name.
        let authority = Authority::from_static("silo1.sys.oxide1.test");
        let ae1 = endpoint_for_authority(&log, &authority, &watch_rx).unwrap();
        assert_eq!(ae1, *e1);
        let authority = Authority::from_static("silo1.sys.oxide2.test");
        let ae1 = endpoint_for_authority(&log, &authority, &watch_rx).unwrap();
        assert_eq!(ae1, *e1);
        let authority = Authority::from_static("silo2.sys.oxide1.test");
        let ae2 = endpoint_for_authority(&log, &authority, &watch_rx).unwrap();
        assert_eq!(ae2, *e2);
        // The port number in the authority should be ignored.
        let authority = Authority::from_static("silo3.sys.oxide1.test:456");
        let ae3 = endpoint_for_authority(&log, &authority, &watch_rx).unwrap();
        assert_eq!(ae3, *e3);
        // We should get back a default endpoint if we use a server name that's
        // not known.  That includes any IPv4 or IPv6 address, too.  The default
        // endpoint should always be silo3 because it's the only one we've
        // created LocalOnly.
        for name in [
            "springfield.sys.oxide1.test",
            "springfield.sys.oxide1.test:123",
            "10.1.2.3:456",
            "[fe80::1]:789",
        ] {
            let authority = Authority::from_static(name);
            let ae =
                endpoint_for_authority(&log, &authority, &watch_rx).unwrap();
            assert_eq!(ae, *e3);
        }

        //
        // Now test the NexusCertResolver.
        //
        let (watch_tx, watch_rx) = tokio::sync::watch::channel(None);
        let cert_resolver =
            NexusCertResolver::new(logctx.log.clone(), watch_rx);

        // At this point we haven't filled in the configuration so any attempt
        // to resolve anything should fail.
        assert!(
            cert_resolver.do_resolve(Some("silo1.sys.oxide1.test")).is_none()
        );

        // Now pass along the configuration and try again.
        watch_tx.send(Some(ee.clone())).unwrap();
        let resolved_c1 =
            cert_resolver.do_resolve(Some("silo1.sys.oxide1.test")).unwrap();
        assert_eq!(resolved_c1.cert, c1.certified_key.cert);
        let resolved_c2 =
            cert_resolver.do_resolve(Some("silo2.sys.oxide1.test")).unwrap();
        assert_eq!(resolved_c2.cert, c2.certified_key.cert);
        assert!(
            cert_resolver.do_resolve(Some("silo3.sys.oxide1.test")).is_none()
        );
        // We should get an expired cert if it's the only option.
        let resolved_c4 =
            cert_resolver.do_resolve(Some("silo4.sys.oxide1.test")).unwrap();
        assert_eq!(resolved_c4.cert, c4.certified_key.cert);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_authority() {
        // Tests for authority_for_request().  The function itself is pretty
        // simple.  That makes it easy to test fairly exhaustively.  It's also
        // useful to verify that we're doing what we think we're doing
        // (identifying the name that the client thinks they're connecting to).

        // First, set up a Dropshot server that just echoes back whatever
        // authority_for_request() returns for a given request.
        let logctx = omicron_test_utils::dev::test_setup_log("test_authority");
        let mut api = dropshot::ApiDescription::new();
        api.register(echo_server_name).unwrap();
        let server = dropshot::ServerBuilder::new(api, (), logctx.log.clone())
            .start()
            .expect("failed to create dropshot server");
        let local_addr = server.local_addr();
        let port = local_addr.port();

        #[derive(Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
        struct AuthorityResponse {
            host: String,
            port: Option<u16>,
        }

        #[endpoint(method = GET, path = "/server_name")]
        async fn echo_server_name(
            rqctx: dropshot::RequestContext<()>,
        ) -> Result<
            dropshot::HttpResponseOk<Result<AuthorityResponse, String>>,
            dropshot::HttpError,
        > {
            Ok(dropshot::HttpResponseOk(
                authority_for_request(&rqctx.request).map(|authority| {
                    AuthorityResponse {
                        host: authority.host().to_string(),
                        port: authority.port_u16(),
                    }
                }),
            ))
        }

        // Generally, the "authority" for a request is determined by the URL
        // provided to the client.  We can test basically two cases this way: an
        // authority with a host and port and an authority with an IP address
        // and port.  We can't test any cases that require the client to connect
        // to a different host/port than what's in the URL.  So we can't test
        // the case of an authority with no port number in it (since our server
        // doesn't run on port 80).
        //
        // With HTTP 1.1, you can generally override the authority by specifying
        // your own "host" header.  That lets us exercise the case of an
        // authority that has no port number, even though the client would be
        // connecting to a URL with a port number in it.  It might also let us
        // test other cases, like an authority with an invalid DNS name.
        // However, it's not clear any of this is possible with HTTP 2 or later.

        async fn test_v2_host(
            hostname: &str,
            addr: SocketAddr,
        ) -> AuthorityResponse {
            let v2_client = reqwest::ClientBuilder::new()
                .http2_prior_knowledge()
                .resolve(hostname, addr)
                .build()
                .unwrap();
            test_request(&v2_client, &format!("{}:{}", hostname, addr.port()))
                .await
        }

        async fn test_v2_ip(addr: SocketAddr) -> AuthorityResponse {
            let v2_client = reqwest::ClientBuilder::new()
                .http2_prior_knowledge()
                .build()
                .unwrap();
            test_request(&v2_client, &addr.to_string()).await
        }

        async fn test_v1_host(
            hostname: &str,
            addr: SocketAddr,
            override_host: Option<&str>,
        ) -> AuthorityResponse {
            let mut v1_builder = reqwest::ClientBuilder::new()
                .http1_only()
                .resolve(hostname, addr);
            if let Some(host) = override_host {
                let mut headers = http::header::HeaderMap::new();
                headers.insert(http::header::HOST, host.try_into().unwrap());
                v1_builder = v1_builder.default_headers(headers);
            }
            let v1_client = v1_builder.build().unwrap();
            test_request(&v1_client, &format!("{}:{}", hostname, addr.port()))
                .await
        }

        async fn test_v1_ip(
            addr: SocketAddr,
            override_host: Option<&str>,
        ) -> AuthorityResponse {
            let mut v1_builder = reqwest::ClientBuilder::new().http1_only();
            if let Some(host) = override_host {
                let mut headers = http::header::HeaderMap::new();
                headers.append(http::header::HOST, host.try_into().unwrap());
                v1_builder = v1_builder.default_headers(headers);
            }
            let v1_client = v1_builder.build().unwrap();
            test_request(&v1_client, &addr.to_string()).await
        }

        async fn test_request(
            client: &reqwest::Client,
            connect_host: &str,
        ) -> AuthorityResponse {
            let url = format!("http://{}/server_name", connect_host);

            let result = client
                .get(&url)
                .send()
                .await
                .unwrap_or_else(|e| panic!("GET {:?}: {:#}", url, e));
            let status = result.status();
            println!("status: {:?}", status);
            if status != http::StatusCode::OK {
                panic!("GET {:?}: unexpected status: {:?}", url, status);
            }

            let body: Result<AuthorityResponse, String> =
                result.json().await.unwrap_or_else(|e| {
                    panic!("GET {:?}: parse json: {:#}", url, e);
                });
            println!("body: {:?}", body);
            body.unwrap()
        }

        // HTTP 2: regular hostname (with port)
        let authority = test_v2_host("foo.example.com", local_addr).await;
        assert_eq!(authority.host, "foo.example.com");
        assert_eq!(authority.port, Some(port));

        // HTTP 2: IP address (with port)
        let authority = test_v2_ip(local_addr).await;
        assert_eq!(authority.host, local_addr.ip().to_string());
        assert_eq!(authority.port, Some(port));

        // HTTP 1.1: regular hostname, no overridden "host" header.
        let authority = test_v1_host("foo.example.com", local_addr, None).await;
        assert_eq!(authority.host, "foo.example.com");
        assert_eq!(authority.port, Some(port));

        // HTTP 1.1: regular hostname, override "host" header with port.
        let authority = test_v1_host(
            "foo.example.com",
            local_addr,
            Some("foo.example.com:123"),
        )
        .await;
        assert_eq!(authority.host, "foo.example.com");
        assert_eq!(authority.port, Some(123));

        // HTTP 1.1: regular hostname, override "host" header with no port.
        let authority = test_v1_host(
            "foo.example.com",
            local_addr,
            Some("foo.example.com"),
        )
        .await;
        assert_eq!(authority.host, "foo.example.com");
        assert_eq!(authority.port, None);

        // HTTP 1.1: IP address, no overridden "host" header.
        let authority = test_v1_ip(local_addr, None).await;
        assert_eq!(authority.host, local_addr.ip().to_string());
        assert_eq!(authority.port, Some(port));

        // HTTP 1.1: IP address, override "host" header with port.
        let authority =
            test_v1_ip(local_addr, Some("foo.example.com:123")).await;
        assert_eq!(authority.host, "foo.example.com");
        assert_eq!(authority.port, Some(123));

        // HTTP 1.1: IP address, override "host" header with no port.
        let authority = test_v1_ip(local_addr, Some("foo.example.com")).await;
        assert_eq!(authority.host, "foo.example.com");
        assert_eq!(authority.port, None);

        server.close().await.expect("failed to shut down dropshot server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_no_endpoint() {
        let logctx =
            omicron_test_utils::dev::test_setup_log("test_no_endpoint");
        let log = &logctx.log;

        // We'll test two configurations at the same time: one where there's no
        // configuration at all, and one where there's a configuration but no
        // default endpoint.  These should always produce errors, no matter what
        // endpoint we're looking up.
        let ee = ExternalEndpoints::new(vec![], vec![], vec![]);
        let (_, none_rx) =
            tokio::sync::watch::channel::<Option<ExternalEndpoints>>(None);
        let (_, empty_rx) =
            tokio::sync::watch::channel::<Option<ExternalEndpoints>>(Some(ee));

        for name in [
            "dummy",
            "dummy.example",
            "dummy.example:123",
            "10.1.2.3:456",
            "[fe80::1]:789",
        ] {
            let authority = Authority::from_static(name);
            for (rx_label, rx_channel) in
                [("empty", &empty_rx), ("none", &none_rx)]
            {
                println!("config {:?} endpoint {:?}", rx_label, name);
                let result =
                    endpoint_for_authority(&log, &authority, rx_channel);
                match result {
                    Err(Error::ServiceUnavailable { internal_message }) => {
                        assert_eq!(rx_label, "none");
                        assert_eq!(internal_message, "endpoints not loaded");
                    }
                    Err(Error::InvalidRequest { message }) => {
                        assert_eq!(rx_label, "empty");
                        assert_eq!(
                            message.external_message(),
                            format!(
                                "HTTP request for unknown server name {:?}",
                                authority.host()
                            )
                        );
                    }
                    result => {
                        panic!(
                            "unexpected result looking up endpoint for \
                            {:?} with config {:?}: {:?}",
                            name, rx_label, result
                        );
                    }
                }
            }
        }

        logctx.cleanup_successful();
    }
}
