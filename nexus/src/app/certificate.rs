// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! x.509 Certificates

use super::silo::silo_dns_name;
use crate::db;
use crate::db::lookup;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::db::model::ServiceKind;
use crate::external_api::params;
use crate::external_api::shared;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use nexus_db_model::DnsGroup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::Name as ExternalName;
use omicron_common::api::external::NameOrId;
use omicron_common::bail_unless;
use openssl::pkey::PKey;
use ref_cast::RefCast;
use rustls::sign::CertifiedKey;
use serde::Serialize;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

// XXX-dap TODO-doc TODO-coverage
#[derive(Clone)]
pub struct SiloDnsCerts {
    by_dns_name: BTreeMap<String, Arc<SiloDnsCert>>,
    warnings: Vec<SiloDnsCertError>,
}

impl SiloDnsCerts {
    pub fn serialize(&self) -> SiloDnsCertsDebug<'_> {
        SiloDnsCertsDebug {
            by_dns_name: self
                .by_dns_name
                .iter()
                .map(|(k, v)| (k.as_str(), SiloDnsCertDebug::new(v)))
                .collect(),
            warnings: self
                .warnings
                .iter()
                .map(|w| format!("{:#}", w))
                .collect(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct SiloDnsCertsDebug<'a> {
    by_dns_name: BTreeMap<&'a str, SiloDnsCertDebug>,
    warnings: Vec<String>,
}

struct SiloDnsCert {
    silo_id: Uuid,
    tls_certs: Vec<Arc<CertifiedKey>>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct SiloDnsCertDebug {
    silo_id: Uuid,
    tls_certs: Vec<CertifiedKeyDebug>,
}

impl SiloDnsCertDebug {
    fn new(s: &SiloDnsCert) -> SiloDnsCertDebug {
        SiloDnsCertDebug {
            silo_id: s.silo_id,
            tls_certs: s
                .tls_certs
                .iter()
                .map(|c| CertifiedKeyDebug::new(c))
                .collect(),
        }
    }
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

#[derive(Clone, Debug, Error)]
enum SiloDnsCertError {
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

impl super::Nexus {
    pub fn certificate_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        certificate: &'a NameOrId,
    ) -> lookup::Certificate<'a> {
        match certificate {
            NameOrId::Id(id) => {
                LookupPath::new(opctx, &self.db_datastore).certificate_id(*id)
            }
            NameOrId::Name(name) => LookupPath::new(opctx, &self.db_datastore)
                .certificate_name(Name::ref_cast(name)),
        }
    }

    pub async fn certificate_create(
        &self,
        opctx: &OpContext,
        params: params::CertificateCreate,
    ) -> CreateResult<db::model::Certificate> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("creating a Certificate")?;
        let kind = params.service;
        let new_certificate = db::model::Certificate::new(
            authz_silo.id(),
            Uuid::new_v4(),
            kind.into(),
            params,
        )?;
        let cert = self
            .db_datastore
            .certificate_create(opctx, new_certificate)
            .await?;

        match kind {
            shared::ServiceUsingCertificate::ExternalApi => {
                // TODO We could improve the latency of other Nexus instances
                // noticing this certificate change with an explicit request to
                // them.  Today, Nexus instances generally don't talk to each
                // other.  That's a very valuable simplifying assumption.
                self.background_tasks
                    .activate(&self.background_tasks.task_tls_certs);
                Ok(cert)
            }
        }
    }

    pub async fn certificates_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Certificate> {
        self.db_datastore
            .certificate_list_for(opctx, None, pagparams, true)
            .await
    }

    pub async fn certificate_delete(
        &self,
        opctx: &OpContext,
        certificate_lookup: lookup::Certificate<'_>,
    ) -> DeleteResult {
        let (.., authz_cert, db_cert) =
            certificate_lookup.fetch_for(authz::Action::Delete).await?;
        self.db_datastore.certificate_delete(opctx, &authz_cert).await?;
        match db_cert.service {
            ServiceKind::Nexus => {
                // See the comment in certificate_create() above.
                self.background_tasks
                    .activate(&self.background_tasks.task_tls_certs);
            }
            _ => (),
        };
        Ok(())
    }
}

// XXX-dap TODO-doc make sure we include a note about this function not
// failing if we can possibly avoid it.
pub async fn silos_load_dns_tls(
    datastore: &DataStore,
    opctx: &OpContext,
) -> Result<SiloDnsCerts, Error> {
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
                    warnings.push(SiloDnsCertError::DupDnsName {
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
        .map(|db_cert| (db_cert.silo_id, db_cert_to_tls_cert(db_cert)))
        .partition(|(_, e)| e.is_ok());
    warnings.extend(cert_warnings.into_iter().map(|(silo_id, e)| {
        let reason = match e {
            // We partitioned above by whether this is an error not, so we
            // shouldn't find a non-error here.  (We cannot use unwrap_err()
            // because the `Ok` type doesn't impl `Debug`.)
            Ok(_) => unreachable!("found certificate in list of errors"),
            Err(e) => Arc::new(e),
        };

        SiloDnsCertError::BadCert { silo_id, reason }
    }));
    let mut certs_by_silo_id = BTreeMap::new();
    for (silo_id, tls_cert) in silo_tls_certs.into_iter() {
        // This was partitioned above so we should only have the non-errors
        // here.
        let tls_cert = tls_cert.unwrap();
        let silo_entry = certs_by_silo_id
            .entry(silo_id)
            .or_insert_with(|| SiloDnsCert { silo_id, tls_certs: Vec::new() });
        silo_entry.tls_certs.push(Arc::new(tls_cert))
    }

    let certs_by_silo_id: BTreeMap<_, _> =
        certs_by_silo_id.into_iter().map(|(k, v)| (k, Arc::new(v))).collect();

    let by_dns_name: BTreeMap<_, _> = dns_names
        .into_iter()
        .map(|(dns_name, silo_id)| {
            let silo_info =
                certs_by_silo_id.get(&silo_id).cloned().unwrap_or_else(|| {
                    Arc::new(SiloDnsCert { silo_id, tls_certs: vec![] })
                });
            (dns_name, silo_info)
        })
        .collect();

    for (dns_name, silo_dns_cert) in &by_dns_name {
        if silo_dns_cert.tls_certs.is_empty() {
            warnings.push(SiloDnsCertError::NoSiloCerts {
                silo_id: silo_dns_cert.silo_id,
                dns_name: dns_name.clone(),
            })
        }
    }

    Ok(SiloDnsCerts { by_dns_name, warnings })
}

// XXX-dap TryFrom?  Put it into the model?
fn db_cert_to_tls_cert(
    db_cert: nexus_db_model::Certificate,
) -> Result<CertifiedKey, anyhow::Error> {
    // XXX-dap all this code needs to be commonized / rationalized with the db
    // model code and the code in Dropshot that already exists to use this stuff
    // XXX-dap this is probably very inefficient

    // Parse and validate what we've got.
    let certs_pem = openssl::x509::X509::stack_from_pem(&db_cert.cert)
        .context("parsing PEM stack")?;
    let end_cert = certs_pem
        .iter()
        .next()
        .ok_or_else(|| anyhow!("no certificates in PEM stack"))?;
    let private_key = PKey::private_key_from_pem(&db_cert.key)
        .context("parsing private key PEM")?;
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
            .into_iter()
            .map(|x509| {
                x509.to_der()
                    .context("serializing cert to DER")
                    .map(rustls::Certificate)
            })
            .collect::<Result<_, _>>()?;
        CertifiedKey::new(rustls_certs, rustls_signing_key)
    };

    Ok(certified_key)
}

pub struct NexusCertResolver {
    log: slog::Logger,
    config_rx: watch::Receiver<Option<SiloDnsCerts>>,
}

impl NexusCertResolver {
    pub fn new(
        log: slog::Logger,
        config_rx: watch::Receiver<Option<SiloDnsCerts>>,
    ) -> NexusCertResolver {
        NexusCertResolver { log, config_rx }
    }

    fn do_resolve(
        &self,
        server_name: Option<&str>,
    ) -> Result<Arc<CertifiedKey>, anyhow::Error> {
        let server_name = match server_name {
            Some(s) => s,
            None => bail!("TLS session had no server name"),
        };

        let config_ref = self.config_rx.borrow();
        let config = match &*config_ref {
            Some(c) => c,
            None => bail!("no TLS config found"),
        };

        let silo_info = match config.by_dns_name.get(server_name) {
            Some(si) => si,
            None => bail!("unrecognized server name: {}", server_name),
        };

        // XXX-dap want a policy here
        let first_cert = match silo_info.tls_certs.iter().next() {
            Some(f) => f,
            None => bail!(
                "matched silo {}, but that has no certificates",
                silo_info.silo_id
            ),
        };

        Ok(first_cert.clone())
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
        match self.do_resolve(server_name) {
            Ok(c) => {
                // XXX-dap would be nice to log silo id and cert fingerprint
                debug!(log, "resolved TLS certificate");
                Some(c)
            }
            Err(error) => {
                // XXX-dap DoS risk here
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
