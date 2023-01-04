// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! x.509 Certificates

use crate::context::OpContext;
use crate::db;
use crate::db::lookup;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::NameOrId;
use openssl::pkey::PKey;
use openssl::x509::X509;
use ref_cast::RefCast;
use std::sync::Arc;
use uuid::Uuid;

fn validate_certs(input: Vec<u8>) -> Result<(), String> {
    let certs = X509::stack_from_pem(&input.as_slice())
        .map_err(|err| format!("Failed to parse certificate as PEM: {err}"))?;
    if certs.is_empty() {
        return Err("could not parse".to_string());
    }
    Ok(())
}

fn validate_private_key(key: Vec<u8>) -> Result<(), String> {
    let _ = PKey::private_key_from_pem(&key.as_slice())
        .map_err(|err| format!("Failed to parse private key as PEM: {err}"))?;

    Ok(())
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
        self: &Arc<Self>,
        opctx: &OpContext,
        params: params::CertificateCreate,
    ) -> CreateResult<db::model::Certificate> {
        validate_certs(params.cert.clone()).map_err(|e| {
            warn!(self.log, "bad cert: {e}");
            Error::InvalidValue { label: String::from("cert"), message: e }
        })?;
        validate_private_key(params.key.clone()).map_err(|e| {
            warn!(self.log, "bad key: {e}");
            Error::InvalidValue { label: String::from("key"), message: e }
        })?;

        let new_certificate = db::model::Certificate::new(
            Uuid::new_v4(),
            db::model::ServiceKind::Nexus,
            params,
        );

        // TODO: Saga?
        info!(self.log, "Creating certificate");
        let cert = self
            .db_datastore
            .certificate_create(opctx, new_certificate)
            .await?;
        // TODO: Refresh other nexus servers?
        self.refresh_tls_config(&opctx).await?;
        info!(self.log, "TLS refreshed successfully");
        Ok(cert)
    }

    pub async fn certificates_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Certificate> {
        self.db_datastore
            .certificate_list_for(
                opctx,
                db::model::ServiceKind::Nexus,
                pagparams,
            )
            .await
    }

    pub async fn certificate_delete(
        self: &Arc<Self>,
        opctx: &OpContext,
        certificate_lookup: lookup::Certificate<'_>,
    ) -> DeleteResult {
        let (.., authz_cert, _db_cert) = certificate_lookup.fetch().await?;
        self.db_datastore.certificate_delete(opctx, &authz_cert).await?;
        // TODO: Refresh other nexus servers?
        self.refresh_tls_config(&opctx).await?;
        Ok(())
    }

    // Helper functions used by Nexus when managing its own server.

    /// Returns the dropshot TLS configuration to run the Nexus external server.
    pub async fn get_nexus_tls_config(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<dropshot::ConfigTls>, Error> {
        // Lookup x509 certificates which might be stored in CRDB, specifically
        // for launching the Nexus service.
        //
        // We only grab one certificate (see: the "limit" argument) because
        // we're currently fine just using whatever certificate happens to be
        // available (as long as it's for Nexus).
        let certs = self
            .datastore()
            .certificate_list_for(
                &opctx,
                db::model::ServiceKind::Nexus,
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(1).unwrap(),
                },
            )
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "Failed to list certificates: {e}"
                ))
            })?;

        let certificate = if let Some(certificate) = certs.get(0) {
            certificate
        } else {
            return Ok(None);
        };

        Ok(Some(dropshot::ConfigTls::AsBytes {
            certs: certificate.cert.clone(),
            key: certificate.key.clone(),
        }))
    }

    /// Refreshes the TLS configuration for the currently-running Nexus external
    /// server.
    pub async fn refresh_tls_config(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        let external_servers_guard = self.external_servers.lock().await;
        let external_servers = if let Some(servers) = &*external_servers_guard {
            servers
        } else {
            // If we aren't running anything, we have nothing to refresh
            info!(self.log, "Refresh TLS: No servers running");
            return Ok(());
        };

        let tls_config = self.get_nexus_tls_config(&opctx).await?;
        let tls_config = if let Some(tls_config) = tls_config {
            tls_config
        } else {
            // TODO: Should we be doing this? We ignore the refresh if no certs
            // exist. We *could* actively remove the TLS cert, but that'll
            // require updating the dropshot API.
            info!(self.log, "Refresh TLS: No certs available");
            return Ok(());
        };
        for server in external_servers {
            if server.using_tls() {
                info!(self.log, "Refresh TLS for {}", server.local_addr());
                server.refresh_tls(&tls_config).await.map_err(|e| {
                    Error::internal_error(&format!("Cannot refresh TLS: {e}"))
                })?;
            } else {
                info!(
                    self.log,
                    "Refresh TLS ignoring {} (not using TLS)",
                    server.local_addr()
                );
            }
        }
        Ok(())
    }
}
