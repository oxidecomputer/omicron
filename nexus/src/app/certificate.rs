// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! x.509 Certificates

use crate::context::OpContext;
use crate::db;
use crate::db::lookup;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::db::model::ServiceKind;
use crate::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::NameOrId;
use ref_cast::RefCast;
use uuid::Uuid;

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
        let kind = params.service;
        let new_certificate =
            db::model::Certificate::new(Uuid::new_v4(), kind.into(), params)?;
        info!(self.log, "Creating certificate");
        let cert = self
            .db_datastore
            .certificate_create(opctx, new_certificate)
            .await?;

        match kind {
            params::ServiceUsingCertificate::Nexus => {
                // TODO: If we make this operation "add a certificate, and try to update
                // nearby Nexus servers to use it", that means it'll be combining a DB
                // operation with a service update request. If we want both to reliably
                // complete together, we should consider making this a saga.
                // TODO: Refresh other nexus servers?
                self.refresh_tls_config(&opctx).await?;
                info!(self.log, "TLS refreshed successfully");
                Ok(cert)
            }
        }
    }

    pub async fn certificates_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Certificate> {
        self.db_datastore.certificate_list_for(opctx, None, pagparams).await
    }

    pub async fn certificate_delete(
        &self,
        opctx: &OpContext,
        certificate_lookup: lookup::Certificate<'_>,
    ) -> DeleteResult {
        let (.., authz_cert, db_cert) = certificate_lookup.fetch().await?;
        self.db_datastore.certificate_delete(opctx, &authz_cert).await?;
        match db_cert.service {
            ServiceKind::Nexus => {
                // TODO: Refresh other nexus servers?
                self.refresh_tls_config(&opctx).await?;
            }
            _ => (),
        };
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
                Some(db::model::ServiceKind::Nexus),
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
    /// server. This involves either:
    /// - Creating a new HTTPS server if one does not exist
    /// - Refreshing an existing HTTPS server if it already exists
    /// - Tearing down an HTTPS server if no certificates exist
    pub async fn refresh_tls_config(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        let tls_config = self.get_nexus_tls_config(&opctx).await?;

        let mut external_servers = self.external_servers.lock().await;

        match (tls_config, external_servers.https.take()) {
            // Create a new server, using server context from an existing HTTP
            // server.
            (Some(tls_config), None) => {
                info!(self.log, "Refresh TLS: Creating HTTPS server");
                let mut cfg = external_servers.config.clone();
                cfg.bind_address.set_port(external_servers.https_port());
                cfg.tls = Some(tls_config);

                let context =
                    external_servers.get_context().ok_or_else(|| {
                        Error::internal_error("No server context available")
                    })?;

                let log =
                    context.log.new(o!("component" => "dropshot_external"));
                let server_starter_external = dropshot::HttpServerStarter::new(
                    &cfg,
                    crate::external_api::http_entrypoints::external_api(),
                    context,
                    &log,
                )
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "Initializing HTTPS server: {e}"
                    ))
                })?;
                external_servers.set_https(server_starter_external.start());
            }
            // Refresh an existing server.
            (Some(tls_config), Some(https)) => {
                info!(
                    self.log,
                    "Refresh TLS: Refreshing HTTPS server at {}",
                    https.local_addr()
                );
                https.refresh_tls(&tls_config).await.map_err(|e| {
                    Error::internal_error(&format!("Cannot refresh TLS: {e}"))
                })?;
                external_servers.set_https(https);
            }
            // Tear down an existing server.
            (None, Some(https)) => {
                info!(
                    self.log,
                    "Refresh TLS: Stopping HTTPS server at {}",
                    https.local_addr()
                );
                https.close().await.map_err(|e| {
                    Error::internal_error(&format!(
                        "Failed to stop server: {e}"
                    ))
                })?;
            }
            // No config, no server.
            (None, None) => (),
        }

        Ok(())
    }
}
