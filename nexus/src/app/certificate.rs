// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! x.509 Certificates

use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::model::Name;
use nexus_db_queries::db::model::ServiceKind;
use nexus_types::external_api::certificate;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::http_pagination::PaginatedBy;
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

    /// Look up a certificate in any silo
    ///
    /// The silo must be provided if `certificate` is a name, and must not be
    /// provided if `certificate` is an ID.
    pub fn system_certificate_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        silo: Option<NameOrId>,
        certificate: NameOrId,
    ) -> LookupResult<lookup::Certificate<'a>> {
        match (silo, certificate) {
            (None, NameOrId::Id(id)) => {
                Ok(LookupPath::new(opctx, &self.db_datastore)
                    .certificate_id(id))
            }
            (Some(silo), NameOrId::Name(name)) => Ok(self
                .silo_lookup(opctx, silo)?
                .certificate_name_owned(name.into())),
            (Some(_), NameOrId::Id(_)) => Err(Error::invalid_request(
                "when providing certificate as an ID, silo should not be \
                 specified",
            )),
            (None, NameOrId::Name(_)) => Err(Error::invalid_request(
                "certificate should either be a UUID or silo should be \
                 specified",
            )),
        }
    }

    /// Create a certificate in the current user's silo
    pub(crate) async fn certificate_create(
        &self,
        opctx: &OpContext,
        params: certificate::CertificateCreate,
    ) -> CreateResult<db::model::Certificate> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("creating a Certificate")?;
        self.certificate_create_for_silo(opctx, &authz_silo, params).await
    }

    /// Create a certificate in the specified silo
    pub(crate) async fn system_certificate_create(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        params: certificate::CertificateCreate,
    ) -> CreateResult<db::model::Certificate> {
        let (authz_silo,) = silo_lookup.lookup_for(authz::Action::Read).await?;
        self.certificate_create_for_silo(opctx, &authz_silo, params).await
    }

    async fn certificate_create_for_silo(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        params: certificate::CertificateCreate,
    ) -> CreateResult<db::model::Certificate> {
        // Check this up front so that we don't use the elevated context below
        // on behalf of a caller who can't create certificates in this silo.
        let authz_cert_list =
            authz::SiloCertificateList::new(authz_silo.clone());
        opctx.authorize(authz::Action::CreateChild, &authz_cert_list).await?;

        // The `opctx` we received is going to be checked for permission to
        // create a cert below in `db_datastore.certificate_create`, but first
        // we need to look up this silo's fully-qualified domain names in order
        // to check that the cert we've been given is valid for this silo.
        // Looking up DNS names requires reading the DNS configuration of the
        // _rack_, which this user may not be able to do (even if they have
        // permission to upload new certs, which almost certainly implies a
        // silo-level admin). We'll use our `opctx_external_authn()` context,
        // which is the same context used to create a silo. This is a higher
        // privilege than the current user may have, but we believe it does not
        // leak any information that a silo admin doesn't already know (the
        // external DNS name(s) of the rack, which leads to their silo's DNS
        // name(s)).
        let silo_fq_dns_names = self
            .silo_fq_dns_names(self.opctx_external_authn(), authz_silo.id())
            .await?;

        let kind = params.service;
        let new_certificate = db::model::Certificate::new(
            authz_silo.id(),
            Uuid::new_v4(),
            kind.into(),
            params,
            &silo_fq_dns_names,
        )?;
        let cert = self
            .db_datastore
            .certificate_create(opctx, authz_silo, new_certificate)
            .await?;

        match kind {
            certificate::ServiceUsingCertificate::ExternalApi => {
                // TODO We could improve the latency of other Nexus instances
                // noticing this certificate change with an explicit request to
                // them.  Today, Nexus instances generally don't talk to each
                // other.  That's a very valuable simplifying assumption.
                self.background_tasks
                    .activate(&self.background_tasks.task_external_endpoints);
                Ok(cert)
            }
        }
    }

    pub(crate) async fn certificates_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Certificate> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("listing Certificates")?;
        self.db_datastore
            .certificate_list_for(opctx, None, pagparams, Some(&authz_silo))
            .await
    }

    pub(crate) async fn system_certificates_list(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Certificate> {
        let (authz_silo,) = silo_lookup.lookup_for(authz::Action::Read).await?;
        self.db_datastore
            .certificate_list_for(opctx, None, pagparams, Some(&authz_silo))
            .await
    }

    pub(crate) async fn certificate_delete(
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
                    .activate(&self.background_tasks.task_external_endpoints);
            }
            _ => (),
        };
        Ok(())
    }
}
