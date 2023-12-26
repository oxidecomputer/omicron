// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! x.509 Certificates

use crate::external_api::params;
use crate::external_api::shared;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::Name;
use nexus_db_queries::db::model::ServiceKind;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::InternalContext;
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

    pub(crate) async fn certificate_create(
        &self,
        opctx: &OpContext,
        params: params::CertificateCreate,
    ) -> CreateResult<db::model::Certificate> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("creating a Certificate")?;

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
            .certificate_create(opctx, new_certificate)
            .await?;

        match kind {
            shared::ServiceUsingCertificate::ExternalApi => {
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
        self.db_datastore
            .certificate_list_for(opctx, None, pagparams, true)
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
