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
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::NameOrId;
use ref_cast::RefCast;
use std::sync::Arc;
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
        self: &Arc<Self>,
        opctx: &OpContext,
        params: params::CertificateCreate,
    ) -> CreateResult<db::model::Certificate> {
        let new_certificate = db::model::Certificate::new(
            Uuid::new_v4(),
            db::model::ServiceKind::Nexus,
            params,
        );

        self.db_datastore.certificate_create(opctx, new_certificate).await
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
        self.db_datastore.certificate_delete(opctx, &authz_cert).await
    }
}
