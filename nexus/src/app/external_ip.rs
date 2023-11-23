// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External IP addresses for instances

use crate::external_api::views::ExternalIp;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::IpKind;
use nexus_types::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use uuid::Uuid;

impl super::Nexus {
    pub(crate) async fn instance_list_external_ips(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> ListResultVec<ExternalIp> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Read).await?;
        Ok(self
            .db_datastore
            .instance_lookup_external_ips(opctx, authz_instance.id())
            .await?
            .into_iter()
            .filter_map(|ip| {
                if ip.kind == IpKind::SNat {
                    None
                } else {
                    Some(ip.try_into().unwrap())
                }
            })
            .collect::<Vec<_>>())
    }

    pub fn floating_ip_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        fip_selector: params::FloatingIpSelector,
    ) -> LookupResult<lookup::Disk<'a>> {
        match fip_selector {
            params::FloatingIpSelector { floating_ip: NameOrId::Id(id), project: None } => {
                // let floating_ip =
                //     LookupPath::new(opctx, &self.db_datastore).floating_ip_id(id);
                // Ok(floating_ip)
                todo!()
            }
            params::FloatingIpSelector {
                floating_ip: NameOrId::Name(name),
                project: Some(project),
            } => {
                // let floating_ip = self
                //     .project_lookup(opctx, params::ProjectSelector { project })?
                //     .floating_ip_name_owned(name.into());
                // Ok(floating_ip)
                todo!()
            }
            params::FloatingIpSelector {
                floating_ip: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing Floating IP as an ID project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "Floating IP should either be UUID or project should be specified",
            )),
        }
    }

    pub(crate) async fn floating_ips_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        // pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<ExternalIp> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::Read).await?;
        Ok(self
            .db_datastore
            .lookup_floating_ips(opctx, authz_project.id())
            .await?
            .into_iter()
            .map(|ip| ip.try_into().unwrap())
            .collect::<Vec<_>>())
    }

    pub(crate) async fn floating_ip_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: &params::FloatingIpCreate,
    ) -> CreateResult<ExternalIp> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        // XXX: support pool by name here.
        let pool_id = match &params.pool {
            Some(NameOrId::Id(ref id)) => Some(*id),
            Some(NameOrId::Name(ref _name)) => {
                return Err(Error::internal_error(
                    "pool ref by name not yet supported",
                ))
            }
            None => None,
        };

        Ok(self
            .db_datastore
            .allocate_floating_ip(
                opctx,
                pool_id,
                authz_project.id(),
                Uuid::new_v4(),
                &params.identity.name.clone().into(),
                &params.identity.description,
                params.address,
            )
            .await?
            .try_into()
            .unwrap())
    }

    pub(crate) async fn floating_ip_delete(
        &self,
        opctx: &OpContext,
        // pool_lookup: &lookup::<'_>,
    ) -> DeleteResult {
        // let (.., authz_pool, db_pool) =
        //     pool_lookup.fetch_for(authz::Action::Delete).await?;
        // self.db_datastore.ip_pool_delete(opctx, &authz_pool, &db_pool).await
        todo!()
    }
}
