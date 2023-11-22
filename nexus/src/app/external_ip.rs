// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External IP addresses for instances

use crate::external_api::views::ExternalIp;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::model::IpKind;
use nexus_types::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
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

    pub(crate) async fn list_floating_ips(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
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

    pub(crate) async fn create_floating_ip(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: &params::FloatingIpCreate,
    ) -> CreateResult<ExternalIp> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        let chosen_addr = match (&params.pool, params.address) {
            (Some(_), _) => {
                todo!("Drawing floating IP from pools not yet supported.")
            }
            (None, Some(ip)) => ip,
            _ => {
                return Err(Error::invalid_request(
                    "floating IP needs a pool or ",
                ))
            }
        };

        Ok(self
            .db_datastore
            .allocate_floating_ip(
                opctx,
                authz_project.id(),
                Uuid::new_v4(),
                &params.identity.name.clone().into(),
                &params.identity.description,
                chosen_addr,
            )
            .await?
            .try_into()
            .unwrap())
    }
}
