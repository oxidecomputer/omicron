// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External IP addresses for instances

use std::sync::Arc;

use crate::external_api::views::ExternalIp;
use crate::external_api::views::FloatingIp;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_model::IpAttachState;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;

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
            .instance_lookup_external_ips(
                opctx,
                InstanceUuid::from_untyped_uuid(authz_instance.id()),
            )
            .await?
            .into_iter()
            .filter_map(|ip| {
                if ip.state != IpAttachState::Attached {
                    None
                } else {
                    Some(ip.try_into().unwrap())
                }
            })
            .collect::<Vec<_>>())
    }

    pub(crate) fn floating_ip_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        fip_selector: params::FloatingIpSelector,
    ) -> LookupResult<lookup::FloatingIp<'a>> {
        match fip_selector {
            params::FloatingIpSelector {
                floating_ip: NameOrId::Id(id),
                project: None,
            } => {
                let floating_ip = LookupPath::new(opctx, &self.db_datastore)
                    .floating_ip_id(id);
                Ok(floating_ip)
            }
            params::FloatingIpSelector {
                floating_ip: NameOrId::Name(name),
                project: Some(project),
            } => {
                let floating_ip = self
                    .project_lookup(opctx, params::ProjectSelector { project })?
                    .floating_ip_name_owned(name.into());
                Ok(floating_ip)
            }
            params::FloatingIpSelector {
                floating_ip: NameOrId::Id(_), ..
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
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<FloatingIp> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;

        Ok(self
            .db_datastore
            .floating_ips_list(opctx, &authz_project, pagparams)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub(crate) async fn floating_ip_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: params::FloatingIpCreate,
    ) -> CreateResult<FloatingIp> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        let params::FloatingIpCreate { identity, pool, ip } = params;

        // resolve NameOrId into authz::IpPool
        let pool = match pool {
            Some(pool) => Some(
                self.ip_pool_lookup(opctx, &pool)?
                    .lookup_for(authz::Action::CreateChild)
                    .await?
                    .0,
            ),
            None => None,
        };

        Ok(self
            .db_datastore
            .allocate_floating_ip(opctx, authz_project.id(), identity, ip, pool)
            .await?
            .try_into()
            .unwrap())
    }

    pub(crate) async fn floating_ip_update(
        &self,
        opctx: &OpContext,
        ip_lookup: lookup::FloatingIp<'_>,
        params: params::FloatingIpUpdate,
    ) -> UpdateResult<FloatingIp> {
        let (.., authz_fip) =
            ip_lookup.lookup_for(authz::Action::Modify).await?;
        Ok(self
            .db_datastore
            .floating_ip_update(opctx, &authz_fip, params.clone().into())
            .await?
            .try_into()
            .unwrap())
    }

    pub(crate) async fn floating_ip_delete(
        &self,
        opctx: &OpContext,
        ip_lookup: lookup::FloatingIp<'_>,
    ) -> DeleteResult {
        let (.., authz_fip) =
            ip_lookup.lookup_for(authz::Action::Delete).await?;

        self.db_datastore.floating_ip_delete(opctx, &authz_fip).await
    }

    pub(crate) async fn floating_ip_attach(
        self: &Arc<Self>,
        opctx: &OpContext,
        fip_selector: params::FloatingIpSelector,
        target: params::FloatingIpAttach,
    ) -> UpdateResult<views::FloatingIp> {
        let fip_lookup = self.floating_ip_lookup(opctx, fip_selector)?;
        let (.., authz_project, authz_fip) =
            fip_lookup.lookup_for(authz::Action::Modify).await?;

        match target.kind {
            params::FloatingIpParentKind::Instance => {
                // Handle the cases where the FIP and instance are specified by
                // name and ID (or ID and name) respectively. We remove the project
                // from the instance lookup if using the instance's ID, and insert
                // the floating IP's project ID otherwise.
                let instance_selector = params::InstanceSelector {
                    project: match &target.parent {
                        NameOrId::Id(_) => None,
                        NameOrId::Name(_) => Some(authz_project.id().into()),
                    },
                    instance: target.parent,
                };

                let instance =
                    self.instance_lookup(opctx, instance_selector)?;

                self.instance_attach_floating_ip(
                    opctx,
                    &instance,
                    authz_fip,
                    authz_project,
                )
                .await
                .and_then(FloatingIp::try_from)
            }
        }
    }

    pub(crate) async fn floating_ip_detach(
        self: &Arc<Self>,
        opctx: &OpContext,
        ip_lookup: lookup::FloatingIp<'_>,
    ) -> UpdateResult<views::FloatingIp> {
        // XXX: Today, this only happens for instances.
        //      In future, we will need to separate out by the *type* of
        //      parent attached to a floating IP. We don't yet store this
        //      in db for user-facing FIPs (is_service => internal-only
        //      at this point).
        let (.., authz_fip, db_fip) =
            ip_lookup.fetch_for(authz::Action::Modify).await?;

        let Some(parent_id) = db_fip.parent_id else {
            return Ok(db_fip.into());
        };

        let instance_selector = params::InstanceSelector {
            project: None,
            instance: parent_id.into(),
        };
        let instance = self.instance_lookup(opctx, instance_selector)?;
        let attach_params = &params::ExternalIpDetach::Floating {
            floating_ip: authz_fip.id().into(),
        };

        self.instance_detach_external_ip(opctx, &instance, attach_params)
            .await
            .and_then(FloatingIp::try_from)
    }
}
