// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External IP addresses for instances

use std::sync::Arc;

use crate::external_api::views::ExternalIp;
use crate::external_api::views::FloatingIp;
use nexus_db_model::IpAttachState;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::IpKind;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;

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
                if ip.kind == IpKind::SNat
                    || ip.state != IpAttachState::Attached
                {
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
            params::FloatingIpSelector { floating_ip: NameOrId::Id(id), project: None } => {
                let floating_ip =
                    LookupPath::new(opctx, &self.db_datastore).floating_ip_id(id);
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

        let pool = match &params.pool {
            Some(pool) => Some(
                self.ip_pool_lookup(opctx, pool)?
                    .lookup_for(authz::Action::Read)
                    .await?
                    .0,
            ),
            None => None,
        };

        Ok(self
            .db_datastore
            .allocate_floating_ip(opctx, authz_project.id(), params, pool)
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
        match target.kind {
            params::FloatingIpParentKind::Instance => {
                // This is surprisingly complicated in order to handle the
                // case where floating IP is specified by name (and therefore
                // a project is given) but instance is specified by ID (and
                // therefore the lookup doesn't want a project), as well as
                // the converse: floating IP specified by ID (and no project
                // given) but instance specified by name, and therefore needs
                // a project. In the latter case, we have to fetch the floating
                // IP by its ID in order to get the project to include with
                // the instance.
                let project = match target.parent {
                    NameOrId::Id(_) => None,
                    NameOrId::Name(_) => match fip_selector.project {
                        Some(p) => Some(p),
                        None => {
                            let fip_lookup = self.floating_ip_lookup(
                                opctx,
                                fip_selector.clone(),
                            )?;
                            let (.., fip) = fip_lookup.fetch().await?;
                            Some(fip.project_id.into())
                        }
                    },
                };

                let instance_selector = params::InstanceSelector {
                    project,
                    instance: target.parent,
                };

                let instance =
                    self.instance_lookup(opctx, instance_selector)?;
                let attach_params = &params::ExternalIpCreate::Floating {
                    floating_ip: fip_selector.floating_ip,
                };
                self.instance_attach_external_ip(
                    opctx,
                    &instance,
                    attach_params,
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
