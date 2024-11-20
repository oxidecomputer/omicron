// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Affinity groups

use std::sync::Arc;

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use omicron_common::api::external;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::AffinityGroupUuid;
use omicron_uuid_kinds::AntiAffinityGroupUuid;
use omicron_uuid_kinds::GenericUuid;

impl super::Nexus {
    pub fn affinity_group_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        affinity_group_selector: params::AffinityGroupSelector,
    ) -> LookupResult<lookup::AffinityGroup<'a>> {
        match affinity_group_selector {
            params::AffinityGroupSelector {
                affinity_group: NameOrId::Id(id),
                project: None
            } => {
                let affinity_group =
                    LookupPath::new(opctx, &self.db_datastore).affinity_group_id(id);
                Ok(affinity_group)
            }
            params::AffinityGroupSelector {
                affinity_group: NameOrId::Name(name),
                project: Some(project)
            } => {
                let affinity_group = self
                    .project_lookup(opctx, params::ProjectSelector { project })?
                    .affinity_group_name_owned(name.into());
                Ok(affinity_group)
            }
            params::AffinityGroupSelector {
                affinity_group: NameOrId::Id(_),
                ..
            } => {
                Err(Error::invalid_request(
                    "when providing affinity_group as an ID project should not be specified",
                ))
            }
            _ => {
                Err(Error::invalid_request(
                    "affinity_group should either be UUID or project should be specified",
                ))
            }
        }
    }

    pub fn anti_affinity_group_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        anti_affinity_group_selector: params::AntiAffinityGroupSelector,
    ) -> LookupResult<lookup::AntiAffinityGroup<'a>> {
        match anti_affinity_group_selector {
            params::AntiAffinityGroupSelector {
                anti_affinity_group: NameOrId::Id(id),
                project: None
            } => {
                let anti_affinity_group =
                    LookupPath::new(opctx, &self.db_datastore).anti_affinity_group_id(id);
                Ok(anti_affinity_group)
            }
            params::AntiAffinityGroupSelector {
                anti_affinity_group: NameOrId::Name(name),
                project: Some(project)
            } => {
                let anti_affinity_group = self
                    .project_lookup(opctx, params::ProjectSelector { project })?
                    .anti_affinity_group_name_owned(name.into());
                Ok(anti_affinity_group)
            }
            params::AntiAffinityGroupSelector {
                anti_affinity_group: NameOrId::Id(_),
                ..
            } => {
                Err(Error::invalid_request(
                    "when providing anti_affinity_group as an ID project should not be specified",
                ))
            }
            _ => {
                Err(Error::invalid_request(
                    "anti_affinity_group should either be UUID or project should be specified",
                ))
            }
        }
    }

    pub(crate) async fn affinity_group_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<views::AffinityGroup> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;

        Ok(self
            .db_datastore
            .affinity_group_list(opctx, &authz_project, pagparams)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub(crate) async fn affinity_group_member_list(
        &self,
        opctx: &OpContext,
        affinity_group_lookup: &lookup::AffinityGroup<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<external::AffinityGroupMember> {
        let (.., authz_affinity_group) = affinity_group_lookup
            .lookup_for(authz::Action::ListChildren)
            .await?;
        Ok(self
            .db_datastore
            .affinity_group_member_list(opctx, &authz_affinity_group, pagparams)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}
