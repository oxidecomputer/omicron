// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Affinity groups

use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_model::AffinityGroup;
use nexus_db_model::AntiAffinityGroup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external;
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
    pub fn affinity_group_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        affinity_group_selector: params::AffinityGroupSelector,
    ) -> LookupResult<lookup::AffinityGroup<'a>> {
        match affinity_group_selector {
            params::AffinityGroupSelector {
                affinity_group: NameOrId::Id(id),
                project: None,
            } => {
                let affinity_group = LookupPath::new(opctx, &self.db_datastore)
                    .affinity_group_id(id);
                Ok(affinity_group)
            }
            params::AffinityGroupSelector {
                affinity_group: NameOrId::Name(name),
                project: Some(project),
            } => {
                let affinity_group = self
                    .project_lookup(opctx, params::ProjectSelector { project })?
                    .affinity_group_name_owned(name.into());
                Ok(affinity_group)
            }
            params::AffinityGroupSelector {
                affinity_group: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing affinity_group as an ID, project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "affinity_group should either be UUID or project should be specified",
            )),
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
                project: None,
            } => {
                let anti_affinity_group =
                    LookupPath::new(opctx, &self.db_datastore)
                        .anti_affinity_group_id(id);
                Ok(anti_affinity_group)
            }
            params::AntiAffinityGroupSelector {
                anti_affinity_group: NameOrId::Name(name),
                project: Some(project),
            } => {
                let anti_affinity_group = self
                    .project_lookup(opctx, params::ProjectSelector { project })?
                    .anti_affinity_group_name_owned(name.into());
                Ok(anti_affinity_group)
            }
            params::AntiAffinityGroupSelector {
                anti_affinity_group: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing anti_affinity_group as an ID, project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "anti_affinity_group should either be UUID or project should be specified",
            )),
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

    pub(crate) async fn anti_affinity_group_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<views::AntiAffinityGroup> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;

        Ok(self
            .db_datastore
            .anti_affinity_group_list(opctx, &authz_project, pagparams)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub(crate) async fn affinity_group_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        affinity_group_params: params::AffinityGroupCreate,
    ) -> CreateResult<views::AffinityGroup> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        let affinity_group =
            AffinityGroup::new(authz_project.id(), affinity_group_params);
        self.db_datastore
            .affinity_group_create(opctx, &authz_project, affinity_group)
            .await
            .map(Into::into)
    }

    pub(crate) async fn anti_affinity_group_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        anti_affinity_group_params: params::AntiAffinityGroupCreate,
    ) -> CreateResult<views::AntiAffinityGroup> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        let anti_affinity_group = AntiAffinityGroup::new(
            authz_project.id(),
            anti_affinity_group_params,
        );
        self.db_datastore
            .anti_affinity_group_create(
                opctx,
                &authz_project,
                anti_affinity_group,
            )
            .await
            .map(Into::into)
    }

    pub(crate) async fn affinity_group_update(
        &self,
        opctx: &OpContext,
        group_lookup: &lookup::AffinityGroup<'_>,
        updates: &params::AffinityGroupUpdate,
    ) -> UpdateResult<views::AffinityGroup> {
        let (.., authz_group) =
            group_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .affinity_group_update(opctx, &authz_group, updates.clone().into())
            .await
            .map(Into::into)
    }

    pub(crate) async fn anti_affinity_group_update(
        &self,
        opctx: &OpContext,
        group_lookup: &lookup::AntiAffinityGroup<'_>,
        updates: &params::AntiAffinityGroupUpdate,
    ) -> UpdateResult<views::AntiAffinityGroup> {
        let (.., authz_group) =
            group_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .anti_affinity_group_update(
                opctx,
                &authz_group,
                updates.clone().into(),
            )
            .await
            .map(Into::into)
    }

    pub(crate) async fn affinity_group_delete(
        &self,
        opctx: &OpContext,
        group_lookup: &lookup::AffinityGroup<'_>,
    ) -> DeleteResult {
        let (.., authz_group) =
            group_lookup.lookup_for(authz::Action::Delete).await?;
        self.db_datastore.affinity_group_delete(opctx, &authz_group).await
    }

    pub(crate) async fn anti_affinity_group_delete(
        &self,
        opctx: &OpContext,
        group_lookup: &lookup::AntiAffinityGroup<'_>,
    ) -> DeleteResult {
        let (.., authz_group) =
            group_lookup.lookup_for(authz::Action::Delete).await?;
        self.db_datastore.anti_affinity_group_delete(opctx, &authz_group).await
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
        self.db_datastore
            .affinity_group_member_list(opctx, &authz_affinity_group, pagparams)
            .await
    }

    pub(crate) async fn anti_affinity_group_member_list(
        &self,
        opctx: &OpContext,
        anti_affinity_group_lookup: &lookup::AntiAffinityGroup<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<external::AntiAffinityGroupMember> {
        let (.., authz_anti_affinity_group) = anti_affinity_group_lookup
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .anti_affinity_group_member_list(
                opctx,
                &authz_anti_affinity_group,
                pagparams,
            )
            .await
    }

    pub(crate) async fn affinity_group_member_view(
        &self,
        opctx: &OpContext,
        affinity_group_lookup: &lookup::AffinityGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> Result<external::AffinityGroupMember, Error> {
        let (.., authz_affinity_group) =
            affinity_group_lookup.lookup_for(authz::Action::Read).await?;
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Read).await?;
        let member = InstanceUuid::from_untyped_uuid(authz_instance.id());

        self.db_datastore
            .affinity_group_member_instance_view(
                opctx,
                &authz_affinity_group,
                member,
            )
            .await
    }

    pub(crate) async fn anti_affinity_group_member_instance_view(
        &self,
        opctx: &OpContext,
        anti_affinity_group_lookup: &lookup::AntiAffinityGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> Result<external::AntiAffinityGroupMember, Error> {
        let (.., authz_anti_affinity_group) =
            anti_affinity_group_lookup.lookup_for(authz::Action::Read).await?;
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Read).await?;
        let member = InstanceUuid::from_untyped_uuid(authz_instance.id());

        self.db_datastore
            .anti_affinity_group_member_instance_view(
                opctx,
                &authz_anti_affinity_group,
                member,
            )
            .await
    }

    pub(crate) async fn affinity_group_member_add(
        &self,
        opctx: &OpContext,
        affinity_group_lookup: &lookup::AffinityGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> Result<external::AffinityGroupMember, Error> {
        let (.., authz_affinity_group) =
            affinity_group_lookup.lookup_for(authz::Action::Modify).await?;
        let (.., authz_instance, instance) =
            instance_lookup.fetch_for(authz::Action::Read).await?;
        let member = InstanceUuid::from_untyped_uuid(authz_instance.id());

        self.db_datastore
            .affinity_group_member_instance_add(
                opctx,
                &authz_affinity_group,
                member,
            )
            .await?;
        Ok(external::AffinityGroupMember::Instance {
            id: authz_instance.id(),
            name: instance.name().clone(),
            // TODO: This is kinda a lie - the current implementation of
            // "affinity_group_member_instance_add" relies on the instance
            // not having a VMM, but that might change in the future.
            run_state: external::InstanceState::Stopped,
        })
    }

    pub(crate) async fn anti_affinity_group_member_instance_add(
        &self,
        opctx: &OpContext,
        anti_affinity_group_lookup: &lookup::AntiAffinityGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> Result<external::AntiAffinityGroupMember, Error> {
        let (.., authz_anti_affinity_group) = anti_affinity_group_lookup
            .lookup_for(authz::Action::Modify)
            .await?;
        let (.., authz_instance, instance) =
            instance_lookup.fetch_for(authz::Action::Read).await?;
        let member = InstanceUuid::from_untyped_uuid(authz_instance.id());

        self.db_datastore
            .anti_affinity_group_member_instance_add(
                opctx,
                &authz_anti_affinity_group,
                member,
            )
            .await?;
        Ok(external::AntiAffinityGroupMember::Instance {
            id: authz_instance.id(),
            name: instance.name().clone(),
            // TODO: This is kinda a lie - the current implementation of
            // "anti_affinity_group_member_instance_add" relies on the instance
            // not having a VMM, but that might change in the future.
            run_state: external::InstanceState::Stopped,
        })
    }

    pub(crate) async fn affinity_group_member_delete(
        &self,
        opctx: &OpContext,
        affinity_group_lookup: &lookup::AffinityGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> Result<(), Error> {
        let (.., authz_affinity_group) =
            affinity_group_lookup.lookup_for(authz::Action::Modify).await?;
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Read).await?;
        let member = InstanceUuid::from_untyped_uuid(authz_instance.id());

        self.db_datastore
            .affinity_group_member_instance_delete(
                opctx,
                &authz_affinity_group,
                member,
            )
            .await
    }

    pub(crate) async fn anti_affinity_group_member_instance_delete(
        &self,
        opctx: &OpContext,
        anti_affinity_group_lookup: &lookup::AntiAffinityGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> Result<(), Error> {
        let (.., authz_anti_affinity_group) = anti_affinity_group_lookup
            .lookup_for(authz::Action::Modify)
            .await?;
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Read).await?;
        let member = InstanceUuid::from_untyped_uuid(authz_instance.id());

        self.db_datastore
            .anti_affinity_group_member_instance_delete(
                opctx,
                &authz_anti_affinity_group,
                member,
            )
            .await
    }
}
