// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Affinity Groups

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::AffinityGroup;
use crate::db::model::AffinityGroupInstanceMembership;
use crate::db::model::AffinityPolicy;
use crate::db::model::AntiAffinityGroup;
use crate::db::model::AntiAffinityGroupInstanceMembership;
use crate::db::model::FailureDomain;
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use crate::db::TransactionError;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_types::external_api::params;
use omicron_common::api::external;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::AffinityGroupUuid;
use omicron_uuid_kinds::AntiAffinityGroupUuid;
use omicron_uuid_kinds::GenericUuid;

impl DataStore {
    pub async fn affinity_group_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<AffinityGroup> {
        todo!();
    }

    pub async fn anti_affinity_group_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<AntiAffinityGroup> {
        todo!();
    }

    pub async fn affinity_group_create(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        group: params::AffinityGroupCreate,
    ) -> CreateResult<AffinityGroup> {
        todo!();
    }

    pub async fn anti_affinity_group_create(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        group: params::AntiAffinityGroupCreate,
    ) -> CreateResult<AntiAffinityGroup> {
        todo!();
    }

    pub async fn affinity_group_delete(
        &self,
        opctx: &OpContext,
        authz_affinity_group: &authz::AffinityGroup,
    ) -> DeleteResult {
        todo!();
    }

    pub async fn anti_affinity_group_delete(
        &self,
        opctx: &OpContext,
        authz_anti_affinity_group: &authz::AntiAffinityGroup,
    ) -> DeleteResult {
        todo!();
    }

    pub async fn affinity_group_member_list(
        &self,
        opctx: &OpContext,
        authz_affinity_group: &authz::AffinityGroup,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<AffinityGroupInstanceMembership> {
        todo!();
    }

    pub async fn anti_affinity_group_member_list(
        &self,
        opctx: &OpContext,
        authz_anti_affinity_group: &authz::AntiAffinityGroup,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<AntiAffinityGroupInstanceMembership> {
        todo!();
    }

    pub async fn affinity_group_member_add(
        &self,
        opctx: &OpContext,
        authz_affinity_group: &authz::AffinityGroup,
        member: external::AffinityGroupMember,
    ) -> Result<(), Error> {
        todo!();
    }

    pub async fn anti_affinity_group_member_add(
        &self,
        opctx: &OpContext,
        authz_anti_affinity_group: &authz::AntiAffinityGroup,
        member: external::AntiAffinityGroupMember,
    ) -> Result<(), Error> {
        todo!();
    }

    pub async fn affinity_group_member_delete(
        &self,
        opctx: &OpContext,
        authz_affinity_group: &authz::AffinityGroup,
        member: external::AffinityGroupMember,
    ) -> Result<(), Error> {
        todo!();
    }

    pub async fn anti_affinity_group_member_delete(
        &self,
        opctx: &OpContext,
        authz_anti_affinity_group: &authz::AntiAffinityGroup,
        member: external::AntiAffinityGroupMember,
    ) -> Result<(), Error> {
        todo!();
    }
}
