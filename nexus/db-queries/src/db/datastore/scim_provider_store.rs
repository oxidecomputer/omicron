// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! scim2-rs uses the pattern of implementing a SCIM "provider" over something
//! that implements a "provider store" trait that durably stores the SCIM
//! related information. Nexus uses cockroachdb as the provider store.

use super::DataStore;
use anyhow::anyhow;
use std::sync::Arc;
use uuid::Uuid;

use scim2_rs::CreateGroupRequest;
use scim2_rs::CreateUserRequest;
use scim2_rs::FilterOp;
use scim2_rs::Group;
use scim2_rs::ProviderStore;
use scim2_rs::ProviderStoreDeleteResult;
use scim2_rs::ProviderStoreError;
use scim2_rs::StoredParts;
use scim2_rs::User;

// XXX temporary until SCIM impl PR
#[allow(dead_code)]
pub struct CrdbScimProviderStore {
    silo_id: Uuid,
    datastore: Arc<DataStore>,
}

impl CrdbScimProviderStore {
    pub fn new(silo_id: Uuid, datastore: Arc<DataStore>) -> Self {
        CrdbScimProviderStore { silo_id, datastore }
    }
}

#[async_trait::async_trait]
impl ProviderStore for CrdbScimProviderStore {
    async fn get_user_by_id(
        &self,
        _user_id: &str,
    ) -> Result<Option<StoredParts<User>>, ProviderStoreError> {
        return Err(ProviderStoreError::StoreError(anyhow!(
            "not implemented!"
        )));
    }

    async fn create_user(
        &self,
        _user_request: CreateUserRequest,
    ) -> Result<StoredParts<User>, ProviderStoreError> {
        return Err(ProviderStoreError::StoreError(anyhow!(
            "not implemented!"
        )));
    }

    async fn list_users(
        &self,
        _filter: Option<FilterOp>,
    ) -> Result<Vec<StoredParts<User>>, ProviderStoreError> {
        return Err(ProviderStoreError::StoreError(anyhow!(
            "not implemented!"
        )));
    }

    async fn replace_user(
        &self,
        _user_id: &str,
        _user_request: CreateUserRequest,
    ) -> Result<StoredParts<User>, ProviderStoreError> {
        return Err(ProviderStoreError::StoreError(anyhow!(
            "not implemented!"
        )));
    }

    async fn delete_user_by_id(
        &self,
        _user_id: &str,
    ) -> Result<ProviderStoreDeleteResult, ProviderStoreError> {
        return Err(ProviderStoreError::StoreError(anyhow!(
            "not implemented!"
        )));
    }

    async fn get_group_by_id(
        &self,
        _group_id: &str,
    ) -> Result<Option<StoredParts<Group>>, ProviderStoreError> {
        return Err(ProviderStoreError::StoreError(anyhow!(
            "not implemented!"
        )));
    }

    async fn create_group(
        &self,
        _group_request: CreateGroupRequest,
    ) -> Result<StoredParts<Group>, ProviderStoreError> {
        return Err(ProviderStoreError::StoreError(anyhow!(
            "not implemented!"
        )));
    }

    async fn list_groups(
        &self,
        _filter: Option<FilterOp>,
    ) -> Result<Vec<StoredParts<Group>>, ProviderStoreError> {
        return Err(ProviderStoreError::StoreError(anyhow!(
            "not implemented!"
        )));
    }

    async fn replace_group(
        &self,
        _group_id: &str,
        _group_request: CreateGroupRequest,
    ) -> Result<StoredParts<Group>, ProviderStoreError> {
        return Err(ProviderStoreError::StoreError(anyhow!(
            "not implemented!"
        )));
    }

    async fn delete_group_by_id(
        &self,
        _group_id: &str,
    ) -> Result<ProviderStoreDeleteResult, ProviderStoreError> {
        return Err(ProviderStoreError::StoreError(anyhow!(
            "not implemented!"
        )));
    }
}
