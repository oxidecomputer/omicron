// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the dependency from the auth system on the datastore.
//!
//! Auth and storage are intertwined, but by isolating the interface from
//! auth on the database, we can avoid a circular dependency.

use crate::context::OpContext;
use nexus_db_model::IdentityType;
use nexus_db_model::RoleAssignment;
use omicron_common::api::external::Error;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    async fn role_asgn_list_for(
        &self,
        opctx: &OpContext,
        identity_type: IdentityType,
        identity_id: Uuid,
        resource_type: ResourceType,
        resource_id: Uuid,
    ) -> Result<Vec<RoleAssignment>, Error>;
}
