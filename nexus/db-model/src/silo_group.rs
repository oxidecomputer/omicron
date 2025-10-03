// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::UserProvisionType;
use crate::DbTypedUuid;
use crate::to_db_typed_uuid;
use db_macros::Asset;
use nexus_db_schema::schema::{silo_group, silo_group_membership};
use omicron_uuid_kinds::SiloGroupKind;
use omicron_uuid_kinds::SiloGroupUuid;
use omicron_uuid_kinds::SiloUserKind;
use omicron_uuid_kinds::SiloUserUuid;
use uuid::Uuid;

/// Describes a silo group within the database.
#[derive(Asset, Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_group)]
#[asset(uuid_kind = SiloGroupKind)]
pub struct SiloGroup {
    #[diesel(embed)]
    pub identity: SiloGroupIdentity,
    pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,

    pub silo_id: Uuid,

    /// If the user provision type is ApiOnly or JIT, then the external id is
    /// the identity provider's ID for this group. There is a database
    /// constraint (`lookup_silo_group_by_silo`) that ensures this field must be
    /// non-null for those provision types.
    ///
    /// For SCIM, this may be null, which would trigger the uniqueness
    /// constraint if that wasn't limited to specific provision types.
    pub external_id: Option<String>,

    pub user_provision_type: UserProvisionType,
}

impl SiloGroup {
    pub fn new_api_only_group(
        id: SiloGroupUuid,
        silo_id: Uuid,
        external_id: String,
    ) -> Self {
        Self {
            identity: SiloGroupIdentity::new(id),
            time_deleted: None,
            silo_id,
            user_provision_type: UserProvisionType::ApiOnly,
            external_id: Some(external_id),
        }
    }

    pub fn new_jit_group(
        id: SiloGroupUuid,
        silo_id: Uuid,
        external_id: String,
    ) -> Self {
        Self {
            identity: SiloGroupIdentity::new(id),
            time_deleted: None,
            silo_id,
            user_provision_type: UserProvisionType::Jit,
            external_id: Some(external_id),
        }
    }
}

/// Describe which silo users belong to which silo groups
#[derive(Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_group_membership)]
pub struct SiloGroupMembership {
    pub silo_group_id: DbTypedUuid<SiloGroupKind>,
    pub silo_user_id: DbTypedUuid<SiloUserKind>,
}

impl SiloGroupMembership {
    pub fn new(
        silo_group_id: SiloGroupUuid,
        silo_user_id: SiloUserUuid,
    ) -> Self {
        Self {
            silo_group_id: to_db_typed_uuid(silo_group_id),
            silo_user_id: to_db_typed_uuid(silo_user_id),
        }
    }
}
