// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::UserProvisionType;
use db_macros::Asset;
use nexus_db_schema::schema::silo_user;
use omicron_uuid_kinds::SiloUserUuid;
use uuid::Uuid;

/// Describes a silo user within the database.
#[derive(Asset, Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_user)]
#[asset(uuid_kind = SiloUserKind)]
pub struct SiloUser {
    #[diesel(embed)]
    pub identity: SiloUserIdentity,

    pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
    pub silo_id: Uuid,

    /// If the user provision type is ApiOnly or JIT, then the external id is
    /// the identity provider's ID for this user. There is a database constraint
    /// (`lookup_silo_user_by_silo`) that ensures this field must be non-null
    /// for those provision types.
    ///
    /// For SCIM, this may be null, which would trigger the uniqueness
    /// constraint if that wasn't limited to specific provision types.
    pub external_id: Option<String>,

    pub user_provision_type: UserProvisionType,

    /// For SCIM users, user name must be Some.
    pub user_name: Option<String>,

    /// For SCIM users, active describes whether or not the user is allowed to
    /// have active sessions.
    pub active: Option<bool>,
}

impl SiloUser {
    pub fn new_api_only_user(
        silo_id: Uuid,
        user_id: SiloUserUuid,
        external_id: String,
    ) -> Self {
        Self {
            identity: SiloUserIdentity::new(user_id),
            time_deleted: None,
            silo_id,
            external_id: Some(external_id),
            user_provision_type: UserProvisionType::ApiOnly,
            user_name: None,
            active: None,
        }
    }

    pub fn new_jit_user(
        silo_id: Uuid,
        user_id: SiloUserUuid,
        external_id: String,
    ) -> Self {
        Self {
            identity: SiloUserIdentity::new(user_id),
            time_deleted: None,
            silo_id,
            external_id: Some(external_id),
            user_provision_type: UserProvisionType::Jit,
            user_name: None,
            active: None,
        }
    }

    pub fn new_scim_user(
        silo_id: Uuid,
        user_id: SiloUserUuid,
        user_name: String,
        external_id: Option<String>,
        active: Option<bool>,
    ) -> Self {
        Self {
            identity: SiloUserIdentity::new(user_id),
            time_deleted: None,
            silo_id,
            external_id,
            user_provision_type: UserProvisionType::Scim,
            user_name: Some(user_name),
            active,
        }
    }
}
