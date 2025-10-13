// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, Project};
use crate::collection::DatastoreCollectionConfig;
use crate::{DatabaseString, Image, impl_enum_type};
use db_macros::Resource;
use nexus_db_schema::schema::{image, project, silo};
use nexus_types::external_api::shared::{
    FleetRole, SiloIdentityMode, SiloRole,
};
use nexus_types::external_api::views;
use nexus_types::external_api::{params, shared};
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use uuid::Uuid;

impl_enum_type!(
    AuthenticationModeEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq, Eq)]
    pub enum AuthenticationMode;

    // Enum values
    Local => b"local"
    Saml => b"saml"
);

impl From<shared::AuthenticationMode> for AuthenticationMode {
    fn from(params: shared::AuthenticationMode) -> Self {
        match params {
            shared::AuthenticationMode::Local => AuthenticationMode::Local,
            shared::AuthenticationMode::Saml => AuthenticationMode::Saml,
        }
    }
}

impl From<AuthenticationMode> for shared::AuthenticationMode {
    fn from(model: AuthenticationMode) -> Self {
        match model {
            AuthenticationMode::Local => Self::Local,
            AuthenticationMode::Saml => Self::Saml,
        }
    }
}

impl_enum_type!(
    UserProvisionTypeEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq, Eq)]
    pub enum UserProvisionType;

    // Enum values
    ApiOnly => b"api_only"
    Jit => b"jit"
    Scim => b"scim"
);

impl From<shared::UserProvisionType> for UserProvisionType {
    fn from(params: shared::UserProvisionType) -> Self {
        match params {
            shared::UserProvisionType::ApiOnly => UserProvisionType::ApiOnly,
            shared::UserProvisionType::Jit => UserProvisionType::Jit,
            shared::UserProvisionType::Scim => UserProvisionType::Scim,
        }
    }
}

impl From<UserProvisionType> for shared::UserProvisionType {
    fn from(model: UserProvisionType) -> Self {
        match model {
            UserProvisionType::ApiOnly => Self::ApiOnly,
            UserProvisionType::Jit => Self::Jit,
            UserProvisionType::Scim => Self::Scim,
        }
    }
}

/// Describes a silo within the database.
#[derive(
    Clone, PartialEq, Eq, Queryable, Insertable, Debug, Resource, Selectable,
)]
#[diesel(table_name = silo)]
pub struct Silo {
    #[diesel(embed)]
    identity: SiloIdentity,

    pub discoverable: bool,

    pub authentication_mode: AuthenticationMode,
    pub user_provision_type: UserProvisionType,

    // The mapping of Silo roles to Fleet roles in each row is stored as a
    // JSON-serialized map.  This is an implementation detail hidden behind this
    // struct.  For writes, consumers provide a BTreeMap in `Silo::new()`.  For
    // reads, consumers use mapped_fleet_roles() to get this information back
    // out.  This struct takes care of converting to/from the stored
    // representation (which happens to be identical).
    mapped_fleet_roles: serde_json::Value,

    /// child resource generation number, per RFD 192
    pub rcgen: Generation,

    /// Store a group name that will be
    ///
    /// 1) automatically created (depending on the provision type of this silo)
    ///    at silo create time.
    /// 2) assigned a policy granting members the silo admin role
    ///
    /// Prior to this column existing, for api_only and jit provision types,
    /// Nexus would create this group, and create a policy where users of the
    /// group would have the silo admin role. It wouldn't store this information
    /// though as groups cannot be deleted with those provision types.
    ///
    /// For provision types that can both create and delete groups, it's
    /// important to store this name so that when groups are created the same
    /// automatic policy can be created as well.
    pub admin_group_name: Option<String>,
}

/// Form of mapped fleet roles used when serializing to the database
// A bunch of structures (e.g., `params::SiloCreate`, `views::Silo`,
// `authn::SiloAuthnPolicy`) store a mapping of silo roles to a set of fleet
// roles.  The obvious, normalized way to store this in the database involves a
// bunch of extra tables and records.  That seems like overkill for a mapping
// that currently has at most 3 keys, each pointing at a set of at most 3
// entries, and that always would be updated together.  Instead, we'll store it
// directly into the database row.  The easiest way to do that is to serialize
// it to JSON and store it there.  This does give up some database-level
// validation, unfortunately, but this isn't the sort of property that could
// _only_ be validated safely at the database.
//
// Given this approach, we could serialize the map directly to JSON.  This
// introduces the risk that somebody modifies the Rust structures in the
// mapping (say, by removing one of the valid Silo roles) without realizing
// that this could break our ability to parse existing database rows.  To avoid
// this, we use a newtype here to translate from the Rust implementation to the
// one that we will serialize to the database.
//
// WARNING: If you're considering changing anything about
// `SerializedMappedFleetRoles`, including the `From` impl below, be sure you've
// considered how to handle database records written prior to your change.
// e.g., if you're removing a role, we won't be able to parse these records any
// more.
struct SerializedMappedFleetRoles(BTreeMap<String, BTreeSet<String>>);
impl<'a> From<&'a BTreeMap<SiloRole, BTreeSet<FleetRole>>>
    for SerializedMappedFleetRoles
{
    fn from(value: &'a BTreeMap<SiloRole, BTreeSet<FleetRole>>) -> Self {
        SerializedMappedFleetRoles(
            value
                .iter()
                .map(|(silo_role, fleet_roles)| {
                    let silo_role_str =
                        silo_role.to_database_string().to_string();
                    let fleet_roles_str = fleet_roles
                        .iter()
                        .map(|f| f.to_database_string().to_string())
                        .collect();
                    (silo_role_str, fleet_roles_str)
                })
                .collect(),
        )
    }
}

impl Silo {
    /// Creates a new database Silo object.
    pub fn new(params: params::SiloCreate) -> Result<Self, Error> {
        Self::new_with_id(Uuid::new_v4(), params)
    }

    pub fn new_with_id(
        id: Uuid,
        params: params::SiloCreate,
    ) -> Result<Self, Error> {
        let mapped_fleet_roles = serde_json::to_value(
            &SerializedMappedFleetRoles::from(&params.mapped_fleet_roles).0,
        )
        .map_err(|e| {
            Error::internal_error(&format!(
                "failed to serialize mapped_fleet_roles: {:#}",
                e
            ))
        })?;
        Ok(Self {
            identity: SiloIdentity::new(id, params.identity),
            discoverable: params.discoverable,
            authentication_mode: params
                .identity_mode
                .authentication_mode()
                .into(),
            user_provision_type: params
                .identity_mode
                .user_provision_type()
                .into(),
            rcgen: Generation::new(),
            mapped_fleet_roles,
            admin_group_name: params.admin_group_name,
        })
    }

    pub fn mapped_fleet_roles(
        &self,
    ) -> Result<BTreeMap<SiloRole, BTreeSet<FleetRole>>, Error> {
        serde_json::from_value(self.mapped_fleet_roles.clone()).map_err(|e| {
            Error::internal_error(&format!(
                "failed to deserialize mapped fleet roles from database: {:#}",
                e
            ))
        })
    }
}

impl TryFrom<Silo> for views::Silo {
    type Error = Error;
    fn try_from(silo: Silo) -> Result<Self, Self::Error> {
        let authn_mode = &silo.authentication_mode;
        let user_type = &silo.user_provision_type;
        let identity_mode = match (authn_mode, user_type) {
            (AuthenticationMode::Saml, UserProvisionType::Jit) => {
                Some(SiloIdentityMode::SamlJit)
            }

            (AuthenticationMode::Local, UserProvisionType::ApiOnly) => {
                Some(SiloIdentityMode::LocalOnly)
            }

            (AuthenticationMode::Saml, UserProvisionType::Scim) => {
                Some(SiloIdentityMode::SamlScim)
            }

            _ => None,
        }
        .ok_or_else(|| {
            Error::internal_error(&format!(
                "unsupported combination of authentication mode ({:?}) and \
                user provision type ({:?})",
                authn_mode, user_type
            ))
        })?;

        let mapped_fleet_roles = silo.mapped_fleet_roles()?;

        Ok(Self {
            identity: silo.identity(),
            discoverable: silo.discoverable,
            identity_mode,
            mapped_fleet_roles,
            admin_group_name: silo.admin_group_name,
        })
    }
}

impl DatastoreCollectionConfig<Project> for Silo {
    type CollectionId = Uuid;
    type GenerationNumberColumn = silo::dsl::rcgen;
    type CollectionTimeDeletedColumn = silo::dsl::time_deleted;
    type CollectionIdColumn = project::dsl::silo_id;
}

impl DatastoreCollectionConfig<Image> for Silo {
    type CollectionId = Uuid;
    type GenerationNumberColumn = silo::dsl::rcgen;
    type CollectionTimeDeletedColumn = silo::dsl::time_deleted;
    type CollectionIdColumn = image::dsl::silo_id;
}
