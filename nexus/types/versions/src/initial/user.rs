// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! User types for version INITIAL.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams, Name, NameOrId,
    ObjectIdentity, UserId,
};
use omicron_uuid_kinds::{SiloGroupUuid, SiloUserUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// View of a User
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct User {
    #[schemars(with = "Uuid")]
    pub id: SiloUserUuid,

    /// Human-readable name that can identify the user
    pub display_name: String,

    /// Uuid of the silo to which this user belongs
    pub silo_id: Uuid,
}

/// Info about the current user
// Add silo name to User because the console needs to display it
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct CurrentUser {
    #[serde(flatten)]
    pub user: User,
    /// Name of the silo to which this user belongs.
    pub silo_name: Name,
    /// Whether this user has the viewer role on the fleet. Used by the web
    /// console to determine whether to show system-level UI.
    pub fleet_viewer: bool,
    /// Whether this user has the admin role on their silo. Used by the web
    /// console to determine whether to show admin-only UI elements.
    pub silo_admin: bool,
}

/// View of a Group
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Group {
    #[schemars(with = "Uuid")]
    pub id: SiloGroupUuid,

    /// Human-readable name that can identify the group
    pub display_name: String,

    /// Uuid of the silo to which this group belongs
    pub silo_id: Uuid,
}

/// View of a Built-in User
///
/// Built-in users are identities internal to the system, used when the control
/// plane performs actions autonomously
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UserBuiltin {
    // TODO-correctness is flattening here (and in all the other types) the
    // intent in RFD 4?
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

// Params

/// Path parameters for Silo User requests
#[derive(Deserialize, JsonSchema)]
pub struct UserParam {
    /// The user's internal ID
    #[schemars(with = "Uuid")]
    pub user_id: SiloUserUuid,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalGroupSelector {
    #[schemars(with = "Option<Uuid>")]
    pub group: Option<SiloGroupUuid>,
}

/// Create-time parameters for a `User`
#[derive(Clone, Deserialize, JsonSchema)]
pub struct UserCreate {
    /// username used to log in
    pub external_id: UserId,
    /// how to set the user's login password
    pub password: UserPassword,
}

/// A password used for authenticating a local-only user
#[derive(Clone, Deserialize)]
#[serde(try_from = "String")]
pub struct Password(pub(crate) omicron_passwords::Password);

impl schemars::JsonSchema for Password {
    fn schema_name() -> String {
        "Password".to_string()
    }

    fn json_schema(
        _: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some(
                    "A password used to authenticate a user".to_string(),
                ),
                description: Some(
                    "Passwords may be subject to additional constraints."
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(
                    u32::try_from(omicron_passwords::MAX_PASSWORD_LENGTH)
                        .unwrap(),
                ),
                min_length: None,
                pattern: None,
            })),
            ..Default::default()
        }
        .into()
    }
}

/// Parameters for setting a user's password
#[derive(Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "mode", content = "value")]
pub enum UserPassword {
    /// Sets the user's password to the provided value
    Password(Password),
    /// Invalidates any current password (disabling password authentication)
    LoginDisallowed,
}

/// Credentials for local user login
#[derive(Clone, Deserialize, JsonSchema)]
pub struct UsernamePasswordCredentials {
    pub username: UserId,
    pub password: Password,
}

/// Create-time parameters for a `UserBuiltin`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UserBuiltinCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct UserBuiltinSelector {
    pub user: NameOrId,
}
