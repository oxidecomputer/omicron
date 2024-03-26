// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that are used as both views and params

use std::net::IpAddr;

use omicron_common::api::external::Name;
use parse_display::FromStr;
use schemars::JsonSchema;
use serde::de::Error as _;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use strum::EnumIter;
use uuid::Uuid;

pub use omicron_common::address::{IpRange, Ipv4Range, Ipv6Range};
pub use omicron_common::api::external::BfdMode;

/// Maximum number of role assignments allowed on any one resource
// Today's implementation assumes a relatively small number of role assignments
// per resource.  Things should work if we bump this up, but we'll want to look
// into scalability improvements (e.g., use pagination for fetching and updating
// the role assignments, and consider the impact on authz checks as well).
//
// Most importantly: by keeping this low to start with, it's impossible for
// customers to develop a dependency on a huge number of role assignments.  That
// maximizes our flexibility in the future.
//
// TODO This should be runtime-configurable.  But it doesn't belong in the Nexus
// configuration file, since it's a constraint on database objects more than it
// is Nexus.  We should have some kinds of config that lives in the database.
pub const MAX_ROLE_ASSIGNMENTS_PER_RESOURCE: usize = 64;

/// Policy for a particular resource
///
/// Note that the Policy only describes access granted explicitly for this
/// resource.  The policies of parent resources can also cause a user to have
/// access to this resource.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[schemars(rename = "{AllowedRoles}Policy")]
pub struct Policy<AllowedRoles: serde::de::DeserializeOwned> {
    /// Roles directly assigned on this resource
    #[serde(deserialize_with = "role_assignments_deserialize")]
    pub role_assignments: Vec<RoleAssignment<AllowedRoles>>,
}

fn role_assignments_deserialize<'de, D, R>(
    d: D,
) -> Result<Vec<RoleAssignment<R>>, D::Error>
where
    D: Deserializer<'de>,
    R: serde::de::DeserializeOwned,
{
    let v = Vec::<_>::deserialize(d)?;
    if v.len() > MAX_ROLE_ASSIGNMENTS_PER_RESOURCE {
        return Err(D::Error::invalid_length(
            v.len(),
            &format!(
                "a list of at most {} role assignments",
                MAX_ROLE_ASSIGNMENTS_PER_RESOURCE
            )
            .as_str(),
        ));
    }
    Ok(v)
}

/// Describes the assignment of a particular role on a particular resource to a
/// particular identity (user, group, etc.)
///
/// The resource is not part of this structure.  Rather, `RoleAssignment`s are
/// put into a `Policy` and that Policy is applied to a particular resource.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[schemars(rename = "{AllowedRoles}RoleAssignment")]
pub struct RoleAssignment<AllowedRoles> {
    pub identity_type: IdentityType,
    pub identity_id: Uuid,
    pub role_name: AllowedRoles,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    EnumIter,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetRole {
    Admin,
    Collaborator,
    Viewer,
    // There are other Fleet roles, but they are not externally-visible and so
    // they do not show up in this enum.
}

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    EnumIter,
    Eq,
    FromStr,
    Ord,
    PartialOrd,
    PartialEq,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SiloRole {
    Admin,
    Collaborator,
    Viewer,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    EnumIter,
    Eq,
    FromStr,
    PartialEq,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectRole {
    Admin,
    Collaborator,
    Viewer,
}

/// Describes what kind of identity is described by an id
// This is a subset of the identity types that might be found in the database
// because we do not expose some (e.g., built-in users) externally.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum IdentityType {
    SiloUser,
    SiloGroup,
}

/// Describes how identities are managed and users are authenticated in this
/// Silo
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SiloIdentityMode {
    /// Users are authenticated with SAML using an external authentication
    /// provider.  The system updates information about users and groups only
    /// during successful authentication (i.e,. "JIT provisioning" of users and
    /// groups).
    SamlJit,

    /// The system is the source of truth about users.  There is no linkage to
    /// an external authentication provider or identity provider.
    // NOTE: authentication for these users is not supported yet at all.  It
    // will eventually be password-based.
    LocalOnly,
}

impl SiloIdentityMode {
    pub fn authentication_mode(&self) -> AuthenticationMode {
        match self {
            SiloIdentityMode::LocalOnly => AuthenticationMode::Local,
            SiloIdentityMode::SamlJit => AuthenticationMode::Saml,
        }
    }

    pub fn user_provision_type(&self) -> UserProvisionType {
        match self {
            SiloIdentityMode::LocalOnly => UserProvisionType::ApiOnly,
            SiloIdentityMode::SamlJit => UserProvisionType::Jit,
        }
    }
}

/// How users are authenticated in this Silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthenticationMode {
    /// Authentication is via SAML using an external authentication provider
    Saml,

    /// Authentication is local to the Oxide system
    Local,
}

/// How users will be provisioned in a silo during authentication.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UserProvisionType {
    /// Identities are managed directly by explicit calls to the external API.
    /// They are not synchronized from any external identity provider nor
    /// automatically created or updated when a user logs in.
    ApiOnly,

    /// Users and groups are created or updated during authentication using
    /// information provided by the authentication provider
    Jit,
}

/// The service intended to use this certificate.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServiceUsingCertificate {
    /// This certificate is intended for access to the external API.
    ExternalApi,
}

/// The kind of an external IP address for an instance
#[derive(
    Debug, Clone, Copy, Deserialize, Eq, Serialize, JsonSchema, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum IpKind {
    Ephemeral,
    Floating,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UpdateableComponentType {
    BootloaderForRot,
    BootloaderForSp,
    BootloaderForHostProc,
    HubrisForPscRot,
    HubrisForPscSp,
    HubrisForSidecarRot,
    HubrisForSidecarSp,
    HubrisForGimletRot,
    HubrisForGimletSp,
    HeliosHostPhase1,
    HeliosHostPhase2,
    HostOmicron,
}

/// Properties that uniquely identify an Oxide hardware component
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
pub struct Baseboard {
    pub serial: String,
    pub part: String,
    pub revision: i64,
}

/// A sled that has not been added to an initialized rack yet
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
pub struct UninitializedSled {
    pub baseboard: Baseboard,
    pub rack_id: Uuid,
    pub cubby: u16,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum BfdState {
    /// A stable down state. Non-responsive to incoming messages.
    AdminDown = 0,

    /// The initial state.
    Down = 1,

    /// The peer has detected a remote peer in the down state.
    Init = 2,

    /// The peer has detected a remote peer in the up or init state while in the
    /// init state.
    Up = 3,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
pub struct BfdStatus {
    pub peer: IpAddr,
    pub state: BfdState,
    pub switch: Name,
    pub local: Option<IpAddr>,
    pub detection_threshold: u8,
    pub required_rx: u64,
    pub mode: BfdMode,
}

#[cfg(test)]
mod test {
    use super::Policy;
    use super::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;
    use serde::Deserialize;

    #[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
    #[serde(rename_all = "kebab-case")]
    pub enum DummyRoles {
        Bogus,
    }

    #[test]
    fn test_policy_parsing() {
        // Success case (edge case: max number of role assignments)
        let role_assignment = serde_json::json!({
            "identity_type": "silo_user",
            "identity_id": "75ec4a39-67cf-4549-9e74-44b92947c37c",
            "role_name": "bogus"
        });
        const MAX: usize = MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;
        let okay_input =
            serde_json::Value::Array(vec![role_assignment.clone(); MAX]);
        let policy: Policy<DummyRoles> =
            serde_json::from_value(serde_json::json!({
                "role_assignments": okay_input
            }))
            .expect("unexpectedly failed with okay input");
        assert_eq!(policy.role_assignments[0].role_name, DummyRoles::Bogus);

        // Failure case: too many role assignments
        let bad_input =
            serde_json::Value::Array(vec![role_assignment; MAX + 1]);
        let error =
            serde_json::from_value::<Policy<DummyRoles>>(serde_json::json!({
                "role_assignments": bad_input
            }))
            .expect_err("unexpectedly succeeded with too many items");
        assert_eq!(
            error.to_string(),
            "invalid length 65, expected a list of at most 64 role assignments"
        );
    }
}
