// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Policy types for version SERVICE_ACCOUNTS.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::de::Error as _;
use uuid::Uuid;

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::policy::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;

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

/// Describes what kind of identity is described by an id
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum IdentityType {
    SiloUser,
    SiloGroup,
    ServiceAccount,
}

impl From<v2025_11_20_00::policy::IdentityType> for IdentityType {
    fn from(old: v2025_11_20_00::policy::IdentityType) -> Self {
        match old {
            v2025_11_20_00::policy::IdentityType::SiloUser => Self::SiloUser,
            v2025_11_20_00::policy::IdentityType::SiloGroup => Self::SiloGroup,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error(
    "this policy includes service account role assignments, which this API \
     version cannot represent; use API version {0} or later"
)]
pub struct ServiceAccountNotRepresentable(pub &'static str);

impl TryFrom<IdentityType> for v2025_11_20_00::policy::IdentityType {
    type Error = ServiceAccountNotRepresentable;

    fn try_from(new: IdentityType) -> Result<Self, Self::Error> {
        match new {
            IdentityType::SiloUser => Ok(Self::SiloUser),
            IdentityType::SiloGroup => Ok(Self::SiloGroup),
            IdentityType::ServiceAccount => {
                Err(ServiceAccountNotRepresentable("2026111700.0.0"))
            }
        }
    }
}

impl<R: serde::de::DeserializeOwned> From<v2025_11_20_00::policy::Policy<R>>
    for Policy<R>
{
    fn from(old: v2025_11_20_00::policy::Policy<R>) -> Self {
        Self {
            role_assignments: old
                .role_assignments
                .into_iter()
                .map(|r| RoleAssignment {
                    identity_type: r.identity_type.into(),
                    identity_id: r.identity_id,
                    role_name: r.role_name,
                })
                .collect(),
        }
    }
}

impl<R: serde::de::DeserializeOwned> TryFrom<Policy<R>>
    for v2025_11_20_00::policy::Policy<R>
{
    type Error = ServiceAccountNotRepresentable;

    fn try_from(new: Policy<R>) -> Result<Self, Self::Error> {
        Ok(Self {
            role_assignments: new
                .role_assignments
                .into_iter()
                .map(|r| {
                    Ok(v2025_11_20_00::policy::RoleAssignment {
                        identity_type: r.identity_type.try_into()?,
                        identity_id: r.identity_id,
                        role_name: r.role_name,
                    })
                })
                .collect::<Result<_, _>>()?,
        })
    }
}
