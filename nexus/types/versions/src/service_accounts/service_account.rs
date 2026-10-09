// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00::policy::{ProjectRole, SiloRole};
use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    Error, IdentityMetadata, Name, NameOrId, Nullable, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use uuid::Uuid;

pub fn default_federation_max_ttl_seconds() -> NonZeroU32 {
    NonZeroU32::new(3600).unwrap()
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServiceAccountFederation {
    /// Name or UUID of an identity provider in the current silo.
    pub identity_provider: NameOrId,
    /// Polar policy defining `assume` with one parameter for verified JWT claims.
    pub policy: String,
    /// Maximum federation token lifetime in seconds. Defaults to one hour.
    #[serde(default = "default_federation_max_ttl_seconds")]
    pub max_ttl_seconds: NonZeroU32,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServiceAccountCreate {
    /// Name within the owning silo or project.
    pub name: Name,
    /// Human-readable description, limited to 512 characters.
    pub description: String,
    /// Built-in roles granted to the service account.
    pub grants: Vec<ServiceAccountGrant>,
    /// Optional configuration for assuming this account through federation.
    pub federation: Option<ServiceAccountFederation>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServiceAccountUpdate {
    /// Replacement name within the same owner.
    pub name: Option<Name>,
    /// Replacement description, limited to 512 characters.
    pub description: Option<String>,
    /// Replacement grants; an empty list removes all grants.
    pub grants: Option<Vec<ServiceAccountGrant>>,
    /// Omit to retain federation, set to null to remove it, or provide a replacement.
    #[serde(
        default,
        deserialize_with = "deserialize_federation_update",
        skip_serializing_if = "Option::is_none"
    )]
    pub federation: Option<Nullable<ServiceAccountFederation>>,
}

fn deserialize_federation_update<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Nullable<ServiceAccountFederation>>, D::Error> {
    Nullable::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ServiceAccountFederationView {
    /// UUID of the configured identity provider.
    pub identity_provider: Uuid,
    /// Polar policy evaluated against verified JWT claims.
    pub policy: String,
    /// Maximum federation token lifetime in seconds.
    pub max_ttl_seconds: NonZeroU32,
}

#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ServiceAccount {
    /// Identifying metadata.
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// Scope of the service account.
    pub scope: ServiceAccountScope,
    /// UUID of the owning silo or project.
    pub resource_id: Uuid,
    /// Roles granted to this service account.
    pub grants: Vec<ServiceAccountGrant>,
    /// Generation of the federation identity provider and policy.
    pub federation_generation: i64,
    /// Federation configuration, if enabled.
    pub federation: Option<ServiceAccountFederationView>,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum ServiceAccountScope {
    Silo,
    Project,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ServiceAccountScopePath {
    /// Scope of the service accounts.
    pub scope: ServiceAccountScope,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ServiceAccountPath {
    /// Scope of the service account.
    pub scope: ServiceAccountScope,
    /// Name or UUID of the service account.
    pub service_account: NameOrId,
}

#[derive(
    Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct ServiceAccountParentSelector {
    /// Owning silo's name or UUID, for silo-scoped service accounts.
    pub silo: Option<NameOrId>,
    /// Owning project's name or UUID, for project-scoped service accounts.
    pub project: Option<NameOrId>,
}

impl ServiceAccountParentSelector {
    pub fn for_scope(
        &self,
        scope: ServiceAccountScope,
    ) -> Result<Option<&NameOrId>, Error> {
        match scope {
            ServiceAccountScope::Silo if self.project.is_none() => {
                Ok(self.silo.as_ref())
            }
            ServiceAccountScope::Project if self.silo.is_none() => {
                Ok(self.project.as_ref())
            }
            _ => Err(Error::invalid_request(
                "parent selector must match service account scope",
            )),
        }
    }

    pub fn required_for_scope(
        &self,
        scope: ServiceAccountScope,
    ) -> Result<&NameOrId, Error> {
        self.for_scope(scope)?.ok_or_else(|| {
            Error::invalid_request(match scope {
                ServiceAccountScope::Silo => "silo selector is required",
                ServiceAccountScope::Project => "project selector is required",
            })
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "resource_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServiceAccountGrant {
    Silo {
        /// UUID of the owning silo.
        resource_id: Uuid,
        /// Built-in silo role to grant.
        role_name: SiloRole,
    },
    Project {
        /// UUID of a project within the service account's scope.
        resource_id: Uuid,
        /// Built-in project role to grant.
        role_name: ProjectRole,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_account_patch_and_ttl() {
        let omitted: ServiceAccountUpdate =
            serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(omitted.federation.is_none());
        let removed: ServiceAccountUpdate =
            serde_json::from_value(serde_json::json!({"federation": null}))
                .unwrap();
        assert!(removed.federation.unwrap().0.is_none());
        let config = serde_json::json!({"identity_provider": "gcp", "policy": "assume(_jwt);"});
        let updated: ServiceAccountUpdate =
            serde_json::from_value(serde_json::json!({"federation": config}))
                .unwrap();
        assert_eq!(
            updated.federation.unwrap().0.unwrap().max_ttl_seconds.get(),
            3600
        );
        for field in ["scope", "resource_id", "silo", "project"] {
            assert!(
                serde_json::from_value::<ServiceAccountUpdate>(
                    serde_json::json!({field: "changed"})
                )
                .is_err()
            );
        }
        for ttl in [0, -1] {
            let mut config = config.clone();
            config["max_ttl_seconds"] = serde_json::json!(ttl);
            assert!(
                serde_json::from_value::<ServiceAccountFederation>(config)
                    .is_err()
            );
        }
    }

    #[test]
    fn service_account_parent_selectors() {
        let project = ServiceAccountParentSelector {
            project: Some(NameOrId::Id(Uuid::new_v4())),
            silo: None,
        };
        assert!(
            project.required_for_scope(ServiceAccountScope::Project).is_ok()
        );
        assert!(project.for_scope(ServiceAccountScope::Silo).is_err());
        let both = ServiceAccountParentSelector {
            silo: Some(NameOrId::Id(Uuid::new_v4())),
            ..project
        };
        for scope in [ServiceAccountScope::Silo, ServiceAccountScope::Project] {
            assert!(both.for_scope(scope).is_err());
            let empty = ServiceAccountParentSelector::default();
            assert!(empty.for_scope(scope).unwrap().is_none());
            assert!(empty.required_for_scope(scope).is_err());
        }
    }
}
