// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{DatabaseString, Generation};
use db_macros::Resource;
use nexus_db_schema::schema::{service_account, service_account_grant};
use nexus_types::external_api::policy::{ProjectRole, SiloRole};
use nexus_types::external_api::service_account as api;
use nexus_types::identity::Resource;
use omicron_common::api::external::{Error, IdentityMetadataCreateParams};
use polar_core::parser::{Line, parse_lines};
use polar_core::sources::Source;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use uuid::Uuid;

#[derive(Queryable, Selectable, Insertable, Clone, Debug, Resource)]
#[diesel(table_name = service_account)]
pub struct ServiceAccount {
    #[diesel(embed)]
    pub identity: ServiceAccountIdentity,
    pub scope: String,
    pub resource_id: Uuid,
    pub federation_generation: Generation,
    pub federation_token_max_ttl_seconds: i64,
    pub identity_provider_id: Option<Uuid>,
    pub trust_policy: Option<String>,
}

impl ServiceAccount {
    pub fn new(
        scope: api::ServiceAccountScope,
        resource_id: Uuid,
        identity_provider_id: Option<Uuid>,
        params: &api::ServiceAccountCreate,
    ) -> Self {
        Self {
            identity: ServiceAccountIdentity::new(
                Uuid::new_v4(),
                IdentityMetadataCreateParams {
                    name: params.name.clone(),
                    description: params.description.clone(),
                },
            ),
            scope: match scope {
                api::ServiceAccountScope::Silo => "silo",
                api::ServiceAccountScope::Project => "project",
            }
            .into(),
            resource_id,
            federation_generation: Generation::new(),
            federation_token_max_ttl_seconds: i64::from(
                params
                    .federation
                    .as_ref()
                    .map(|f| f.max_ttl_seconds)
                    .unwrap_or_else(api::default_federation_max_ttl_seconds)
                    .get(),
            ),
            identity_provider_id,
            trust_policy: params.federation.as_ref().map(|f| f.policy.clone()),
        }
    }

    pub fn into_view(
        self,
        mut grants: Vec<ServiceAccountGrant>,
    ) -> Result<api::ServiceAccount, Error> {
        let scope = match self.scope.as_str() {
            "silo" => api::ServiceAccountScope::Silo,
            "project" => api::ServiceAccountScope::Project,
            _ => {
                return Err(Error::internal_error(
                    "invalid stored service account scope",
                ));
            }
        };
        let identity = self.identity();
        let federation = match (self.identity_provider_id, self.trust_policy) {
            (Some(identity_provider), Some(policy)) => {
                Some(api::ServiceAccountFederationView {
                    identity_provider,
                    policy,
                    max_ttl_seconds: u32::try_from(
                        self.federation_token_max_ttl_seconds,
                    )
                    .ok()
                    .and_then(NonZeroU32::new)
                    .ok_or_else(|| {
                        Error::internal_error("invalid stored federation TTL")
                    })?,
                })
            }
            (None, None) => None,
            _ => {
                return Err(Error::internal_error(
                    "incomplete stored federation configuration",
                ));
            }
        };
        grants.sort_unstable_by(|a, b| a.key().cmp(&b.key()));
        Ok(api::ServiceAccount {
            identity,
            scope,
            resource_id: self.resource_id,
            federation_generation: i64::from(&self.federation_generation.0),
            grants: grants
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            federation,
        })
    }
}

#[derive(Queryable, Selectable, Insertable, Clone, Debug)]
#[diesel(table_name = service_account_grant)]
pub struct ServiceAccountGrant {
    pub id: Uuid,
    pub service_account_id: Uuid,
    pub resource_kind: String,
    pub resource_id: Uuid,
    pub role_name: String,
}

impl ServiceAccountGrant {
    pub fn new(
        service_account_id: Uuid,
        grant: &api::ServiceAccountGrant,
    ) -> Self {
        let (resource_kind, resource_id, role_name) = match grant {
            api::ServiceAccountGrant::Silo { resource_id, role_name } => {
                ("silo", *resource_id, role_name.to_database_string())
            }
            api::ServiceAccountGrant::Project { resource_id, role_name } => {
                ("project", *resource_id, role_name.to_database_string())
            }
        };
        Self {
            id: Uuid::new_v4(),
            service_account_id,
            resource_kind: resource_kind.to_owned(),
            resource_id,
            role_name: role_name.into_owned(),
        }
    }

    pub fn key(&self) -> (&str, Uuid, &str) {
        (&self.resource_kind, self.resource_id, &self.role_name)
    }
}

impl TryFrom<ServiceAccountGrant> for api::ServiceAccountGrant {
    type Error = Error;

    fn try_from(grant: ServiceAccountGrant) -> Result<Self, Error> {
        let invalid = |e| {
            Error::internal_error(format!(
                "invalid stored service account role: {e}"
            ))
        };
        match grant.resource_kind.as_str() {
            "silo" => Ok(Self::Silo {
                resource_id: grant.resource_id,
                role_name: SiloRole::from_database_string(&grant.role_name)
                    .map_err(invalid)?,
            }),
            "project" => Ok(Self::Project {
                resource_id: grant.resource_id,
                role_name: ProjectRole::from_database_string(&grant.role_name)
                    .map_err(invalid)?,
            }),
            _ => Err(Error::internal_error(
                "invalid stored service account grant kind",
            )),
        }
    }
}

pub fn validate_service_account(
    description: Option<&str>,
    policy: Option<&str>,
    grants: Option<&[api::ServiceAccountGrant]>,
) -> Result<(), Error> {
    if description.is_some_and(|s| s.chars().count() > 512) {
        return Err(Error::invalid_request(
            "description must be at most 512 characters",
        ));
    }
    if let Some(policy) = policy {
        let lines = parse_lines(Source::new(policy)).map_err(|e| {
            Error::invalid_request(format!("invalid Polar policy: {e}"))
        })?;
        if !lines.iter().any(|line| {
            matches!(line, Line::Rule(rule) if rule.name.0 == "assume" && rule.params.len() == 1)
        }) {
            return Err(Error::invalid_request(
                "policy must define an assume rule with exactly one parameter",
            ));
        }
        if lines.iter().any(|line| matches!(line, Line::Query(_))) {
            return Err(Error::invalid_request(
                "inline Polar queries are not supported",
            ));
        }
    }
    if let Some(grants) = grants {
        let rows: Vec<_> = grants
            .iter()
            .map(|grant| ServiceAccountGrant::new(Uuid::nil(), grant))
            .collect();
        let mut keys = BTreeSet::new();
        for row in &rows {
            if !keys.insert(row.key()) {
                return Err(Error::invalid_request("duplicate role grant"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_account_policy_validation() {
        for policy in [
            "assume(jwt) if jwt.sub = \"builder\";",
            "builder(jwt) if jwt.sub = \"builder\"; assume(jwt) if builder(jwt);",
            "assume(jwt) if jwt.sub = \"builder\"; assume(jwt) if jwt.sub = \"deployer\";",
        ] {
            assert!(validate_service_account(None, Some(policy), None).is_ok());
        }
        for policy in [
            "",
            "# empty",
            "assume(",
            "check_claims(_claims);",
            "assume();",
            "assume(_claims, _resource);",
            "helper(_claims); # assume(claims) if true;",
            "assume(_claims); ?= assume({});",
        ] {
            assert!(
                validate_service_account(None, Some(policy), None).is_err(),
                "{policy}"
            );
        }
    }

    #[test]
    fn service_account_grant_validation() {
        let project = Uuid::new_v4();
        let grant = api::ServiceAccountGrant::Project {
            resource_id: project,
            role_name: ProjectRole::LimitedCollaborator,
        };
        let row = ServiceAccountGrant::new(Uuid::new_v4(), &grant);
        assert_eq!(row.role_name, "limited-collaborator");
        assert_eq!(api::ServiceAccountGrant::try_from(row).unwrap(), grant);
        assert!(
            validate_service_account(None, None, Some(&[grant.clone(), grant]))
                .is_err()
        );
        assert!(serde_json::from_value::<api::ServiceAccountGrant>(serde_json::json!({
            "resource_kind": "fleet", "resource_id": project, "role_name": "admin"
        })).is_err());
        assert!(serde_json::from_value::<api::ServiceAccountGrant>(serde_json::json!({
            "resource_kind": "project", "resource_id": project, "role_name": "unknown"
        })).is_err());
    }
}
