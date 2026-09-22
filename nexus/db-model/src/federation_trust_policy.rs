// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::DatabaseString;
use db_macros::Resource;
use nexus_db_schema::schema::{federation_role_grant, federation_trust_policy};
use nexus_types::external_api::federation as api;
use nexus_types::external_api::policy::{ProjectRole, SiloRole};
use nexus_types::identity::Resource;
use omicron_common::api::external::{Error, IdentityMetadataCreateParams};
use polar_core::parser::{Line, parse_lines};
use polar_core::sources::Source;
use std::collections::BTreeSet;
use uuid::Uuid;

#[derive(Queryable, Selectable, Insertable, Clone, Debug, Resource)]
#[diesel(table_name = federation_trust_policy)]
pub struct FederationTrustPolicy {
    #[diesel(embed)]
    pub identity: FederationTrustPolicyIdentity,
    pub silo_id: Uuid,
    pub revision: i64,
    pub idp_id: Uuid,
    pub policy: String,
}

impl FederationTrustPolicy {
    pub fn new(
        silo_id: Uuid,
        idp_id: Uuid,
        params: &api::FederationTrustPolicyCreate,
    ) -> Self {
        Self {
            identity: FederationTrustPolicyIdentity::new(
                Uuid::new_v4(),
                IdentityMetadataCreateParams {
                    name: params.name.clone(),
                    description: params.description.clone(),
                },
            ),
            silo_id,
            revision: 1,
            idp_id,
            policy: params.policy.clone(),
        }
    }

    pub fn into_view(
        self,
        mut grants: Vec<FederationRoleGrant>,
    ) -> Result<api::FederationTrustPolicy, Error> {
        grants.sort_unstable_by(|a, b| a.key().cmp(&b.key()));
        Ok(api::FederationTrustPolicy {
            identity: self.identity(),
            revision: self.revision,
            idp_id: self.idp_id,
            policy: self.policy,
            grants: grants
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Queryable, Selectable, Insertable, Clone, Debug)]
#[diesel(table_name = federation_role_grant)]
pub struct FederationRoleGrant {
    pub id: Uuid,
    pub resource_kind: String,
    pub resource_id: Uuid,
    pub role_name: String,
    pub trust_policy_id: Uuid,
}

impl FederationRoleGrant {
    pub fn new(
        trust_policy_id: Uuid,
        grant: &api::FederationRoleGrant,
    ) -> Self {
        let (kind, resource_id, role_name) = match grant {
            api::FederationRoleGrant::Silo { resource_id, role_name } => {
                ("silo", *resource_id, role_name.to_database_string())
            }
            api::FederationRoleGrant::Project { resource_id, role_name } => {
                ("project", *resource_id, role_name.to_database_string())
            }
        };
        Self {
            id: Uuid::new_v4(),
            resource_kind: kind.to_owned(),
            resource_id,
            role_name: role_name.into_owned(),
            trust_policy_id,
        }
    }

    pub fn key(&self) -> (&str, Uuid, &str) {
        (&self.resource_kind, self.resource_id, &self.role_name)
    }
}

impl TryFrom<FederationRoleGrant> for api::FederationRoleGrant {
    type Error = Error;

    fn try_from(grant: FederationRoleGrant) -> Result<Self, Error> {
        let invalid = |e| {
            Error::internal_error(format!(
                "invalid stored federation role: {e}"
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
                "invalid stored federation grant resource kind",
            )),
        }
    }
}

pub fn validate_federation_trust_policy(
    description: Option<&str>,
    policy: Option<&str>,
    grants: Option<&[api::FederationRoleGrant]>,
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
            .map(|grant| FederationRoleGrant::new(Uuid::nil(), grant))
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
    fn federation_trust_policy_validation() {
        assert!(
            validate_federation_trust_policy(
                None,
                Some("assume(claims) if claims.sub = \"builder\";"),
                None
            )
            .is_ok()
        );
        for policy in
            ["", "# empty", "assume(", "assume(_claims); ?= assume({});"]
        {
            assert!(
                validate_federation_trust_policy(None, Some(policy), None)
                    .is_err()
            );
        }
        let grant = api::FederationRoleGrant::Project {
            resource_id: Uuid::new_v4(),
            role_name: ProjectRole::LimitedCollaborator,
        };
        assert!(
            validate_federation_trust_policy(
                None,
                None,
                Some(&[grant.clone(), grant.clone()])
            )
            .is_err()
        );
        let row = FederationRoleGrant::new(Uuid::new_v4(), &grant);
        assert_eq!(row.role_name, "limited-collaborator");
        assert_eq!(api::FederationRoleGrant::try_from(row).unwrap(), grant);
    }

    #[test]
    fn federation_trust_policy_entry_point() {
        for policy in [
            "check_claims(_claims);",
            "assume();",
            "assume(_claims, _resource);",
            "helper(_claims); # assume(claims) if true;",
        ] {
            let error =
                validate_federation_trust_policy(None, Some(policy), None)
                    .unwrap_err();
            assert_eq!(
                error,
                Error::invalid_request(
                    "policy must define an assume rule with exactly one parameter"
                ),
            );
        }
        for policy in [
            "assume(jwt) if jwt.sub = \"builder\";",
            "builder(jwt) if jwt.sub = \"builder\"; assume(jwt) if builder(jwt);",
            "assume(jwt) if jwt.sub = \"builder\"; assume(jwt) if jwt.sub = \"deployer\";",
        ] {
            assert!(
                validate_federation_trust_policy(None, Some(policy), None)
                    .is_ok()
            );
        }
    }
}
