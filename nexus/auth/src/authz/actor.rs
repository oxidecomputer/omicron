// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Oso integration for Actor types

use super::roles::RoleSet;
use crate::authn;
use crate::authz::SiloUser;
use nexus_db_model::DatabaseString;
use nexus_types::external_api::shared::FleetRole;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

/// Represents [`authn::Context`] (which is either an authenticated or
/// unauthenticated actor) for Polar
#[derive(Clone, Debug)]
pub struct AnyActor {
    actor: Option<authn::Actor>,
    silo_policy: Option<authn::SiloAuthnPolicy>,
    roles: RoleSet,
}

impl AnyActor {
    pub fn new(authn: &authn::Context, roles: RoleSet) -> Self {
        let actor = authn.actor().cloned();
        let silo_policy = authn.silo_authn_policy().cloned();
        AnyActor { actor, silo_policy, roles }
    }
}

impl oso::PolarClass for AnyActor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .add_attribute_getter("authenticated", |a: &AnyActor| {
                a.actor.is_some()
            })
            .add_attribute_getter("authn_actor", |a: &AnyActor| {
                a.actor.map(|actor| AuthenticatedActor {
                    actor,
                    roles: a.roles.clone(),
                    silo_policy: a.silo_policy.clone(),
                })
            })
    }
}

/// Represents an authenticated [`authn::Context`] for Polar
#[derive(Clone, Debug)]
pub struct AuthenticatedActor {
    actor: authn::Actor,
    roles: RoleSet,
    silo_policy: Option<authn::SiloAuthnPolicy>,
}

impl AuthenticatedActor {
    /// Returns whether this actor has explicitly been granted the given role
    /// for the given resource
    pub fn has_role_resource(
        &self,
        resource_type: ResourceType,
        resource_id: Uuid,
        role: &str,
    ) -> bool {
        self.roles.has_role(resource_type, resource_id, role)
    }

    /// Returns the list of Silo roles that confer the given Fleet roles for
    /// this actor's Silo
    pub fn confers_fleet_role(&self, fleet_role_str: &str) -> Vec<String> {
        let Ok(fleet_role) = FleetRole::from_database_string(fleet_role_str)
        else {
            return vec![];
        };
        let Some(silo_policy) = self.silo_policy.as_ref() else {
            return vec![];
        };
        silo_policy
            .mapped_fleet_roles()
            .into_iter()
            .filter_map(|(silo_role, fleet_roles)| {
                if fleet_roles.contains(&fleet_role) {
                    Some(silo_role.to_database_string().to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns whether this actor's Silo restricts networking actions to Silo
    /// Admins only
    pub fn silo_restricts_networking(&self) -> bool {
        self.silo_policy
            .as_ref()
            .map(|policy| policy.restrict_network_actions())
            .unwrap_or(false)
    }
}

impl PartialEq for AuthenticatedActor {
    fn eq(&self, other: &Self) -> bool {
        self.actor == other.actor
    }
}

impl Eq for AuthenticatedActor {}

impl oso::PolarClass for AuthenticatedActor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_constant(
                AuthenticatedActor {
                    actor: authn::Actor::UserBuiltin {
                        user_builtin_id: authn::USER_DB_INIT.id,
                    },
                    roles: RoleSet::new(),
                    silo_policy: None,
                },
                "USER_DB_INIT",
            )
            .add_constant(
                AuthenticatedActor {
                    actor: authn::Actor::UserBuiltin {
                        user_builtin_id: authn::USER_INTERNAL_API.id,
                    },
                    roles: RoleSet::new(),
                    silo_policy: None,
                },
                "USER_INTERNAL_API",
            )
            .add_attribute_getter("silo", |a: &AuthenticatedActor| {
                match a.actor {
                    authn::Actor::SiloUser { silo_id, .. } => {
                        Some(super::Silo::new(
                            super::FLEET,
                            silo_id,
                            LookupType::ById(silo_id),
                        ))
                    }

                    authn::Actor::UserBuiltin { .. } => None,
                }
            })
            .add_method(
                "confers_fleet_role",
                |a: &AuthenticatedActor, role: String| {
                    a.confers_fleet_role(&role)
                },
            )
            .add_method(
                "equals_silo_user",
                |a: &AuthenticatedActor, u: SiloUser| match a.actor {
                    authn::Actor::SiloUser { silo_user_id, .. } => {
                        silo_user_id == u.id()
                    }

                    authn::Actor::UserBuiltin { .. } => false,
                },
            )
            .add_method(
                "silo_restricts_networking",
                |a: &AuthenticatedActor| a.silo_restricts_networking(),
            )
    }
}
