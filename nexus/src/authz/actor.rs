// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Oso integration for Actor types

use super::roles::RoleSet;
use crate::authn;
use crate::authz::SiloUser;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

/// Represents [`authn::Context`] (which is either an authenticated or
/// unauthenticated actor) for Polar
#[derive(Clone, Debug)]
pub struct AnyActor {
    actor: Option<authn::Actor>,
    roles: RoleSet,
}

impl AnyActor {
    pub fn new(authn: &authn::Context, roles: RoleSet) -> Self {
        let actor = authn.actor().cloned();
        AnyActor { actor, roles }
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
                    actor_id: actor.actor_id(),
                    silo_id: actor.silo_id(),
                    roles: a.roles.clone(),
                })
            })
    }
}

/// Represents an authenticated [`authn::Context`] for Polar
#[derive(Clone, Debug)]
pub struct AuthenticatedActor {
    actor_id: Uuid,
    silo_id: Option<Uuid>,
    roles: RoleSet,
}

impl AuthenticatedActor {
    /// Returns whether this actor has the given role for the given resource
    pub fn has_role_resource(
        &self,
        resource_type: ResourceType,
        resource_id: Uuid,
        role: &str,
    ) -> bool {
        self.roles.has_role(resource_type, resource_id, role)
    }
}

impl PartialEq for AuthenticatedActor {
    fn eq(&self, other: &Self) -> bool {
        self.actor_id == other.actor_id
    }
}

impl Eq for AuthenticatedActor {}

impl oso::PolarClass for AuthenticatedActor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_constant(
                AuthenticatedActor {
                    actor_id: authn::USER_DB_INIT.id,
                    silo_id: None,
                    roles: RoleSet::new(),
                },
                "USER_DB_INIT",
            )
            .add_attribute_getter("silo", |a: &AuthenticatedActor| {
                a.silo_id.map(|silo_id| {
                    super::Silo::new(
                        super::FLEET,
                        silo_id,
                        LookupType::ById(silo_id),
                    )
                })
            })
            .add_method("equals_silo_user", |a: &AuthenticatedActor, u: SiloUser| {
                a.actor_id == u.id()
            })
    }
}
