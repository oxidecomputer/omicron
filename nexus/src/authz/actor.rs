// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Oso integration for Actor types

use crate::authn;
use crate::authz::RoleSet;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

/// Represents [`authn::Context`] (which is either an authenticated or
/// unauthenticated actor) for Polar
#[derive(Clone, Debug)]
pub struct AnyActor {
    authenticated: bool,
    actor_id: Option<Uuid>,
    roles: RoleSet,
}

impl AnyActor {
    pub fn new(authn: &authn::Context, roles: RoleSet) -> Self {
        let actor = authn.actor();
        AnyActor {
            authenticated: actor.is_some(),
            actor_id: actor.map(|a| a.0),
            roles,
        }
    }
}

impl PartialEq for AnyActor {
    fn eq(&self, other: &Self) -> bool {
        self.actor_id == other.actor_id
    }
}

impl Eq for AnyActor {}

impl oso::PolarClass for AnyActor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_attribute_getter("authenticated", |a: &AnyActor| {
                a.authenticated
            })
            .add_attribute_getter("authn_actor", |a: &AnyActor| {
                a.actor_id.map(|actor_id| AuthenticatedActor {
                    actor_id,
                    roles: a.roles.clone(),
                })
            })
    }
}

/// Represents an authenticated [`authn::Context`] for Polar
#[derive(Clone, Debug)]
pub struct AuthenticatedActor {
    actor_id: Uuid,
    roles: RoleSet,
}

impl AuthenticatedActor {
    /**
     * Returns whether this actor has the given role for the given resource
     */
    pub fn has_role_resource(
        &self,
        resource_type: ResourceType,
        resource_id: Uuid,
        role: &str,
    ) -> bool {
        // This particular part of the policy needs to be hardcoded because it's
        // used to bootstrap the rest of the built-in roles.
        // XXX
        (resource_type == ResourceType::Fleet
            && role == "admin"
            && self.actor_id == authn::USER_DB_INIT.id)
            || self.roles.has_role(resource_type, resource_id, role)
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
            .add_attribute_getter("id", |a: &AuthenticatedActor| {
                a.actor_id.to_string()
            })
    }
}
