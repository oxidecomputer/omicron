//! Types and impls used for integration with Oso

// Most of the types here are used in the Polar configuration, which means they
// must impl [`oso::PolarClass`].  There is a derive(PolarClass), but it's
// pretty limited: it doesn't define an equality operator even when the type
// itself impls PartialEq and Eq.  It also doesn't let you define methods.  We
// may want to define our own macro(s) to avoid having to impl this by hand
// everywhere.
//
// Many of these are newtypes that wrap existing types, either simple types from
// Nexus (like `authn::Context`) or database model types.  The main reason is
// that [`Oso::is_allowed()`] consumes owned values, not references.  Consumers
// that want to do an authz check almost always want to keep using the thing
// they had.  We could instead require that the database model types all impl
// `Clone`, impl `PolarClass` on those types, and then just copy them when we
// pass them to `is_allowed()`.  Using newtypes is a way to capture just the
// parts we need for authorization.

use crate::authn;
use crate::db;
use crate::db::identity::Resource;
use anyhow::Context;
use omicron_common::api::external::ResourceType;
use oso::Oso;
use oso::PolarClass;
use std::fmt;
use uuid::Uuid;

/// Polar configuration describing control plane authorization rules
pub const OMICRON_AUTHZ_CONFIG: &str = include_str!("omicron.polar");

/// Returns an Oso handle suitable for authorizing using Omicron's authorization
/// rules
pub fn make_omicron_oso() -> Result<Oso, anyhow::Error> {
    let mut oso = Oso::new();
    let classes = [
        Action::get_polar_class(),
        AnyActor::get_polar_class(),
        AuthenticatedActor::get_polar_class(),
        Database::get_polar_class(),
        Fleet::get_polar_class(),
        Organization::get_polar_class(),
        Project::get_polar_class(),
        ProjectResource::get_polar_class(),
    ];
    for c in classes {
        oso.register_class(c).context("registering class")?;
    }
    oso.load_str(OMICRON_AUTHZ_CONFIG)
        .context("loading built-in Polar (Oso) config")?;
    Ok(oso)
}

//
// Helper types
// See the note above about why we don't use derive(PolarClass).
//

/// Describes an action being authorized
///
/// There's currently just one enum of Actions for all of Omicron.  We expect
/// most objects to support mosty the same set of actions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Query, // only used for [`Database`]
    Read,
    Modify,
    Delete,
    ListChildren,
    CreateChild,
}

impl oso::PolarClass for Action {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .name("Action")
            .set_equality_check(|a1, a2| a1 == a2)
            .add_method("to_perm", |a: &Action| {
                match a {
                    Action::Query => Perm::Query,
                    Action::Read => Perm::Read,
                    Action::Modify => Perm::Modify,
                    Action::Delete => Perm::Modify,
                    Action::ListChildren => Perm::ListChildren,
                    Action::CreateChild => Perm::CreateChild,
                }
                .to_string()
            })
    }
}

/// Describes a permission used in the Polar configuration
///
/// Note that Polar (appears to) require that all permissions actually be
/// strings in the configuration.  This type is used only in Rust.  It doesn't
/// even impl [`PolarClass`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Perm {
    Query, // Only for [`Database`]
    Read,
    Modify,
    ListChildren,
    CreateChild,
}

impl fmt::Display for Perm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // This implementation MUST be kept in sync with the Polar configuration
        // for Omicron, which uses literal strings for permissions.
        f.write_str(match self {
            Perm::Query => "query",
            Perm::Read => "read",
            Perm::Modify => "modify",
            Perm::ListChildren => "list_children",
            Perm::CreateChild => "create_child",
        })
    }
}

/// Represents [`authn::Context`] (which is either an authenticated or
/// unauthenticated actor) for Polar
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnyActor {
    authenticated: bool,
    actor_id: Option<Uuid>,
}

impl oso::PolarClass for AnyActor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .name("AnyActor")
            .set_equality_check(|a1: &AnyActor, a2: &AnyActor| a1 == a2)
            .add_attribute_getter("authenticated", |a: &AnyActor| {
                a.authenticated
            })
            .add_attribute_getter("authn_actor", |a: &AnyActor| {
                a.actor_id.map(|actor_id| AuthenticatedActor { actor_id })
            })
    }
}

impl From<&authn::Context> for AnyActor {
    fn from(authn: &authn::Context) -> Self {
        let actor = authn.actor();
        AnyActor {
            authenticated: actor.is_some(),
            actor_id: actor.map(|a| a.0),
        }
    }
}

/// Represents an authenticated [`authn::Context`] for Polar
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticatedActor {
    actor_id: Uuid,
}

impl oso::PolarClass for AuthenticatedActor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .add_attribute_getter("is_test_user", |a: &AuthenticatedActor| {
                a.actor_id.to_string() == authn::TEST_USER_UUID_PRIVILEGED
            })
            .set_equality_check(
                |a1: &AuthenticatedActor, a2: &AuthenticatedActor| a1 == a2,
            )
            .add_attribute_getter("id", |a: &AuthenticatedActor| {
                a.actor_id.to_string()
            })
    }
}

impl From<&authn::Actor> for AuthenticatedActor {
    fn from(omicron_actor: &authn::Actor) -> Self {
        AuthenticatedActor { actor_id: omicron_actor.0 }
    }
}

//
// Newtypes for model types that are exposed to Polar
// These all impl [`oso::PolarClass`].
// See the note above about why we use newtypes and why we don't use
// derive(PolarClass).
//
/// Represents the whole Oxide fleet to Polar (used essentially to mean
/// "global").  See RFD 24.
#[derive(Clone, Copy, Debug, Eq, PartialEq, oso::PolarClass)]
pub struct Fleet;
pub const FLEET: Fleet = Fleet;

/// Wraps [`db::model::Organization`] for Polar
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Organization {
    organization_id: Uuid,
}

impl oso::PolarClass for Organization {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .set_equality_check(|a1, a2| a1 == a2)
            .add_attribute_getter("fleet", |_: &Organization| Fleet {})
    }
}

impl From<&db::model::Organization> for Organization {
    fn from(omicron_organization: &db::model::Organization) -> Self {
        Organization { organization_id: omicron_organization.id() }
    }
}

/// Wraps [`db::model::Project`] for Polar
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Project {
    organization_id: Uuid,
    project_id: Uuid,
}

impl oso::PolarClass for Project {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .set_equality_check(|a1, a2| a1 == a2)
            .add_attribute_getter("organization", |p: &Project| Organization {
                organization_id: p.organization_id,
            })
    }
}

impl From<&db::model::Project> for Project {
    fn from(omicron_project: &db::model::Project) -> Self {
        Project {
            organization_id: omicron_project.organization_id,
            project_id: omicron_project.id(),
        }
    }
}

impl Project {
    fn resource(
        &self,
        resource_type: ResourceType,
        resource_id: Uuid,
    ) -> ProjectResource {
        ProjectResource {
            organization_id: self.organization_id,
            project_id: self.project_id,
            resource_type,
            resource_id,
        }
    }
}

/// Wraps any resource that lives inside a Project for Polar
///
/// This would include [`db::model::Instance`], [`db::model::Disk`], etc.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectResource {
    organization_id: Uuid,
    project_id: Uuid,
    resource_type: ResourceType,
    resource_id: Uuid,
}

impl oso::PolarClass for ProjectResource {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .set_equality_check(|a1, a2| a1 == a2)
            .add_attribute_getter("project", |pr: &ProjectResource| Project {
                organization_id: pr.organization_id,
                project_id: pr.project_id,
            })
    }
}

/// Represents the database itself to Polar (so that we can have roles with no
/// access to the database at all)
#[derive(Clone, Copy, Debug, Eq, PartialEq, oso::PolarClass)]
pub struct Database;
pub const DATABASE: Database = Database;
