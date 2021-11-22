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
        ProjectChild::get_polar_class(),
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

impl AuthenticatedActor {
    /**
     * Returns whether this actor has the given role for the given resource
     */
    fn has_role_resource(
        &self,
        _resource_type: ResourceType,
        _resource_id: Uuid,
        _role: &str,
    ) -> bool {
        /*
         * TODO We will probably want to either pre-load the list of roles that
         * the actor has for this resource or else at this point we will go
         * fetch them from the database.
         */
        self.actor_id.to_string() == authn::TEST_USER_UUID_PRIVILEGED
    }

    /**
     * Returns whether this actor has the given role for a fleet
     */
    /*
     * This is special-cased because Fleets don't exist in the database and do
     * not have an id.  It might be a good idea to put them in the database with
     * their own id.  But it's not needed for anything right now.  (It might
     * also mean that many authz checks would need to do an extra database query
     * to load the fleet_id from the Organization.)
     */
    fn has_role_fleet(&self, _fleet: &Fleet, _role: &str) -> bool {
        self.actor_id.to_string() == authn::TEST_USER_UUID_PRIVILEGED
    }
}

impl oso::PolarClass for AuthenticatedActor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fleet;
pub const FLEET: Fleet = Fleet;

impl Fleet {
    pub fn organization(&self, organization_id: Uuid) -> Organization {
        Organization { organization_id }
    }
}

impl oso::PolarClass for Fleet {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder().set_equality_check(|a1, a2| a1 == a2).add_method(
            "has_role",
            |fleet: &Fleet, actor: AuthenticatedActor, role: String| {
                actor.has_role_fleet(fleet, &role)
            },
        )
    }
}

/// Wraps [`db::model::Organization`] for Polar
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Organization {
    organization_id: Uuid,
}

impl Organization {
    pub fn project(&self, project_id: Uuid) -> Project {
        Project { organization_id: self.organization_id, project_id }
    }
}

impl oso::PolarClass for Organization {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .set_equality_check(|a1, a2| a1 == a2)
            .add_method(
                "has_role",
                |o: &Organization, actor: AuthenticatedActor, role: String| {
                    actor.has_role_resource(
                        ResourceType::Organization,
                        o.organization_id,
                        &role,
                    )
                },
            )
            .add_attribute_getter("fleet", |_: &Organization| Fleet {})
    }
}

impl From<&db::model::Organization> for Organization {
    fn from(omicron_organization: &db::model::Organization) -> Self {
        Organization { organization_id: omicron_organization.id() }
    }
}

/// Wraps [`db::model::Project`] for Polar
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Project {
    organization_id: Uuid,
    project_id: Uuid,
}

impl Project {
    pub fn resource(
        &self,
        resource_type: ResourceType,
        resource_id: Uuid,
    ) -> ProjectChild {
        ProjectChild {
            organization_id: self.organization_id,
            project_id: self.project_id,
            resource_type,
            resource_id,
        }
    }
}

impl oso::PolarClass for Project {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .set_equality_check(|a1, a2| a1 == a2)
            .add_method(
                "has_role",
                |p: &Project, actor: AuthenticatedActor, role: String| {
                    actor.has_role_resource(
                        ResourceType::Project,
                        p.project_id,
                        &role,
                    )
                },
            )
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

/// Wraps any resource that lives inside a Project for Polar
///
/// This would include [`db::model::Instance`], [`db::model::Disk`], etc.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectChild {
    organization_id: Uuid,
    project_id: Uuid,
    resource_type: ResourceType,
    resource_id: Uuid,
}

impl ProjectChild {
    pub fn id(&self) -> &Uuid {
        &self.resource_id
    }
}

impl oso::PolarClass for ProjectChild {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .set_equality_check(|a1, a2| a1 == a2)
            .add_method(
                "has_role",
                |pr: &ProjectChild, actor: AuthenticatedActor, role: String| {
                    actor.has_role_resource(
                        pr.resource_type,
                        pr.resource_id,
                        &role,
                    )
                },
            )
            .add_attribute_getter("project", |pr: &ProjectChild| Project {
                organization_id: pr.organization_id,
                project_id: pr.project_id,
            })
    }
}

/// Represents the database itself to Polar (so that we can have roles with no
/// access to the database at all)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Database;
pub const DATABASE: Database = Database;

impl oso::PolarClass for Database {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder().add_method(
            "has_role",
            |_d: &Database, _actor: AuthenticatedActor, role: String| {
                assert_eq!(role, "user");
                true
            },
        )
    }
}
