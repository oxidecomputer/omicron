// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types and impls used for integration with Oso

// Most of the types here are used in the Polar configuration, which means they
// must impl [`oso::PolarClass`].  There is a derive(PolarClass), but it's
// pretty limited: It also doesn't let you define methods.  We
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
use crate::authn::USER_DB_INIT;
use crate::context::OpContext;
use crate::db;
use crate::db::fixed_data::FLEET_ID;
use crate::db::identity::Resource;
use crate::db::DataStore;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::FutureExt;
use omicron_common::api::external::Error;
use omicron_common::api::external::ResourceType;
use oso::Oso;
use oso::PolarClass;
use std::collections::BTreeSet;
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
        FleetChild::get_polar_class(),
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
        oso::Class::builder().set_equality_check(|a1, a2| a1 == a2).add_method(
            "to_perm",
            |a: &Action| {
                match a {
                    Action::Query => Perm::Query,
                    Action::Read => Perm::Read,
                    Action::Modify => Perm::Modify,
                    Action::Delete => Perm::Modify,
                    Action::ListChildren => Perm::ListChildren,
                    Action::CreateChild => Perm::CreateChild,
                }
                .to_string()
            },
        )
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
    fn has_role_resource(
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
            && self.actor_id == USER_DB_INIT.id)
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

pub trait AuthzResource: Send + Sync + 'static {
    fn fetch_all_related_roles_for_user<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f;
}

#[derive(Clone, Debug)]
pub struct RoleSet {
    roles: BTreeSet<(ResourceType, Uuid, String)>,
}

impl RoleSet {
    pub fn new() -> RoleSet {
        RoleSet { roles: BTreeSet::new() }
    }

    fn has_role(
        &self,
        resource_type: ResourceType,
        resource_id: Uuid,
        role_name: &str,
    ) -> bool {
        self.roles.contains(&(
            resource_type,
            resource_id,
            role_name.to_string(),
        ))
    }

    fn insert(
        &mut self,
        resource_type: ResourceType,
        resource_id: Uuid,
        role_name: &str,
    ) {
        self.roles.insert((
            resource_type,
            resource_id,
            String::from(role_name),
        ));
    }
}

// XXX document
// XXX should this be part of the Oso policy file?
pub trait PermTarget: Send + Sync + 'static {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)>;
    fn parent(&self) -> Option<Box<dyn AuthzResource>>;
}

impl<T: PermTarget> AuthzResource for T {
    fn fetch_all_related_roles_for_user<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        async move {
            if let Some(actor_id) = authn.actor() {
                if let Some((resource_type, resource_id)) = self.db_resource() {
                    trace!(opctx.log, "loading roles";
                        "actor_id" => actor_id.0.to_string(),
                        "resource_type" => ?resource_type,
                        "resource_id" => resource_id.to_string(),
                    );
                    let roles = datastore
                        .role_asgn_builtin_list_for(
                            opctx,
                            actor_id.0,
                            resource_type,
                            resource_id,
                        )
                        .await?;
                    for role_asgn in roles {
                        assert_eq!(
                            resource_type.to_string(),
                            role_asgn.resource_type
                        );
                        trace!(opctx.log, "found role";
                            "actor_id" => actor_id.0.to_string(),
                            "resource_type" => ?resource_type,
                            "resource_id" => resource_id.to_string(),
                            "role_name" => &role_asgn.role_name
                        );

                        roleset.insert(
                            resource_type,
                            resource_id,
                            &role_asgn.role_name,
                        );
                    }
                }

                if let Some(parent) = self.parent() {
                    parent
                        .fetch_all_related_roles_for_user(
                            opctx, datastore, authn, roleset,
                        )
                        .await?;
                }
            }

            Ok(())
        }
        .boxed()
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

    pub fn child_generic(&self) -> FleetChild {
        FleetChild {}
    }
}

impl oso::PolarClass for Fleet {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder().with_equality_check().add_method(
            "has_role",
            |_: &Fleet, actor: AuthenticatedActor, role: String| {
                actor.has_role_resource(ResourceType::Fleet, *FLEET_ID, &role)
            },
        )
    }
}

impl PermTarget for Fleet {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        Some((ResourceType::Fleet, *FLEET_ID))
    }

    fn parent(&self) -> Option<Box<dyn AuthzResource>> {
        None
    }
}

/// Wraps [`db::model::Organization`] for Polar
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Organization {
    organization_id: Uuid,
}

impl Organization {
    pub fn id(&self) -> Uuid {
        self.organization_id
    }

    pub fn project(&self, project_id: Uuid) -> Project {
        Project { organization_id: self.organization_id, project_id }
    }
}

impl oso::PolarClass for Organization {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
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

impl PermTarget for Organization {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        Some((ResourceType::Organization, self.organization_id))
    }

    fn parent(&self) -> Option<Box<dyn AuthzResource>> {
        Some(Box::new(Fleet {}))
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
            .with_equality_check()
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

impl PermTarget for Project {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        Some((ResourceType::Project, self.project_id))
    }

    fn parent(&self) -> Option<Box<dyn AuthzResource>> {
        Some(Box::new(Organization { organization_id: self.organization_id }))
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
            .with_equality_check()
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

impl PermTarget for ProjectChild {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        // TODO This is perhaps surprising.  But the behavior we want from this
        // function is that it returns a Some value iff it's possible to assign
        // a _role_ to this resource.  That's not true for these resources.
        None
    }

    fn parent(&self) -> Option<Box<dyn AuthzResource>> {
        Some(Box::new(Project {
            project_id: self.project_id,
            organization_id: self.organization_id,
        }))
    }
}

/// Wraps any resource that lives inside a Fleet but outside an Organization
///
/// This includes [`db::model::UserBuiltin`] and future resources describing
/// hardware.
// We do not currently store any state here because we don't do anything with
// it, so if callers provided it, we'd have no way to test that it was correct.
// If we wind up supporting attaching roles to these, then we could add a
// resource type and id like we have with ProjectChild.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FleetChild {}

impl oso::PolarClass for FleetChild {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_method(
                "has_role",
                /* Roles are not supported on FleetChilds today. */
                |_: &FleetChild, _: AuthenticatedActor, _: String| false,
            )
            .add_attribute_getter("fleet", |_: &FleetChild| Fleet {})
    }
}

// XXX
impl PermTarget for FleetChild {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        None
    }

    fn parent(&self) -> Option<Box<dyn AuthzResource>> {
        Some(Box::new(Fleet {}))
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

// XXX consider
// - right now, we're prefetching all roles we might possibly need, sticking
//   them onto the Actor, then we impl a Polar method that looks at the data
//   structure.  We could instead have the Polar method reach out to the
//   database.  This is tricky because Polar is not async.
impl AuthzResource for Database {
    fn fetch_all_related_roles_for_user<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        _: &'b OpContext,
        _: &'c DataStore,
        _: &'d authn::Context,
        _: &'e mut RoleSet,
    ) -> BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        // XXX
        futures::future::ready(Ok(())).boxed()
    }
}
