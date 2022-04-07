// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authz types for resources in the API hierarchy

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

use super::actor::AnyActor;
use super::context::AuthorizedResource;
use super::roles::{
    load_roles_for_resource, load_roles_for_resource_tree, RoleSet,
};
use super::Action;
use super::{actor::AuthenticatedActor, Authz};
use crate::authn;
use crate::context::OpContext;
use crate::db::fixed_data::FLEET_ID;
use crate::db::DataStore;
use futures::future::BoxFuture;
use futures::FutureExt;
use omicron_common::api::external::{Error, LookupType, ResourceType};
use uuid::Uuid;

/// Describes an authz resource that corresponds to an API resource that has a
/// corresponding ResourceType and is stored in the database
pub trait ApiResource: Clone + Send + Sync + 'static {
    /// If roles can be assigned to this resource, return the type and id of the
    /// database record describing this resource
    ///
    /// If roles cannot be assigned to this resource, returns `None`.
    fn db_resource(&self) -> Option<(ResourceType, Uuid)>;

    /// If this resource has a parent in the API hierarchy whose assigned roles
    /// can affect access to this resource, return the parent resource.
    /// Otherwise, returns `None`.
    fn parent(&self) -> Option<&dyn AuthorizedResource>;
}

/// Practically, all objects which implement [`ApiResourceError`]
/// also implement [`ApiResource`]. However, [`ApiResource`] is not object
/// safe because it implements [`std::clone::Clone`].
///
/// This allows callers to use [`ApiResourceError`] as a trait object.
pub trait ApiResourceError {
    /// Returns an error as though this resource were not found, suitable for
    /// use when an actor should not be able to see that this resource exists
    fn not_found(&self) -> Error;
}

impl<T: ApiResource + ApiResourceError + oso::PolarClass> AuthorizedResource
    for T
{
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
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
        load_roles_for_resource_tree(self, opctx, datastore, authn, roleset)
            .boxed()
    }

    fn on_unauthorized(
        &self,
        authz: &Authz,
        error: Error,
        actor: AnyActor,
        action: Action,
    ) -> Error {
        if action == Action::Read {
            return self.not_found();
        }

        // If the user failed an authz check, and they can't even read this
        // resource, then we should produce a 404 rather than a 401/403.
        match authz.is_allowed(&actor, Action::Read, self) {
            Err(error) => Error::internal_error(&format!(
                "failed to compute read authorization to determine visibility: \
                {:#}",
                error
            )),
            Ok(false) => self.not_found(),
            Ok(true) => error,
        }
    }
}

/// Represents the Oxide fleet for authz purposes
///
/// Fleet-level resources are essentially global.  See RFD 24 for more on
/// Fleets.
///
/// This object is used for authorization checks on a Fleet by passing it as the
/// `resource` argument to [`crate::context::OpContext::authorize()`].  You
/// typically don't construct a `Fleet` yourself -- use the global [`FLEET`].
///
/// You can perform authorization checks on children of the Fleet (e.g.,
/// Organizations) using the methods below that return authz objects
/// representing those children.
#[derive(Clone, Copy, Debug)]
pub struct Fleet;
/// Singleton representing the [`Fleet`] itself for authz purposes
pub const FLEET: Fleet = Fleet;

impl Fleet {
    /// Returns an authz resource representing a child Organization
    pub fn organization(
        &self,
        organization_id: Uuid,
        lookup_type: LookupType,
    ) -> Organization {
        Organization { organization_id, lookup_type }
    }

    /// Returns an authz resource representing some other kind of child (e.g.,
    /// a built-in user, built-in role, etc. -- but _not_ an Organization or
    /// Sled)
    ///
    /// Aside from Organizations (which you create with
    /// [`Fleet::organization()`] instead), all instances of all types of Fleet
    /// children are treated interchangeably by the authz subsystem.  That's
    /// because we do not currently support assigning roles to these resources,
    /// so all that matters for authz is that they are a child of the Fleet.
    pub fn child_generic(
        &self,
        resource_type: ResourceType,
        lookup_type: LookupType,
    ) -> FleetChild {
        FleetChild { resource_type, lookup_type }
    }

    /// Returns an authz resource representing a Sled
    pub fn sled(&self, sled_id: Uuid, lookup_type: LookupType) -> Sled {
        Sled { sled_id, lookup_type }
    }
}

impl Eq for Fleet {}
impl PartialEq for Fleet {
    fn eq(&self, _: &Self) -> bool {
        // There is only one Fleet.
        true
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

impl AuthorizedResource for Fleet {
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> futures::future::BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f,
    {
        load_roles_for_resource(
            opctx,
            datastore,
            authn,
            ResourceType::Fleet,
            *FLEET_ID,
            roleset,
        )
        .boxed()
    }

    fn on_unauthorized(
        &self,
        _: &Authz,
        error: Error,
        _: AnyActor,
        _: Action,
    ) -> Error {
        error
    }
}

/// Represents any child of a Fleet _except_ for Organizations for authz
/// purposes
///
/// This includes [`crate::db::model::UserBuiltin`], similar authz-related
/// resources, and potentially future resources describing hardware.
///
/// This object is used for authorization checks on such resources by passing
/// this as the `resource` argument to
/// [`crate::context::OpContext::authorize()`].  You construct one of these
/// using [`Fleet::child_generic()`].
// We do not currently store any state here because we don't do anything with
// it, so if callers provided it, we'd have no way to test that it was correct.
// If we wind up supporting attaching roles to these, then we could add a
// resource type and id like we have with ProjectChild.
#[derive(Clone, Debug)]
pub struct FleetChild {
    resource_type: ResourceType,
    lookup_type: LookupType,
}

impl oso::PolarClass for FleetChild {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .add_method(
                "has_role",
                // Roles are not supported on FleetChilds today.
                |_: &FleetChild, _: AuthenticatedActor, _: String| false,
            )
            .add_attribute_getter("fleet", |_: &FleetChild| FLEET)
    }
}

impl ApiResource for FleetChild {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        None
    }

    fn parent(&self) -> Option<&dyn AuthorizedResource> {
        Some(&FLEET)
    }
}

impl ApiResourceError for FleetChild {
    fn not_found(&self) -> Error {
        self.lookup_type.clone().into_not_found(self.resource_type)
    }
}

/// Represents a Sled for authz purposes
///
/// This object is used for authorization checks on such resources by passing
/// this as the `resource` argument to
/// [`crate::context::OpContext::authorize()`].  You construct one of these
/// using [`Fleet::sled()`].
#[derive(Clone, Debug)]
pub struct Sled {
    sled_id: Uuid,
    lookup_type: LookupType,
}

impl Sled {
    pub fn id(&self) -> Uuid {
        self.sled_id
    }
}

impl oso::PolarClass for Sled {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .add_method(
                "has_role",
                // Roles are not supported on Sleds today.
                |_: &Sled, _: AuthenticatedActor, _: String| false,
            )
            .add_attribute_getter("fleet", |_: &Sled| FLEET)
    }
}

impl ApiResource for Sled {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        None
    }

    fn parent(&self) -> Option<&dyn AuthorizedResource> {
        Some(&FLEET)
    }
}

impl ApiResourceError for Sled {
    fn not_found(&self) -> Error {
        self.lookup_type.clone().into_not_found(ResourceType::Sled)
    }
}

/// Represents a [`crate::db::model::Organization`] for authz purposes
///
/// This object is used for authorization checks on an Organization by passing
/// it as the `resource` argument to [`crate::context::OpContext::authorize()`].
/// You typically construct one of these with [`Fleet::organization()`].  You
/// can perform authorization checks on children of the Organization (e.g.,
/// Projects) using one of the methods below that return authz objects
/// representing those children.
#[derive(Clone, Debug)]
pub struct Organization {
    organization_id: Uuid,
    lookup_type: LookupType,
}

impl Organization {
    // TODO-cleanup It seems like we shouldn't need this.  It's currently used
    // for compatibility so we don't have to change a ton of callers as we
    // iterate on the lookup APIs.
    pub fn id(&self) -> Uuid {
        self.organization_id
    }

    /// Returns an authz resource representing a child Project
    pub fn project(
        &self,
        project_id: Uuid,
        lookup_type: LookupType,
    ) -> Project {
        Project { parent: self.clone(), project_id, lookup_type }
    }
}

impl Eq for Organization {}
impl PartialEq for Organization {
    fn eq(&self, other: &Self) -> bool {
        self.organization_id == other.organization_id
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

impl ApiResource for Organization {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        Some((ResourceType::Organization, self.organization_id))
    }

    fn parent(&self) -> Option<&dyn AuthorizedResource> {
        Some(&FLEET)
    }
}

impl ApiResourceError for Organization {
    fn not_found(&self) -> Error {
        self.lookup_type.clone().into_not_found(ResourceType::Organization)
    }
}

/// Represents a [`crate::db::model::Project`] for authz purposes
///
/// This object is used for authorization checks on a Project by passing it as
/// the `resource` argument to [`crate::context::OpContext::authorize()`].  You
/// typically construct one of these with [`Organization::project()`].  You can
/// perform authorization checks on children of the Project (e.g., Instances)
/// using one of the methods below that return authz objects representing those
/// children.
#[derive(Clone, Debug)]
pub struct Project {
    parent: Organization,
    project_id: Uuid,
    lookup_type: LookupType,
}

impl Project {
    // TODO-cleanup see note on Organization::id above.
    pub fn id(&self) -> Uuid {
        self.project_id
    }

    /// Returns an authz resource representing any child of this Project (e.g.,
    /// an Instance or Disk)
    ///
    /// All instances of all types of Project children are treated
    /// interchangeably by the authz subsystem.  That's because we do not
    /// currently support assigning roles to these resources, so all that
    /// matters for authz is that they are a child of a particular Project.
    pub fn child_generic(
        &self,
        resource_type: ResourceType,
        resource_id: Uuid,
        lookup_type: LookupType,
    ) -> ProjectChild {
        ProjectChild {
            parent: ProjectChildKind::Direct(self.clone()),
            resource_type,
            resource_id,
            lookup_type,
        }
    }

    pub fn organization(&self) -> &Organization {
        &self.parent
    }
}

impl Eq for Project {}
impl PartialEq for Project {
    fn eq(&self, other: &Self) -> bool {
        self.project_id == other.project_id
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
            .add_attribute_getter("organization", |p: &Project| {
                p.parent.clone()
            })
    }
}

impl ApiResource for Project {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        Some((ResourceType::Project, self.project_id))
    }

    fn parent(&self) -> Option<&dyn AuthorizedResource> {
        Some(&self.parent)
    }
}

impl ApiResourceError for Project {
    fn not_found(&self) -> Error {
        self.lookup_type.clone().into_not_found(ResourceType::Project)
    }
}

/// Represents any child of a Project for authz purposes
///
/// This includes [`crate::db::model::Instance`], [`crate::db::model::Disk`],
/// etc.
///
/// This object is used for authorization checks on such resources by passing
/// this as the `resource` argument to
/// [`crate::context::OpContext::authorize()`].  You construct one of these
/// using [`Project::child_generic()`].
#[derive(Clone, Debug)]
pub struct ProjectChild {
    parent: ProjectChildKind,
    resource_type: ResourceType,
    resource_id: Uuid,
    lookup_type: LookupType,
}

#[derive(Clone, Debug)]
enum ProjectChildKind {
    Direct(Project),
    Indirect(Box<ProjectChild>),
}

impl ProjectChild {
    pub fn id(&self) -> Uuid {
        self.resource_id
    }

    pub fn project(&self) -> &Project {
        match &self.parent {
            ProjectChildKind::Direct(p) => p,
            ProjectChildKind::Indirect(p) => p.project(),
        }
    }

    /// Returns an authz resource representing a child of this Project child.
    ///
    /// This is currently only used for children of Vpc, which include
    /// VpcSubnets.
    // TODO-cleanup It would be more type-safe to have a more explicit resource
    // hierarchy -- i.e., Project -> Vpc -> VpcSubnet.  However, it would also
    // mean a bunch more boilerplate and it'd be more confusing: you'd have
    // Projects with children Vpc _or_ ProjectChild.
    pub fn child_generic(
        &self,
        resource_type: ResourceType,
        resource_id: Uuid,
        lookup_type: LookupType,
    ) -> ProjectChild {
        ProjectChild {
            parent: ProjectChildKind::Indirect(Box::new(self.clone())),
            resource_type,
            resource_id,
            lookup_type,
        }
    }
}

impl oso::PolarClass for ProjectChild {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
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
            .add_attribute_getter("project", |pr: &ProjectChild| {
                pr.project().clone()
            })
    }
}

impl ApiResource for ProjectChild {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        // We do not support assigning roles to children of Projects.
        None
    }

    fn parent(&self) -> Option<&dyn AuthorizedResource> {
        match &self.parent {
            ProjectChildKind::Direct(p) => Some(p),
            ProjectChildKind::Indirect(p) => Some(p.as_ref()),
        }
    }
}

impl ApiResourceError for ProjectChild {
    fn not_found(&self) -> Error {
        self.lookup_type.clone().into_not_found(self.resource_type)
    }
}

pub type Disk = ProjectChild;
pub type Instance = ProjectChild;
pub type RouterRoute = ProjectChild;
pub type Vpc = ProjectChild;
pub type VpcRouter = ProjectChild;
pub type VpcSubnet = ProjectChild;
pub type NetworkInterface = ProjectChild;
