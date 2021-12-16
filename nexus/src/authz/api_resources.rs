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

use super::actor::AuthenticatedActor;
use super::roles::AuthzApiResource;
use super::roles::AuthzResource;
use crate::db::fixed_data::FLEET_ID;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fleet;
pub const FLEET: Fleet = Fleet;

impl Fleet {
    /// Returns an authz resource representing a child Organization
    pub fn organization(&self, organization_id: Uuid) -> Organization {
        Organization { organization_id }
    }

    /// Returns an authz resource representing some other kind of child (e.g.,
    /// a built-in user, built-in role, etc. -- but _not_ an Organization)
    ///
    /// Aside from Organizations (which you create with [`Fleet::organization()`
    /// instead), all instances of all types of Fleet children are treated
    /// interchangeably by the authz subsystem.  That's because we do not
    /// currently support assigning roles to these resources, so all that
    /// matters for authz is that they are a child of the Fleet.
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

impl AuthzApiResource for Fleet {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        Some((ResourceType::Fleet, *FLEET_ID))
    }

    fn parent(&self) -> Option<Box<dyn AuthzResource>> {
        None
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

impl AuthzApiResource for FleetChild {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        None
    }

    fn parent(&self) -> Option<Box<dyn AuthzResource>> {
        Some(Box::new(Fleet {}))
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Organization {
    organization_id: Uuid,
}

impl Organization {
    // TODO-cleanup It seems like we shouldn't need this.  It's currently used
    // for compatibility so we don't have to change a ton of callers as we
    // iterate on the lookup APIs.
    pub fn id(&self) -> Uuid {
        self.organization_id
    }

    /// Returns an authz resource representing a child Project
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

impl AuthzApiResource for Organization {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        Some((ResourceType::Organization, self.organization_id))
    }

    fn parent(&self) -> Option<Box<dyn AuthzResource>> {
        Some(Box::new(Fleet {}))
    }
}

/// Represents a [`db::model::Project`] for authz purposes
///
/// This object is used for authorization checks on a Project by passing it as
/// the `resource` argument to [`crate::context::OpContext::authorize()`].  You
/// typically construct one of these with [`Organization::project()`].  You can
/// perform authorization checks on children of the Project (e.g., Instances)
/// using one of the methods below that return authz objects representing those
/// children.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Project {
    organization_id: Uuid,
    project_id: Uuid,
}

impl Project {
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

impl AuthzApiResource for Project {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        Some((ResourceType::Project, self.project_id))
    }

    fn parent(&self) -> Option<Box<dyn AuthzResource>> {
        Some(Box::new(Organization { organization_id: self.organization_id }))
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

impl AuthzApiResource for ProjectChild {
    fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
        // We do not support assigning roles to children of Projects.
        None
    }

    fn parent(&self) -> Option<Box<dyn AuthzResource>> {
        Some(Box::new(Project {
            project_id: self.project_id,
            organization_id: self.organization_id,
        }))
    }
}
