// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Authorization subsystem
//!
//! ## Authorization basics
//!
//! Most of our external authorization policy is expressed in terms of
//! role-based access control (RBAC), meaning that an *actor* can perform
//! an *action* on a *resource* if the actor is associated with a *role* on the
//! resource that grants *permissions* for the action.  Let's unpack that.
//!
//! - **actor** is a built-in user, a service account, or a user from the
//!   customer's Identity Provider (IdP, such as company LDAP or Active
//!   Directory or the like).
//! - **resource** is usually an API resource, like a Project or Instance.
//! - **action** is usually one of a handful of things like "modify", "delete",
//!   or "create a child resource".  Actions are nearly the same as
//!   **permissions**.  The set of actions is fixed by the system.
//! - **role** is just a set of permissions.  Currently, only built-in roles are
//!   supported.
//!
//! The Oso **policy** determines what roles grant what permissions.  This is
//! currently baked into Nexus (via the Oso policy file) and cannot be changed
//! at runtime.
//!
//! The Oso policy defines rules saying things like:
//!
//! - a user can perform an action on a resource if they have a role that grants
//!   the corresponding permission on that resource
//! - for Projects, the "viewer" role is granted the "read" permission
//! - for Projects, the "viewer" role is granted to anyone with the
//!   "collaborator" role
//! - Projects have a "parent" relationship with a Silo, such that
//!   someone with the "admin" role on the parent Silo automatically
//!   gets the "admin" role on the Project
//!
//! These are just examples.  To make them more concrete, suppose we have:
//!
//! - a Silo "Sesame-Street"
//! - a Project "monster-foodies"
//! - three users:
//!   - "cookie-monster", who has been explicitly granted the "viewer" role on
//!     Project "monster-foodies"
//!   - "Gonger", who has been explicitly granted the "collaborator" role on
//!     Project "monster-foodies"
//!   - "big-bird", who has been explicitly granted the "admin" role on the
//!     Silo "Sesame-Street"
//!
//! All three users have the "read" permission on the Project by virtue of their
//! "viewer" role on the Project.  But the path to determining that varies:
//!
//! - Cookie Monster has explicitly been granted the "viewer" role on this
//!   Project.
//! - Gonger has the "collaborator" role on the Project, which the policy says
//!   implicitly grants the "viewer" role.
//! - Big Bird has the "admin" role on the parent Silo, which the policy
//!   says implicitly grants the "collaborator" role on the Project, which
//!   (again) is granted the "viewer" role on the Project.
//!
//! So to determine if someone has access, we wind up checking for a variety of
//! roles on several different objects.  Oso does the tricky parts.  We plug
//! into the Oso policy at various points.  A key integration point is
//! determining whether an actor has a _particular_ role on a _particular_
//! resource.
//!
//! ## Role lookup
//!
//! Users and API resources are stored in the database, as is the relationship
//! that says a particular user has a particular role for a particular resource.
//!
//! Suppose a built-in user "cookie-monster" has the "viewer" role for a Project
//! "monster-foodies".  It looks like this:
//!
//! ```text
//! +---> table: "project"
//! |     +-------------------------+-----+
//! |     |  id | name              | ... |
//! |     +-------------------------------+
//! |   +-> 123 | "monster-foodies" | ... |
//! |   | +-------------------------+-----+
//! |   |
//! |   | table: "user_builtin"
//! |   | +------------------------------+
//! |   | |  id | name             | ... |
//! |   | +------------------------------|
//! |   | | 234 | "cookie-monster" | ... |
//! |   | +--^---------------------------+
//! |   |    |
//! |   |    +---------------------------------------------------------------+
//! |   |                                                                    |
//! |   | table: "role_assignment"                                           |
//! |   | (assigns roles to users on arbitrary resources)                    |
//! |   | +---------------+-----------+-------------+-------------+---+      |
//! |   | | resource_type | role_name | resource_id | identity_id |...|      |
//! |   | +---------------+-----------+-------------+-------------+---+      |
//! |   | | "project "    | "viewer"  |         123 |         234 |...|      |
//! |   | +--^------------+--^--------+----------^--+-----------^-+---+      |
//! |   |    |                                   |              |            |
//! +---|----+                                   |              +------------+
//!     |                                        |
//!     +----------------------------------------+
//! ```
//!
//! This record means that user "cookie-monster" has the "viewer" role on the
//! "project" with id 123.  (Note that ids are really uuids, and some of these
//! tables have other columns.)  See the `roles` module for more details on
//! how we find these records and make them available for the authz check.
//!
//! Built-in users are only one possible target for role assignments.  IdP users
//! (Silo users) an also be assigned roles.  This all works the same way, except
//! that in that case `role_assignment.identity_id` refers to an entry in the
//! `silo_user` table rather than `user_builtin`.  How do we know the
//! difference?  There's also an `identity_type` column in the "role_assignment"
//! table that specifies which foreign table contains the identity.  It would
//! have value `silo_user`.
//!
//! ## Authorization control flow
//!
//! Suppose we receive a request from Abby to modify Project "monster-foodies".
//! How do we know if the request is authorized?  The project fetch code checks
//! whether the actor (`Abby`) can perform the "modify" [`crate::authz::Action`]
//! for a resource of type `Project` with id `234` (the id of the
//! "monster-foodies" Project).  Then:
//!
//! 1. The authorization subsystem loads all of Abby's roles related to this
//!    resource.  Much more on this in the `roles` submodule.
//! 2. The authorization subsystem asks Oso whether the action should be
//!    allowed.  Oso answers this based on the policy we've defined.  As part of
//!    evaluating this:
//!    1. The policy determines which permission is required for
//!       this action.  Currently, actions and permissions are nearly
//!       identical -- to perform the "modify" action, you need the "modify"
//!       permission.
//!    2. The policy says that the actor can perform this action on the
//!       resource if they have a role granting the corresponding permission
//!       on the resource.
//!    3. Oso checks:
//!       1. whether the user has any role for this resource that grants
//!          the permission (e.g., does the user have a "collaborator"
//!          role that grants the "modify" permission?)
//!       2. whether the user has a role for this resource that grants
//!          another role that grants the permission (e.g., does the
//!          user have an "admin" role that grants the "collaborator"
//!          role that grants the "modify" permission?)
//!       3. for every relationship between this resource and another
//!          resource, whether the user has a role on the other resource
//!          that grants the permission on this resource (e.g., does
//!          the user have a "silo admin" role on the parent
//!          silo that grants the "admin" role on the Project
//!          that grants the "modify" permission)
//!
//! Each of these role lookups uses data that we provide to Oso.  (Again, more
//! in `roles` about how this is set up.)  If Oso finds a role granting this
//! permission that's associated with this actor and resource, the action is
//! allowed.  Otherwise, it's not.

mod actor;
pub use actor::AnyActor;
pub use actor::AuthenticatedActor;

mod api_resources;
pub use api_resources::*;

mod context;
pub use context::AuthorizedResource;
pub use context::Authz;
pub use context::Context;

mod oso_generic;
pub use oso_generic::Action;
pub use oso_generic::DATABASE;
pub use oso_generic::Database;

mod roles;
pub use roles::RoleSet;
