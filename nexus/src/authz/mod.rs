// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authorization subsystem
//!
//! # Background
//!
//! Most of our external authorization policy is expressed in terms of
//! role-based access control (RBAC), meaning that an *actor* can perform
//! an *action* on a *resource* if the actor is associated with a *role* on the
//! resource that grants *permissions* for the action.  Let's unpack that.
//!
//! * **actor** is a built-in user, a service account, or in the future a user
//!   from the customer's Identity Provider (IdP, such as company LDAP or Active
//!   Directory or the like).
//! * **resource** is usually an API resource, like a Project or Instance
//! * **action** is usually one of a handful of things like "modify", "delete",
//!   or "create a child resource".  Actions are nearly the same as
//!   **permissions**.  The set of actions is fixed by the system.
//! * **role** is just a set of permissions.  Currently, only built-in roles are
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
//! - Projects have a "parent" relationship with an Organization, such that
//!   someone with the "admin" role on the parent Organization automatically
//!   gets the "viewer" role on the Project
//!
//! These are just examples.
//!
//! We plug into the Oso policy at various points.  A key integration point is
//! determining whether an actor has a particular role on a particular resource.
//! We describe this below and in the [`roles`] submodule docs.  First, let's
//! walk through the authorization process at a high level.
//!
//!
//! # Example
//!
//! Suppose we receive a request from user `A` to modify a Project `P`.  How do
//! we know if the request is authorized?  The project modify code checks
//! whether the actor (`A`) can perform the "modify" [`crate::authz::Action`]
//! for a resource of type `Project` with id `P`.  Then:
//!
//! 1. The authorization subsystem loads all of the user's roles related to this
//!    resource.  Much more on this in the [`roles`] submodule.
//! 2. The authorization subsystem asks Oso whether the action should be
//!    allowed.  Oso answers this based on the policy we've defined.  As part of
//!    evaluating this:
//!        a. The policy determines which permission is required for
//!           this action.  Currently, actions and permissions are nearly
//!           identical -- to perform the "modify" action, you need the "modify"
//!           permission.
//!        b. The policy says that the actor can perform this action on the
//!           resource if they have a role granting the corresponding permission
//!           on the resource.
//!        c. Oso checks:
//!               1. whether the user has any role for this resource that grants
//!                  the permission (e.g., does the user have a "collaborator"
//!                  role that grants the "modify" permission?)
//!               2. whether the user has a role for this resource that grants
//!                  another role that grants the permission (e.g., does the
//!                  user have an "admin" role that grants the "collaborator"
//!                  role that grants the "modify" permission?)
//!               3. for every relationship between this resource and another
//!                  resource, whether the user has a role on the other resource
//!                  that grants the permission on this resource (e.g., does
//!                  the user have an "organization admin" role on the parent
//!                  organization that grants the "admin" role on the Project
//!                  that grants the "modify" permission)
//!
//! If Oso finds a role granting this permission that's associated with this
//! actor and resource, the action is allowed.  Otherwise, it's not.
//!
//!
//! # Database representations
//!
//! Built-in users and built-in roles are stored in the database.  API resources
//! (e.g., Projects) are also stored in the database.  (Eventually, other kinds
//! of users and service accounts will live in the database, too.)
//!
//! Suppose a built-in user "ursula" has the "modify" permission for a Project
//! "bear-cubs".
//!
//! ```text
//! +---> table: "project"
//! |     +-------------------+-----+
//! |     |  id | name        | ... |
//! |     +-------------------+-----+
//! |   +-> 123 | "bear-cubs" | ... |
//! |   | +-------------------+-----+
//! |   |
//! |   | table: "user_builtin"
//! |   | +-------------------+-----+
//! |   | |  id | name        | ... |
//! |   | +-------------------+-----+
//! |   | | 234 | "ursula"    | ... |
//! |   | +--^----------------+-----+
//! |   |    |
//! |   |    +---------------------------------------------------------------+
//! |   |                                                                    |
//! |   | table: "role_builtin"                                              |
//! |   | primary key: (resource_type, role_name)
//! |   | +---------------+-----------+-----+                                |
//! |   | | resource_type | role_name | ... |                                |
//! |   | +---------------+-----------+-----+                                |
//! +---|-> "project "    | "admin"   | ... |                                |
//! |   | +---------------+--^--------+-----+                                |
//! |   |                    |
//! | +-|--------------------+                                               |
//! | | |                                                                    |
//! | | | table: "role_assignment_builtin"                                   |
//! | | | (assigns built-in roles to built-in users on arbitrary resources)  |
//! | | | +---------------+-----------+-------------+-----------------+      |
//! | | | | resource_type | role_name | resource_id | user_builtin_id |      |
//! | | | +---------------+-----------+-------------+-----------------+      |
//! | | | | "project "    | "admin"   |         234 |             123 <------+
//! | | | +--^------------+--^--------+----------^--+-----------------+
//! | | |    |               |                   |
//! +-|-|----+               |                   |
//!   +-|--------------------+                   |
//!     +----------------------------------------+
//! ```
//!
//! This record means: user 123 has the "admin" role on the "project" with id
//! 234.  (Note that ids are really uuids, and some of these tables have other
//! columns.)
//!
//! See the [`roles`] module for more details on how we check whether an actor
//! has a particular role on a resource.

mod actor;

mod api_resources;
pub use api_resources::Fleet;
pub use api_resources::FleetChild;
pub use api_resources::Organization;
pub use api_resources::Project;
pub use api_resources::ProjectChild;
pub use api_resources::FLEET;

mod context;
pub use context::Authz;
pub use context::Context;

mod oso_generic;
pub use oso_generic::Action;
pub use oso_generic::DATABASE;

mod roles;
pub use roles::AuthzResource;
