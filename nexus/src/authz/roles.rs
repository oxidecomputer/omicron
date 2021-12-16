// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Role lookup
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
//! We'll describe this below.  First, let's walk through the authorization
//! process at a high level.
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
//!    resource.  Much more on this below.
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
//!
//! # How do we actually check roles?
//!
//! We said above that in evaluating the authorization decision, Oso winds up
//! checking whether the actor has one of many different roles on many different
//! resources.  It's essentially looking for specific rows in the
//! "role_assignment_builtin" table.
//!
//! To achieve this, before calling into Oso, we load _all_ of the roles that
//! the actor has on this resource _or any related resource_ that might affect
//! the authorization decision.  In practice, that means if the user is trying
//! to modify a Project, we'll fetch all the roles they have on that Project or
//! its parent Organization or the parent Fleet.  This isn't as large as it
//! might sound, since the hierarchy is not _that_ deep, but we do have to make
//! multiple database queries to load all this.  This is all done by
//! [`AuthzResource::fetch_all_related_roles_for_user()`].  This is really done
//! by the impl of [`AuthzResource`] for [`AuthzApiResource`].
//!
//! Once we've got the complete list of roles, we include that in the data
//! structure we pass into Oso.   When it asks whether an actor has a role on a
//! resource, we just look it up in our data structure.  In principle, we could
//! skip the prefetching and look up the specific roles we need when Oso asks
//! that question.  This might let us short-circuit the role fetching, since we
//! might find we don't even need to check a parent's roles.  There are two
//! problems with this: first, Oso will separately ask about every related role
//! for a given resource.  Is this person a project admin?  Are they a
//! collaborator?  We want to collapse these into the same database query --
//! otherwise, this approach will result in _more_ database queries, not fewer.
//! Second, our database operations are async, but Oso is synchronous.  We're
//! doing authorization in the context of whatever task is handling this
//! request, and we don't want that thread to block while we hit the database.
//! Both of these issues could be addressed with considerably more work.

use crate::authn;
use crate::context::OpContext;
use crate::db::DataStore;
use futures::future::BoxFuture;
use futures::FutureExt;
use omicron_common::api::external::Error;
use omicron_common::api::external::ResourceType;
use std::collections::BTreeSet;
use uuid::Uuid;

/// A set of built-in roles, used for quickly checking whether a particular role
/// is contained within the set
///
/// For more on roles, see dbinit.rs.
#[derive(Clone, Debug)]
pub struct RoleSet {
    roles: BTreeSet<(ResourceType, Uuid, String)>,
}

impl RoleSet {
    pub fn new() -> RoleSet {
        RoleSet { roles: BTreeSet::new() }
    }

    pub fn has_role(
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

/// Describes how to fetch the roles for an authz resource
pub trait AuthzResource: Send + Sync + 'static {
    /// Find all roles for the user described in `authn` that might be used to
    /// make an authorization decision on `self` (a resource)
    ///
    /// You can imagine that this function would first find roles that are
    /// explicitly associated with this resource in the database.  Then it would
    /// also find roles associated with its parent, since, for example, an
    /// Organization Administrator can access things within Projects in the
    /// organization.  This process continues up the hierarchy.
    ///
    /// That's how this works for most resources.  There are other kinds of
    /// resources (like the Database itself) that aren't stored in the database
    /// and for which a different mechanism might be used.
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

/// Describes an authz resource that corresponds to an API resource that has a
/// corresponding ResourceType and is stored in the database
///
/// This is a helper trait used to impl [`AuthzResource`].
pub trait AuthzApiResource: Send + Sync + 'static {
    /// If roles can be assigned to this resource, return the type and id of the
    /// database record describing this resource
    ///
    /// If roles cannot be assigned to this resource, returns `None`.
    fn db_resource(&self) -> Option<(ResourceType, Uuid)>;

    /// If this resource has a parent in the API hierarchy whose assigned roles
    /// can affect access to this resource, return the parent resource.
    /// Otherwise, returns `None`.
    fn parent(&self) -> Option<Box<dyn AuthzResource>>;
}

impl<T: AuthzApiResource> AuthzResource for T {
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
            // If the user is authenticated ...
            if let Some(actor_id) = authn.actor() {
                // ... and if roles can be assigned directly on this resource ...
                if let Some((resource_type, resource_id)) = self.db_resource() {
                    // ... then start by fetching all the roles for this user
                    // that are associated with this resource.
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
                    // Add each role to the output roleset.
                    for role_asgn in roles {
                        assert_eq!(
                            resource_type.to_string(),
                            role_asgn.resource_type
                        );
                        roleset.insert(
                            resource_type,
                            resource_id,
                            &role_asgn.role_name,
                        );
                    }
                }

                // If this resource has a parent, the user's roles on the parent
                // might grant them access to this resource.  We have to fetch
                // those, too.  This process is recursive up to the root.
                //
                // (In general, there could be another resource with _any_ kind
                // of relationship to this one that grants them a role that
                // grants access to this resource.  In practice, we only use
                // "parent", and it's clearer to just call this "parent" than
                // "related_resources_whose_roles_might_grant_access_to_this".)
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
