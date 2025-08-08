// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Role lookup
//!
//! For important background, see the [`crate::authz`] module documentation.  We
//! said there that in evaluating the authorization decision, Oso winds up
//! checking whether the actor has one of many different roles on many different
//! resources.  It's essentially looking for specific rows in the
//! "role_assignment" table.
//!
//! To achieve this, before calling into Oso, we load _all_ of the roles that
//! the actor has on this resource _or any related resource_ that might affect
//! the authorization decision.  In practice, that means if the user is trying
//! to modify a Project, we'll fetch all the roles they have on that Project or
//! its parent Organization or the parent Fleet.  This isn't as large as it
//! might sound, since the hierarchy is not _that_ deep, but we do have to make
//! multiple database queries to load all this.  This is all done by
//! [`load_roles_for_resource_tree`].
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

use super::api_resources::ApiResource;
use crate::authn;
use crate::context::OpContext;
use omicron_common::api::external::Error;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::GenericUuid;
use slog::trace;
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

pub async fn load_roles_for_resource_tree<R>(
    resource: &R,
    opctx: &OpContext,
    authn: &authn::Context,
    roleset: &mut RoleSet,
) -> Result<(), Error>
where
    R: ApiResource,
{
    // If roles can be assigned directly on this resource, load them.
    if let Some(with_roles) = resource.as_resource_with_roles() {
        let resource_type = resource.resource_type();
        let resource_id = with_roles.resource_id();
        load_directly_attached_roles(
            opctx,
            authn,
            resource_type,
            resource_id,
            roleset,
        )
        .await?;

        // If roles can be conferred by another resource, load that resource's
        // roles, too.
        if let Some((resource_type, resource_id)) =
            with_roles.conferred_roles_by(authn)?
        {
            load_directly_attached_roles(
                opctx,
                authn,
                resource_type,
                resource_id,
                roleset,
            )
            .await?;
        }
    }

    // If this resource has a parent, the user's roles on the parent
    // might grant them access to this resource.  We have to fetch
    // those, too.  This process is recursive up to the root.
    //
    // (In general, there could be another resource with _any_ kind of
    // relationship to this one that grants them a role that grants
    // access to this resource.  In practice, we only use "parent", and
    // it's clearer to just call this "parent" than
    // "related_resources_whose_roles_might_grant_access_to_this".)
    if let Some(parent) = resource.parent() {
        parent.load_roles(opctx, authn, roleset).await?;
    }

    Ok(())
}

async fn load_directly_attached_roles(
    opctx: &OpContext,
    authn: &authn::Context,
    resource_type: ResourceType,
    resource_id: Uuid,
    roleset: &mut RoleSet,
) -> Result<(), Error> {
    // If the user is authenticated ...
    if let Some(actor) = authn.actor() {
        // ... then fetch all the roles for this user that are associated with
        // this resource.
        trace!(opctx.log, "loading roles";
            "actor" => ?actor,
            "resource_type" => ?resource_type,
            "resource_id" => resource_id.to_string(),
        );

        let roles = opctx
            .datastore()
            .role_asgn_list_for(
                opctx,
                actor.into(),
                match &actor {
                    authn::Actor::SiloUser { silo_user_id, .. } => {
                        silo_user_id.into_untyped_uuid()
                    }
                    authn::Actor::UserBuiltin { user_builtin_id, .. } => {
                        user_builtin_id.into_untyped_uuid()
                    }
                },
                resource_type,
                resource_id,
            )
            .await?;

        // Add each role to the output roleset.
        for role_asgn in roles {
            assert_eq!(resource_type.to_string(), role_asgn.resource_type);
            roleset.insert(resource_type, resource_id, &role_asgn.role_name);
        }
    }

    Ok(())
}
