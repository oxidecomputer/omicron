#
# Oso configuration for Omicron
# This file is augmented by generated snippets.
#

#
# ACTOR TYPES AND BASIC RULES
#

# `AnyActor` includes both authenticated and unauthenticated users.
actor AnyActor {}

# An `AuthenticatedActor` has an identity in the system.  All of our operations
# today require that an actor be authenticated.
actor AuthenticatedActor {}

# For any resource, `actor` can perform action `action` on it if they're
# authenticated and their role(s) give them the corresponding permission on that
# resource.
allow(actor: AnyActor, action: Action, resource) if
    actor.authenticated and
    has_permission(actor.authn_actor.unwrap(), action.to_perm(), resource);

# Define role relationships
has_role(actor: AuthenticatedActor, role: String, resource: Resource)
	if resource.has_role(actor, role);

#
# ROLES AND PERMISSIONS IN THE FLEET/SILO/ORGANIZATION/PROJECT HIERARCHY
#
# We define the following permissions for most resources in the system:
#
# - "create_child": required to create child resources (of any type)
#
# - "list_children": required to list child resources (of all types) of a
#   resource
#
# - "modify": required to modify or delete a resource
#
# - "read": required to read a resource
#
# We define the following predefined roles for only a few high-level resources:
# the Fleet (see below), Silo, Organization, and Project.  The specific roles
# are oriented around intended use-cases:
#
# - "admin": has all permissions on the resource
#
# - "collaborator": has "read", "list_children", and "create_child", plus
#   the "admin" role for child resources.  The idea is that if you're an
#   Organization Collaborator, you have full control over the Projects within
#   the Organization, but you cannot modify or delete the Organization itself.
#
# - "viewer": has "read" and "list_children" on a resource
#
# Below the Project level, permissions are granted via roles at the Project
# level.  For example, for someone to be able to create, modify, or delete any
# Instances, they must be granted project.collaborator, which means they can
# create, modify, or delete _all_ resources in the Project.
#
# The complete set of predefined roles:
#
# - fleet.admin           (superuser for the whole system)
# - fleet.collaborator    (can manage Silos)
# - fleet.viewer          (can read most resources in the system)
# - silo.admin            (superuser for the silo)
# - silo.collaborator     (can create and own Organizations)
# - silo.viewer           (can read most resources within the Silo)
# - organization.admin    (complete control over an organization)
# - organization.collaborator (can manage Projects)
# - organization.viewer   (can read most resources within the Organization)
# - project.admin         (complete control over a Project)
# - project.collaborator  (can manage all resources within the Project)
# - project.viewer        (can read most resources within the Project)
#
# Outside the Silo/Organization/Project hierarchy, we (currently) treat most
# resources as nested under Fleet or else a synthetic resource (see below).  We
# do not yet support role assignments on anything other than Fleet, Silo,
# Organization, or Project.
#

# "Fleet" is a global singleton representing the whole system.  The name comes
# from the idea described in RFD 24, but it's not quite right.  This probably
# should be more like "Region" or "AvailabilityZone".  The precise boundaries
# have not yet been figured out.
resource Fleet {
	permissions = [
	    "list_children",
	    "modify",
	    "read",
	    "create_child",
	];

	roles = [
	    # Roles that can be attached by users
	    "admin",
	    "collaborator",
	    "viewer",

	    # Internal-only roles
	    "external-authenticator"
	];

	# Roles implied by other roles on this resource
	"viewer" if "collaborator";
	"collaborator" if "admin";

	# Permissions granted directly by roles on this resource
	"list_children" if "viewer";
	"read" if "viewer";
	"create_child" if "collaborator";
	"modify" if "admin";
}

resource Silo {
	permissions = [
	    "list_children",
	    "modify",
	    "read",
	    "create_child",
	];
	roles = [ "admin", "collaborator", "viewer" ];

	# Roles implied by other roles on this resource
	"viewer" if "collaborator";
	"collaborator" if "admin";

	# Permissions granted directly by roles on this resource
	"list_children" if "viewer";
	"read" if "viewer";
	"create_child" if "collaborator";
	"modify" if "admin";

	# Roles implied by roles on this resource's parent (Fleet)
	relations = { parent_fleet: Fleet };
	"admin" if "collaborator" on "parent_fleet";
	"viewer" if "viewer" on "parent_fleet";
}

has_relation(fleet: Fleet, "parent_fleet", silo: Silo)
	if silo.fleet = fleet;

# As a special case, all authenticated users can read their own Silo.  That's
# not quite the same as having the "viewer" role.  For example, they cannot list
# Organizations in the Silo.
#
# One reason this is necessary is because if an unprivileged user tries to
# create an Organization using "POST /organizations", they should get back a 403
# (which implies they're able to see /organizations, which is essentially seeing
# the Silo itself) rather than a 404.  This behavior isn't a hard constraint
# (i.e., you could reasonably get a 404 for an API you're not allowed to call).
# Nor is the implementation (i.e., we could special-case this endpoint somehow).
# But granting this permission is the simplest way to keep this endpoint's
# behavior consistent with the rest of the API.
#
# It's unclear what else would break if users couldn't see their own Silo.
has_permission(actor: AuthenticatedActor, "read", silo: Silo)
	# TODO-security TODO-coverage We should have a test that exercises this
	# syntax.
	if silo in actor.silo;

resource Organization {
	permissions = [
	    "list_children",
	    "modify",
	    "read",
	    "create_child",
	];
	roles = [ "admin", "collaborator", "viewer" ];

	# Roles implied by other roles on this resource
	"viewer" if "collaborator";
	"collaborator" if "admin";

	# Permissions granted directly by roles on this resource
	"list_children" if "viewer";
	"read" if "viewer";
	"create_child" if "collaborator";
	"modify" if "admin";

	# Roles implied by roles on this resource's parent (Silo)
	relations = { parent_silo: Silo };
	"admin" if "collaborator" on "parent_silo";
	"viewer" if "viewer" on "parent_silo";
}
has_relation(silo: Silo, "parent_silo", organization: Organization)
	if organization.silo = silo;

resource Project {
	permissions = [
	    "list_children",
	    "modify",
	    "read",
	    "create_child",
	];
	roles = [ "admin", "collaborator", "viewer" ];

	# Roles implied by other roles on this resource
	"viewer" if "collaborator";
	"collaborator" if "admin";

	# Permissions granted directly by roles on this resource
	"list_children" if "viewer";
	"read" if "viewer";
	"create_child" if "collaborator";
	"modify" if "admin";

	# Roles implied by roles on this resource's parent (Organization)
	relations = { parent_organization: Organization };
	"admin" if "collaborator" on "parent_organization";
	"viewer" if "viewer" on "parent_organization";
}
has_relation(organization: Organization, "parent_organization", project: Project)
	if project.organization = organization;

#
# GENERAL RESOURCES OUTSIDE THE SILO/ORGANIZATION/PROJECT HIERARCHY
#
# Many resources use snippets of Polar generated by the `authz_resource!` Rust
# macro.  Some resources require custom Polar code.  Those appear here.
#

resource SiloUser {
	permissions = [
	    "list_children",
	    "modify",
	    "read",
	    "create_child",
	];

	relations = { parent_silo: Silo };
	"list_children" if "viewer" on "parent_silo";
	"read" if "viewer" on "parent_silo";
	"modify" if "admin" on "parent_silo";
	"create_child" if "admin" on "parent_silo";
}
has_relation(silo: Silo, "parent_silo", user: SiloUser)
	if user.silo = silo;

resource SshKey {
	permissions = [ "read", "modify" ];
	relations = { silo_user: SiloUser };

	"read" if "read" on "silo_user";
	"modify" if "modify" on "silo_user";
}
has_relation(user: SiloUser, "silo_user", ssh_key: SshKey)
	if ssh_key.silo_user = user;

#
# SYNTHETIC RESOURCES OUTSIDE THE SILO HIERARCHY
#
# The resources here do not correspond to anything that appears explicitly in
# the API or is stored in the database.  These are used either at the top level
# of the API path (e.g., "/images") or as an implementation detail of the system
# (in the case of console sessions and "Database").  The policies are
# either statically-defined in this file or driven by role assignments on the
# Fleet.  None of these resources defines their own roles.
#

# Describes the policy for accessing "/images" (in the API)
resource GlobalImageList {
	permissions = [
	    "list_children",
	    "modify",
	    "create_child",
	];

	# Fleet Administrators can create or modify the global images list.
	relations = { parent_fleet: Fleet };
	"modify" if "admin" on "parent_fleet";
	"create_child" if "admin" on "parent_fleet";

	# Fleet Viewers can list global images.
	"list_children" if "viewer" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", global_image_list: GlobalImageList)
	if global_image_list.fleet = fleet;

# Describes the policy for creating and managing web console sessions.
resource ConsoleSessionList {
	permissions = [ "create_child" ];
	relations = { parent_fleet: Fleet };
	"create_child" if "external-authenticator" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", collection: ConsoleSessionList)
	if collection.fleet = fleet;

# These rules grants the external authenticator role the permissions it needs to
# read silo users and modify their sessions.  This is necessary for login to
# work.
has_permission(actor: AuthenticatedActor, "read", user: SiloUser)
	if has_role(actor, "external-authenticator", user.silo.fleet);
has_permission(actor: AuthenticatedActor, "read", session: ConsoleSession)
	if has_role(actor, "external-authenticator", session.fleet);
has_permission(actor: AuthenticatedActor, "modify", session: ConsoleSession)
	if has_role(actor, "external-authenticator", session.fleet);


# Describes the policy for who can access the internal database.
resource Database {
	permissions = [
	    # "query" is required to perform any query against the database,
	    # whether a read or write query.  This is checked when an operation
	    # checks out a database connection from the connection pool.
	    #
	    # Any authenticated user gets this permission.  There's generally
	    # some other authz check involved in the database query.  For
	    # example, if you're querying the database to "read" a "Project", we
	    # should also be checking that.  So why do we do this at all?  It's
	    # a belt-and-suspenders measure so that if we somehow introduced an
	    # unauthenticated code path that hits the database, it cannot be
	    # used to DoS the database because we won't allow the operation to
	    # make the query.  (As long as the code path _is_ authenticated, we
	    # can use throttling mechanisms to prevent DoS.)
	    "query",

	    # "modify" is required to populate database data that's delivered
	    # with the system.  It should also be required for schema changes,
	    # when we support those.  This is separate from "query" so that we
	    # cannot accidentally invoke these code paths from API calls and
	    # other general functions.
	    "modify"
	];
}

# All authenticated users have the "query" permission on the database.
has_permission(_actor: AuthenticatedActor, "query", _resource: Database);

# The "db-init" user is the only one with the "init" role.
has_permission(actor: AuthenticatedActor, "modify", _resource: Database)
	if actor = USER_DB_INIT;
