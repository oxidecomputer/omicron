#
# Oso configuration for Omicron
# This file is augmented by generated snippets.
#


#
# General types and rules
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

#
# Resources
#

# The "database" resource allows us to limit what users are allowed to perform
# operations that query the database (whether those read or write queries).
resource Database {
	permissions = [ "query", "modify" ];
	roles = [ "user", "init" ];

	"query" if "user";

	"modify" if "init";
	"user" if "init";
}

# All authenticated users have the "user" role on the database.
has_role(_actor: AuthenticatedActor, "user", _resource: Database);
# The "db-init" user is the only one with the "init" role.
has_role(actor: AuthenticatedActor, "init", _resource: Database)
	if actor = USER_DB_INIT;

# Define role relationships
has_role(actor: AuthenticatedActor, role: String, resource: Resource)
	if resource.has_role(actor, role);

#
# Permissions and predefined roles for resources in the
# Fleet/Silo/Organization/Project hierarchy
#
# For now, we define the following permissions for most resources in the system:
#
# - "create_child": required to create child resources.
#
# - "list_children": required to list children (of all types) of a resources
#
# - "modify": required to modify or delete a resource or any of its children
#
# - "read": required to read a resource
#
# We define the following predefined roles for only a few high-level resources:
#
# - "admin": has all permissions on the resource
#
# - "collaborator": has "list_children" and "create_$child" for all children.
#   They'll inherit the "admin" role for any resources that they create.
#
# - "viewer": has "read" and "list_children" on a resource
#
# Below the project level, permissions are granted at the Project level.  For
# example, for someone to be able to create, modify, or delete any Instances,
# they must be granted project.collaborator, which means they can create,
# modify, or delete _all_ resources in the Project.
#
# The complete set of predefined roles:
#
# - fleet.admin           (superuser for the whole system)
# - fleet.collaborator    (can create and own silos)
# - fleet.viewer          (can read fleet-wide data)
# - silo.admin            (superuser for the silo)
# - silo.collaborator     (can create and own orgs)
# - silo.viewer           (can read silo-wide data)
# - organization.admin    (complete control over an organization)
# - organization.collaborator (can create, modify, and delete projects)
# - project.admin         (complete control over a project)
# - project.collaborator  (can create, modify, and delete all resources within
#                         the project, but cannot modify or delete the project
#                         itself)
# - project.viewer        (can see everything in the project, but cannot modify
#                         anything)
#

# At the top level is the "Fleet" resource.
resource Fleet {
	permissions = [
	    "list_children",
	    "modify",
	    "read",
	    "create_child",
	];

	roles = [
	    "admin",
	    "collaborator",
	    "viewer",

	    # internal roles
	    "external-authenticator"
	];

	# Fleet viewers can view Fleet-wide data
	"list_children" if "viewer";
	"read" if "viewer";

	# Fleet collaborators can create Organizations and see fleet-wide
	# information, including Organizations that they don't have permissions
	# on.  (They cannot list projects within those organizations, however.)
	# They cannot modify fleet-wide information.
	"viewer" if "collaborator";
	"create_child" if "collaborator";

	# Fleet administrators are whole-system superusers.
	"collaborator" if "admin";
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

	"list_children" if "viewer";
	"read" if "viewer";

	"viewer" if "collaborator";
	"create_child" if "collaborator";
	"collaborator" if "admin";
	"modify" if "admin";
	relations = { parent_fleet: Fleet };
	"admin" if "admin" on "parent_fleet";
	"collaborator" if "collaborator" on "parent_fleet";
	"viewer" if "viewer" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", silo: Silo)
	if silo.fleet = fleet;
has_role(actor: AuthenticatedActor, "viewer", silo: Silo)
	# TODO-coverage We should have a test that exercises this case.
	if silo in actor.silo;

resource Organization {
	permissions = [
	    "list_children",
	    "modify",
	    "read",
	    "create_child",
	];
	roles = [ "admin", "collaborator" ];

	# Organization collaborators can create Projects and see
	# organization-wide information, including Projects that they don't have
	# permissions on.  (They cannot see anything inside those Projects,
	# though.)  They cannot modify or delete the organization itself.
	"list_children" if "collaborator";
	"read" if "collaborator";
	"create_child" if "collaborator";
	
	# Organization administrators can modify and delete the Organization
	# itself.  They can also see and administer everything in the
	# Organization (recursively).
	"collaborator" if "admin";
	"modify" if "admin";

	relations = { parent_silo: Silo };
	"admin" if "admin" on "parent_silo";
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

	# Project viewers can see everything in the Project.
	"list_children" if "viewer";
	"read" if "viewer";

	# Project collaborators can see, modify, and delete everything inside
	# the Project recursively.  (This is different from Fleet and
	# Organization-level collaborators, who can only modify and delete child
	# resources that they have specific permissions on.  That's because
	# we're not implementing fine-grained permissions within Projects yet.)
	# They cannot modify or delete the Project itself.
	"viewer" if "collaborator";
	"create_child" if "collaborator";

	# Project administrators can modify and delete the Project" itself.
	"collaborator" if "admin";
	"modify" if "admin";

	relations = { parent_organization: Organization };
	"admin" if "admin" on "parent_organization";
}
has_relation(organization: Organization, "parent_organization", project: Project)
	if project.organization = organization;

resource GlobalImageList {
	permissions = [
	    "list_children",
	    "modify",
	    "create_child",
	];

	# Only admins can create or modify the global images list
	relations = { parent_fleet: Fleet };
	"modify" if "admin" on "parent_fleet";
	"create_child" if "admin" on "parent_fleet";

	# Anyone with viewer can list global images
	"list_children" if "viewer" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", global_image_list: GlobalImageList)
	if global_image_list.fleet = fleet;

# ConsoleSessionList is a synthetic resource used for modeling who has access
# to create sessions.
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
