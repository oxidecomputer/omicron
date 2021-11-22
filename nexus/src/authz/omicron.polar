#
# Oso configuration for Omicron
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
	permissions = [ "query" ];
	roles = [ "user" ];

	"query" if "user";
}

# All authenticated users have the "user" role on the database.
# XXX This rule doesn't seem to get used for some reason.
has_role(_actor: AuthenticatedActor, "user", _resource: Database);

#
# Permissions and predefined roles
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
# - fleet.collaborator    (can create and own orgs)
# - organization.admin    (complete control over an organization)
# - organization.collaborator (can create, modify, and delete projects)
# - project.admin         (complete control over a project)
# - project.collaborator  (can create, modify, and delete all resources within
#                         the project, but cannot modify or delete the project
#                         itself)
# - project.viewer        (can see everything in the project, but cannot modify
#     			  anything)
#

# At the top level is the "Fleet" resource.
resource Fleet {
	permissions = [
	    "list_children",
	    "modify",
	    "read",
	    "create_child",
	];

	roles = [ "admin", "collaborator" ];

	# Fleet collaborators can create Organizations and see fleet-wide
	# information, including Organizations that they don't have permissions
	# on.  (They cannot list projects within those organizations, however.)
	# They cannot modify fleet-wide information.
	"list_children" if "collaborator";
	"read" if "collaborator";
	"create_child" if "collaborator";

	# Fleet administrators are whole-system superusers.
	"collaborator" if "admin";
	"modify" if "admin";
}

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

	relations = { parent_fleet: Fleet };
	"admin" if "admin" on "parent_fleet";
}

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

# For now, we use one generic resource to represent every kind of thing inside
# the Project.  That's because they all have the same behavior.
resource ProjectChild {
	permissions = [
		"list_children",
		"modify",
		"read",
		"create_child",
	];

	relations = { parent_project: Project };
	"list_children" if "viewer" on "parent_project";
	"read" if "viewer" on "parent_project";

	"modify" if "collaborator" on "parent_project";
	"create_child" if "collaborator" on "parent_project";
}

# Define relationships
has_relation(fleet: Fleet, "parent_fleet", organization: Organization)
	if organization.fleet = fleet;
has_relation(organization: Organization, "parent_organization", project: Project)
	if project.organization = organization;
has_relation(project: Project, "parent_project", project_child: ProjectChild)
	if project_child.project = project;

# Define role relationships
has_role(actor: AuthenticatedActor, role: String, resource: Resource)
	if resource.has_role(actor, role);
