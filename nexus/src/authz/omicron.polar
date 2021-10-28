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

# For now, we hardcode some fixed actor ids and what roles they have.
# This will eventually need to be replaced with data that comes from the
# database.
has_role(actor: AuthenticatedActor, _role: String, _resource: Resource) if
    actor.id == "00000000-0000-0000-0000-000000000000";


#
# Resources
#

# The "database" resource allows us to limit what users are allowed to perform
# operations that query the database (whether those read or write).
resource Database {
	permissions = [ "query" ];
	roles = [ "user" ];

	"query" if "user";
}

# All authenticated users have the "user" role on the database.
has_role(_actor: AuthenticatedActor, "user", _resource: Database);


# At the top level is the "Fleet" resource.  Fleet administrators can create
# Organizations, which essentially gives them permissions to do anything with
# the Fleet.  (They're not exactly superusers, though.  They only inherit an
# "admin" role on Organizations they create.  They cannot necessarily even see
# Organizations created by other Fleet administrators.)
resource Fleet {
	permissions = [ "create_organization" ];
	roles = [ "admin" ];
	"create_organization" if "admin";
}

resource Organization {
	permissions = [ "create_project"  ];
	roles = [ "admin" ];

	# Administrator permissions
	"create_project" if "admin";
}
