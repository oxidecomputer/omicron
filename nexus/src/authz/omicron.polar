actor AuthenticatedActor {}
actor AnyActor {}

resource Database {
	permissions = [ "query" ];
	roles = [ "user" ];

	"query" if "user";
}

allow(actor: AnyActor, Action::Query, database: Database) if
	actor.authenticated and
	has_permission(actor.authn_actor.unwrap(), action.to_perm(), database)

resource Organization {
	## This is currently a straight translation of RFD 43.
	## "organization.project.*" have been omitted because they appear to be
	## redundant with the implied behavior, but I'm not sure about that!
	## "organization.{billing,user}.*" have been omitted because the
	## corresponding functionality in Nexus does not exist yet.
	#permissions = [ "read", "modify", "create", "admin" ];
	#roles = [ "read", "modify", "create", "admin" ]
	permissions = [ "read" ];
	roles = [ "reader" ];

	"read" if "reader";
}

allow(actor: AnyActor, action: Action, resource) if
    actor.authenticated and
    has_permission(actor.authn_actor.unwrap(), action.to_perm(), resource);

has_role(actor: AuthenticatedActor, "reader", _resource: Organization) if
    actor.id == "00000000-0000-0000-0000-000000000000";
