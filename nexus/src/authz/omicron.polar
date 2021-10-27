actor Actor2 {}

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

allow(actor, action, resource) if
    has_permission(actor, action, resource);

has_role(actor: Actor, "reader", _resource: Organization) if
    actor.id == "00000000-0000-0000-0000-000000000000";
