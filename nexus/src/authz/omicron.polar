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
}
