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
# ROLES AND PERMISSIONS IN THE FLEET/SILO/PROJECT HIERARCHY
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
# - fleet.viewer          (can read most non-siloed resources in the system)
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
	    "external-authenticator",
	    "external-scim"
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

# For fleets specifically, roles can be conferred by roles on the user's Silo.
has_role(actor: AuthenticatedActor, role: String, _: Fleet) if
	silo_role in actor.confers_fleet_role(role) and
	has_role(actor, silo_role, actor.silo.unwrap());

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

	# Permissions implied by roles on this resource's parent (Fleet).  Fleet
	# privileges allow a user to see and potentially administer the Silo,
	# but they do not give anyone permission to look at anything inside the
	# Silo.  To achieve this, we use permission rules here.  (If we granted
	# Fleet administrators _roles_ on the Silo, then those would cascade
	# into the Silo as well.)
	relations = { parent_fleet: Fleet };
	"read" if "viewer" on "parent_fleet";
	"modify" if "collaborator" on "parent_fleet";

	# external authenticator has to create silo users
	"list_children" if "external-authenticator" on "parent_fleet";
	"create_child" if "external-authenticator" on "parent_fleet";

	# external scim has to be able to read SCIM tokens
	"list_children" if "external-scim" on "parent_fleet";
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
# This rule is also used to determine if a user can list the identity providers
# in the Silo (which they should be able to), since that's predicated on being
# able to read the Silo.
#
# It's unclear what else would break if users couldn't see their own Silo.
has_permission(actor: AuthenticatedActor, "read", silo: Silo)
	if silo in actor.silo;

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

	# Roles implied by roles on this resource's parent (Silo)
	relations = { parent_silo: Silo };
	"admin" if "collaborator" on "parent_silo";
	"viewer" if "viewer" on "parent_silo";
}
has_relation(silo: Silo, "parent_silo", project: Project)
	if project.silo = silo;

#
# GENERAL RESOURCES OUTSIDE THE SILO/PROJECT HIERARCHY
#
# Many resources use snippets of Polar generated by the `authz_resource!` Rust
# macro.  Some resources require custom Polar code.  Those appear here.
#

resource Certificate {
	permissions = [ "read", "modify" ];
	relations = { parent_silo: Silo, parent_fleet: Fleet };

	# Fleet-level and silo-level roles both grant privileges on certificates.
	"read" if "admin" on "parent_silo";
	"modify" if "admin" on "parent_silo";
	"read" if "admin" on "parent_fleet";
	"modify" if "admin" on "parent_fleet";
}
has_relation(silo: Silo, "parent_silo", certificate: Certificate)
	if certificate.silo = silo;
has_relation(fleet: Fleet, "parent_fleet", certificate: Certificate)
	if certificate.silo.fleet = fleet;

resource SiloUser {
	permissions = [
	    "list_children",
	    "modify",
	    "read",
	    "create_child",
	];

	# Fleet and Silo administrators can manage a Silo's users.  This is one
	# of the only areas of Silo configuration that Fleet Administrators have
	# permissions on.
	relations = { parent_silo: Silo, parent_fleet: Fleet };
	"list_children" if "read" on "parent_silo";
	"read" if "read" on "parent_silo";
	"modify" if "admin" on "parent_silo";
	"create_child" if "admin" on "parent_silo";
	"list_children" if "read" on "parent_fleet";
	"read" if "read" on "parent_fleet";
	"modify" if "admin" on "parent_fleet";
	"create_child" if "admin" on "parent_fleet";
}
has_relation(silo: Silo, "parent_silo", user: SiloUser)
	if user.silo = silo;
has_relation(fleet: Fleet, "parent_fleet", user: SiloUser)
	if user.silo.fleet = fleet;

# authenticated actors have all permissions on themselves
has_permission(actor: AuthenticatedActor, _perm: String, silo_user: SiloUser)
    if actor.equals_silo_user(silo_user);

has_permission(actor: AuthenticatedActor, "read", silo_user: SiloUser)
    if silo_user.silo in actor.silo;

resource SiloGroup {
	permissions = [
	    "list_children",
	    "modify",
	    "read",
	    "create_child",
	];

	relations = { parent_silo: Silo };
	"list_children" if "read" on "parent_silo";
	"read" if "read" on "parent_silo";
	"modify" if "admin" on "parent_silo";
	"create_child" if "admin" on "parent_silo";
}
has_relation(silo: Silo, "parent_silo", group: SiloGroup)
	if group.silo = silo;

resource SshKey {
	permissions = [ "read", "modify" ];
	relations = { silo_user: SiloUser };

	"read" if "read" on "silo_user";
	"modify" if "modify" on "silo_user";
}
has_relation(user: SiloUser, "silo_user", ssh_key: SshKey)
	if ssh_key.silo_user = user;

resource IdentityProvider {
	permissions = [
	    "read",
	    "modify",
	    "create_child",
	    "list_children",
	];
	relations = { parent_silo: Silo, parent_fleet: Fleet };

	# Silo-level roles grant privileges on identity providers.
	"read" if "viewer" on "parent_silo";
	"list_children" if "viewer" on "parent_silo";
	"modify" if "admin" on "parent_silo";
	"create_child" if "admin" on "parent_silo";

	# Fleet-level roles also grant privileges on identity providers.
	"read" if "viewer" on "parent_fleet";
	"list_children" if "viewer" on "parent_fleet";
	"modify" if "admin" on "parent_fleet";
	"create_child" if "admin" on "parent_fleet";
}
has_relation(silo: Silo, "parent_silo", identity_provider: IdentityProvider)
	if identity_provider.silo = silo;
has_relation(fleet: Fleet, "parent_fleet", collection: IdentityProvider)
	if collection.silo.fleet = fleet;

resource SamlIdentityProvider {
	permissions = [
	    "read",
	    "modify",
	    "create_child",
	    "list_children",
	];
	relations = { parent_silo: Silo, parent_fleet: Fleet };

	# Silo-level roles grant privileges on identity providers.
	"read" if "viewer" on "parent_silo";
	"list_children" if "viewer" on "parent_silo";
	"modify" if "admin" on "parent_silo";
	"create_child" if "admin" on "parent_silo";

	# Fleet-level roles also grant privileges on identity providers.
	"read" if "viewer" on "parent_fleet";
	"list_children" if "viewer" on "parent_fleet";
	"modify" if "admin" on "parent_fleet";
	"create_child" if "admin" on "parent_fleet";
}
has_relation(silo: Silo, "parent_silo", saml_identity_provider: SamlIdentityProvider)
	if saml_identity_provider.silo = silo;
has_relation(fleet: Fleet, "parent_fleet", collection: SamlIdentityProvider)
	if collection.silo.fleet = fleet;

#
# SYNTHETIC RESOURCES OUTSIDE THE SILO HIERARCHY
#
# The resources here do not correspond to anything that appears explicitly in
# the API or is stored in the database.  These are used either at the top level
# of the API path (e.g., "/v1/system/ip-pools") or as an implementation detail of the system
# (in the case of console sessions and "Database").  The policies are
# either statically-defined in this file or driven by role assignments on the
# Fleet.  None of these resources defines their own roles.
#

# Describes the quiesce state of a particular Nexus instance.
#
# These authz checks must not require the database.  We grant this directly to
# callers of the internal API.
resource QuiesceState {
	permissions = [ "read", "modify" ];
}
has_permission(USER_INTERNAL_API: AuthenticatedActor, "read", _q: QuiesceState);
has_permission(
    USER_INTERNAL_API: AuthenticatedActor,
    "modify",
    _q: QuiesceState
);

# Describes the policy for reading and modifying DNS configuration
# (both internal and external)
resource DnsConfig {
	permissions = [ "read", "modify" ];
	relations = { parent_fleet: Fleet };
	# "external-authenticator" requires these permissions because that's the
	# context that Nexus uses when creating and deleting Silos.  These
	# operations necessarily need to read and modify DNS configuration.
	"read" if "external-authenticator" on "parent_fleet";
	"modify" if "external-authenticator" on "parent_fleet";
	# "admin" on the parent fleet also gets these permissions, primarily for
	# the test suite.
	"read" if "admin" on "parent_fleet";
	"modify" if "admin" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", dns_config: DnsConfig)
	if dns_config.fleet = fleet;

# Describes the policy for accessing blueprints
resource BlueprintConfig {
	permissions = [
	    "list_children", # list blueprints
	    "create_child",  # create blueprint
	    "read",          # read the current target
	    "modify",        # change the current target
	];

	relations = { parent_fleet: Fleet };
	"create_child" if "admin" on "parent_fleet";
	"modify" if "admin" on "parent_fleet";
	"list_children" if "viewer" on "parent_fleet";
	"read" if "viewer" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", list: BlueprintConfig)
	if list.fleet = fleet;

# Describes the policy for accessing "/v1/system/update/trust-roots" in the API
resource UpdateTrustRootList {
	permissions = [ "list_children", "create_child" ];
	relations = { parent_fleet: Fleet };
	"list_children" if "viewer" on "parent_fleet";
	"create_child" if "admin" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", collection: UpdateTrustRootList)
	if collection.fleet = fleet;

# Describes the policy for accessing blueprints
resource TargetReleaseConfig {
	permissions = [
	    "read",          # read the current target release
	    "modify",        # change the current target release
	];

	relations = { parent_fleet: Fleet };
	"read" if "viewer" on "parent_fleet";
	"modify" if "admin" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", resource: TargetReleaseConfig)
	if resource.fleet = fleet;

# Describes the policy for reading and modifying low-level inventory
resource Inventory {
	permissions = [ "read", "modify" ];
	relations = { parent_fleet: Fleet };
	"read" if "viewer" on "parent_fleet";
	"modify" if "admin" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", inventory: Inventory)
	if inventory.fleet = fleet;

# Describes the policy for accessing "/v1/system/ip-pools" in the API
resource IpPoolList {
	permissions = [
	    "list_children",
	    "modify",
	    "create_child",
	];

	# Fleet Administrators can create or modify the IP Pools list.
	relations = { parent_fleet: Fleet };
	"modify" if "admin" on "parent_fleet";
	"create_child" if "admin" on "parent_fleet";

	# Fleet Viewers can list IP Pools
	"list_children" if "viewer" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", ip_pool_list: IpPoolList)
	if ip_pool_list.fleet = fleet;

# Any authenticated user can create a child of a provided IP Pool.
# This is necessary to use the pools when provisioning instances.
has_permission(actor: AuthenticatedActor, "create_child", ip_pool: IpPool)
	if silo in actor.silo and silo.fleet = ip_pool.fleet;

# Describes the policy for reading and writing the audit log 
resource AuditLog {
	permissions = [
	    "list_children", # retrieve audit log
	    "create_child",  # create audit log entry
	];

	relations = { parent_fleet: Fleet };

	# Fleet viewers can read the audit log
	"list_children" if "viewer" on "parent_fleet";
}

# Any actor should be able to write to the audit log because we need to be able
# to write to the log from any request, authenticated or not. Audit log writes
# are always a byproduct of other operations: there are no endpoints that allow
# the user to write to the log deliberately. Note we use AuthenticatedActor
# because we don't really mean unauthenticated -- in the case of login
# operations, we use the external authenticator actor that creates the session
# to authorize the audit log write.
has_permission(_actor: AuthenticatedActor, "create_child", _audit_log: AuditLog);

has_relation(fleet: Fleet, "parent_fleet", audit_log: AuditLog)
	if audit_log.fleet = fleet;

# Describes the policy for creating and managing web console sessions.
resource ConsoleSessionList {
	permissions = [ "create_child" ];
	relations = { parent_fleet: Fleet };
	"create_child" if "external-authenticator" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", collection: ConsoleSessionList)
	if collection.fleet = fleet;

# Allow silo admins to delete and list user sessions
resource SiloUserSessionList {
    permissions = [ "modify", "list_children" ];
    relations = { parent_silo: Silo };

    # A silo admin can modify (e.g., delete) a user's sessions.
    "modify" if "admin" on "parent_silo";

    # A silo admin can list a user's sessions.
    "list_children" if "admin" on "parent_silo";
}
has_relation(silo: Silo, "parent_silo", authn_list: SiloUserSessionList)
    if authn_list.silo_user.silo = silo;

# give users 'modify' and 'list_children' on their own sessions
has_permission(actor: AuthenticatedActor, "modify", authn_list: SiloUserSessionList)
    if actor.equals_silo_user(authn_list.silo_user);
has_permission(actor: AuthenticatedActor, "list_children", authn_list: SiloUserSessionList)
    if actor.equals_silo_user(authn_list.silo_user);

# Allow silo admins to delete and list user access tokens
resource SiloUserTokenList {
    permissions = [ "modify", "list_children" ];
    relations = { parent_silo: Silo };

    # A silo admin can modify (e.g., delete) a user's tokens.
    "modify" if "admin" on "parent_silo";

    # A silo admin can list a user's tokens.
    "list_children" if "admin" on "parent_silo";
}
has_relation(silo: Silo, "parent_silo", authn_list: SiloUserTokenList)
    if authn_list.silo_user.silo = silo;

# give users 'modify' and 'list_children' on their own tokens
has_permission(actor: AuthenticatedActor, "modify", authn_list: SiloUserTokenList)
    if actor.equals_silo_user(authn_list.silo_user);
has_permission(actor: AuthenticatedActor, "list_children", authn_list: SiloUserTokenList)
    if actor.equals_silo_user(authn_list.silo_user);

# Describes the policy for creating and managing device authorization requests.
resource DeviceAuthRequestList {
	permissions = [ "create_child" ];
	relations = { parent_fleet: Fleet };
	"create_child" if "external-authenticator" on "parent_fleet";
}
has_relation(fleet: Fleet, "parent_fleet", collection: DeviceAuthRequestList)
	if collection.fleet = fleet;

# Describes the policy for creating and managing Silo certificates
resource SiloCertificateList {
	permissions = [ "list_children", "create_child" ];

	relations = { parent_silo: Silo, parent_fleet: Fleet };

	# Both Fleet and Silo administrators can see and modify the Silo's
	# certificates.
	"list_children" if "admin" on "parent_silo";
	"list_children" if "admin" on "parent_fleet";
	"create_child" if "admin" on "parent_silo";
	"create_child" if "admin" on "parent_fleet";
}
has_relation(silo: Silo, "parent_silo", collection: SiloCertificateList)
	if collection.silo = silo;
has_relation(fleet: Fleet, "parent_fleet", collection: SiloCertificateList)
	if collection.silo.fleet = fleet;

# Describes the policy for creating and managing Silo identity providers
resource SiloIdentityProviderList {
	permissions = [ "list_children", "create_child" ];

	relations = { parent_silo: Silo, parent_fleet: Fleet };

	# Everyone who can read the Silo (which includes all the users in the
	# Silo) can see the identity providers in it.
	"list_children" if "read" on "parent_silo";

	# Fleet and Silo administrators can manage the Silo's identity provider
	# configuration.  This is one of the only areas of Silo configuration
	# that Fleet Administrators have permissions on.  This is also one of
	# the only cases where we need to look two levels up the hierarchy to
	# see if somebody has the right permission.  For most other things,
	# permissions cascade down the hierarchy so we only need to look at the
	# parent.
	"create_child" if "admin" on "parent_silo";
	"create_child" if "admin" on "parent_fleet";
}
has_relation(silo: Silo, "parent_silo", collection: SiloIdentityProviderList)
	if collection.silo = silo;
has_relation(fleet: Fleet, "parent_fleet", collection: SiloIdentityProviderList)
	if collection.silo.fleet = fleet;

# Describes the policy for creating and managing Silo users (mostly intended for
# API-managed users)
resource SiloUserList {
	permissions = [ "list_children", "create_child" ];

	relations = { parent_silo: Silo, parent_fleet: Fleet };

	# Everyone who can read the Silo (which includes all the users in the
	# Silo) can see the users in it.
	"list_children" if "read" on "parent_silo";

	# Fleet and Silo administrators can manage the Silo's users.  This is
	# one of the only areas of Silo configuration that Fleet Administrators
	# have permissions on.  This is also one of the few cases (so far) where
	# we need to look two levels up the hierarchy to see if somebody has the
	# right permission.  For most other things, permissions cascade down the
	# hierarchy so we only need to look at the parent.
	"create_child" if "admin" on "parent_silo";
	"list_children" if "admin" on "parent_fleet";
	"create_child" if "admin" on "parent_fleet";
}
has_relation(silo: Silo, "parent_silo", collection: SiloUserList)
	if collection.silo = silo;
has_relation(fleet: Fleet, "parent_fleet", collection: SiloUserList)
	if collection.silo.fleet = fleet;

# These rules grants the external authenticator role the permissions it needs to
# read silo users and modify their sessions.  This is necessary for login to
# work.
has_permission(actor: AuthenticatedActor, "read", silo: Silo)
	if has_role(actor, "external-authenticator", silo.fleet);
has_permission(actor: AuthenticatedActor, "read", user: SiloUser)
	if has_role(actor, "external-authenticator", user.silo.fleet);
has_permission(actor: AuthenticatedActor, "modify", user: SiloUser)
	if has_role(actor, "external-authenticator", user.silo.fleet);
has_permission(actor: AuthenticatedActor, "read", group: SiloGroup)
	if has_role(actor, "external-authenticator", group.silo.fleet);
has_permission(actor: AuthenticatedActor, "modify", group: SiloGroup)
	if has_role(actor, "external-authenticator", group.silo.fleet);

has_permission(actor: AuthenticatedActor, "read", session: ConsoleSession)
	if has_role(actor, "external-authenticator", session.fleet);
has_permission(actor: AuthenticatedActor, "modify", session: ConsoleSession)
	if has_role(actor, "external-authenticator", session.fleet);

# All authenticated users can read and delete device authn requests because
# by necessity these operations happen before we've figured out what user (or
# even Silo) the device auth is associated with.  Any user can claim a device
# auth request with the right user code (that's how it works) -- it's the user
# code and associated logic that prevents unauthorized access here.
has_permission(_actor: AuthenticatedActor, "read", _device_auth: DeviceAuthRequest);
has_permission(_actor: AuthenticatedActor, "modify", _device_auth: DeviceAuthRequest);

has_permission(actor: AuthenticatedActor, "read", device_token: DeviceAccessToken)
	if has_role(actor, "external-authenticator", device_token.fleet);

has_permission(actor: AuthenticatedActor, "read", identity_provider: IdentityProvider)
	if has_role(actor, "external-authenticator", identity_provider.silo.fleet);

has_permission(actor: AuthenticatedActor, "read", saml_identity_provider: SamlIdentityProvider)
	if has_role(actor, "external-authenticator", saml_identity_provider.silo.fleet);

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

# The "db-init" user is the only one with the "modify" permission.
has_permission(USER_DB_INIT: AuthenticatedActor, "modify", _resource: Database);
has_permission(USER_DB_INIT: AuthenticatedActor, "create_child", _resource: IpPoolList);
# It also has "admin" on the internal silo to populate it with built-in resources.
# TODO-completeness: actually limit to just internal silo and not all silos
has_role(USER_DB_INIT: AuthenticatedActor, "admin", _silo: Silo);

# Allow the internal API admin permissions on all silos.
has_role(USER_INTERNAL_API: AuthenticatedActor, "admin", _silo: Silo);

resource WebhookSecret {
	permissions = [ "read", "modify" ];
	relations = { parent_alert_receiver: AlertReceiver };

	"read" if "read" on "parent_alert_receiver";
	"modify" if "modify" on "parent_alert_receiver";
}

has_relation(rx: AlertReceiver, "parent_alert_receiver", secret: WebhookSecret)
	if secret.alert_receiver = rx;

resource AlertClassList {
	permissions = [ "list_children" ];
	relations = { parent_fleet: Fleet };

	"list_children" if "viewer" on "parent_fleet";
}

has_relation(fleet: Fleet, "parent_fleet", collection: AlertClassList)
	if collection.fleet = fleet;

# These rules grant the external scim authenticator role the permission
# required to create the SCIM provider implementation for a Silo

has_permission(actor: AuthenticatedActor, "read", silo: Silo)
	if has_role(actor, "external-scim", silo.fleet);

resource ScimClientBearerToken {
	permissions = [
	    "read",
	    "modify",
	    "create_child",
	    "list_children",
	];
	relations = { parent_silo: Silo, parent_fleet: Fleet };

	# Silo-level roles grant privileges for SCIM client tokens.
	"read" if "admin" on "parent_silo";
	"list_children" if "admin" on "parent_silo";
	"modify" if "admin" on "parent_silo";
	"create_child" if "admin" on "parent_silo";

	# Fleet-level roles also grant privileges for SCIM client tokens.
	"read" if "admin" on "parent_fleet";
	"list_children" if "admin" on "parent_fleet";
	"modify" if "admin" on "parent_fleet";
	"create_child" if "admin" on "parent_fleet";
}
has_relation(silo: Silo, "parent_silo", scim_client_bearer_token: ScimClientBearerToken)
	if scim_client_bearer_token.silo = silo;
has_relation(fleet: Fleet, "parent_fleet", collection: ScimClientBearerToken)
	if collection.silo.fleet = fleet;
