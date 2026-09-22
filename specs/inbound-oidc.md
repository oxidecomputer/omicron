# Inbound OIDC

As described in RFD 745 (prediscussion at ~/code/rfd), Oxide currently lacks support for *inbound OIDC*, or generically, a way for outside services to authenticate to Nexus without long-lived credentials. We described inbound OIDC as blocked on the implementation of service accounts in that draft RFD, but that's not really true. We can build inbound OIDC so that external services assume or impersonate a role, or set of role grants, or write a more generic policy that could support both roles and service accounts. For example:

---
name: example-role-trust-policy
grants:
- kind: role
  project_id: ...
  role_name: ...
idp_id: ...
policy: |-
  # polar policy follows
---

---
name: example-sa-trust-policy
grants:
- kind: service_account
  project_id: ...
  service_account_id: ...
idp_id: ...
policy: |-
  # polar policy follows
---

Note that we use `role_name` in the example because roles are currently a fixed set and not user-configurable (or configurable at all). In the future, role-based grants could refer to the name of a predefined role, or the id of a user-managed role.

The goal of this spec is to validate this idea with a proof of concept branch. We should go far enough to include tests and pass CI to validate the idea, but once we have a working PoC, we'll set it aside, write an RFD, and verify possibly start the implementation from scratch if we decide to move forward at all. The PoC will be largely LLM-authored, but any real implementation will involve much more human authorship or review.

=== Concepts

First, we'll introduce the policy of a `FederationTrustPolicy`. This is what we sketched above: these policies define what kind of external actor can assume the policy, and what access they're granted on assumption. To begin with, these policies should be managed only by silo admins, although we could eventually allow project admins to manage policies for their own projects (but not other projects). This resource will be a database model containing a name/uuid, list of grants, and a Polar policy. The Polar policy will be evaluated against the OIDC JWT claims from the external service (sub, iss, custom claims).

For the PoC, trust policies will describe a list of `grants`, each of which comprises a resource type (silo or project), a resource id, and a role name. Fleet-level permissions are out of scope. Custom roles are out of scope, since they don't exist, but could be supported in the future using a role id as well as a role name. Service accounts are also out of scope, since they also don't exist, but could be supported in the future as a separate top-level field (`service_accounts`) or nested field (`grants.service_account_id`).

Endpoints:
* GET /v1/federation/trust_policies/{policy_id}
* GET /v1/federation/trust_policies/
* POST /v1/federation/trust_policies/
  {
    name,
    description,
    identity_provider (NameOrId),
    grants,
    policy,
  }
  ---
  {
    id,
  }
* PATCH /v1/federation/trust_policies/{policy_id}
  {
    name,
    description,
    identity_provider,
    grants,
    policy,
  }
  ---
  {
    revision,
  }
* DELETE /v1/federation/inbound/trust_policies/{policy_id}

Database models:
create table if not exists federation_trust_policy (
  id UUID PRIMARY KEY,
  name STRING(63) NOT NULL,
  description STRING(512) NOT NULL,
  time_created TIMESTAMPTZ NOT NULL,
  time_modified TIMESTAMPTZ NOT NULL,
  time_deleted TIMESTAMPTZ,

  silo_id UUID NOT NULL,
  revision INT8 NOT NULL DEFAULT 1,

  idp_id uuid NOT NULL,
  conditions TEXT NOT NULL
)

create table if not exists federation_role_grant (
  id UUID PRIMARY KEY,
  resource_kind STRING NOT NULL,
  resource_id UUID NOT NULL,
  role_name STRING NOT NULL,
  trust_policy_id UUID NOT NULL
)

Next, we'll teach Nexus to act as an OIDC RP. When it receives a request for a federated token, at e.g. /v1/oidc/token, it will verify the token signature against the public key of the well-known OIDC endpoint, as well as `iss`, `aud`, etc. This entails a subtask of configuring each trusted OIDC IDP: its well-known url and JWKS metadata. This will be a separate database resource--call it FederationIdentityProvider`. Then each trust policy can point to a given identity provider as an extra layer of intention. We'll also require silo admin to manage this OIDC IdP resource.

Endpoints:
* GET /v1/federation/inbound/identity_providers/{idp_id}
* GET /v1/federation/inbound/identity_providers/
* POST /v1/federation/inbound/identity_provivders/
  {
    name,
    description,
    audience,
    issuer,
    verification_type,
    discovery_url,
    signing_keys,
  }
  ---
  201
  {
    id,
  }
* PATCH /v1/federation/inbound/identity_providers/{idp_id}
  {
    name,
    description,
    signing_keys,
  }
* DELETE /v1/federation/inbound/identity_providers/{idp_id}

Database models:
create table if not exists federation_identity_provider (
  id UUID PRIMARY KEY,
  name STRING(63) NOT NULL,
  description STRING(512) NOT NULL,
  time_created TIMESTAMPTZ NOT NULL,
  time_modified TIMESTAMPTZ NOT NULL,
  time_deleted TIMESTAMPTZ,

  silo_id UUID NOT NULL,

  audience text not null,
  issuer text not null,
  verification_type text not null,
  discovery_url text,
  signing_keys jsonb
)

Note: in the full implementation, we'll want to cache the JWKS metadata, but we'll skip that for the PoC.

On each authenticated request, we'll optionally accept a `FederationSession`, which will be returned by /v1/federation/inbound/token. This `FederationSession` will be another database resource, analogous to `ClientSession` and `DeviceAuthToken`, but including the id of the assumed trust policy, as well as the verified evidence (i.e. claims) from the incoming JWT. If the token is set in the request, we'll look it up in the database, then verify whether the associated grants can authorize the request.

Like the ConsoleSession and DeviceAccessToken methods, we'll also store FederationSession records in the database.

We can make the token ttl configurable in the future, but the PoC will use a fixed ttl of 5m.

Endpoints:

* POST /v1/federation/inbound/token
  {
    trust_policy (NameOrId),
    oidc_jwt,
  }
  ---
  {
    token,
    expires_at,
    revision,
  }

```
create table if not exists omicron.public.federation_session (
  id UUID PRIMARY KEY,
  time_created TIMESTAMPTZ NOT NULL,
  time_last_used TIMESTAMPTZ NOT NULL,
  trust_policy_id UUID NOT NULL,
  trust_policy_revision INT8 NOT NULL,
  jwt_claims JSONB NOT NULL,
  token STRING(40) NOT NULL
)  
```

When the user makes a request using a `FederationToken`, we'll represent their authentication using a new `Actor`:

```
/// Who is performing an operation
#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum Actor {
    UserBuiltin { user_builtin_id: BuiltInUserUuid },
    SiloUser { silo_user_id: SiloUserUuid, silo_id: Uuid },
    Scim { silo_id: Uuid },
    Federated { federation_token_id: FederatedSessionUuid, silo_id: Uuid },
}
```

On each request authenticated by a FederationToken, we load the token from the database, check whether it's been invalidated, look up its grants, and proceed with authorization as normal. Note that we'll invalidate the FederationToken on any change to the relevant FederationTrustPolicy: to its grants, access policy, or identity provider. Deleting a policy invalidates any outstanding tokens.

Audit logs: even for a PoC, this is critical for a security-relevant feature. We'll log the credential type (FederationToken) and ID (the token's uuid). It might be worth thinking about including more structured information about the federation token, as well as other token types--but that's out of scope for now.

Invalidation and revisions: when a trust policy or identity provider is updated or deleted, what do we do with active federation tokens? If a low-impact field like `description` changes, we don't have to do anything. If a load-bearing field like the trust policy's identity provider, grants, conditions, etc., changes, we'll invalidate any extant tokens. We also invalidate on deleting a trust policy. To invalidate on update, we increment the `revision` of the trust policy. Each federation token carries the revision number of the trust policy from when it was issued; on each request, we check both that the token isn't expired, and that its revision matches the current revision of its trust policy, and that its trust policy isn't deleted. Arguably, we should also invaliate outstanding tokens when an identity provider updates--but to avoid the complication, we simply prevent mutation of IdP fields that would require invalidation. We'll also prevent deletion of identity policies until no trust policies reference them.

PoC implementation, commit by commit:
* Identity provider: model sql, database model, endpoints
* Trust policy: model sql (trust policy, role grant), database model, endpoints
* Federation token: model sql, database model, endpoints
* Authentication flow: FederationSession actor, verify token expiry/revision/policy, load roles
* Audit logs (actions run using the federation session)
* Auth failure logs (login and auth failures getting a federation session)
  * Bonus out of scope feature: extend existing auth methods to log more action failures

This is a somewhat high-level spec. We don't include full definitions for all tables, request/responses, etc. And there are probably mistakes! Implementation should honor the spec as best we can, but flag ambiguities and errors in the spec as needed.

Security notes:
* We'll use `ExternalHttpClient` for JWKS discovery so that a malicious user can't use OIDC for SSRF.
* We'll use crdb transactions to guard against race conditions: e.g. an admin updates or deletes a trust policy mid-login.

Testing/validation strategy:

Our PoC implementation will include at least basic unit tests, but we'll rely more on external validation. We'll spin up a fake OIDC IdP and configure its static signing key, as well as a GCP IDP. Both for demo purposes and to validate the PoC, we'll exercise both flows (static vs discovery) against a simulated Omicron setup to verify the flows end to end.

The full PoC test flow:

* We'll use GCP as our OIDC identity provider. First, set up a GCP service account, and allow it to make OIDC tokens.
* On simulated omicron (or whatever omicron), create an identity policy to match (discovery url https://accounts.google.com/.well-known/openid-configuration, etc.).
* Create test resources: silos a, b, and c. In silo a, projects 1, 2, and 3; in silo b, project 4; in silo c, project 5. We'll also create an anti-affinity policy in each project.
* Create a trust policy granting multiple project-level roles in silo a: read on project 1, write on project 2.
* Create a trust policy granting view access to silo b.
* Get an OIDC token with gcloud, using an audience matching the omicron identity provider.
* Request a federation token from omicron using the GCP OIDC token.
* Use the federation token to do allowed actions on omicron: read (but not write) project 1, read and write project 2. With the second trust policy, read (but not write) project 4. We'll test the other negative cases as well: neither trust policy can read/write project 5.
* We'll verify audit logs for each authentication attempt, and authorized actions using federation tokens.
