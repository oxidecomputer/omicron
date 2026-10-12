# Inbound OIDC with service accounts

As described in RFD 745 (prediscussion at ~/code/rfd), Oxide currently lacks support for *inbound OIDC*, or generically, a way for outside services to authenticate to Nexus without long-lived credentials. We described inbound OIDC as blocked on the implementation of service accounts in that draft RFD, and while that's not strictly true, it's a natural fit to model the permissions associated with an external user as a service account. For a discussion of inbound OIDC without service accounts, see https://github.com/oxidecomputer/omicron/compare/jmcarp/inbound-oidc-poc. This spec contemplates inbound OIDC as an extension of service accounts.

== Concepts

=== FederatedIdentityProvider

First, we'll teach Nexus to act as an OIDC RP. When it receives a request for a federated token, at e.g. /v1/oidc/token, it will verify the token signature against the public key of the well-known OIDC endpoint, as well as `iss`, `aud`, etc. This entails a subtask of configuring each trusted OIDC IdP: its well-known url and JWKS metadata. This will be a separate database resource--call it `FederatedIdentityProvider`. Then a service account can be associated with a given IdP to allow external users to act on its behalf.

Endpoints:
* GET /v1/federation/inbound/identity-providers/{idp_id}
* GET /v1/federation/inbound/identity-providers/
* POST /v1/federation/inbound/identity-provivders/
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
* PATCH /v1/federation/inbound/identity-providers/{idp_id}
  {
    name,
    description,
    signing_keys,
  }
* DELETE /v1/federation/inbound/identity-providers/{idp_id}

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

=== ServiceAccount

Next, we need to model service accounts. Fundamentally, a service account is a named bundle of roles. At the moment, we only support pre-defined roles, at the fleet, silo, and project levels. For this spec, we'll support silo- and project- scoped service accounts. Silo-scoped service accounts can grant access at the level of the configured silo, or at the level of any project contained within the silo. Project-scoped service accounts can grant access to the target project only:

```
name: example-silo-sa
scope: silo
silo: <silo_id>
grants:
- resource_kind: silo
  resource_id: <silo_id>
  role_name: viewer
```

```
name: example-silo-sa
scope: silo
silo: <silo_id>
grants:
- resource_kind: project
  resource_id: <project_1_id>
  role_name: viewer
- resource_kind: project
  resource_id: <project_2_id>
  role_name: collaborator
```

```
name: example-project-sa
scope: project
project: <project_id>
grants:
- resource_kind: project
  resource_id: <project_id>
  role_name: viewer
```

There are two levels of permission to think about here: permission required to modify a service account, and permission required to act on its behalf (i.e. to "assume" it). Service accounts are inherently prone to privilege escalation: if a user can manage a service account and grant it arbitrary permissions, they can also grant it permissions they don't have themselves, and grant themselves the ability to assume it. Public cloud providers with fine-grained IAM systems manage this problem by creating separate permissions for creating service accounts, assigning them roles or permissions, and provisioning access tokens on their behalf. As of this writing, we don't have custom roles, per-resource controls, or an equivalent of IAM conditions--so in order to avoid the risk of privilege escalation, we'll simply require silo admin to create *or* assume a silo service account, and project admin to create *or* assume a project service account.

We'll allow Oxide users with adequate privileges to "assume" service accounts via Nexus, yielding a bearer token with permission to act on behalf of the service account. We'll also allow users to configure service accounts to be assumed by external identities via OIDC federation:

```
name: example-federated-sa
scope: project
project: <project_id>
grants:
- resource_kind: project
  resource_id: <project_id>
  role_name: viewer
federation:
  identity_provider: <idp_id>
  policy: |-
    assume(claims) if
      claims.email = "metrics-reader@example-project.iam.gserviceaccount.com" and
      claims.google.organization_number = 123456789012;
max_ttl_seconds: 300
default_ttl_seconds: 60
```

Service account tokens can also set a ttl up to `max_ttl_seconds`. If the max ttl is unset, the user can also omit the ttl from the request and pass `unbounded: true` in order to request a token that never expires. Note that federation tokens cannot be unbounded, and non-federated tokens also cannot be unbounded if the authenticated user requesting the token has a time-bounded credential.

We'll allow admins to revoke service account tokens, but as a convenience, we'll also store a `federation_generation`, which is incremented with the federation configuration (identity provider or policy) changes. Previously-issued service account tokens whose generation doesn't match the service account's current generation will be considered invalid.

Endpoints:

* GET /v1/service-accounts/{scope}/{sa_id}?project=project
* GET /v1/service-accounts/{scope}/
* POST /v1/service-accounts/{scope}/
  * Requires silo/project admin
  {
    name,
    description,
    grants,
    federation,
  }
  ---
  201
  {
    id: ...
  }
* PATCH /v1/service-accounts/{scope}/{sa_id}
  * Requires silo/project admin
* DELETE /v1/service-accounts/{scope}/{sa_id}
  * Requires silo/project admin

Database models:

create table if not exists service_account (
  id UUID PRIMARY KEY,
  name STRING(63) NOT NULL,
  description STRING(512) NOT NULL,
  time_created TIMESTAMPTZ NOT NULL,
  time_modified TIMESTAMPTZ NOT NULL,
  time_deleted TIMESTAMPTZ,

  scope STRING NOT NULL,
  resource_id UUID NOT NULL,
  token_max_ttl_seconds INT8 NOT NULL DEFAULT 3600,
  token_default_ttl_seconds INT8 NOT NULL DEFAULT 3600,

  federation_generation INT8 NOT NULL DEFAULT 1,
  identity_provider_id UUID,
  trust_policy STRING,
)

create table if not exists service_account_grant (
  id UUID PRIMARY KEY,
  service_account_id UUID NOT NULL,
  resource_kind STRING NOT NULL,
  resource_id UUID NOT NULL,
  role_name STRING NOT NULL,
)

=== ServiceAccountToken

Once a service account exists, and optionally configures a federation trust policy and identity provider, users need a way to act on behalf of the service account, or assume it. We'll implement this via bearer tokens, using two paths.

First, authenticated Oxide users with adequate permission (silo admin for silo-scoped service accounts, project admin for project-scoped service accounts) can request a bearer token via the external API at /v1/service-account/token.

Second, users can assume a service account via OIDC federation. If the service account configures a `federation_policy`, users can also request a bearer token on behalf of the service account by presenting a signed JWT to a separate unauthenticated /v1/service-account/federation-token endpoint.

In both cases, a successful request receives a bearer token, recorded in a new ServiceAccountToken table. If the token was requested via federation, we'll additionally record federation details for the token: the IdP, JWT evidence, etc.:

Endpoints:

* POST /v1/service-accounts/{scope}/{sa_id}/token
  {
    ttl_seconds,
    unbounded,
  }
  ---
  201
  {
    id,
    token,
    expires,
  }
* POST /v1/service-accounts/{scope}/{sa_id}/federation-token
  {
    jwt,
    ttl_seconds,
  }
  ---
  201
  {
    id,
    token,
    time_expires,
  }

create table if not exists service_account_token (
  id UUID PRIMARY KEY,
  time_created TIMESTAMPTZ NOT NULL,
  time_last_used TIMESTAMPTZ NOT NULL,

  service_account_id UUID NOT NULL,
  token STRING(40) NOT NULL,

  -- Nullable fields specific to federated auth
  idp_id UUID,
  federation_jwt_claims JSONB,
  federation_generation INT8,

  -- Nullable fields specific to expiration and revocation
  time_expires TIMESTAMPTZ,
  time_deleted TIMESTAMPTZ,
)

== Security considerations

As mentioned above, service accounts can be a vector for privilege escalation if we're not careful.

First, users can escalate their privileges by creating or assuming service accounts that have privileges beyond their own. Given the lack of fine-grained authorization in Oxide today, the most straightforward mitigation is to restrict service account management and assumption to admin users of the (silo|project) in which they reside. We'll also require silo admin to manage IdP resources, since trusting a malicious IdP would potentially allow untrustworthy users to authenticate.

Second, users can use service account tokens to request more service account tokens, circumventing the max ttl for service accounts that set one. This is analogous to the exploit fixed in a recent (security advisory)[https://docs.oxide.computer/security/advisories/20251117-1], such that device tokens with an expiry time could be used to request additional device tokens whose expiry exceeded the expiry of the previous token. We'll mitigate this potential escalation by ensuring that the expiry time of any given service account token is no later than the expiry of the credential used to request it (device token, service account token, etc.).

== Implementation

PoC implementation, commit by commit:
* Identity provider: model sql, database model, endpoints. This should be shared with the previous POC.
* Service account: model sql, database model, endpoints.
* Service account token: model sql, database model, endpoints.
  * /v1/service-account/{sa_id}/token: Get a service account token as an authenticated Oxide user.
  * /v1/service-account/{sa_id}/federation-token: Get a service account token via inbound OIDC.
* Authentication flow: ServiceAccountSession actor, verify token expiry/generation/policy, load roles.
* Audit logs (actions run using the service account session).
* Auth failure logs (login and auth failures getting a service account session).
