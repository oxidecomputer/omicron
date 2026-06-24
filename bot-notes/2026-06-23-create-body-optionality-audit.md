# Create-body optionality audit (is `description` uniquely "required-but-trivial"?)

Date: 2026-06-23

## Goal / question

The API's `IdentityMetadataCreateParams` (`common/src/api/external/mod.rs:1102`)
is `#[serde(flatten)]`ed into most create bodies and contains a **required**
`name: Name` and a **required** `description: String`. Hypothesis: `description`
being required is silly — callers routinely pass `""` to satisfy it. We're
considering making `description` optional on create (`Nullable` on update).

**Question:** is `description` the standout case of a field that is nominally
required but effectively meaningless (people pass `""`), or are there other
create-body fields with the same smell?

Framing: lots of genuinely-optional `Option<T>` fields are NOT a problem (e.g.,
instance create's many knobs — fine, expected). The smell we hunt is the
opposite: fields declared **required** (plain `T`, no `Option`, no
`#[serde(default)]`) but with an obvious trivial empty/zero/default value a
caller would just pass to get past the requirement. Be conservative; default to
"genuinely meaningful."

```rust
// common/src/api/external/mod.rs:1102
pub struct IdentityMetadataCreateParams {
    pub name: Name,         // required
    pub description: String, // required  <-- the archetype
}
```

## Method

Same technique as the PUT audit
(`.claude/notes/2026-06-23-put-body-optionality-audit.md`, "Method" section):
read field optionality off the latest OpenAPI spec
(`openapi/nexus/nexus-latest.json`, symlink → `nexus-2026060800.0.0-f1db6e.json`)
and cross-check Rust in `nexus/types/versions/src/`.

Classification per field from the spec:
- **plain required `T`**: in the schema's `required` array, `nullable:false`,
  no `default`. (This is what we're auditing — the requiredness candidates.)
- **`#[serde(default)]`**: NOT in `required`, has a `default`, not nullable.
  (The "done right" version: optional-with-default.)
- **bare `Option<T>`** / **`Nullable<T>`**: `nullable:true` (not-required vs
  required respectively). Genuinely-optional knobs — not in scope as a problem.

The spec already reflects the latest field shape, so its `required`/`default`/
`nullable` flags ARE the latest Rust attributes; no per-version chasing needed.

## Create endpoint inventory

POST endpoints with a `*Create`-shaped body (operationId matching `create`),
plus create-like POSTs that take a body:

**`*_create` operations (32 with JSON bodies; `scim_token_create` and
`system_update_trust_root_create` have no body):**
affinity_group, anti_affinity_group, certificate, current_user_ssh_key,
disk, external_subnet, floating_ip, image, instance,
instance_network_interface, internet_gateway, internet_gateway_ip_address,
internet_gateway_ip_pool, local_idp_user (UserCreate),
networking_address_lot, networking_bgp_config, networking_loopback_address,
networking_switch_port_settings, probe, project, saml_identity_provider,
silo, snapshot, support_bundle, system_ip_pool, system_subnet_pool, vpc,
vpc_router, vpc_router_route, vpc_subnet, webhook_receiver (WebhookCreate).

**Create-like POSTs with bodies (not named `*_create`):**
alert_receiver_subscription_add (AlertSubscriptionCreate),
instance_ephemeral_ip_attach (EphemeralIpCreate),
webhook_secrets_add (WebhookSecretCreate). (Plus many action POSTs:
attach/detach/start/stop/range_add/etc. — not create-shaped, excluded.)

## Per-body required-field analysis

Legend: **req T** = plain required (the candidates); **def** = `#[serde(default)]`
(optional-with-default); **opt** = bare `Option`/`Nullable` (genuine optional,
not a problem). `id` = the flattened identity pair (`name` req T + `description`
req T).

| Body | required `T` fields (besides identity) | def fields | opt/nullable fields |
|---|---|---|---|
| AffinityGroupCreate | id; failure_domain, policy | — | — |
| AntiAffinityGroupCreate | id; failure_domain, policy | — | — |
| CertificateCreate | id; cert, key, service | — | — |
| SshKeyCreate | id; public_key | — | — |
| DiskCreate | id; disk_backend, size | — | — |
| ExternalSubnetCreate | id; allocator | — | — |
| FloatingIpCreate | id | address_allocator | — |
| ImageCreate | id; os, version, source | — | — |
| InstanceCreate | id; hostname, memory, ncpus | anti_affinity_groups, disks, enable_jumbo_frames, external_ips, multicast_groups, network_interfaces, start, user_data | auto_restart_policy, boot_disk, cpu_platform, ssh_public_keys |
| InstanceNetworkInterfaceCreate | id; subnet_name, vpc_name | ip_config | — |
| InternetGatewayCreate | id | — | — |
| InternetGatewayIpAddressCreate | id; address | — | — |
| InternetGatewayIpPoolCreate | id; ip_pool | — | — |
| UserCreate | external_id, password | — | — |
| AddressLotCreate | id; kind, **blocks** (Vec) | — | — |
| BgpConfigCreate | id; asn, bgp_announce_set_id | max_paths | vrf |
| LoopbackAddressCreate | address, address_lot, anycast, mask, rack_id, switch_slot | — | — |
| SwitchPortSettingsCreate | id; **addresses** (Vec), **links** (Vec), port_config | bgp_peers, groups, interfaces, routes | — |
| ProbeCreate | id; sled | pool_selector | — |
| ProjectCreate | id | — | — |
| SamlIdentityProviderCreate | id; acs_url, idp_entity_id, idp_metadata_source, slo_url, sp_client_id, technical_contact_email | — | group_attribute_name, signing_keypair |
| SiloCreate | id; **discoverable** (bool), identity_mode, quotas, **tls_certificates** (Vec) | mapped_fleet_roles | admin_group_name |
| SnapshotCreate | id; disk | — | — |
| SupportBundleCreate | — | — | user_comment |
| IpPoolCreate | id | ip_version, pool_type | — |
| SubnetPoolCreate | id; ip_version | — | — |
| VpcCreate | id; dns_name | — | ipv6_prefix |
| VpcRouterCreate | id | — | — |
| RouterRouteCreate | id; destination, target | — | — |
| VpcSubnetCreate | id; ipv4_block | — | custom_router, ipv6_block |
| WebhookCreate | id; endpoint, **secrets** (Vec) | subscriptions | — |
| AlertSubscriptionCreate | subscription | — | — |
| EphemeralIpCreate | — | pool_selector | — |
| WebhookSecretCreate | secret | — | — |

## (1) Description baseline — confirmed

`description: String` is **required, plain `String`, non-nullable, no default**
in every create body that flattens `IdentityMetadataCreateParams`.

**28 create bodies flatten `IdentityMetadataCreateParams`** (verified against the
spec: both `name` and `description` present, required, `name` is a `$ref` to
`Name`, `description` is a plain required string):

affinity_group, anti_affinity_group, certificate, ssh_key, disk,
external_subnet, floating_ip, image, instance, instance_network_interface,
internet_gateway, internet_gateway_ip_address, internet_gateway_ip_pool,
address_lot, bgp_config, switch_port_settings, probe, project,
saml_identity_provider, silo, snapshot, ip_pool, subnet_pool, vpc, vpc_router,
router_route, vpc_subnet, webhook.

**Bodies that do NOT flatten identity** (no required `name`+`description` pair):
- `UserCreate` — `external_id` + `password` only (silo-local user).
- `LoopbackAddressCreate` — operational networking fields, no name/description.
- `SupportBundleCreate` — only `user_comment` (bare Option/nullable). No name,
  no required description. **This body already has no required description.**
- `AlertSubscriptionCreate` — `subscription` only.
- `EphemeralIpCreate` — `pool_selector` (defaulted) only.
- `WebhookSecretCreate` — `secret` only.

No create body makes `description` *optional while still carrying identity* —
i.e., none has already done the change we're considering. The only "deviations"
are bodies that simply don't have identity at all (above).

## (2) Other required-but-trivial fields?

Walking every plain-required field and judging "trivial empty/zero/default
exists" vs "genuinely must be supplied":

**Genuinely meaningful (the overwhelming majority)** — no sensible trivial
default; requiredness is correct:
- Identity `name` (a resource with no name is meaningless; not "" — `Name` is
  validated non-empty).
- CIDR/address/size/target fields: `VpcSubnetCreate.ipv4_block`,
  `VpcCreate.dns_name`, `RouterRouteCreate.{destination,target}`,
  `InternetGatewayIpAddressCreate.address`, `LoopbackAddressCreate.{address,mask,...}`,
  `DiskCreate.size`, `BgpConfigCreate.{asn,bgp_announce_set_id}`,
  `SubnetPoolCreate.ip_version`.
- Credential/material fields: `CertificateCreate.{cert,key}`,
  `SshKeyCreate.public_key`, `UserCreate.{external_id,password}`,
  `WebhookSecretCreate.secret`, all of `SamlIdentityProviderCreate`'s URLs/IDs.
- Enums / discriminated sources with no neutral default:
  `*AffinityGroupCreate.{policy,failure_domain}`, `ImageCreate.source`,
  `DiskCreate.disk_backend`, `SiloCreate.identity_mode`, `AddressLotCreate.kind`.
- `WebhookCreate.endpoint` (a receiver with no URL is useless).
- `WebhookCreate.secrets` (Vec): **explicitly documented "A non-empty list"**
  (alert.rs:393). Requiredness is load-bearing and the authors even contrast it
  with `subscriptions` next to it, which IS `#[serde(default)]` ("if empty… not
  subscribed"). Genuinely meaningful.

**Has the `description` smell (required but a trivial default obviously exists)** —
candidates, in rough descending order of confidence:

1. **`description: String`** (28 bodies) — the archetype. Empty string is the
   obvious trivial value; a description conveys nothing the system needs.

2. **`SiloCreate.tls_certificates: Vec<CertificateCreate>`** (required Vec). An
   empty list is a structurally valid value (`Vec::default()`), and the sibling
   collection `mapped_fleet_roles` in the *same struct* is already
   `#[serde(default)]`. Whether a silo can sensibly be created with zero certs
   is a domain question (HTTP-only / cert-added-later), but mechanically this is
   a required collection with a trivial empty default and no `#[serde(default)]`
   — the same shape as the smell. Weaker than `description` because an empty
   cert list may be genuinely undesirable, but worth flagging.

3. **`SiloCreate.discoverable: bool`** (required bool). Booleans always have a
   defaultable value; the question is only whether the default is obvious.
   `discoverable` plausibly defaults to `true` (most silos are discoverable),
   so this is arguably "required but a sensible default exists." Lower
   confidence — picking the wrong default silently changes behavior, which is
   exactly why the authors may have left it required. Flagged but conservative.

**Borderline / rejected on conservatism:**
- `AddressLotCreate.blocks: Vec` and `SwitchPortSettingsCreate.{addresses,links}:
  Vec` are required collections, but an address lot with no blocks / a switch
  port settings group with no addresses or links is a degenerate/useless object.
  Sibling collections on `SwitchPortSettingsCreate` (`bgp_peers`, `groups`,
  `interfaces`, `routes`) ARE `#[serde(default)]`, which shows the authors
  deliberately kept `addresses`/`links` required. Treat as **meaningful**.

**Bottom line for (2):** `description` is essentially the standout. The only
fields sharing its exact shape (required, trivially-emptyable, no
`#[serde(default)]`) are `SiloCreate.tls_certificates` and — more weakly —
`SiloCreate.discoverable`. Both are single-endpoint, niche (silo creation is
rack-operator-level), and debatable on the merits, whereas `description` is the
cross-cutting 28-endpoint case where the trivial default is unambiguous. There
is no broad pattern of required-but-trivial fields hiding elsewhere.

## (3) `#[serde(default)]` precedent — already widely used

`#[serde(default)]` is the established "optional-with-default" pattern across
create bodies (this is the "done right" shape we'd apply to description). Fields
already using it (req=false, has default, non-nullable), from the spec:

- `FloatingIpCreate.address_allocator`
- `InstanceCreate`: `anti_affinity_groups`, `disks`, `enable_jumbo_frames`,
  `external_ips`, `multicast_groups`, `network_interfaces`, `start`, `user_data`
  (8 fields — the heaviest user)
- `InstanceNetworkInterfaceCreate.ip_config`
- `BgpConfigCreate.max_paths`
- `SwitchPortSettingsCreate`: `bgp_peers`, `groups`, `interfaces`, `routes`
- `ProbeCreate.pool_selector`
- `SiloCreate.mapped_fleet_roles`
- `IpPoolCreate`: `ip_version`, `pool_type`
- `WebhookCreate.subscriptions`
- `EphemeralIpCreate.pool_selector`

So ~21 create-body fields across ~10 bodies already use `#[serde(default)]`.
It's a well-worn precedent, applied to both scalars (`start: bool`,
`enable_jumbo_frames: bool`) and collections (Vecs/maps). Making `description`
a `#[serde(default)] String` (defaulting to `""`) or `#[serde(default)]
Option<String>` would be entirely consistent with existing practice — indeed
`WebhookCreate.subscriptions` vs `secrets` is a side-by-side example of the
authors choosing `#[serde(default)]` for the "fine to omit" field and required
for the "must supply" one.

## (4) Blast radius of making `description` optional on create

`IdentityMetadataCreateParams` is a **single shared struct** flattened into all
28 identity-bearing create bodies. Editing the shared struct is **one source
edit** that ripples to all 28 endpoints at once — you would NOT touch each
`*Create` individually (unless a per-type opt-out were desired).

Mechanics / cost:
- The external API is **versioned** (`nexus/types/versions/src/<version>/`).
  Changing a flattened shared type means defining a new versioned variant of
  `IdentityMetadataCreateParams` (the `initial/` copy stays put) and wiring the
  28 create bodies in the new version to use it. That's one type change but a
  **version bump that simultaneously alters every create endpoint's body** — a
  large, coupled diff, even though it's a single conceptual edit.
- Then `cargo xtask openapi generate` regenerates the spec + `nexus_tags.txt`,
  and the TS client (`/Users/david/oxide/console/app/api/__generated__/Api.ts`)
  regenerates downstream.
- Datastore impact is minimal: `description` already lands as a `String`;
  defaulting an omitted one to `""` matches today's caller behavior exactly
  (callers already send `""`). No merge/null semantics to reason about on create
  (unlike the PUT/update side, where the parallel decision is `Nullable<String>`).
- Backward compatibility: making a previously-required field optional is a
  non-breaking widening for clients (old clients still send it; new clients may
  omit it). The new version is additive in that sense.

If instead a *per-type* choice were wanted (e.g., keep description required on
silo/cert but optional elsewhere), it would stop being one edit and become
per-struct — but there's no apparent reason to; the field is uniformly trivial.

## Decision (2026-06-23) — parked, not yet

**Do make `description` optional on create**, but **not yet** — it's simple and
orthogonal to the update value-semantics sweep, so it ships as its own change
when we get to it, not bundled with the PoC.

Recommended shape (pending the open question below): `#[serde(default)]` keeping
it a plain `String` defaulting to `""`. One edit to the shared
`IdentityMetadataCreateParams`, matches what callers already do, **zero
datastore/model change**.

### How this relates to the update sweep (resolves a plan open question)

It feels like it cuts against "make update fields required," but it doesn't:
create-absent and update-absent are different semantics.
- **Create**: absent = *use the default*. Stateless, unambiguous (no prior
  object). Optional-with-default is correct — already done 21× via `#[serde(default)]`.
- **Update PUT**: absent = *default? or unchanged?* — the ambiguous PATCH-via-PUT
  reading we're removing, so we force presence.

The principle is "**absent must have one clear meaning**," not "required
everywhere." So "optional on create, sent on update" is just the normal
create/update asymmetry (like `name`: no default → required on create;
`description`: has `""` default → optional). Consequently, if `description`
stays a non-nullable `String`, on **update** it's just a required `String` under
value semantics — **no `Nullable<String>` needed**. (See the plan note's open
questions, now resolved conditionally on the choice below.)

### Open question: go all the way and migrate existing `""` → `NULL`?

Two coherent end-states:

- **A (low-friction, recommended):** `description` stays non-nullable `String`;
  `#[serde(default)]` → `""`. `""` remains the empty state. No column change, no
  data migration. On update it's a required `String`.
- **B ("go all the way"):** make `description` genuinely nullable —
  `Option<String>` on create (default `None`), `Nullable<String>` on update
  (settable or explicit-null-to-clear) — and **migrate existing `""` → `NULL`**
  so "no description" has a single representation.

Trade-offs:
- B is semantically cleaner (one empty representation; distinguishes "never set"
  from "explicitly empty"), but that distinction has little practical payoff
  here — nothing is known to treat `""` and `NULL` differently.
- B's cost is real: `description` is on **every identity-bearing resource table**
  (the identity-metadata pattern), so making the column nullable + the `"" → NULL`
  data migration touches *dozens* of tables. A `dbinit.sql`/migration change
  (see `schema/crdb/README.adoc`), mechanically simple but broad.
- **Half-measures are the worst option:** going nullable *without* migrating
  leaves a mix of `""` (old rows) and `NULL` (new empties) — permanent
  inconsistency. So it's genuinely A vs B, not a spectrum.

Leaning A unless someone wants the NULL/"" distinction badly enough to justify a
dozens-of-tables migration. **Decision owner: David.**

## Answers (summary)

- **(a) Just `description`, mostly.** It's the cross-cutting archetype (28
  bodies). The only other fields with the same "required but trivially
  emptyable, no `#[serde(default)]`" shape are `SiloCreate.tls_certificates`
  (required `Vec`, empty is a valid default, sibling map is already defaulted)
  and — weakly — `SiloCreate.discoverable` (required bool, plausible `true`
  default). Both are single-endpoint and debatable; not a broad pattern.
  Everything else required is genuinely meaningful (CIDRs, sizes, credentials,
  enums, `WebhookCreate.secrets` which is explicitly "non-empty").
- **(b) Blast radius: 28 create bodies, one shared edit.** `description` lives
  in the shared flattened `IdentityMetadataCreateParams`, so it's a single
  source change that ripples to all 28 identity-bearing create endpoints — but
  as a versioned-API change it's one new version touching every create endpoint
  at once (large coupled diff + `cargo xtask openapi generate` + TS-client
  regen). No datastore complications on create.
- **(c) `#[serde(default)]` is a well-established precedent**: ~21 fields across
  ~10 create bodies already use it (InstanceCreate alone has 8). The
  `WebhookCreate` struct shows the authors deliberately using it for "fine to
  omit" and requiredness for "must supply" side by side. Applying it to
  `description` is idiomatic.
- **(d) Already-deviating bodies**: six create bodies don't flatten identity at
  all and thus have no required `description`: `UserCreate`,
  `LoopbackAddressCreate`, `SupportBundleCreate` (its only field, `user_comment`,
  is already an optional/nullable free-text comment — the closest thing to an
  "already optional description"), `AlertSubscriptionCreate`, `EphemeralIpCreate`,
  `WebhookSecretCreate`. No identity-bearing body has pre-emptively made
  `description` optional.
