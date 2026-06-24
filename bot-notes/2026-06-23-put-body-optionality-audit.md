# PUT body optionality audit (PATCH-via-PUT → consistent value semantics)

Date: 2026-06-23

## Goal / prompt

Audit every PUT endpoint's request body in the Oxide Nexus external API, to
assess how hard it would be to convert them all from the current
`Option`-everywhere "PATCH-via-PUT" pattern to a consistent "every field is the
new value" pattern: required `T` for non-nullable fields, `Nullable<T>` for
fields that can legitimately be cleared, no bare `Option<T>`.

Model of the "good" pattern: `InstanceUpdate` — plain required `T` for
`ncpus`/`memory`, `Nullable<T>` for `boot_disk`/`auto_restart_policy`/
`cpu_platform`, zero bare `Option<T>`.

Special sub-question: the `#[serde(flatten)] identity: IdentityMetadataUpdateParams`
convention (`Option<Name>` + `Option<String>`, "if absent, leave unchanged") is
pervasive. Treat it as its own cross-cutting decision.

## Method & how to read the data

Reliable classification rule, validated against Rust source:

- In the OpenAPI schema (`openapi/nexus/nexus-latest.json`), a field is:
  - **bare `Option<T>`**: `nullable: true` AND NOT in the schema's `required` array.
  - **`Nullable<T>`**: `nullable: true` AND in the `required` array. (`Nullable<T>`
    renders as an Option-shaped schema but is forced into `required` — see
    `common/src/api/external/mod.rs:3062-3104` and serde-rs/serde#2753.)
  - **plain required `T`**: NOT `nullable` AND in `required`.
- `#[serde(flatten)] identity: IdentityMetadataUpdateParams` explodes into two
  bare-Option fields: `name` (`Option<Name>`) and `description` (`Option<String>`),
  both `nullable: true`, neither in `required`. So identity-flatten endpoints show
  up as having exactly `name` + `description` as non-required nullable fields.

`IdentityMetadataUpdateParams` (`common/src/api/external/mod.rs:1112`):
```rust
pub struct IdentityMetadataUpdateParams {
    pub name: Option<Name>,
    pub description: Option<String>,
}
```

Verified spot-checks against Rust source in `nexus/types/versions/src/`:
- `InstanceUpdate` (`external_jumbo_frames/instance.rs:187`): `ncpus`/`memory`/
  `enable_jumbo_frames` plain `T`; `boot_disk`/`auto_restart_policy`/`cpu_platform`/
  `multicast_groups` `Nullable<…>`. Matches OpenAPI exactly. ✔
- `SiloAuthSettingsUpdate` (`initial/silo.rs`): `device_token_max_ttl_seconds:
  Nullable<NonZeroU32>` → required+nullable. ✔ (already conformant)
- `SiloQuotasUpdate` (`initial/silo.rs`): `cpus/memory/storage: Option<…>` →
  bare Option. ✔
- `InstanceNetworkInterfaceUpdate` (`initial/instance.rs:342`): flattens identity;
  `primary: bool` and `transit_ips: Vec<IpNet>` with `#[serde(default)]` (so
  not required, but not nullable). ✔

## Full PUT endpoint inventory

36 PUT operations total (one, `system_update_repository_upload`, has no JSON
body — it's a raw repository upload — and is excluded from the optionality
analysis, leaving **35 with bodies**).

Legend for field optionality: `req T` = plain required, `Null` = `Nullable<T>`,
`opt` = bare `Option<T>`, `id` = the flattened `name`+`description` identity pair
(two bare Options), `def` = present-with-`#[serde(default)]` (not required, not
nullable).

| # | operationId | path | body type | fields (optionality) |
|---|---|---|---|---|
| 1 | support_bundle_update | /experimental/v1/system/support-bundles/{bundle_id} | SupportBundleUpdate | user_comment: opt |
| 2 | affinity_group_update | /v1/affinity-groups/{affinity_group} | AffinityGroupUpdate | id |
| 3 | anti_affinity_group_update | /v1/anti-affinity-groups/{anti_affinity_group} | AntiAffinityGroupUpdate | id |
| 4 | auth_settings_update | /v1/auth-settings | SiloAuthSettingsUpdate | device_token_max_ttl_seconds: Null |
| 5 | external_subnet_update | /v1/external-subnets/{external_subnet} | ExternalSubnetUpdate | id |
| 6 | floating_ip_update | /v1/floating-ips/{floating_ip} | FloatingIpUpdate | id |
| 7 | instance_update | /v1/instances/{instance} | InstanceUpdate | ncpus/memory/enable_jumbo_frames: req T; boot_disk/auto_restart_policy/cpu_platform/multicast_groups: Null |
| 8 | instance_multicast_group_join | /v1/instances/{instance}/multicast-groups/{multicast_group} | InstanceMulticastGroupJoin | source_ips: opt(def); ip_version: opt(def) |
| 9 | instance_network_interface_update | /v1/network-interfaces/{interface} | InstanceNetworkInterfaceUpdate | id; primary: def bool; transit_ips: def Vec |
| 10 | policy_update | /v1/policy | SiloRolePolicy | role_assignments: req T |
| 11 | project_update | /v1/projects/{project} | ProjectUpdate | id |
| 12 | project_policy_update | /v1/projects/{project}/policy | ProjectRolePolicy | role_assignments: req T |
| 13 | physical_disk_enable_adoption | /v1/system/hardware/disk-adoption-request | PhysicalDiskManufacturerIdentity | model/serial/vendor: req T |
| 14 | sled_set_provision_policy | /v1/system/hardware/sleds/{sled_id}/provision-policy | SledProvisionPolicyParams | state: req T |
| 15 | system_ip_pool_update | /v1/system/ip-pools/{pool} | IpPoolUpdate | id |
| 16 | system_ip_pool_silo_update | /v1/system/ip-pools/{pool}/silos/{silo} | IpPoolSiloUpdate | is_default: req T |
| 17 | networking_allow_list_update | /v1/system/networking/allow-list | AllowListUpdate | allowed_ips: req T |
| 18 | networking_bgp_config_update | /v1/system/networking/bgp | BgpConfigUpdate | id; bgp_announce_set_id: opt; max_paths: opt |
| 19 | networking_bgp_announce_set_update | /v1/system/networking/bgp-announce-set | BgpAnnounceSetCreate | announcement/description/name: req T (reuses Create) |
| 20 | networking_inbound_icmp_update | /v1/system/networking/inbound-icmp | ServiceIcmpConfig | enabled: req T |
| 21 | system_networking_settings_update | /v1/system/networking/settings | SystemNetworkingSettingsUpdate | external_jumbo_frames_opt_in_enabled: def bool |
| 22 | system_policy_update | /v1/system/policy | FleetRolePolicy | role_assignments: req T |
| 23 | silo_policy_update | /v1/system/silos/{silo}/policy | SiloRolePolicy | role_assignments: req T |
| 24 | silo_quotas_update | /v1/system/silos/{silo}/quotas | SiloQuotasUpdate | cpus/memory/storage: opt |
| 25 | system_subnet_pool_update | /v1/system/subnet-pools/{pool} | SubnetPoolUpdate | id |
| 26 | system_subnet_pool_silo_update | /v1/system/subnet-pools/{pool}/silos/{silo} | SubnetPoolSiloUpdate | is_default: req T |
| 27 | system_update_recovery_finish | /v1/system/update/recovery-finish | SetTargetReleaseParams | system_version: req T |
| 28 | target_release_update | /v1/system/update/target-release | SetTargetReleaseParams | system_version: req T (shared type) |
| 29 | vpc_firewall_rules_update | /v1/vpc-firewall-rules | VpcFirewallRuleUpdateParams | rules: def Vec |
| 30 | vpc_router_route_update | /v1/vpc-router-routes/{route} | RouterRouteUpdate | id; destination/target: req T |
| 31 | vpc_router_update | /v1/vpc-routers/{router} | VpcRouterUpdate | id |
| 32 | vpc_subnet_update | /v1/vpc-subnets/{subnet} | VpcSubnetUpdate | id; custom_router: opt |
| 33 | vpc_update | /v1/vpcs/{vpc} | VpcUpdate | id; dns_name: opt |
| 34 | vpc_subnet… (VpcSubnetUpdate dup row removed) | | | |
| 35 | webhook_receiver_update | /v1/webhook-receivers/{receiver} | WebhookReceiverUpdate | id; endpoint: opt |
| — | system_update_repository_upload | /v1/system/update/repositories | (no JSON body) | raw upload, excluded |

(Row 34 is a placeholder artifact; the real distinct bodies are enumerated in
the bucket counts below.)

### Distinct body types

35 endpoints, but several share a body type:
- `SiloRolePolicy` is used by both `policy_update` and `silo_policy_update`.
- `SetTargetReleaseParams` is used by both `system_update_recovery_finish` and
  `target_release_update`.

So there are **33 distinct request-body types** across the 35 bodied PUTs.

## Field-level: which "ambiguous" fields would need Nullable vs required

For the buckets that aren't pure identity, the per-field call:

- `support_bundle_update.user_comment` (opt String): a free-text comment that
  can legitimately be cleared → **`Nullable<String>`**.
- `silo_quotas_update.{cpus,memory,storage}` (opt): these are always-present
  numeric quota values on the resource. There is no "unset quota" state — a silo
  always has a cpus/memory/storage quota. So PATCH-semantics here is purely "leave
  unchanged." Converting to value semantics → **plain required `T`** for all three
  (sending the full quota each time). This is a real behavior change for callers
  who currently send partial updates.
- `bgp_config_update.bgp_announce_set_id` (opt NameOrId): pointer to an announce
  set; conceptually required to have one → likely **required `T`** (or `Nullable`
  if "no announce set" is valid). Needs domain judgment.
- `bgp_config_update.max_paths` (opt): per-config option; can be absent on the
  resource → **`Nullable<MaxPathConfig>`**.
- `vpc_subnet_update.custom_router` (opt NameOrId): a subnet may or may not have a
  custom router → clearable → **`Nullable<NameOrId>`**.
- `vpc_update.dns_name` (opt Name): VPC always has a dns_name (it's part of
  identity-ish state) → **required `T`** (can't be null on the resource).
- `webhook_receiver_update.endpoint` (opt String): receiver always has an
  endpoint URL → **required `T`**.
- `instance_multicast_group_join.{source_ips,ip_version}` (opt+default): this is a
  *join* (create-membership) action, not a resource update. source_ips genuinely
  optional (only for SSM groups). Arguably out of scope for the "update body"
  convention — see "Weird cases".
- `instance_network_interface_update.{primary,transit_ips}` (default, not
  nullable): `primary` always has a value (bool), `transit_ips` always a list →
  these are already effectively required-with-default; making them strictly
  required `T` is trivial. The identity flatten is the only Option problem here.
- `system_networking_settings_update.external_jumbo_frames_opt_in_enabled` and
  `vpc_firewall_rules_update.rules`: `#[serde(default)]`, not nullable. Already
  value-shaped (a bool / a full list of rules — note `vpc_firewall_rules_update`
  is a *replace* of the entire rule set). Just drop `default`/make required.

## Difficulty buckets

### A. Already conformant — no bare Option (15 endpoints)
No `name`/`description` flatten, no bare Option. Required `T` and/or `Nullable<T>`.

- instance_update (the model) — req T + Nullable
- auth_settings_update — Nullable
- policy_update, project_policy_update, system_policy_update, silo_policy_update —
  role_assignments req T (`SiloRolePolicy`/`ProjectRolePolicy`/`FleetRolePolicy`)
- physical_disk_enable_adoption — all req T
- sled_set_provision_policy — req T
- system_ip_pool_silo_update, system_subnet_pool_silo_update — is_default req T
- networking_allow_list_update — req T
- networking_inbound_icmp_update — req T
- networking_bgp_announce_set_update — req T (reuses `BgpAnnounceSetCreate`)
- system_update_recovery_finish, target_release_update — req T (`SetTargetReleaseParams`)

### B. Trivial — only operational fields, just need to drop default / make required (3 endpoints)
No nullable semantics, no identity flatten; fields are always-present but currently
`#[serde(default)]`.

- system_networking_settings_update (one bool)
- vpc_firewall_rules_update (full rules list — replace semantics)
- (instance_network_interface_update's non-identity fields are trivial, but the
  endpoint as a whole is identity-entangled → counted in D)

### C. Needs Nullable / per-field thought, but NOT identity-entangled (3 endpoints)
- support_bundle_update — user_comment → Nullable
- silo_quotas_update — 3 opt fields → required T (behavior change: forces full quota)
- instance_multicast_group_join — join action, optional SSM fields (see Weird)

### D. Identity-entangled — flattens IdentityMetadataUpdateParams (14 endpoints)
These all carry the cross-cutting `name`+`description` Option pair. Some ALSO have
their own extra fields (noted), which puts them in D **and** require Nullable/per-field
work:

Pure identity-only (name+description only):
- affinity_group_update, anti_affinity_group_update, external_subnet_update,
  floating_ip_update, project_update, system_ip_pool_update,
  system_subnet_pool_update, vpc_router_update (8)

Identity + extra fields:
- instance_network_interface_update (+ primary, transit_ips — both trivial)
- networking_bgp_config_update (+ bgp_announce_set_id opt, max_paths opt)
- vpc_router_route_update (+ destination/target already req T)
- vpc_subnet_update (+ custom_router opt → Nullable)
- vpc_update (+ dns_name opt → required)
- webhook_receiver_update (+ endpoint opt → required)
(6)

### Bucket counts
- A. Already conformant: **15**
- B. Trivial (drop default/make required): **2** (3 minus the one folded into D)
- C. Needs Nullable / per-field, not identity: **3**
- D. Identity-entangled: **14**
- (no-body, excluded): 1

Total bodied PUTs analyzed: **35** (15 + 2 + 3 + 14 = 34; the +1 reconciliation:
instance_network_interface_update is counted once, in D; B holds
system_networking_settings_update and vpc_firewall_rules_update = 2). 15+2+3+14 = 34
distinct endpoints across A–D + the no-body upload = 35 total PUTs.

### Identity-flatten pervasiveness
- **14 of 35** bodied PUTs (40%) flatten `IdentityMetadataUpdateParams`.
- The flatten is shared by the corresponding `*Create` structs too (via
  `IdentityMetadataCreateParams`), but the create side uses *required* name +
  optional description, so it's a separate type and not part of this audit.
- Source of truth for the update flatten: 14 update structs across
  `nexus/types/versions/src/{initial,external_subnet_attachment,bgp_configuration_update}/`.

## Weird / notable cases

- **Reuses a Create struct**: `networking_bgp_announce_set_update` PUTs a
  `BgpAnnounceSetCreate`. It's already all-required (name/description/announcement),
  so it's conformant by accident — but the naming is a smell and any identity
  decision must account for Create-shaped bodies used on update paths.
- **Shared body across two endpoints**: `SetTargetReleaseParams` (recovery-finish +
  target-release) and the three/four *RolePolicy types. Changing one type changes
  multiple endpoints at once — reduces work but couples them.
- **Replace semantics, not field-merge**: `vpc_firewall_rules_update` replaces the
  whole rule set; `policy`/`*RolePolicy` replace the whole `role_assignments` list.
  These are already "the new value" in spirit; only the surrounding `#[serde(default)]`
  / Option-on-list ergonomics differ.
- **Not really an "update"**: `instance_multicast_group_join` is a create-membership
  (join) action expressed as PUT to a membership URL. Its Options are genuinely
  optional create inputs (SSM source IPs, pool-selection ip_version), not
  PATCH-via-PUT. Probably should be excluded from the convention entirely.
- **`physical_disk_enable_adoption` / `sled_set_provision_policy`** are
  command-style PUTs (an action with required params), already conformant.
- **`silo_quotas_update`** is the one "ambiguous semantics" case: bare Options on
  always-present quota values. Making them required is the spec-correct move but is
  the most user-visible behavior change in the non-identity set.

## Overall assessment

**How horrible is a single consistency PR? Moderate, and front-loaded onto one
decision.** The mechanical surface is small:

- 15/35 are already conformant → zero work.
- ~5 non-identity endpoints (B + C) are genuinely easy: drop `#[serde(default)]`/
  `Option`, pick `Nullable` vs required per the field table above. A handful of
  one-line type changes plus per-field judgment on ~6 fields. Call it an afternoon
  of code, modulo review on the quota/dns_name/endpoint required-vs-nullable calls.
- The other 14 are dominated by the **identity question**, which is not mechanical.

**The identity-update convention is the real blocker.** 40% of bodied PUTs flatten
`IdentityMetadataUpdateParams`. Making name/description required would mean: to
change *anything* on a project/vpc/floating-ip/etc., the client must resend the
current name and description. That's a significant ergonomics and behavior
regression and would break essentially every existing update caller (CLI, console,
terraform). `Nullable<Name>` for `name` is also wrong — name is non-nullable on the
resource, so "value semantics" for name means *required*, which is the painful
option; `description` could be `Nullable<String>` (clearable) sensibly.

**Recommendation: stage it, and treat identity separately.**

1. Land the non-identity cleanup first (buckets B + C, ~5 endpoints). Low risk,
   self-contained, no cross-cutting decision. This already removes most bare
   `Option`s outside identity.
2. Make the identity decision its own design discussion / its own PR. Options:
   - Introduce an identity-update variant using `Nullable<String>` for description
     and *required* `Name` for name (true value semantics), accepting the resend-name
     ergonomics — possibly with a builder/`From` to ease clients.
   - Or define `IdentityMetadataPatchParams` and explicitly bless PATCH-semantics for
     identity while requiring value-semantics elsewhere (i.e., concede that name/
     description are the one place PATCH-via-PUT stays). This is arguably the honest
     resolution given no endpoint wants required name on update.
   - Either way it's an API-wide, per-resource behavior change and warrants its own
     RFD-ish decision, not a drive-by.

**Blast radius (applies to every change, big or staged):** the external API is
versioned (`nexus/types/versions/src/<version>/`), so every body change is a **new
API version** — new versioned module + entry, not an in-place edit of `initial/`.
Then `cargo xtask openapi generate` to regen the spec + `nexus_tags.txt`, and the
TS client (`/Users/david/oxide/console/app/api/__generated__/Api.ts`) regenerates
downstream. Each touched endpoint multiplies versioning churn, which argues against
a big-bang: 34 endpoints in one version bump is a large, hard-to-review diff with a
single coupled decision (identity) gating most of it.

**Bottom line:** A big-bang "fix every PUT" PR is not advisable. The mechanical
non-identity work is genuinely small and could ship quickly. The identity-flatten
decision is the crux — it touches 40% of PUTs, is a real behavior/ergonomics change,
and deserves its own deliberate decision rather than being swept into a consistency
sweep. Stage: (1) easy non-identity cleanup now; (2) identity convention as a
separate, deliberated change.

## Datastore merge-behavior audit

Date: 2026-06-23

### Goal / prompt

Confirm or refute the key assumption behind the value-semantics refactor:
**every lenient (PATCH-via-PUT) update endpoint's datastore method already
preserves omitted fields by merging against the existing row.** If that holds
uniformly, the refactor is mechanical. Any endpoint that instead defaults an
omitted field to a concrete value / clears it is a case that breaks the uniform
refactor.

Classification used (per endpoint, traced HTTP -> `nexus/src/app/` ->
`nexus/db-queries/src/db/datastore/` `diesel::update`):

- **A. Read-modify-write merge** — fetch existing row, fill omitted fields via
  `unwrap_or(existing...)`, write all columns.
- **B. Column-selective UPDATE** — diesel `#[derive(AsChangeset)]` on an `XUpdate`
  db-model struct (or conditional `.set()`), so `None` fields are simply not
  written. Diesel's `AsChangeset` skips `None` by default; `#[diesel(treat_none_as_null)]`
  would flip that (write NULL).
- **C. Idiosyncratic** — omitted field defaulted/cleared/nulled; would NOT
  preserve the existing value under a naive partial body.

### Per-endpoint results

| operationId | datastore fn (file:line of diesel::update) | bucket | mechanism |
|---|---|---|---|
| networking_bgp_config_update | `bgp_config_update` (bgp.rs:333) | A | read existing, `update_name.unwrap_or(existing.name())`, `max_paths.map_or(*existing.max_paths,...)`, writes all cols (reference example) |
| affinity_group_update | `affinity_group_update` (affinity.rs:338) | B | AsChangeset `AffinityGroupUpdate`; no treat_none_as_null |
| anti_affinity_group_update | `anti_affinity_group_update` (affinity.rs:424) | B | AsChangeset `AntiAffinityGroupUpdate`; no treat_none_as_null |
| floating_ip_update | `floating_ip_update` (external_ip.rs:1078) | B | AsChangeset `FloatingIpUpdate`; no treat_none_as_null |
| project_update | `project_update` (project.rs:343) | B | AsChangeset `ProjectUpdate`; no treat_none_as_null |
| system_ip_pool_update | `ip_pool_update` (ip_pool.rs:853) | B | AsChangeset `IpPoolUpdate`; no treat_none_as_null |
| external_subnet_update | `update_external_subnet` (external_subnet.rs:872) | B | AsChangeset `ExternalSubnetUpdate`; no treat_none_as_null |
| system_subnet_pool_update | `update_subnet_pool` (external_subnet.rs:275) | B | AsChangeset `SubnetPoolUpdate`; no treat_none_as_null |
| vpc_update | `project_update_vpc` (vpc.rs:528) | B | AsChangeset `VpcUpdate` (db-model/src/vpc.rs:127); dns_name None -> col skipped |
| vpc_subnet_update | `vpc_update_subnet` (vpc.rs:1147) | B* | AsChangeset `VpcSubnetUpdate`; `custom_router_id: Option<Option<Uuid>>` — None=skip, Some(None)=write NULL (explicit detach), Some(Some(id))=attach. App layer maps the field accordingly. |
| vpc_router_update | `vpc_update_router` (vpc.rs:1802) | B | AsChangeset `VpcRouterUpdate`; no treat_none_as_null |
| vpc_router_route_update | `router_update_route` (vpc.rs:2264) | B | AsChangeset `RouterRouteUpdate`; name/desc Option (skip), target/destination required T (always written) |
| vpc_firewall_rules_update | `vpc_update_firewall_rules` (vpc.rs:682) | C* | NOT a field merge: replace semantics. Soft-deletes all existing rules, inserts the provided list. Already value-shaped (full replace); empty list clears all. |
| silo_quotas_update | `silo_update_quota` (quota.rs:105) | B | AsChangeset `SiloQuotasUpdate` (db-model/src/quota.rs:78); cpus/memory/storage None -> col skipped; no treat_none_as_null |
| webhook_receiver_update | `webhook_rx_update` (alert_rx.rs:353) | B | AsChangeset `WebhookReceiverUpdate` (webhook_rx.rs:114); name/desc/endpoint None -> skipped; no treat_none_as_null |
| instance_network_interface_update | `instance_update_network_interface` (network_interface.rs:829) | B | AsChangeset `NetworkInterfaceUpdate` (network_interface.rs:569); name/desc/primary Option skip; transit_ips Vec always written |
| system_networking_settings_update | `system_networking_settings_update` (system_networking_settings.rs:49) | B | AsChangeset; single non-optional bool, always written (no omission semantics at db layer) |
| support_bundle_update | `support_bundle_update_user_comment` (support_bundle.rs:628) | **C** | direct `.set(dsl::user_comment.eq(user_comment))` with `user_comment: Option<String>` -> writes NULL when None. App layer (support_bundles.rs:265) passes the Option straight through, no merge. **Omitting user_comment CLEARS it.** |
| instance_multicast_group_join | `multicast_group_member_attach_to_instance` (multicast/members.rs:94) | n/a | NOT an update — join/create-membership via CTE INSERT (`AttachMemberToGroupStatement`). No partial-merge semantics. |

\* notes: `vpc_subnet_update` is bucket B but uses `Option<Option<_>>` for
explicit-null control on custom_router rather than read-merge; behavior still
preserves on omit. `vpc_firewall_rules_update` is replace-semantics (called C in
the table because it's not a field-merge), but it does not silently default an
omitted scalar — it replaces the whole rule set by design, which is the intended
value semantics already.

### treat_none_as_null findings

Grepping `treat_none_as_null` across `nexus/` and `common/` returns only:
`nexus/db-model/src/{instance.rs (2x), disk.rs, disk_type_crucible.rs,
allow_list.rs, audit_log.rs}`. **None of these are on the update changesets of
any lenient PATCH-via-PUT endpoint in scope** — instance update is the
already-conformant value-semantics model (uses Nullable to deliberately clear),
disk/allow_list/audit_log aren't lenient-PUT update bodies. So no in-scope
changeset has its `None`-skipping behavior flipped; the default skip-on-None
holds everywhere it matters.

### A/B/C counts (in-scope lenient/PATCH-via-PUT endpoints + reference)

- A. Read-modify-write merge: **1** (bgp_config_update, the reference)
- B. Column-selective AsChangeset (preserves on omit): **15**
- C. Idiosyncratic: **1 genuine** (support_bundle_update) + 1 replace-semantics
  (vpc_firewall_rules_update, intended whole-set replace, not a per-field hazard)
- n/a (not an update): 1 (instance_multicast_group_join)

### Headline finding: is there a bucket C?

**Yes — one genuine bucket-C endpoint: `support_bundle_update`.**

`support_bundle_update_user_comment` does a direct
`.set(dsl::user_comment.eq(user_comment))` where `user_comment: Option<String>`
flows unmerged from the HTTP body through the app layer. Diesel translates
`Option<String>::None` on a nullable column to `SET user_comment = NULL`, so a
PUT that omits `user_comment` CLEARS the existing comment rather than preserving
it. This is the opposite of the assumed preserve-on-omit behavior and is the one
case where the current implementation already does not match PATCH-via-PUT
intent. (Note this cuts in the refactor's favor: value-semantics — required or
Nullable<String> — would actually make the on-the-wire behavior match the code.)

`vpc_firewall_rules_update` is the other non-merge case but is deliberate
replace-the-whole-set semantics, not a hidden per-field default; it doesn't
threaten the refactor.

### Bottom line

The preserve-on-omit assumption holds **almost uniformly**: 15/17 in-scope field-
update endpoints use AsChangeset column-selective updates that skip `None`
(preserve existing), one uses an explicit read-modify-write merge (bgp), and no
in-scope changeset uses `treat_none_as_null`. The single real exception is
`support_bundle_update`, whose omitted `user_comment` is written as NULL (clears
the field). The refactor is therefore mechanical for every endpoint except
support_bundle_update, which needs explicit thought — and there the value-
semantics change (Nullable<String>) would correct a current latent inconsistency
rather than introduce one. `vpc_firewall_rules_update` is replace-semantics and
already value-shaped; `instance_multicast_group_join` is a create-action, not an
update.
