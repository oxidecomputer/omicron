# Plan: convert update (PUT) bodies to value semantics

Date: 2026-06-23

Companion to the reference audit: [`2026-06-23-put-body-optionality-audit.md`](./2026-06-23-put-body-optionality-audit.md)
(full PUT inventory, optionality buckets, and datastore merge-behavior audit).

## Goal

Make update request bodies use **value semantics** instead of the current
`Option`-everywhere "PATCH-via-PUT" pattern:

- non-nullable resource fields → required `T`
- clearable fields → `Nullable<T>` (present on the wire, may be explicit `null`)
- no bare `Option<T>` "absent = leave unchanged"

`InstanceUpdate` is the existing model of the good pattern. This makes a `PUT`
body actually be "the complete new representation," which is what a reviewer
(ahl) asked for on the new BGP update endpoint, and resolves the standing
ambiguity of "did the client omit this or want it cleared."

This doc is about whether we can do it **across all ~19 lenient update
endpoints** without breaking existing clients, and a proof-of-concept plan.

## The key enabler: versioning absorbs the back-compat break

The API is versioned. Each endpoint has a real handler at the latest version
plus shim handlers for older versions. Request bodies translate **upward**
(old → … → latest) via **pure, synchronous `From`/`Into`**; those shims live in
the `nexus/external-api` *trait crate*, which has **no datastore access**.

Consequence that matters here: **you cannot pure-`From`-convert a lenient old
body (Option fields) up to a strict new body (required fields)** — you can't
synthesize a required value from an absent one. So a strict latest sitting above
a lenient old version needs *something* more than the stock machinery.

### Two ways to provide that "something"

**(A) Stateful translation — fetch the existing object and fill omitted fields
in a compat shim.** Datastore becomes a clean overwrite; back-compat merge is
quarantined in shims that get deleted when old versions are retired (cruft gets
an expiry date). Costs: an extra DB read per old-client update, and it splits
the read (shim) from the write (datastore), **breaking the atomic
read-modify-write** — two concurrent old-client PUTs can clobber each other
(TOCTOU). To keep atomicity you'd push the merge back into the datastore
transaction, which re-introduces a partial type there.

**(B) Strict wire type, lenient internal — chosen.** Keep the existing
datastore merge (it already preserves omitted fields — see below). Make only the
*new wire type* strict. Each version's handler independently converts its own
wire body into the existing internal/db update representation and calls the same
nexus method:

- **new strict body** → internal params with every field present
- **old lenient body** → internal params preserving `None` (exactly as today)

No DB read in translation, datastore transaction untouched, no up-conversion
needed (versions don't chain to each other — each handler calls nexus directly).
The "fill from existing" step is the datastore merge we already have.

We pick **(B)**: the datastore already does the merge, so (A)'s only payoff (a
clean datastore) is largely cancelled by its atomicity cost. ahl's concern is
about the *wire* contract, which (B) satisfies for new clients.

### Why this dissolves the `name`/`description` blocker

The reason "just make the fields required" looked impossible: every identity
update flattens `IdentityMetadataUpdateParams` (`Option<Name>` + `Option<String>`),
and requiring `name` would force every existing client to resend it.

But the **version boundary is exactly where changing the contract is allowed.**
Old clients keep omitting `name`; their omission is absorbed by the datastore
merge they already rely on. New-version clients must send it. No shipped client
breaks. So "required name breaks everyone" stops being a compat argument and
becomes a pure design choice about new clients — and for a `PUT`, requiring the
full object is defensible.

## Why the datastore barely changes (audit result)

Of the in-scope lenient endpoints (see companion audit, "Datastore
merge-behavior audit"):

- **15** use `#[derive(AsChangeset)]`, which skips `None` columns → preserve-on-omit for free.
- **1** (BGP) does explicit `unwrap_or(existing...)`.
- **1 exception**: `support_bundle_update` does `.set(user_comment.eq(None))`,
  so omitting it **clears** the field today. Value semantics (`Nullable<String>`)
  *corrects* this latent bug rather than breaking it.
- No in-scope changeset uses `treat_none_as_null`, so none have their
  `None`-skipping flipped.

So feeding old lenient bodies through the existing datastore path preserves
current behavior everywhere. The work is at the wire-type + handler-wiring layer,
not the datastore.

## Per-endpoint conversion recipe (shape B)

For a shipped endpoint `x_update` currently lenient at latest:

1. **New version + strict type.** Add a dated version (use the `add-api-version`
   skill). Define a strict `XUpdate` in the new version module: required `T`,
   `Nullable<T>` for clearable fields, identity inlined as required `name` +
   (required or `Nullable`) `description` instead of flattened
   `IdentityMetadataUpdateParams`. Re-export as `latest::…::XUpdate`.
2. **Freeze the old shape** as the prior version's `XUpdate` (the
   `add-api-version` skill moves the current type into the frozen version
   module); annotate the existing handler `versions = ..VERSION_NEW`,
   `operation_id = "x_update"`.
3. **Handlers (in `http_entrypoints`, where nexus is reachable).** Both the new
   strict handler and the old lenient handler convert their wire type into the
   existing internal/db update representation and call the **same**
   `nexus.x_update(...)`. They do *not* delegate to each other (no impossible
   up-conversion). Strict → all fields present; lenient → preserves `None`.
4. **Datastore / nexus app layer: unchanged** (AsChangeset / explicit merge
   already handle it). Exception: `support_bundle_update`, where the strict
   `Nullable` path now sends an explicit value.
5. **Response type unchanged** — these are request-body-only changes, so no
   response down-translation is needed.
6. Regen: `cargo xtask openapi generate`, then the TS client in
   `../console/app/api/__generated__/Api.ts`.

Blast radius per endpoint: one new version module + one strict type + one added
handler + regen. One compat version per endpoint (all prior clients shared the
lenient shape). Mechanical, not deep.

## Proof-of-concept: one endpoint per category

Convert one representative of each conversion shape in a single PoC PR to prove
the recipe end-to-end before mechanizing the rest. Suggested shared version
(e.g., `UPDATE_VALUE_SEMANTICS_POC`) introducing all four strict types at once.

| # | Category | Endpoint | What it proves | Datastore bucket |
|---|----------|----------|----------------|------------------|
| 1 | Identity-only (the pervasive case) | `project_update` | required `name`; old clients still omit it via the merge | B (AsChangeset) |
| 2 | Identity + required operational + clearable | `vpc_subnet_update` | mixed: required name/desc + `Nullable<NameOrId>` for `custom_router_id` (already `Option<Option<Uuid>>` internally — cleanest Nullable mapping) | B |
| 3 | Operational scalars, no identity | `silo_quotas_update` | required `cpus`/`memory`/`storage` with no identity entanglement (a real behavior tightening) | B |
| 4 | Clearable-only / latent-bug fix | `support_bundle_update` | `Nullable<String>` user_comment; value semantics corrects today's clear-on-omit | C |

Each exercises a distinct part of the recipe:
- **#1** the identity → required-name conversion and the old-client merge path
  (the single most-replicated pattern; ~14 endpoints flatten identity).
- **#2** the full mixed struct + the `Nullable` ↔ `Option<Option<_>>` mapping.
- **#3** required scalars with no `Nullable` and no identity.
- **#4** the one datastore exception, confirming the version split preserves
  old (clearing) behavior while new clients get explicit-null semantics.

### PoC acceptance checks

- New strict version rejects a body missing a required field (e.g., `name`),
  returns the full updated object.
- A request pinned to the **prior** API version still accepts a partial body and
  leaves omitted fields unchanged (regression test per endpoint).
- `nt unauthorized` (verify-endpoints) passes for the changed endpoints; add the
  new PUT bodies to `endpoints.rs` as needed.
- OpenAPI + TS client regen produce the expected `required` arrays.

## Rollout after PoC

- Land the PoC; review the recipe and the diff-per-endpoint cost.
- Decide identity policy once, globally: does `description` become required or
  `Nullable` (clearable)? `name` is required (non-nullable on the resource).
- Mechanize the remaining ~15 endpoints, likely a few per version bump to keep
  diffs reviewable (each version bump = OpenAPI + TS regen). Avoid one big-bang
  version touching everything.

## Architectural shift: where does the old→changeset conversion live?

Strict-latest forces a small but real change to the versioning mental model,
surfaced by the PoC.

**The pre-existing invariant:** inner layers only ever see `latest`. Every older
version's request body is up-converted to the latest wire type *at the edge*
(the `_vN` trait shims do `body.map(Into::into)`), so nothing past the handler
boundary knows about old versions. That's why `nexus-db-model` depended only on
`nexus-types` (which re-exports `latest`) and never on `nexus-types-versions`
(the crate holding *all* versions).

**Why strict-latest breaks it:** that invariant held only because old→latest was
always a lossless pure conversion. When the latest body is *stricter* (required
fields) than the old one, old→latest can't be a pure `From` — you can't
synthesize a required value from an absent one. So the old shape can no longer
be collapsed at the edge; it must survive inward to wherever it gets merged
against existing state. That merge layer becomes the first inner code that must
know a non-`latest` wire type.

**The orphan-rule fork.** A `From<old_wire> for db_model::XUpdate` impl must live
in whichever crate owns one of the types. db-model owns the `AsChangeset`, so the
impl lands in db-model — which is exactly what drags `nexus-types-versions` into
db-model. The alternative is to *not* use a `From` impl and build the changeset
in the nexus crate, which already legitimately depends on every wire version.

- **db-model `From` (first PoC pass):** conversions co-located with the changeset,
  but db-model becomes permanently version-aware and accumulates a `From<vN>` +
  version-module import per tightened resource across the ~19-endpoint sweep —
  version-awareness migrates *into* the domain layer.
- **nexus-side conversion (chosen):** db-model keeps only `From<latest>` (it
  always knew `latest`); the prior-version merge lives in nexus app methods
  (`project_update_v2025_11_20_00`, `vpc_update_subnet_v2025_11_20_00`,
  `silo_update_quota_v2025_11_20_00`), which the `_vN` HTTP handlers call.
  db-model drops its `nexus-types-versions` dependency. The asymmetry is
  intentional: **db-model knows the current shape (always did); the nexus app
  layer owns the back-compat merges for prior versions** — the version-specific,
  eventually-deletable logic stays where the rest of the version-awareness already
  lives (the edge), and gets deleted when old versions are retired.

**Decision (David, 2026-06-23): nexus-side is the standard for the sweep.**
db-model must not become aware of the API version timeline — that is the
deciding principle, weighed above the relative code cost. Tradeoffs accepted:
the nexus app layer accumulates one `*_vN` merge method per tightened
(resource, version), and the per-endpoint conversion is split (strict body via
db-model `From<latest>`, lenient body via the nexus `*_vN` method) rather than
co-located. In exchange, db-model keeps only `From<latest>` and has no
dependency on `nexus-types-versions`.

The rejected alternative — a `From<vN>` impl in db-model per tightened resource
— was simpler to find (both conversions adjacent) and more consistent with
db-model's role as the wire→db conversion hub, but it makes a foundational crate
import dated version modules, which we don't want.

Implementation: the PoC rev holds the db-model `From` approach; the rev on top
of it converts to this nexus-side approach (to be squashed in).

## Related, orthogonal: `description` optional on create (parked)

`description` being required on create bodies is silly (callers pass `""`); an
audit ([`2026-06-23-create-body-optionality-audit.md`](./2026-06-23-create-body-optionality-audit.md))
confirms it's essentially the *only* required-but-trivial create field. We'll
make it optional on create, but as its **own separate change** — it touches
create bodies, not updates, and is mechanically simple. **Not doing it yet.**

This resolves the update-side `description` question (below) *conditionally*: if
`description` stays a non-nullable `String` (the low-friction "option A" in the
create note), then on update it's just a required `String` under value
semantics — **no `Nullable<String>`**. The only way it becomes `Nullable<String>`
on update is if we choose "option B" (genuinely nullable + migrate existing
`""` → `NULL` across every identity-bearing table) — an open question owned by
David, leaning no.

## Open questions

- Identity `description` on update: **required `String`** (if create-side stays
  non-nullable, recommended) **or `Nullable<String>`** (only if we go nullable +
  migrate `"" → NULL`). Gated on the create-note A-vs-B decision; resolve before
  scaling the identity conversions.
- Should the strict identity fields be a reusable struct
  (e.g., `IdentityMetadataUpdateParamsRequired`) to avoid re-inlining name/desc
  in ~14 types?
- `vpc_firewall_rules_update` and `bgp_announce_set_update` are already
  replace-style (whole-set) PUTs reusing Create-shaped bodies — already
  value-shaped, exclude from the sweep.
- BGP config update hasn't shipped yet — it can simply be made strict now with
  no compat version, independent of this sweep.
