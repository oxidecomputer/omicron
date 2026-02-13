# RFD 619 compliance review: nexus-external-api migration

Reviewed commit `02aaa1c141` (`[nexus-external-api] reorganize per RFD 619`)
against the determinations in RFD 619.

## Critical violations (fixed in this session)

### 1. `InstanceDiskAttach` re-exported from wrong version in `latest.rs` -- FIXED

**File:** `nexus/types/versions/src/latest.rs:178`
**Rule:** `determinations-earliest-version`

`InstanceDiskAttach` is `{ pub name: Name }` in every version where it
appears (initial, local_storage, read_only_disks_nullable). It never changes.
It was re-exported from `v2026013100` but should come from the earliest
version (`v2025112000`). Additionally, the type was redundantly redefined in
`local_storage/instance.rs` and `read_only_disks_nullable/instance.rs`.

**Fix applied:**
- Moved re-export in `latest.rs` to `v2025112000` group.
- Removed redundant struct definitions in `local_storage` and
  `read_only_disks_nullable`.
- Updated imports in `instances_external_subnets` and `read_only_disks` to
  point to `v2025112000`.
- Simplified `From` impls that no longer need to convert between identical
  types.

### 2. Version module sort order violation in `lib.rs` -- FIXED (by user)

**File:** `nexus/types/versions/src/lib.rs:53-55`
**Rule:** `determinations-named-subdirectories` (rustfmt-consistent sort order)

`v2026011601` (EXTERNAL_SUBNET_ATTACHMENT) appeared after `v2026012100` but
is numerically smaller. Fixed by reordering.

### 3. `external_subnet_attachment` (v2026011601) references later version v2026012200 -- FIXED

**Files:** `external_subnet_attachment/external_subnet.rs`,
`external_subnet_attachment/subnet_pool.rs`
**Rule:** `determinations-only-prior-versions`

The v2026011601 module contained `TryFrom` and `From` impls that converted
**to** `v2026012200::*` types (a later version). Per the RFD, conversion
code from version N to version M (where M > N) should live in version M's
module.

**Fix applied:**
- Removed conversion impls and `use crate::v2026012200;` from
  `external_subnet_attachment/` files (type definitions remain).
- Added equivalent conversion impls in `floating_ip_allocator_update/`
  files as `TryFrom<v2026011601::...> for ...` and
  `From<v2026011601::...> for ...`.

## Moderate violations (not yet fixed)

### 4. Missing conversion for `ExternalIpDetach` in v2026012300

**File:** `nexus/types/versions/src/dual_stack_ephemeral_ip/instance.rs`
**Rule:** `determinations-one-prior-version`

`ExternalIpDetach` changes from a unit `Ephemeral` variant (v2025112000) to
a struct `Ephemeral { ip_version: Option<IpVersion> }` variant (v2026012300).
No `From<v2025112000::instance::ExternalIpDetach>` conversion exists. The
natural conversion maps `Ephemeral` to `Ephemeral { ip_version: None }`.

### 5. `latest.rs` disk module uses curly-brace grouped imports

**File:** `nexus/types/versions/src/latest.rs:100-109`
**Rule:** `determinations-reexports` (no wildcards, each re-export by name)

Every other module in `latest.rs` lists each re-export on its own `pub use`
line. The `disk` module uses curly-brace grouped imports and should follow
the same one-per-line pattern.

### 6. Types re-exported from `omicron_common` in types crate facade modules

**Files:** `nexus/types/src/external_api/bfd.rs`, `ip_pool.rs`, `system.rs`
**Rule:** `determinations-types-in-versions`

These facade modules re-export types like `BfdMode`, `IpRange`, `IpVersion`,
`Ipv4Range`, `Ipv6Range` from `omicron_common` rather than from the versions
crate. If these are published API types, they should be in a versions crate.
If `omicron-common-versions` doesn't exist yet, this is a known gap.

## Minor issues (not yet fixed)

### 7. Missing doc comment on `v2026013001_instance_create`

**File:** `nexus/external-api/src/lib.rs:3396`

This prior-version endpoint lacks a `///` doc comment, unlike every other
endpoint.

### 8. `nexus-client` spec path doesn't use `-latest.json` symlink

**File:** `clients/nexus-client/src/lib.rs:9`

Uses a specific version file name rather than the `-latest.json` symlink.
Functionally equivalent since the internal API is frozen, but the RFD
prescribes using the symlink.

### 9. `impls/multicast.rs` validation functions in `latest.rs`

**File:** `nexus/types/versions/src/latest.rs:237-238`

Free functions from `impls` re-exported through `latest.rs`. They don't fit
the "grouped by version" pattern. Consider whether these belong here or in a
separate utilities module.

### 10. `policy.rs` test module has `Deserialize`/`Debug` derives in `impls`

**File:** `nexus/types/versions/src/impls/policy.rs:10-12`

A test-only dummy type `DummyRoles` derives `Deserialize` and `Debug` in the
`impls` module. The RFD says `impls` should not contain serialization code,
but this is on a test-only type, not a versioned API type.

## What passes cleanly

- **Named subdirectory paths** all correctly match the lowercase
  `api_versions!` identifiers.
- **`mod impls;`** is correctly private, before `pub mod latest;`.
- **All `impls` code** uses `latest::` identifiers, not versioned ones.
- **Types crate facade** modules all use wildcard re-exports from `latest`.
- **Latest API endpoints** all use floating `latest::` identifiers.
- **Prior API endpoints** all use versioned identifiers, set `operation_id`,
  and are in descending order.
- **Most conversion chains** are correct (per-type prior version, proper
  direction).
- **No floating identifiers** in any version module (except the forward
  reference fixed above).
- **Types in the initial version** appear correct.
- **Lockstep API and client** properly updated.
