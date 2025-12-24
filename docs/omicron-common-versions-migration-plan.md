# Omicron-Common Versions Migration Plan (per RFD 619)

## Status: Context Window 1 - BLOCKED (Architectural Challenge)

This document follows the `nexus-external-api-migration-plan.md` pattern and analyzes the feasibility of implementing the `omicron-common-versions` crate per RFD 619.

## TL;DR

Creating `omicron-common-versions` is **blocked by a circular dependency challenge**. The current approach in `nexus-types-versions` (defining v1 Disk/DiskType locally and re-exporting latest from `omicron-common`) is a valid intermediate state per RFD 619.

## Analysis

### The Problem

Per RFD 619, each types crate should have a corresponding versions crate:
- `sled-agent-types` → `sled-agent-types-versions` ✓
- `nexus-types` → `nexus-types-versions` ✓
- `omicron-common` → `omicron-common-versions` (this migration)

The RFD states: "each types crate depends on the corresponding versions crate" (line 116).

This means:
- `omicron-common` would depend on `omicron-common-versions`
- Therefore `omicron-common-versions` CANNOT depend on `omicron-common` (circular!)

### Why This Is a Problem

The `Disk` type in `omicron-common-versions` would need access to:
- `ByteCount` (defined in `omicron-common`)
- `DiskState` (defined in `omicron-common`)
- `IdentityMetadata` (defined in `omicron-common`)

Without being able to depend on `omicron-common`, these would all need to be moved to `omicron-common-versions` as well. This cascades because these types have their own dependencies.

### How Other Crates Handle This

Looking at `sled-agent-types-versions/Cargo.toml`:
```toml
[dependencies]
omicron-common.workspace = true
```

`sled-agent-types-versions` CAN depend on `omicron-common` because it's a downstream crate. The pattern is:
- Versions crates can depend on OTHER types crates
- Versions crates CANNOT depend on their OWN parent types crate

Since `omicron-common` is the root types crate (nothing is more central), `omicron-common-versions` has nothing to depend on for foundational types.

### Options

1. **Move foundational types to omicron-common-versions**
   - Would require moving `ByteCount`, `DiskState`, `IdentityMetadata`, `Name`, `NameOrId`, etc.
   - Cascades to many more types due to dependencies
   - Very large undertaking

2. **Create an even more foundational crate**
   - E.g., `omicron-primitives` or similar
   - Contains `ByteCount`, `Name`, `NameOrId`, `IdentityMetadata`, etc.
   - Both `omicron-common` and `omicron-common-versions` depend on it
   - Still significant work

3. **Accept current intermediate state**
   - `nexus-types-versions` defines its own v1 `DiskType` and `Disk`
   - `nexus-types-versions::latest` re-exports from `omicron-common`
   - This works and is valid per RFD 619

## Current State (Valid Intermediate)

The current state in `nexus-types-versions`:

```rust
// In latest.rs
pub mod disk {
    // View types from omicron-common (latest version)
    pub use omicron_common::api::external::{Disk, DiskType};
    // ...
}
```

```rust
// In initial/disk.rs (v1)
pub enum DiskType {
    Crucible,  // v1 only had this
}

pub struct Disk {
    // ... uses v1 DiskType
}
```

This pattern:
- ✓ Works correctly
- ✓ Allows versioned endpoints to use correct types
- ✓ OpenAPI documents are correct
- ✓ Conversions work properly

## Recommendation

**Defer `omicron-common-versions` to a future effort** that includes architectural changes:

1. Create an `omicron-primitives` (or similar) crate with foundational types
2. Have both `omicron-common` and `omicron-common-versions` depend on it
3. Move changed types (`Disk`, `DiskType`) to `omicron-common-versions`
4. Update downstream crates to use `omicron-common-versions`

This is a larger architectural effort that should be planned separately.

## References

- [RFD 619](/home/rain/dev/oxide/rfd/rfd/0619/README.adoc)
- [nexus-external-api-migration-plan.md](./nexus-external-api-migration-plan.md)
- [sled-agent-types-versions](../sled-agent/types/versions/) - Example of downstream versions crate

---

## What Was Done (Context Window 1)

### Analysis performed:
1. Attempted to create `omicron-common-versions` crate
2. Discovered circular dependency challenge
3. Analyzed how other versions crates (sled-agent-types-versions) handle dependencies
4. Determined that current intermediate state is valid per RFD 619
5. Documented the architectural challenge and options

### Conclusion:
The `omicron-common-versions` migration requires architectural changes beyond a simple migration. The current state (nexus-types-versions defining its own v1 types) is valid and working. Creating `omicron-common-versions` should be deferred to a dedicated effort that addresses the foundational types dependency issue.
