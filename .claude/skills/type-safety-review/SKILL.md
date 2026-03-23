---
name: type-safety-review
description: This skill should be used when the user asks to "review types", "review for type safety", "check for weak types", "look for stringly-typed code", "RFD 643 review", "check static invariants", "review the types in this file", or asks to review a branch or set of files for Rust type-safety issues. Applies RFD 643 patterns to identify where runtime errors could instead be caught at compile time.
---

# Type Safety Review

Review Rust code in the Omicron repository for type-safety issues: places where runtime failures could be caught at compile time, where implicit behavior could be made explicit, and where weak representations (strings, booleans, sentinels) could be replaced by stronger types.

The relevant principles come from RFD 643 ("Effective Rust"). The goal is to find code that works today but creates traps for future maintainers—including LLMs—by hiding invariants in documentation or convention rather than the type system.

## Two modes of operation

### Mode 1: Branch diff review

When the user asks to review the current branch or "the PR":

1. Find the merge base with `origin/main`:
   ```
   git merge-base HEAD origin/main
   ```
2. Get the diff of Rust files since that point:
   ```
   git diff <merge-base> HEAD -- '*.rs'
   ```
3. Identify which types, structs, enums, and functions were added or meaningfully changed.
4. Review those types and their usage context (read surrounding code as needed).

### Mode 2: Targeted review

When the user points at a specific file, module, type, or set of types:

1. Read the specified files.
2. Review all types, structs, enums, and functions in scope.
3. Look at call sites in nearby code to understand how types are used in practice.

---

## Review process

For each piece of code in scope, work through all the anti-pattern categories below. For each finding:

- Record the **file and line number**
- State which **category** it falls into (by name, not number)
- Explain **why** the current code is weak
- Propose a **concrete fix** (code sketch, not necessarily compilable)
- Classify as **Blocking** (likely to cause bugs or silent data loss) or **Suggestion** (robustness and maintainability improvement)

When nothing problematic is found in a category, say so briefly. Don't pad the output.

---

## Anti-pattern categories

### 1. Stringly-typed values

**What to look for:**
- `String` or `&str` fields/parameters where only a small fixed set of values is valid
- `match s.as_str() { "foo" => ..., "bar" => ... }` or `if s == "foo"`
- Database columns typed as `TEXT` (in SQL or diesel) for values that are an enumerated set
- API request/response types with `String` fields that are documented as having a fixed set of values

**Why it matters:** The compiler cannot catch typos, missing match arms, or invalid values. Invalid strings reach deep into the system before erroring.

**Fix direction:** Define an `enum` with `#[derive(Deserialize, Serialize)]` (and diesel `AsExpression`/`FromSql` if stored in the DB). For database types, see the `crdb-change` skill for migration guidance.

---

### 2. `Display` / `FromStr` as footguns

**What to look for:**
- `impl fmt::Display for SomeDomainType` where the string representation is context-specific (e.g., it's used for SMF properties in one place and API output in another)
- `impl FromStr for SomeDomainType` that parses strings from multiple unrelated sources
- Code that calls `.to_string()` on a domain type and passes it to an API or config file
- Code that calls `.parse::<SomeDomainType>()` from a source-of-truth that isn't user input

**Why it matters:** Generic `Display`/`FromStr` implementations lock in one string format globally. When different consumers need different representations (e.g., `rx_only` vs. `rx only`), the implementations diverge or produce silent mismatches.

**Fix direction:** Remove the generic `impl` and replace with context-specific methods: `to_smf_property() -> &'static str`, `to_display_label() -> &str`, etc. Let the compiler find all the call sites.

---

### 3. Multiple representations / sentinel values

**What to look for:**
- `Option<IpAddr>` (or `Option<SomeType>`) where both `None` and a special value like `Some(0.0.0.0)` or `Some("")` mean "not present"
- Fields documented as "use X to mean Y" (sentinel value pattern)
- Multiple code paths that convert between representations of the same concept
- Structs with `is_foo: bool` plus a field that only makes sense when `is_foo` is true

**Why it matters:** Every consumer must know which representation to use and check for both. Invariants expressed only in documentation will eventually be violated.

**Fix direction:** Use an explicit `enum`:
```rust
enum PeerAddress {
    Numbered(SpecifiedIpAddr),
    Unnumbered,
}
```
Or use typestates when the distinction affects which operations are valid.

---

### 4. Magic literals / missing constants

**What to look for:**
- Numeric literals repeated in multiple places (timeouts, port numbers, retry counts, sizes)
- String literals repeated across files
- Values that are documented as related to each other but expressed as independent literals
- Configuration defaults scattered through code without a single source of truth

**Why it matters:** Scattered literals can drift apart. The relationship between related values is invisible to the compiler and future readers.

**Fix direction:** Define named `const` or `static` values. Group related constants in a module. When values are structurally related (e.g., hold time must be 3× keepalive), document or enforce that relationship.

---

### 5. Missing newtypes for domain values

**What to look for:**
- Bare `Uuid` fields where multiple different UUID kinds appear in the same context (instance IDs, sled IDs, disk IDs, etc.)
- Numeric fields without units (`u64` for a byte count, or a duration in seconds)
- `String` fields for things like names, user IDs, or other constrained identifiers that have their own validation rules
- Functions that take two or more parameters of the same primitive type in an order that's easy to reverse

**Why it matters:** The compiler cannot catch passing a sled ID where an instance ID is expected, or passing bytes where MiB are expected.

**Fix direction:** Use `newtype-uuid` for UUID types; define newtype structs for other domain values. For byte counts, consider `ByteCount` (already defined in `common/src/api/external/`). Validate constraints in the constructor.

---

### 6. Implicit runtime panics

**What to look for:**
- `.unwrap()` or `.expect()` without an explanatory comment (`// unwrap: reason why this cannot fail`)
- Slice/Vec subscript access `v[i]` on a dynamically-sized collection (use `.get(i)` to get an `Option`)
- Map subscript `map["key"]` (use `.get("key")`)
- `as` casts between numeric types (use `from`/`try_from` or clippy's `cast_lossless`)
- `unsafe` blocks without a `// SAFETY:` comment

**Why it matters:** Undocumented panics are surprising; they can turn into crash loops. Lossless-looking `as` casts silently truncate when types change.

**Fix direction:** Add `// unwrap:` comments explaining why the panic is impossible. Replace subscripts with `.get()` + explicit handling. Replace `as` with `i64::from(x)` or `i64::try_from(x)?`.

---

### 7. Weak enum / bool usage

**What to look for:**
- `bool` parameters or fields where the meaning isn't obvious at the call site (`do_thing(true, false, true)`)
- `Option<T>` used as a boolean flag with no semantic meaning beyond presence/absence (when absence has a specific business meaning that should be named)
- `match` arms with a `_` wildcard in contexts where it matters to handle every case (new variants would be silently ignored)
- Enum variants with inline struct fields when those fields could instead be a named struct that functions could accept directly

**Why it matters:** `bool` arguments are easy to get backwards. Wildcard match arms cause silent incorrect behavior when new variants are added.

**Fix direction:** Replace `bool` parameters with two-variant enums (`enum Verbose { Yes, No }`). Replace semantically loaded `Option<T>` with an explicit enum. Replace `_` wildcards with explicit arms in exhaustive matches. Define named structs for enum variant payloads when functions need to accept a specific variant.

---

### 8. Missing full-struct destructuring in serialization

**What to look for:**
- Manual SQL `INSERT` statements that bind struct fields individually via `.bind()` calls, without a preceding `let StructName { field1, field2 } = value;` destructuring
- Manual JSON/TOML/etc. serialization or `From` implementations that access fields via `.field_name` rather than destructuring
- Any code that "knows" the complete set of fields in a struct without making that dependency compile-time-checked

**Why it matters:** When a new field is added to the struct, the compiler will not flag the serialization site. The new field will be silently omitted from the serialized form, causing data loss or subtle bugs.

**Fix direction:** Add an explicit destructuring before the serialization code:
```rust
// This statement exists to cause a compile error if fields are added
// or removed. If you get an error here, update the query below.
let MyStruct { field1, field2, field3 } = *value;
```
Then use the local variables in the `.bind()` calls.

---

## Output format

Structure the report as follows:

```
## Type Safety Review

### Mode
[Branch diff from <merge-base> | Targeted review of <files/types>]

### Findings

#### [BLOCKING | SUGGESTION] Category Name — file.rs:line
**Problem:** ...
**Fix:** ...
[code sketch if helpful]

... (repeat for each finding)

### No issues found in:
- Category "name": [brief reason]
- ...

### Summary
X blocking issues, Y suggestions.
```

If there are no findings at all, say so clearly and briefly.

---

## Additional resources

- **`references/patterns.md`** — Detailed before/after code examples for each category, drawn from real Omicron PRs
