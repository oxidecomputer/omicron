# API Docstring Style Guide

This guide covers public docstrings (`///`) on endpoint handlers in
`nexus/external-api/src/lib.rs` and on structs, fields, and enums in
`nexus/types/src/external_api/`. These docstrings become user-facing API
documentation.

## Capitalization

Start all docstrings with a capital letter.

```rust
// Bad
/// the source of an identity provider metadata descriptor
pub idp_metadata_source: IdpMetadataSource,

// Good
/// The source of an identity provider metadata descriptor.
pub idp_metadata_source: IdpMetadataSource,
```

## Punctuation

Short, label-like descriptions do not need periods. This includes endpoint
summary lines, struct-level descriptions, and brief field annotations. Longer
explanatory text (especially multi-clause or multi-sentence docstrings) should
end sentences with periods.

```rust
// Endpoint summary lines: no period
/// Fetch silo
/// List identity providers for silo
/// Create IP pool

// Struct-level descriptions: no period
/// View of a Silo
/// A collection of resource counts used to describe capacity and utilization

// Short field descriptions: no period
/// Number of virtual CPUs
/// Common identifying metadata
/// Size of blocks in bytes

// Longer explanatory text: use periods
/// Accounts for resources allocated to running instances or storage
/// allocated via disks or snapshots.
///
/// Note that CPU and memory resources associated with stopped instances
/// are not counted here, whereas associated disks will still be counted.
```

## Articles

Omit articles ("a", "an", "the") in endpoint summary lines. Look at existing
endpoints in `nexus/external-api/src/lib.rs` for reference. A few examples of
the different shapes:

```rust
// Bad
/// Fetch a silo
/// Create an IP pool
/// Delete the project
/// List the silos

// Good
/// Fetch silo
/// Create IP pool
/// Delete project
/// List silos
/// Fetch resource utilization for user's current silo
/// List identity providers for silo
/// Add range to IP pool
```

## Paragraph Separation

Separate distinct thoughts or notes with a blank `///` line. This produces
proper paragraph breaks in rendered documentation.

```rust
// Bad - renders as one run-on paragraph
/// A unique, immutable, system-controlled identifier for the token.
/// Note that this ID is not the bearer token itself, which starts with
/// "oxide-token-".
pub id: Uuid,

// Good - renders as two paragraphs
/// A unique, immutable, system-controlled identifier for the token.
///
/// Note that this ID is not the bearer token itself, which starts with
/// "oxide-token-".
pub id: Uuid,
```

## Acronyms and Abbreviations

Use standard casing for acronyms:
  - "IdP" (Identity Provider)
  - "SP" (Service Provider)
  - "SAML", "ID", "IP", "VPC", "CPU", "RAM", "DER"

## Doc Comments vs Regular Comments

Use `///` for public API documentation that users will see. Use `//` for
internal implementation notes that should not appear in generated docs.

```rust
// Bad - this won't appear in API docs
// A list containing the IDs of the secret keys.
pub secrets: Vec<WebhookSecret>,

// Good - this will appear in API docs
/// A list containing the IDs of the secret keys.
pub secrets: Vec<WebhookSecret>,
```

## Scope

These rules apply to:
- Endpoint handler docstrings (`/// Fetch silo`)
- Public struct docstrings (`/// View of a Silo`)
- Public field docstrings (`/// The IP address held by this resource.`)
- Public enum variant docstrings (`/// The sled is currently active.`)

These rules do NOT apply to:
- Private functions or structs
- Internal comments (`//`)
- Module-level documentation (`//!`)

## General

Follow standard English grammatical rules: correct articles ("a" vs "an"),
subject-verb agreement, proper spelling, etc.
