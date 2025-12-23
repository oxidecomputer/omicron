# API Docstring Style Guide

This guide covers public docstrings (`///` or `/** ... */`) on structs, fields,
and enums in `nexus/types/src/external_api/`. These docstrings become user-facing
API documentation.

## Capitalization

- Start all docstrings with a capital letter.

```rust
// Bad
/// the source of an identity provider metadata descriptor
pub idp_metadata_source: IdpMetadataSource,

// Good
/// The source of an identity provider metadata descriptor.
pub idp_metadata_source: IdpMetadataSource,
```

## Punctuation

- End all sentences with periods, including single-sentence docstrings.
- Non-sentence fragments (e.g., short labels) do not need to end with periods.

```rust
// Bad
/// The IP address held by this resource
pub ip: IpAddr,

// Good
/// The IP address held by this resource.
pub ip: IpAddr,

// Also acceptable - fragment, not a sentence
/// Common identifying metadata
pub identity: IdentityMetadata,
```

## Paragraph Separation

- Separate distinct thoughts or notes with a blank `///` line. This produces
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

- Use standard casing for acronyms:
  - "IdP" (Identity Provider)
  - "SP" (Service Provider)
  - "SAML", "ID", "IP", "VPC", "CPU", "RAM"

## Doc Comments vs Regular Comments

- Use `///` for public API documentation that users will see.
- Use `//` for internal implementation notes that should not appear in generated
docs.

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
- Public struct docstrings (`/// View of a Silo`)
- Public field docstrings (`/// The IP address held by this resource.`)
- Public enum variant docstrings (`/// The sled is currently active.`)

These rules do NOT apply to:
- Private functions or structs
- Internal comments (`//`)
- Module-level documentation (`//!`)

## General

- Follow standard English grammatical rules.
