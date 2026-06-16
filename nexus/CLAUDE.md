# omicron-nexus

Nexus is the control plane's central service. The `omicron-nexus` crate
(this directory) hosts the API servers and ties everything together; the
surrounding `nexus-*` crates hold the layers it builds on.

## Where things live

- `nexus/src/external_api/` — customer-facing HTTP endpoints
  (`http_entrypoints.rs`).
- `nexus/src/internal_api/` — internal service-to-service endpoints.
- `nexus/db-queries/` — database query layer (business logic against the DB).
- `nexus/db-model/` — Diesel models for DB tables.
- `nexus/types/`, `nexus/auth/` — shared Nexus types and authn/authz.

## Tips

- Typecheck with `cargo check -p omicron-nexus` (add `--all-targets` for tests).
- After adding or changing an endpoint, run `cargo xtask openapi generate` to
  regenerate the OpenAPI spec.
- For integration tests, prefer the helpers in
  `nexus/test-utils/src/resource_helpers.rs` over building requests by hand.

## Docs

- `docs/adding-an-endpoint.adoc` — adding an external API endpoint.
- `docs/authz-dev-guide.adoc` — the authz macro system (read before touching
  authorization).
