# Omicron

Omicron is the Oxide control plane. It's a large Cargo workspace.

## Repo map

This covers the areas that see the most day-to-day activity, not a full
inventory — the workspace has dozens of top-level directories. For the complete
list, see the `[workspace]` members in the root `Cargo.toml`.

- `nexus/` — the control plane brain: external + internal HTTP APIs, database
  access, background tasks, sagas, blueprint/reconfigurator logic. See
  `nexus/CLAUDE.md`.
- `sled-agent/` — per-sled agent that manages instances, storage, and
  networking on each server.
- `schema/` — CockroachDB schema and migrations.
- `common/` — shared types and utilities (`omicron-common`).
- `clients/` — generated inter-service API clients.
- `oximeter/` — metrics collection and timeseries storage.
- `dev-tools/` — developer tooling (`omdb`, `reconfigurator-cli`, xtasks, etc.).

Plenty more lives at the top level — standalone services (`wicket/` +
`wicketd/`, `clickhouse-admin/`, …) and lower-level crates (`illumos-utils/`, …)
among them.

## Docs

Prose docs for humans live in `docs/` (architecture, networking, authz, how to
run, reconfigurator, etc.) — browse it before assuming something is
undocumented. Start with `README.adoc`.

A couple of pointers where the right time to read isn't obvious from the name:

- Read `schema/crdb/README.adoc` before making **any** database schema change —
  it's the authoritative guide to the migration process.
- Read `docs/how-to-run-simulated.adoc` to run the stack locally without
  hardware.

## Building and testing

- Use `cargo nextest run` for tests, not `cargo test`.
- Narrow compilation with `-p <crate>` whenever you can.
- Run clippy with `cargo xtask clippy`.

## Adding CLAUDE.md files

More of these are welcome, scoped to individual crates or subsystems. Keep them
useful by following a few conventions:

- Keep each file short and specific to its directory. When a file deep in the
  tree is read, every CLAUDE.md from the repo root down to that file loads
  together — they compose, so don't repeat what a parent file already says.
- Prefer durable, general guidance (layout, workflows, gotchas) over detail
  that churns or belongs in code comments.
- Point at existing docs rather than restating them.
