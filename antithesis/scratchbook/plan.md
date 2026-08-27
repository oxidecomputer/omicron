# Antithesis for Nexus and CockroachDB: plan

Date: 2026-08-27. Repo state: omicron `main` at `[9/n gen] [nexus] add typed InstanceStateGeneration (#11175)`.

## Summary

The goal is to run Nexus against CockroachDB inside Antithesis so that its fault injector (network partitions, congestion, node hangs, CPU throttling, clock jitter, thread pauses, and, once enabled, process kills) exercises the Nexus↔CRDB path and Nexus's own state machines: sagas, background tasks, and the instance and disk lifecycles. Properties are checked with the Antithesis Rust SDK from a workload container and from a few feature-gated hooks inside Nexus.

Recommendation:

1. Add an `antithesis/` harness directory to omicron laid out the way the `antithesis-skills` expect (`config/`, `scratchbook/`, `test/`, one Dockerfile), so the Antithesis skills and `snouty` work unmodified.
2. Build one multi-stage Dockerfile that produces an instrumented `omicron-antithesis` image (nexus, sled-agent-sim, dns-server, and a new workload binary, all built in a single `cargo build` with the sancov rustflags) and an `omicron-antithesis-cockroach` image from the pinned Oxide CockroachDB fork with a pre-seeded `omicron` database.
3. Phase 1 topology is five containers on an IPv6 network: `cockroach`, `internal-dns`, `nexus`, `sled-agent-sim`, `workload`. Grow to a three-node CockroachDB cluster with DNS-based discovery (phase 3), then multiple Nexus instances (phase 4).
4. Make a small set of changes to omicron proper: fix and generalize standalone `sled-agent-sim` (it is documented broken and hard-codes `::1`), add an `antithesis` cargo feature to `omicron-nexus` that turns on the SDK and instrumentation, and add three or four assertions in guaranteed code paths.
5. Write the workload in Rust as a workspace crate that reuses `oxide-client` and `nexus-lockstep-client`, exposed through thin `/opt/antithesis/test/v1/nexus/*` wrappers.

The biggest unknowns, all resolvable in a one-week derisking phase: whether Antithesis's compose networking supports IPv6 (omicron's internal DNS is IPv6-only), whether `-Cpasses=sancov-module` works with the pinned rustc 1.97.1, and how long an instrumented release build of Nexus takes.

## What Antithesis requires, and where it bites omicron

Sources: [setup overview](https://antithesis.com/docs/setup/overview/), [docker compose setup](https://antithesis.com/docs/setup/docker_compose/), [test command reference](https://antithesis.com/docs/product/writing_tests/test_templates/test_composer_reference/), [Rust instrumentation](https://antithesis.com/docs/reference/sdk/rust/instrumentation/), and the [antithesis-skills](https://github.com/antithesishq/antithesis-skills) reference files.

| Requirement | Consequence for omicron |
|---|---|
| Linux x86-64 container images, hermetic (no internet at runtime). All downloads happen in `RUN` steps. | Nexus already builds and passes its full test suite on Linux (`build-and-test (ubuntu-22.04)` job). CockroachDB, ClickHouse, dpd stub, mgd, and ddmd all have Linux artifacts via `cargo xtask download`. Nothing needed at runtime is illumos-only. |
| `docker-compose.yaml` in `antithesis/config/`; every service has `container_name` == `hostname` (no underscores), `platform: linux/amd64`, `init: true`, `image:` plus `build:`; `depends_on` with `condition: service_healthy`; no `logging:` override, no `internal: true` networks, no `pull_policy`. | Straightforward. The container names below follow this. |
| Services reach each other by hostname; Antithesis generates DNS from service names. | Omicron does not use Docker DNS. Nexus and `sled-agent-sim` need literal addresses, and internal DNS records are IPv6 (`DnsConfigBuilder::host_zone(_, Ipv6Addr)`). The compose network must be IPv6 with static `ipv6_address` per service. **Confirm IPv6 support with Antithesis in phase 0.** |
| A `setup_complete` JSON line written to `$ANTITHESIS_OUTPUT_DIR/sdk.jsonl` by any process; the first one starts testing. | Emitted by the workload container once Nexus's external API answers, login works, and the simulated sled is registered (which proves the RSS handoff completed). |
| Test commands are executables in `/opt/antithesis/test/v1/<template>/` with prefixes `first_`, `parallel_driver_`, `serial_driver_`, `singleton_driver_`, `anytime_`, `eventually_`, `finally_`. Faults pause during `first_`, `eventually_`, `finally_`. | Wrappers in `antithesis/test/v1/nexus/` exec the workload binary. Invariant checks that need a quiescent system go in `eventually_`. |
| Rust: `antithesis_sdk = "0.2"` (no-op with `default-features = false`), `antithesis-instrumentation = "0.1"` plus rustflags `-Ccodegen-units=1 -Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=3 -Cllvm-args=-sanitizer-coverage-trace-pc-guard -Clink-args=-Wl,--build-id`; DWARF symbols in `/symbols`. | Flags go in a separate `antithesis/cargo-config.toml` passed with `--config`, **not** in `.cargo/config.toml`. Because `target.<triple>.rustflags` replaces `build.rustflags`, the file must also carry `--cfg tokio_unstable`. |
| Assertion names must be inline string literals (static cataloging). `NO_COLOR=1` everywhere. | Nexus logs bunyan JSON via slog; use `mode = "stderr-terminal"`, `level = "info"`. |
| Node termination faults are off by default. | Ask Antithesis to enable them for `nexus` once saga recovery properties exist (phase 2). |
| `snouty` CLI: `snouty validate antithesis/config` checks the harness reaches `setup_complete` locally; `snouty launch` pushes images and starts a run. Supports GitHub Actions OIDC. | Needs a tenant, registry (`us-central1-docker.pkg.dev/molten-verve-216720/<tenant>-repository`), and credentials. Request these from Antithesis now; they gate phase 1. |

## Current state of omicron (what we build on)

- **CockroachDB** is an Oxide fork pinned at `v22.1.22-64-g86fdbfca06` (`tools/cockroachdb_version`, `tools/cockroachdb_checksums`), downloaded from buildomat by `cargo xtask download cockroach` (`dev-tools/downloader/src/lib.rs:676`) with a Linux checksum. Test startup flags live in `test-utils/src/dev/db.rs`: `start-single-node --insecure --http-addr=:0 --store=path=<dir>,ballast-size=0 --listen-addr [::1]:<port> --max-sql-memory 256MiB`. Schema comes from `schema/crdb/dbinit.sql` (version 296.0.0); `cargo xtask db-dev -- run|populate|wipe` wraps this. The test suite pre-seeds a store tarball (`test-utils/src/dev/seed.rs`), which is the pattern to copy for the container image.
- **Nexus** boots with a config file (`nexus/examples/config.toml`). Hard requirements at boot: CockroachDB (the pool is created in `ApiContext::for_internal`), an internal DNS address (`from_address` or `from_subnet`), and a debug dropbox directory. The **external API only comes up after the RSS handoff** (`nexus/src/lib.rs`, `Server::start`), which is a `rack_initialization_complete` call on the lockstep API carrying an initial blueprint, recovery silo, DNS config, and disks/zpools/datasets. ClickHouse is optional (`timeseries_db.address` is `Option`). Dendrite, mgd, MGS, oximeter, and crucible pantry are looked up via internal DNS by background tasks, which log warnings and retry when absent. `nexus/` has essentially no `cfg(target_os)` gating (five lines in `db-queries/src/db/pool.rs`).
- **`sled-agent-sim`** (`sled-agent/src/bin/sled-agent-sim.rs`, `sled-agent/src/sim/server.rs::run_standalone_server`) is the only standalone thing that performs the handoff: it registers a simulated Gimlet (10 × 1 TiB U.2, 32 threads, 64 GiB), runs an in-process internal DNS server and crucible pantry, builds the `RackInitializationRequest`, and calls `handoff_to_nexus`. It takes `SocketAddrV6` for its own and the DNS addresses and hard-codes `Ipv6Addr::LOCALHOST` for `gz_address` and `rack_subnet` (`server.rs:467,724`). It is documented as broken (`docs/how-to-run-simulated.adoc` CAUTION, [omicron#4421](https://github.com/oxidecomputer/omicron/issues/4421): the sled agent fails to bind because the repo depot server already took the same address). `omicron-dev run-all` works but is the integration-test harness running everything in one process, which gives Antithesis nothing to partition.
- **No OCI packaging exists.** `package-manifest.toml` emits illumos zones only. There is no Dockerfile anywhere in the repo.
- **No fault-injection framework exists.** `RetargetableTcpProxy` (`test-utils/src/dev/tcp_proxy.rs`) with `ProxyTarget::Refuse` and dendrite stop/restart are the only primitives. Determinism tooling (`typed-rng`, `nexus-reconfigurator-simulation`) covers the planner, not the live control plane. Antithesis supplies the missing piece from outside the process.
- **Existing test surface to mine for properties:** 489 `#[nexus_test]` integration tests (all run on Linux), saga unwind tests (`nexus/src/app/sagas/test_helpers.rs`), `nexus/tests/integration_tests/{quiesce,schema,cockroach,crucible_replacements,instances,disks}.rs`, and the `live-tests` for Nexus add/remove and handoff.
- **Build config:** `.cargo/config.toml` sets `build.rustflags = "--cfg tokio_unstable"`; `[profile.release]` has `panic = "abort"` (good: a panic becomes a process crash Antithesis notices) and `[profile.dev]` uses `debug = "line-tables-only"`. Nexus links libpq dynamically (`pq-sys`; the zone ships `/opt/ooce/pgsql-18/lib/amd64`), so the runtime image needs `libpq5`.

## Target topology

### Phase 1 (minimal, five containers)

Network `underlay`: `enable_ipv6: true`, subnet `fd00:1122:3344:101::/64` (omicron's conventional rack prefix, one sled subnet). Every service gets a static `ipv6_address`.

| Container | Image | Role | Process | Talks to |
|---|---|---|---|---|
| `cockroach` | `omicron-antithesis-cockroach` | dependency | `cockroach start-single-node --insecure --store=path=/data,ballast-size=0 --listen-addr=[::]:32221 --http-addr=[::]:8080 --max-sql-memory=256MiB` on a store pre-seeded with `dbinit.sql` at image build | (none) |
| `internal-dns` | `omicron-antithesis` | dependency | `dns-server --config-file /opt/oxide/config/dns-server.toml --http-address [addr]:5353 --dns-address [addr]:53` | (none) |
| `nexus` | `omicron-antithesis` | service (instrumented) | `nexus /opt/oxide/config/nexus.toml` | `cockroach:32221`, `internal-dns:53`, `sled-agent-sim`, `internal-dns:5353` (DNS propagation) |
| `sled-agent-sim` | `omicron-antithesis` | service | `sled-agent-sim <uuid> [addr]:12345 [nexus]:12221 12232 --rss-... --rack-subnet ...` (after phase 0 changes) | `nexus:12221`, `nexus:12232` (handoff), `internal-dns:5353` |
| `workload` | `omicron-antithesis` | client | `omicron-antithesis-workload wait-ready && sleep infinity`; mounts `antithesis/test` at `/opt/antithesis/test/v1` | `nexus:12220` (external), `nexus:12232` (lockstep, for saga list), `cockroach:32221` (read-only invariant queries) |

Nexus and the handoff point at the separate `internal-dns` container rather than the DNS server `sled-agent-sim` runs in-process today, so DNS propagation crosses a container boundary and can be faulted. Phase 0 makes the in-process server optional (`--rss-internal-dns-http-addr` pointing at an external server).

Nexus config: start from `nexus/examples/config.toml` and change only the `[deployment]` block (external `[::]:12220`, internal and lockstep on the container's static address, `internal_dns.type = "from_address"`, `database.type = "from_url"` pointing at `cockroach`), drop `[deployment.dropshot_external.tls]`, `[dendrite]`, `[mgd]`, and `[pkg.timeseries_db].address`, set `[log] level = "info"`, and keep `[default_region_allocation_strategy] seed = 0`.

### Phase 3 (realism)

- `cockroach-1..3` running `cockroach start --join=...`, `cockroach init` and `dbinit.sql` applied by a `first_` step or by the workload's readiness loop. Nexus switches to `database.type = "from_dns"` and discovers nodes via SRV records that the handoff's `internal_dns_zone_config` must include (add `host_zone_with_one_backend` records for the three cockroach zones in the generalized `sled-agent-sim`).
- `sp-sim` + `mgs` so inventory collection, the blueprint planner, and executor run. Both are pure Rust and portable.
- `clickhouse` + `oximeter` only if metrics properties are wanted; otherwise leave out (every extra container expands the state space).

### Phase 4

Two or three `nexus` containers, with the extra ones registered in the initial blueprint. This unlocks saga ownership, blueprint execution leadership, DNS generation races, and the quiesce/handoff flow tested today only by `live-tests`.

## Work plan

### Phase 0: derisk (about one week)

Exit criteria: every unknown below has an answer written into `antithesis/scratchbook/`.

1. **Instrumented build.** Add `[profile.antithesis]` (`inherits = "release"`, `debug = "line-tables-only"`, `strip = "none"`) and `antithesis/cargo-config.toml`:
   ```toml
   [target.x86_64-unknown-linux-gnu]
   rustflags = [
       "--cfg", "tokio_unstable",
       "-Ccodegen-units=1",
       "-Cpasses=sancov-module",
       "-Cllvm-args=-sanitizer-coverage-level=3",
       "-Cllvm-args=-sanitizer-coverage-trace-pc-guard",
       "-Clink-args=-Wl,--build-id",
   ]
   ```
   Run `cargo build --profile antithesis --config antithesis/cargo-config.toml --features omicron-nexus/antithesis --bin nexus` on an x86-64 Linux box. Verify with `nm target/x86_64-unknown-linux-gnu/antithesis/nexus | grep antithesis_load_libvoidstar` and `readelf -S ... | grep sancov`. Record wall-clock time and binary size (this decides whether CI builds nightly or weekly).
2. **Standalone `sled-agent-sim`.** Fix omicron#4421 (repo depot and sled agent binding the same address) and add arguments for the rack subnet, the sled's underlay address, an external internal-DNS HTTP address, and the recovery silo. Prove `dns-server`, `cockroach`, `nexus`, `sled-agent-sim` run by hand on non-loopback IPv6 addresses on a Linux host and the external API comes up. This also un-breaks `docs/how-to-run-simulated.adoc`.
3. **IPv6 in Antithesis.** Ask Antithesis whether compose networks with `enable_ipv6` and static `ipv6_address` are supported. Fallback if not: run `nexus`, `internal-dns`, and `sled-agent-sim` in one container on `::1` and keep `cockroach` and `workload` separate over IPv4 (`from_url` accepts an IPv4 URL). That preserves the Nexus↔CRDB fault surface, which is the primary target.
4. **Credentials and tooling.** Obtain tenant, registry, and API key; install `snouty`; run `snouty doctor`. Install the `antithesis-skills` plugin for Claude Code.
5. **CockroachDB image.** Build `debian:stable-slim` + the buildomat tarball (use `cargo xtask download cockroach` in the builder stage so the pin and checksum stay single-sourced), confirm the binary runs on Debian's glibc, and seed `/data` with `dbinit.sql` at build time.

### Phase 1: harness (one to two weeks)

Exit criteria: `snouty validate antithesis/config` reaches `setup_complete` with no internet; a 30-minute setup-mode run shows "Software was instrumented" and the bootstrap property.

1. Run the `antithesis-research` skill with this document as the external reference; it produces `antithesis/scratchbook/{sut-analysis,property-catalog,deployment-topology}.md`. Edit the topology to match the table above.
2. `antithesis/Dockerfile` (multi-stage):
   - `builder`: `rust:1.97.1-bookworm` (or `debian:stable-slim` + rustup pinned by `rust-toolchain.toml`), `apt-get install libpq-dev pkg-config clang`, `cargo xtask download cockroach`, then the instrumented `cargo build` of `nexus`, `sled-agent-sim`, `dns-server`, `schema-updater`, `omdb`, `omicron-antithesis-workload`.
   - `omicron-antithesis`: `debian:stable-slim` + `libpq5 ca-certificates`, binaries in `/opt/oxide/bin`, unstripped binaries symlinked into `/symbols`, `schema/crdb` in `/opt/oxide/schema`, `ENV NO_COLOR=1`.
   - `omicron-antithesis-cockroach`: as in phase 0, with a `/opt/oxide/bin/healthcheck.sh` hitting `http://localhost:8080/health?ready=1`.
   - `omicron-antithesis-config`: `FROM scratch`, `COPY config/ /`.
3. `antithesis/config/docker-compose.yaml` with the five services, healthchecks (`cockroach` HTTP health, `internal-dns` HTTP `GET /config`, `nexus` `GET /v1/ping` on the external port), `depends_on ... service_healthy`, the IPv6 network, and `volumes: - ../test:/opt/antithesis/test/v1` on `workload`.
4. `antithesis/config/nexus.toml`, `antithesis/config/dns-server.toml`, plus a `README.adoc` in `antithesis/` describing how to build and run locally (`docker compose -f antithesis/config/docker-compose.yaml up`, and `unshare -n` to prove hermeticity).
5. Workload crate `antithesis/workload` (`omicron-antithesis-workload`, in the workspace) with a `wait-ready` subcommand: poll `GET /v1/ping`, log in as `test-privileged`/`oxide` in `test-suite-silo`, wait for `GET /v1/system/hardware/sleds` to list the simulated sled, then `antithesis_sdk::lifecycle::setup_complete`.
6. Bootstrap assertion in Nexus: `antithesis_sdk::antithesis_init()` and `use antithesis_instrumentation as _;` in `nexus/src/bin/nexus.rs` behind `#[cfg(feature = "antithesis")]`, and `assert_reachable!("nexus: external API started")` after `Server::start` brings up the external server.
7. `.snouty.toml` at the repo root (tenant and repository; credentials via environment only).

### Phase 2: first workload (one to two weeks)

Exit criteria: a 2-hour run with at least three "sometimes" properties reached and no vacuous "always" properties; node termination enabled for `nexus`.

Test template `antithesis/test/v1/nexus/`, each a two-line shell wrapper around the workload binary:

- `first_seed.sh`: create project `antithesis`, an IP pool with a range, and link it to the silo (the harness equivalent is `create_default_ip_pool`).
- `parallel_driver_instances.sh`: loop choosing with `antithesis_sdk::random::random_choice` among create / start / stop / reboot / delete instance, attach / detach disk, attach / detach external IP. Treat 503, timeouts, and conflicts as expected; `assert_always!(status != 500, "external API never returns 500")`; `assert_sometimes!` per operation succeeding.
- `parallel_driver_disks.sh` and `parallel_driver_snapshots.sh`: disk create / delete / snapshot / delete; the `volume_delete` and `region_snapshot_replacement_*` sagas are historically fragile.
- `anytime_read.sh`: list endpoints across projects, instances, disks, sleds, blueprints; reads never 500.
- `eventually_sagas_settle.sh`: poll `saga_list` on the lockstep API until every saga is done or unwound; `assert_always!` no saga remains running after a bounded wait.
- `eventually_invariants.sh`: after sagas settle, check virtual provisioning accounting against the instance list, disk attachment exclusivity, external IP uniqueness, and VMM reservations on the sled versus its capacity (raw SQL via `tokio-postgres`, with `omdb db` as the interactive counterpart).

In-Nexus assertions (no-ops unless `antithesis_sdk/full` is on): `assert_sometimes!("CRDB transaction retried")` in `nexus/db-queries/src/transaction_retry.rs::retry_callback`, `assert_sometimes!("saga unwound")` and `assert_sometimes!("saga completed")` where saga completion is recorded, and `assert_always_or_unreachable!` that saga recovery on startup finds only sagas owned by this Nexus.

### Phase 3: realism (two weeks)

Three-node CockroachDB with `from_dns` discovery, `sp-sim` + `mgs`, node termination on `cockroach-*`. New properties: the external API recovers within N seconds of a single CRDB node partition; `crdb_node_id_collector` and the blueprint planner make progress; inventory collections eventually complete; blueprint target generation is monotonic with exactly one target.

### Phase 4: multi-Nexus and upgrades

Two or three Nexus containers; properties around saga ownership, one executor per blueprint, DNS generation monotonicity, and quiesce/handoff (`nexus/tests/integration_tests/quiesce.rs`, `live-tests/tests/test_nexus_handoff.rs` are the specifications). Schema upgrade under faults: start from an image seeded at an older `dbinit` (`cargo xtask schema generate-base`), then run `schema-updater` as a `singleton_driver_` while faults are active, asserting the `db_metadata` version ends at `SCHEMA_VERSION` and every `upNN.sql` is idempotent on retry.

### Phase 5: CI

A GitHub Actions workflow (`workflow_dispatch` plus a weekly cron) on a large x86-64 runner: build the images, `snouty validate`, then `snouty launch --duration 120` with OIDC credentials, posting the triage report link. Buildomat can build the images too, but `snouty` has native GitHub OIDC support and Docker is already present on GitHub runners. Also add a fast Linux CI check that the `antithesis` feature builds without the sancov flags (so `check-features` and hakari stay green).

## Changes to omicron proper

| Area | Change | Why |
|---|---|---|
| `sled-agent/src/bin/sled-agent-sim.rs`, `sled-agent/src/sim/server.rs` | Fix the double bind (omicron#4421). Add `--rack-subnet`, `--sled-underlay-ip`, `--rss-internal-dns-http-addr` (use an external DNS server instead of the in-process one), `--rss-external-dns-*` already exists, and options for the recovery silo. Emit cockroach zone records into `internal_dns_zone_config` when given `--cockroach-addr` (repeatable). | Standalone handoff on real container addresses; also fixes the "run the pieces by hand" docs. |
| `Cargo.toml` | `[profile.antithesis]`; workspace deps `antithesis_sdk = { version = "0.2", default-features = false }` and `antithesis-instrumentation = "0.1"`; new workspace member `antithesis/workload`. | One instrumented profile; no-op SDK everywhere else. |
| `nexus/Cargo.toml`, `nexus/src/bin/nexus.rs` | Feature `antithesis = ["antithesis_sdk/full", "dep:antithesis-instrumentation"]`; `antithesis_init()` and the `use antithesis_instrumentation as _;` behind it. | Nothing changes for normal builds or the zone. |
| `nexus/src/lib.rs`, `nexus/db-queries/src/transaction_retry.rs`, `nexus/src/app/saga.rs` | Three or four `assert_*` calls with literal names. | Bootstrap property and proof that faults reach the DB and saga paths. |
| `workspace-hack`, `dev-tools/xtask` | Run `cargo hakari generate`; `check-workspace-deps` allowances if needed; `check-features` includes the new feature. | Keep the workspace lints green. |
| `docs/how-to-run-simulated.adoc` | Replace the CAUTION with the working standalone procedure; link `antithesis/README.adoc`. | The fix is a side effect of phase 0. |

Everything else (Dockerfile, compose, configs, wrappers, scratchbook) lives under `antithesis/` and is inert for the rest of the repo.

## Seed property catalog

These seed `antithesis/scratchbook/property-catalog.md`; the research skill will expand and re-rank them. Type key: S = safety (`assert_always!`), L = liveness (`assert_sometimes!` or `eventually_` checker), R = reachability.

| Slug | Type | Property | Where checked | Phase |
|---|---|---|---|---|
| `nexus-started` | R | Nexus brings up the external API. | in Nexus | 1 |
| `no-internal-server-errors` | S | External and lockstep APIs never return 500 for well-formed requests; CRDB unavailability surfaces as 503. | workload | 2 |
| `nexus-never-crashes` | S | `panic = "abort"` means any panic is a crash Antithesis records natively. | platform | 1 |
| `sagas-settle` | L | Every saga reaches done or unwound within a bounded time after faults stop. | `eventually_` | 2 |
| `saga-recovery-after-restart` | S+L | After a Nexus kill, `saga_recovery` resumes or unwinds every in-flight saga and resources end consistent. Needs node termination. | `eventually_` + in Nexus | 2 |
| `crdb-transaction-retries-exercised` | L | The `RetryHelper` retry path runs at least once (proves faults reach the DB layer). | in Nexus | 2 |
| `virtual-provisioning-accounting` | S | Silo and project `virtual_provisioning_collection` equals the sum over live instances and disks. Historically bug-prone. | `eventually_` | 2 |
| `disk-attach-exclusive` | S | A disk is attached to at most one instance. | `eventually_` | 2 |
| `external-ip-unique-and-in-pool` | S | No two instances share an external IP; every allocated IP is inside a linked pool range. | `eventually_` | 2 |
| `sled-vmm-reservations-bounded` | S | Sum of VMM reservations on a sled never exceeds its advertised threads and memory. | `eventually_` | 2 |
| `instance-lifecycle-converges` | L | Start reaches Running, stop reaches Stopped, delete removes the record. | workload | 2 |
| `db-schema-version-stable` | S | `db_metadata.version` equals `SCHEMA_VERSION` for the whole run (phase 4 relaxes this during upgrade). | `anytime_` | 2 |
| `internal-dns-converges` | L | Each DNS server's generation eventually equals Nexus's latest generation. | `eventually_` | 2 |
| `api-recovers-from-crdb-node-loss` | L | With one of three CRDB nodes partitioned, the external API answers within N seconds. | workload | 3 |
| `blueprint-target-monotonic` | S | Target blueprint generation never decreases; exactly one target exists. | `anytime_` | 3 |
| `inventory-collections-complete` | L | Inventory collection succeeds at least once under faults. | in Nexus | 3 |
| `single-blueprint-executor` | S | Two Nexus instances never both execute the same blueprint generation. | in Nexus | 4 |
| `schema-upgrade-resumable` | S | `schema-updater` interrupted mid-migration completes on retry and lands at `SCHEMA_VERSION`. | `singleton_driver_` | 4 |

## Risks and open questions

- **IPv6 in Antithesis compose networks.** Unverified in their docs. Fallback described in phase 0.
- **`sancov-module` on rustc 1.97.1.** Antithesis documents stable rustc, but the LLVM pass name and `-Cllvm-args` acceptance can drift across LLVM majors. Verify first.
- **Build time and image size.** `codegen-units=1` plus a release build of the whole workspace on one target is slow (expect well over an hour on a laptop) and the unstripped binaries are large. Mitigation: build only the four binaries, cache the builder stage in CI, and accept weekly cadence if needed.
- **Log volume.** Nexus's 51 background tasks retry failed DNS lookups for services that are not deployed (dpd, mgd, MGS, oximeter, pantry). Antithesis ingests every log line. Mitigation: `level = "info"`, lengthen `period_secs` for tasks whose services are absent in phase 1, and add the missing services in phase 3 rather than silencing tasks.
- **Snapshot-vs-live invariants.** Accounting checks are only meaningful on a quiescent system, which is why they run in `eventually_`. `anytime_` checks stay restricted to invariants that hold at every instant.
- **Time-dependent behaviour.** Clock jitter affects session expiry, `audit_log_timeout_incomplete`, and `time_deleted` ordering. Nothing blocks on it; expect surprises here.
- **Feature unification and hakari.** `antithesis_sdk` with `default-features = false` will appear in `workspace-hack`; enabling `full` only through `--features omicron-nexus/antithesis` in the Docker build keeps normal builds no-op. Confirm `cargo xtask check-features` and `check-workspace-deps` pass.
- **CockroachDB is uninstrumented.** It is a Go binary from the fork; Antithesis still injects faults against it but gets no coverage guidance inside it. Acceptable for now; instrumenting the fork with Antithesis's Go instrumentor is a later option if CRDB-internal bugs become the target.
- **Hermeticity of Nexus startup.** Nexus fetches nothing at runtime, but confirm with `unshare -n docker compose up` in phase 1; `external_dns_servers` must be non-empty (Nexus refuses to start otherwise), so it points at the internal DNS container, which answers NXDOMAIN for external names.
- **Ownership.** Phases 0 and 1 are one engineer for two to three weeks; phase 2 onward benefits from whoever owns sagas and the DB layer reviewing the property catalog.
