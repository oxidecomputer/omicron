# jbor

`jbor` spins up N simulated Oxide racks for integration testing with mutiple
racks. Each "rack" is a full `omicron-dev`-style control plane (CockroachDB,
Clickhouse, Nexus, sled agent, dendrite, mgd, etc.) running in the same process.

## Permissions

**Must be run as root or via `pfexec`.** Network operations require elevated
privileges.

## Prerequisites

- Helios host (Linux may work today but it might not be supported in the future)
- Omicron development environment set up (`./tools/install_prerequisites.sh`)
- Binaries built (`cargo build`)
- Environment set up (`source env.sh` or use preferred env setup tool)

## Usage

```
# Single rack (default)
pfexec ./target/debug/jbor

# Three racks, two peer routers
pfexec ./target/debug/jbor --count 3 --peer-routers 2
```

## Flags

| Flag | Default | Description |
|---|---|---|
| `--count N` | `1` | Number of simulated racks to launch (1–100) |
| `--peer-routers M` | `0` | Number of external peer router mgd instances (0–100) |
| `--nexus-config PATH` | `nexus/examples/config.toml` | Nexus configuration file |
| `--gateway-config PATH` | (bundled test config) | Gateway server configuration file |

## IP and ASN scheme

All BGP addresses use `100.64.x.x` (RFC 6598 shared address space)

| Entity | Address range | ASN range |
|---|---|---|
| Peer router BGP dispatchers | `100.64.0.1` – `100.64.0.M` | 65100 – 65199 |
| Rack 0 switch0 / switch1 MGD | `100.64.1.0` / `100.64.1.1` | — |
| Rack N switch0 / switch1 MGD | `100.64.N+1.0` / `100.64.N+1.1` | — |

## Output

Peer router information is printer first:

```
peer router 0: mgd api:            http://[::1]:<port>
peer router 0: mgd bgp-dispatcher: tcp 100.64.0.1:179
peer router 0: bgp ASN:            65100
```

Per-rack service information is printed next:

```
jbor: nexus external API:     ...
jbor: nexus internal API:     ...
jbor: cockroachdb URL:        ...
jbor: clickhouse http addr:   ...
jbor: internal DNS:           ...
jbor: mgd api:                http://[::1]:<port> (Switch0)
jbor: mgd bgp-dispatcher:     tcp 100.64.1.0:179 (Switch0)
jbor: silo name:              ...
jbor: privileged user name:   ...
jbor: privileged password:    ...
```

## Teardown

Press **Ctrl-C** (SIGINT). `jbor` cleans up all processes and ip address allocations
before exiting.

In the event that the runtime crashes or some other failure occurs that prevents the
cleanup process from running, you will need to remove the lockfiles for the address
allocation at `/tmp/maghemite-ip-<ADDRESS>.lock` to prevent conflicts when you run
the tool again.
