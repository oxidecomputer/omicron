# Oxide API Prototype

This repo houses a prototype for the Oxide Rack API.  See:

- [RFD 4 User Facing API Design](https://github.com/oxidecomputer/rfd/tree/master/rfd/0004)
- [RFD 10 API Prototype and Simulated Implementation](https://github.com/oxidecomputer/rfd/tree/master/rfd/0010)

## Status

The code here is **very rough** at this point.

There are a couple of somewhat independent efforts, in no particular order:

- API server guts: work out the shape of the HTTP and API layers.  (See
  DESIGN.md.)
- OpenAPI integration.  See RFD 10 for notes on this.  Figure out the approach
  and implement it (e.g., decide that the server code is the source of truth,
  define some syntax for noting that in code, and then implement a tool to
  process this and produce an OpenAPI spec file).
- Rest of the server: logging, tracing, error handling, etc. that we want.

## Build and run

Build and run:

    $ cargo run
    ...
    listening: http://127.0.0.1:12220

Use `curl` to hit the server:

    $ curl -i http://127.0.0.1:12220/projects
    HTTP/1.1 200 OK
    transfer-encoding: chunked
    content-type: application/x-json-stream
    date: Thu, 06 Feb 2020 01:01:21 GMT

    {"name":"project1"}

    {"name":"project3"}

## TODO

- Clean up changes from recent router implementation and merge into master.
- Update the handler functions so that they can return a Serialize or a Stream
  of Serialize.  (Otherwise, how will the OpenAPI tooling know what the return
  types are?)
- Flesh out more endpoints and simulator
- Write out road map
  - regions, AZs, etc. need to be added per RFD 24
- General work:
  - OpenAPI support
    - Rust macros
    - Tooling to generate spec
    - API Docs
    - Client library/libraries and CLI
  - Basic configuration
  - Debugging / monitoring
    - Request ids
    - Prometheus?
    - Server / request logging
    - OpenTracing
    - Dynamic tracing?
    - Core files?
    - Internal APIs for:
      - showing connected clients, recent requests, ...?
  - Rack-Internal API?
    - Alerts?
    - Faults?  Support bundles?
  - Audit logging
  - Authentication
  - Authorization
  - Data storage
    - What's the scope of these servers?
    - CAP choices
    - Active/Active, Active/Passive, Consensus?
  - TLS
  - Versioning (header? path? translators for older versions?)
  - Pagination? (Opaque token?  What does Stripe do?)
  - Hardening (see RFD 10)
    - throttling / load shedding
  - Automated testing
    - Basic functionality
    - Performance testing
    - Stress testing
    - Fault testing / under load
    - Fuzz testing
  - Security review
