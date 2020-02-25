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

- Lots of clean up from all the recent changes for error handling and the test
  suite.
- Consider implementing something similar to Actix's magic for extracting query
  parameters and JSON bodies as arguments to handler functions.  Straw man:
  - Define a Handler type similar to Actix's Factory/Handler trait/struct.
  - Statically define a handful of valid function signatures:
    - handler()
    - handler(server)
    - handler(server, request)
    - handler(server, request, query)
    - handler(server, request, json)
    - handler(server, request, query, json)
  - define extractor types for query, json and maybe a trait like FromRequest,
    similar to what Actix does.  I don't think we need an explicit Path
    extractor because it's not part of the OpenAPI signature, and I don't think
    we need an explicit Data extractor because we'll always have the "server"
    and "request" parameters.
  - define implementations of our Handler trait for each of these signatures
    that does the expected thing
- Then go and implement a proper routing implementation.  (This probably depends
  on the extractor work, because a side effect of that will be creating a
  wrapper for handlers that abstracts over the specific handler function's type,
  and that's probably important to be able to put references into a routing
  table.)
- Consider: should the handler functions return a Serialize or a
  Stream of Serialize?  Otherwise, how will the OpenAPI tooling know what the
  return types are?
- Flesh out more endpoints and simulator
- Write out road map
  - regions, AZs, etc. need to be added per RFD 24

Longer term:

- Versioning (header? path? translators for older versions?)
- Pagination? (Opaque token?  What does Stripe do?)
