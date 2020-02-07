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

- Revisit the Error type
- Flesh out endpoints and simulator

Longer term:

- Versioning (header? path? translators for older versions?)
- Pagination? (Opaque token?  What does Stripe do?)
- It seems like there are cases where Actix spits out text error messages to the
  response (e.g., if you haven't configured app\_data() and try to use it).
  That seems really bad.  We should understand these and remove them.
- There are also cases where Actix reports a 400 and it's not clear why (e.g.,
  trying to use curl's "-d" flag when `POST`ing to a JSON endpoint).  I'm sure
  there's a good reason for this being a 400-level error, but how can we have
  visibility into these requests?
