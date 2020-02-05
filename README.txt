This is an incredibly rough playground for building a prototype of the Oxide API.

There are a couple of different avenues to go down:

- API infrastructure: figure out how to structure the code to implement a bunch
  of resources with two possible backends
- server skeleton: set up an HTTP server with all the logging, tracing, error
  handling, etc. that we want
- openapi infrastructure: this is to some extent dependent on the API
  infrastructure, but we could for example put together an openapi.yaml file and
  generate some models or work on a system/tool for annotating models to
  generate such a file.

Currently, there's a tiny bit of the server skeleton in src/main.rs.

Next, I'm working on the API infrastructure since that's the next step in wiring
something up to the API and it's an area where I feel like we can make some
useful abstractions but need to work through it.  That's in "src/api.rs" and
supporting files.

Next steps:
- Document some design ideas (e.g., Server itself vs. API layer vs. Model layer
  vs.  Backend layer) along with code layout.
- Start implementing some API infrastructure
  - Revisit the Error type
  - See how this looks.  If it's good, consider building something to spit out
    an OpenAPI spec?

TODO:
- Versioning? (Header? Path?)
- Pagination? (Opaque token?  What does Stripe do?)
