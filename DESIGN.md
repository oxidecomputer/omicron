# Oxide API Prototype Design Notes

XXX update me

**Status note:** This is all aspirational.  To see what actually exists at this
point, see the source.

For background, see [RFD 10 "API Prototype and Simulated
Implementation"](https://github.com/oxidecomputer/rfd/tree/master/rfd/0010).  In
summary, this program is intended to serve the Oxide API documented in [RFD
4](https://github.com/oxidecomputer/rfd/tree/master/rfd/0004) in two forms: (1)
using a simulated backend, for prototyping and validating; and (2) using a real
backend to control an Oxide rack.

## Layers

(Remember: all aspirational at this point and probably wrong.)

The implementation can be separated into several layers with separate concerns:

- The HTTP layer runs an HTTP server that receives requests, validates them,
  parses them into API Model objects and operations, and takes the results of
  those operations (which are other API Model objects) and serializes them back
  over HTTP (using facilities provided by the API Model layer)
- The API Model layer defines data types and operations agnostic to the HTTP
  transport.  The model is responsible for common facilities like pagination,
  limits, etc.  It also provides facilities for deserializing and serializing 
  objects to/from a representation suitable for HTTP (e.g., JSON).
- The API Backend layer ultimately implements the operations provided by the
  API.  Two backends are planned: one for a simulated rack and one for an actual
  rack.

To make this more concrete, here's what might happen when a request arrives to
list instances (VMs) for a project:

1. The server receives an HTTP `GET` request for
   `/projects/project123/instances`.  The HTTP layer routes this to a handler
   function `api_http_project_list_instances`.  This function authenticates the
   request, authorizes it, validates all parameters (e.g., `accept` header,
   that the project id is syntactically valid, etc.).
2. The HTTP layer handler function calls into the model layer function
   `api_model_project_list_instances`.  This function will produce a stream of
   `ApiModelProject` objects that can be serialized by the HTTP layer as an HTTP
   response.
3. The API layer handler function determines the appropriate backend to use
   (either by server configuration, request parameters, or some other TBD
   criteria) and invokes the appropriate backend implementation function.  This
   function may return either an error or a stream of `ApiModelProject` objects.
4. If an error is returned up to this point, an appropriate 400-level or
   500-level response is sent back.
5. Otherwise, an appropriate 200-level response is sent back.  As the backend
   emits `ApiModelProject` objects (via the stream it returned), these are
   serialized to the HTTP response body.

Visually:

    Incoming request over HTTP
        \   (e.g., invoke api_http_project_list_instances())
	 |
         v
      HTTP layer: parse, validate, dispatch to API Model layer
          \  (e.g., invoke api_model_validate_project(),
           | api_model_project_list_instances())
           v
        API Model layer: dispatch to appropriate backend
	    \  (e.g., invoke api_sim_project_list_instances())
             |
             v
          API Backend layer: handle request
          - may make calls to its own database or other services in the rack
          - will use calls into API Model layer to instantiate objects to be
    	    returned as part of this call
	     /
	    |
	    v
	API Model layer
	  /   (e.g., invoke api_serialize_object_for_stream())
	 |
	 v
      HTTP layer

