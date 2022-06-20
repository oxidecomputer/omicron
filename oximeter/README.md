# Oximeter

The Oxide Rack metric collection system

## Overview

Oximeter is the system use to describe, generate, and store metrics and
telemetry data in the Oxide Rack. The overall functionality is provided by a set
of Rust crates:

- [`oximeter`](1): The core crate, for describing and generating metric data
  samples
- [`collector`](2): The `oximeter` binary program run by the control plane,
  which pulls metrics from other sources.
- [`producer`](3): A library that allows a program to produce its metric data for
  the control plane. This allows consumers to register with the control plane,
  and provide an HTTP endpoint from which the `oximeter` binary will pull data.
- [`db`](4): A library for interacting with the telemetry database,
  [ClickHouse](5)

In general, as a program or library wishing to _produce_ data, one would use the
`oximeter` crate to define the metrics, and the `producer` crate to communicate
it to the collection program.

## Defining metrics

The `oximeter` library crate provides two traits for describing data,
[`Target`](target) and [`Metric`](metric). A _target_ is something that we're
monitoring or collecting data about, such as an HTTP service or other program,
or a hardware component like a fan. A _metric_ describes a feature of the target
that we're measuring. Keeping with the above examples, a metric could be: the
number of 500-level responses the server generates, or the current speed of the
fan.

The `Target` and `Metric` features can be derived on a Rust struct. Those
structs define the schema for each. The fields of those structs can be one of
several supported types, such as `String`s, `i64`s, or `std::net::IpAddr`s. The
`Metric` struct must have one additional field, which is the _datum_, the actual
data value of the metric. There are many supported types for that too, such as
`f64`s or [`oximeter::Histogram`](hist)s.

Together, the `Target` and `Metric` define one _timeseries_. One produces
`Sample`s from the timeseries, and these samples are what are pulled by the
`oximeter` collector program.

## Generating samples

After defining a timeseries, applications will want to generate actual samples
from that. This is done by implementing the [`oximeter::Producer`](producer)
trait. The only method required here is [`produce()`](produce), which generates
a stream of [`oximeter::Sample`](sample)s. This is a single, timestamped
datapoint from a single timeseries. A `Producer` implementor can generate as
many samples as needed, from as many timeseries as needed.

To simplify the generation of data from many timeseries, consumers can use the
[`oximeter::ProducerRegistry`](registry). This is just a collection of types
that implement `Producer`; it has its own `collect()` method, which concatenates
the samples from each of its contained producers.

### The `ProducerRegistry`

Most applications will have more than one timeseries. All of these can be
aggregated in one place, the `ProducerRegistry`. That's a thread-safe, cloneable
type that keeps track of any number of objects that `impl Producer`.
Applications should create one `ProducerRegistry`, and share it as needed with
different parts of the code that generate samples. Any `Producer` implementation
can be registered with it, and when its own `ProducerRegistry::collect` method
is called, it'll collect all the samples from all its contained `Producer`s.

## Registering for collection

Once an application has defined its timeseries and produced samples, they need
to be communicated to the rest of the control plane. There are two aspects to
this:

- Registering as a producer of metric data with Nexus
- Providing an HTTP endpoint for the `oximeter` collection program to collect
  from

### Overview

The diagram below provides an overview of the different components at play.

```
            +-------------+
            | Application |
            +-------------+
                |  ^    |
                |  |    +-------------+
        --------+  +---------+        |
        |                    |        |
   Registration           Collect   Collect
        |                 request   response
        |                    |        |
        |                    |        |
        v                    |        v
    +-------+               +----------+
    | Nexus |-- Assign ---->| Oximeter |
    +-------+               +----------+
        |                        |
        |                        |
 Store registration           Process
  Select `oximeter`             and
        |                      Store
        |                        |
        |                        |
        v                        v
  +------------+          +------------+
  | CocroachDB |          | ClickHouse |
  +------------+          +------------+
```

The first step an application takes is to register itself as a producer with
Nexus. Nexus records that registration information in CockroachDB, selects an
`oximeter` collector instance, and provides it with that registration. The
registration information is encapsulated in the `ProducerEndpoint` type, which
tells `oximeter` how to reach the application. That is, the application is a
_server_ and `oximeter` is its _client_.

With that information in hand, `oximeter` will start making HTTP requests to the
application to collect its current `Sample`s. Any number of samples, from any
number of timeseries, may be returned in the collection response. (That will
likely change, to support pagination of some form.) `oximeter` will crunch the
data, derive a schema from it, and store it in the telemetry database,
ClickHouse.

### Nexus registration API

Registration with Nexus is done with the [`oximeter_producer::register`](register)
function. That accepts a [`ProducerEndpoint`](prod-end) and an address for
Nexus, and communicates the relevant details to Nexus.

### Creating a server

As mentioned above, the application generating metrics is a _server_ for the
`oximeter` collector. The application must have an HTTP endpoint that `oximeter`
can make requests to, which returns a list of `Samples`.

If the application does _not_ already run an HTTP server, the simplest thing is
to use [`oximeter_producer::Server`](server). This starts a Dropshot server for
you with the right endpoint. The server's
[`oximeter_producer::Server::registry`](srv-registry) method returns a reference
to its `ProducerRegistry`, to which you can add your `Producer` implementations.
These can be added at any point, even after the server starts running.

If the applicaiton already runs a Dropshot server, then it's easier to add a new
endpoint rather than spinning up a whole new server. There's no builtin way
(yet) to do this, since Dropshot servers are parametrized by their context type,
and must be defined inside a macro. The easiest thing to do at this point is to
create an endpoint that looks like the private function
[`oximeter_producer::collect_endpoint`](collect-end). That just calls the function
[`oximeter_producer::collect`](collect), which _is_ public. That takes a
`ProducerRegistry`, and just spits out all the samples from its producers.

## Example

There's a complete, self-contained example in
[`./oximeter/producer/examples/producer.rs`](example). That shows how to:

- Define a `Target` and `Metric` type
- Create a `Producer` implementation that generates samples from a timeseries
- Configure and start the batteries-included `oximeter_producer::Server`
- Add the `Producer` implementation to the server.
- Register with nexus.

[1]: ../target/doc/oximeter/index.html
[2]: ../target/doc/oximeter_collector/index.html
[3]: ../target/doc/oximeter_producer/index.html
[4]: ../target/doc/oximeter_db/index.html
[5]: https://clickhouse.com
[target]: ../target/doc/oximeter/traits/trait.Target.html
[metric]: ../target/doc/oximeter/traits/trait.Metric.html
[hist]: ../target/doc/oximeter/histogram/struct.Histogram.html
[producer]: ../target/doc/oximeter/traits/trait.Producer.html
[produce]: ../target/doc/oximeter/traits/trait.Producer.html#tymethod.produce
[sample]: ../target/doc/oximeter/types/struct.Sample.html
[registry]: ../target/doc/oximeter/types/struct.ProducerRegistry.html
[register]: ../target/doc/oximeter_producer/fn.register.html
[prod-end]: ../target/doc/omicron_common/api/internal/nexus/struct.ProducerEndpoint.html
[server]: ../target/doc/oximeter_producer/struct.Server.html
[srv-registry]: ../target/doc/oximeter_producer/struct.Server.html#method.registry
[collect-end]: ../target/doc/oximeter_producer/struct.collect_endpoint.html
[collect]: ../target/doc/oximeter_producer/fn.collect.html
[example]: oximeter/producer/examples/producer.rs
