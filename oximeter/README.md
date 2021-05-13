# `oximeter`

Toolkit for producing metric and telemetry data in an Oxide rack.

## Overview

`oximeter` is a Rust library for generating and collecting metrics. Client code
can define their own _targets_ (sources of data) and _metrics_ (a measured
feature of a target), as standard Rust structs. These structs define the schema
for a timeseries, including the field names/types/values and a type of
measurement. See the docs for [`oximeter`](./oximeter/src/lib.rs) for details.
