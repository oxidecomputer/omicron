// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

//! Tools for generating and collecting metric data in the Oxide rack.
//!
//! Overview
//! --------
//!
//! `oximeter` is a crate for working with metric data. It includes tools for defining new metrics
//! in Rust code, exporting them to a collection server, storing them in a ClickHouse database, and
//! retrieving the data through queries. The system is designed to enable reporting of lots of
//! metric and telemetry information from the Oxide rack, and into Omicron, the Oxide control
//! plane.
//!
//! In general, a _metric_ is a quantity or value exposed by some software. For example, this might
//! be "requests per second" from a service, or the current temperature on a DIMM temperature
//! sensor. A _target_ is the thing being measured -- the service or the DIMM, in these cases. Both
//! targets and metrics may have key-value pairs associated with them, called _fields_.
//!
//! Defining timeseries schema
//! --------------------------
//!
//! Creating a timeseries starts by defining its schema. This includes the name
//! for the target and metric, as well as other metadata such as descriptions,
//! data types, and version numbers. Let's start by looking at an example:
//!
//! ```text
//! [target]
//! name = "foo"
//! description = "Statistics about the foo server"
//! authz_scope = "fleet"
//! versions = [
//!     { version = 1, fields = ["name", "id"],
//! ]
//!
//! [[metrics]]
//! name = "total_requests"
//! description = "The cumulative number of requests served"
//! datum_type = "cumulative_u64"
//! units = "count"
//! versions = [
//!     { added_in = 1, fields = ["route", "method", "response_code"],
//! ]
//!
//! [fields.name]
//! type = "string"
//! description = "The name of this server"
//!
//! [fields.id]
//! type = "uuid"
//! description = "Unique ID of this server"
//!
//! [fields.route]
//! type = "string"
//! description = "The route used in the HTTP request"
//!
//! [fields.method]
//! type = "string"
//! description = "The method used in the HTTP request"
//!
//! [fields.response_code]
//! type = u16
//! description = "Status code the server responded with"
//! ```
//!
//! In this case, our target is an HTTP server, which we identify with the
//! fields "name" and "id". Those fields are described in the `fields` TOML key,
//! and referred to by name in the target definition. A target also needs to
//! have a description and an _authorization scope_, which describes the
//! visibility of the timeseries and its data. See
//! [`crate::schema::AuthzScope`] for details.
//!
//! A target can have one or more metrics defined for it. As with the target, a
//! metric also has a name and description, and additionally a datum type and
//! units. It may also have fields, again referred to by name.
//!
//! This file should live in the `oximeter/schema` subdirectory, so that it can
//! be used to generate Rust code for producing data.
//!
//! Versions
//! --------
//!
//! Both targets and metrics have version numbers associated with them. For a
//! target, these numbers must be the numbers 1, 2, 3, ... (increasing, no
//! gaps). As the target or any of its metrics evolves, these numbers are
//! incremented, and the new fields for that version are specified.
//!
//! The fields on the metrics may be specified a bit more flexibly. The first
//! version they appear in must have the form: `{ added_in = X, fields = [ ...
//! ] }`. After, the versions may be omitted, meaning that the previous version
//! of the metric is unchanged; or the metric may be removed entirely with a
//! version like `{ removed_in = Y }`.
//!
//! In all cases, the TOML definition is checked for consistency. Any existing
//! metric versions must match up with a target version, and they must have
//! distinct field names (targets and metrics cannot share fields).
//!
//! Using these primitives, fields may be added, removed, or renamed, with one
//! caveat: a field may not **change type**.
//!
//! Generated code
//! --------------
//!
//! This TOML definition can be used in a number of ways, but the most relevant
//! is to actually produce data from the resulting timeseries. This can be done
//! with the `[use_timeseries]` proc-macro, like this:
//!
//! ```ignore
//! oximeter::use_timeseries!("http-server.toml");
//! ```
//!
//! The macro will first validate the timeseries definition, and then generate
//! Rust code like the following:
//!
//! ```rust
//! #[derive(oximeter::Target)]
//! struct HttpServer {
//!     name: String,
//!     id: uuid::Uuid,
//! }
//!
//! #[derive(oximeter::Metric)]
//! struct TotalRequests {
//!     route: String,
//!     method: String,
//!     response_code: u16,
//!     #[datum]
//!     total: oximeter::types::Cumulative<u64>,
//! }
//! ```
//!
//! This code can be used to create **samples** from this timeseries. The target
//! and metric structs can be filled in with the timeseries's _fields_, and the
//! _datum_ may be populated to generate new samples.
//!
//! Datum, measurement, and samples
//! -------------------------------
//!
//! In the above example, the field `total` in the `TotalRequests` struct is annotated with the
//! `#[datum]` helper. This identifies this field as the _datum_, or underlying quantity or value
//! that the metric captures. (Note that the field may also just be _named_ `datum`. These two
//! methods for identifying the field of interest are the same.) A datum may be:
//!
//! - a **gauge**, an instantaneous value at some point in time, or
//! - **cumulative**, an aggregated value over some defined time interval.
//!
//! The supported data types are identified by the [`traits::Datum`] trait.
//!
//! When the current value for a metric is to be retrieved, the [`Metric::measure`] method is
//! called. This generates a [`Measurement`], which contains the value of the datum, along with the
//! current timestamp.
//!
//! While the measurement captures the timestamped datum, it must be combined with information
//! about the target and metric to be meaningful. This is represented by a [`Sample`], which
//! includes the measurement and the collection of fields from both the target and metric.
//!
//! Producers
//! ---------
//!
//! How are samples generated? Consumers should define a type that implements the [`Producer`]
//! trait. This is a simple interface for generating an iterator over `Sample`s. A producer can
//! generate any number of samples, from any number of targets and metrics. For example, we might
//! have a producer that keeps track of all requests the above `HttpServer` receives, by storing
//! one instance of the `TotalRequests` for each unique combination of route, method, and response
//! code. This producer could then produce a collection of `Sample`s, one from each request counter
//! it tracks.
//!
//! Exporting data
//! --------------
//!
//! Data can be exported from a software component, and stored in the control plane's timeseries
//! database. The easiest way to export data is to use an
//! [`oximeter_producer::Server`](../producer/struct.Server). Once running, any `Producer` may be
//! registered with the server. The server will register itself for periodic data collection with
//! the rest of the Oxide control plane. When its configurable API endpoint is hit, it will call
//! `produce` method of all registered producers, and submit the resulting data for storage in the
//! telemetry database.
//!
//! Keep in mind that, although the data may be collected on one interval, `Producer`s may opt to
//! sample the data on any interval they choose. Consumers may wish to sample data every second,
//! but only submit that once per minute to the rest of the control plane. Similarly, different
//! `Producer`s may be registered with the same `ProducerServer`, each with potentially different
//! sampling intervals.

pub use oximeter_impl::*;
pub use oximeter_timeseries_macro::use_timeseries;

#[cfg(test)]
mod test {
    use oximeter_impl::schema::ir::load_schema;
    use oximeter_impl::schema::{FieldSource, SCHEMA_DIRECTORY};
    use oximeter_impl::TimeseriesSchema;
    use std::collections::BTreeMap;
    use std::fs;

    /// This test checks that changes to timeseries schema are all consistent.
    ///
    /// Timeseries schema are described in a TOML format that makes it relatively
    /// easy to add new versions of the timeseries. Those definitions are ingested
    /// at compile-time and checked for self-consistency, but it's still possible
    /// for two unrelated definitions to conflict. This test catches those.
    #[test]
    fn timeseries_schema_consistency() {
        let mut all_schema = BTreeMap::new();
        for entry in fs::read_dir(SCHEMA_DIRECTORY).unwrap() {
            let entry = entry.unwrap();
            println!(
                "examining timeseries schema from: '{}'",
                entry.path().canonicalize().unwrap().display()
            );
            let contents = fs::read_to_string(entry.path()).unwrap();
            let list = load_schema(&contents).unwrap_or_else(|_| {
                panic!(
                    "Expected a valid timeseries definition in {}",
                    entry.path().canonicalize().unwrap().display()
                )
            });
            println!("found {} schema", list.len());
            for schema in list.into_iter() {
                let key = (schema.timeseries_name.clone(), schema.version);
                if let Some(dup) = all_schema.insert(key, schema.clone()) {
                    panic!(
                        "Timeseries '{}' version {} is duplicated.\
                        \noriginal:\n{}\nduplicate:{}\n",
                        schema.timeseries_name,
                        schema.version,
                        pretty_print_schema(&schema),
                        pretty_print_schema(&dup),
                    );
                }
            }
        }
    }

    fn pretty_print_schema(schema: &TimeseriesSchema) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        writeln!(out, " name: {}", schema.timeseries_name).unwrap();
        writeln!(out, " version: {}", schema.version).unwrap();
        writeln!(out, " target").unwrap();
        writeln!(out, "   description: {}", schema.description.target).unwrap();
        writeln!(out, "   fields:").unwrap();
        for field in schema
            .field_schema
            .iter()
            .filter(|field| field.source == FieldSource::Target)
        {
            writeln!(
                out,
                "    {} ({}): {}",
                field.name, field.field_type, field.description
            )
            .unwrap();
        }
        writeln!(out, " metric").unwrap();
        writeln!(out, "   description: {}", schema.description.metric).unwrap();
        writeln!(out, "   fields:").unwrap();
        for field in schema
            .field_schema
            .iter()
            .filter(|field| field.source == FieldSource::Metric)
        {
            writeln!(
                out,
                "    {} ({}): {}",
                field.name, field.field_type, field.description
            )
            .unwrap();
        }
        writeln!(out, "   datum type: {}", schema.datum_type).unwrap();
        writeln!(out, "   units: {:?}", schema.units).unwrap();
        out
    }
}
